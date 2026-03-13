#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import threading
import http.client
import urllib
import decimal
import csv
from datetime import datetime
import collections
from pathlib import Path

import pkg_resources
from jsonschema.validators import Draft4Validator
from jsonschema.exceptions import ValidationError
import singer

from azure.storage.blob import BlockBlobService, AppendBlobService, ContentSettings

logger = singer.get_logger()
USER_HOME = os.path.expanduser('~')


def _sanitize_value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    if isinstance(value, decimal.Decimal):
        return str(value)
    return value


def _normalize_output_format(raw_value):
    output_format = (raw_value or 'csv').lower()
    if output_format not in ('csv', 'parquet'):
        raise ValueError('Invalid output_format "{}". Expected one of: csv, parquet'.format(output_format))
    return output_format


def _content_type_for_format(output_format):
    return 'application/parquet' if output_format == 'parquet' else 'text/csv'


def _flush_parquet_stream(stream_state):
    if not stream_state['buffer']:
        return

    rows = stream_state['buffer']
    columns = stream_state['columns']
    if columns is None:
        columns = list(rows[0].keys())
        stream_state['columns'] = columns

    normalized_rows = []
    for row in rows:
        normalized_rows.append({
            column: _sanitize_value(row.get(column))
            for column in columns
        })

    table = stream_state['pa'].Table.from_pylist(normalized_rows)
    if stream_state['writer'] is None:
        stream_state['writer'] = stream_state['pq'].ParquetWriter(
            stream_state['file_path'],
            table.schema,
            compression=stream_state['compression']
        )
    stream_state['writer'].write_table(table)
    stream_state['buffer'] = []


def _flush_all_parquet_streams(parquet_streams):
    for stream_state in parquet_streams.values():
        _flush_parquet_stream(stream_state)


def _close_all_parquet_writers(parquet_streams):
    for stream_state in parquet_streams.values():
        if stream_state['writer'] is not None:
            stream_state['writer'].close()
            stream_state['writer'] = None


def _upload_local_files(block_blob_service, blob_container_name, parent_dir, content_type):
    if not os.path.exists(parent_dir):
        return

    for file_name in os.listdir(parent_dir):
        file_path = os.path.join(parent_dir, file_name)
        if not os.path.isfile(file_path):
            continue

        block_blob_service.create_blob_from_path(
            blob_container_name,
            file_name,
            file_path,
            content_settings=ContentSettings(content_type=content_type)
        )
        os.remove(file_path)


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)


def persist_lines(block_blob_service, append_blob_service, blob_container_name, lines, config):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    output_format = _normalize_output_format(config.get('output_format'))
    parquet_batch_size = int(config.get('parquet_batch_size', 10000))
    parquet_compression = config.get('parquet_compression', 'snappy')

    if output_format == 'parquet':
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as err:
            raise ImportError('Parquet output requires pyarrow. Install pyarrow and retry.') from err
    else:
        pa = None
        pq = None

    csv_files = {}
    csv_headers = {}
    parquet_streams = {}

    now = datetime.now().strftime('%Y%m%dT%H%M%S')
    parent_dir = os.path.join(USER_HOME, blob_container_name)
    Path(parent_dir).mkdir(parents=True, exist_ok=True)
    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception(
                "Line is missing required key 'type': {}".format(line))
        t = o['type']

        logger.info("Type {} in message {}"
                    .format(o['type'], o))
        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            # Get schema for this record's stream
            schema = schemas[o['stream']]

            # Validate record
            try:
                validators[o['stream']].validate(o['record'])
            except ValidationError as e:
                if "is not a multiple of 0.01" in str(e):
                    logger.error(f'Validation error:{e}')
            except Exception as e:
                raise e

            # If the record needs to be flattened, uncomment this line
            stream = o['stream']
            record = {key: _sanitize_value(value) for key, value in o['record'].items()}

            if output_format == 'csv':
                filename = stream + '.csv'
                stream_path = csv_files.get(stream)
                if stream_path is None:
                    stream_path = os.path.join(parent_dir, filename)
                    csv_files[stream] = stream_path

                if stream not in csv_headers:
                    csv_headers[stream] = list(record.keys())
                    with open(stream_path, 'w', newline='', encoding='utf-8') as file_obj:
                        writer = csv.writer(file_obj)
                        writer.writerow(csv_headers[stream])

                with open(stream_path, 'a', newline='', encoding='utf-8') as file_obj:
                    writer = csv.writer(file_obj)
                    writer.writerow([record.get(key) for key in csv_headers[stream]])
            else:
                stream_state = parquet_streams.get(stream)
                if stream_state is None:
                    filename = stream + '.parquet'
                    stream_state = {
                        'file_path': os.path.join(parent_dir, filename),
                        'buffer': [],
                        'writer': None,
                        'columns': None,
                        'pa': pa,
                        'pq': pq,
                        'compression': parquet_compression
                    }
                    parquet_streams[stream] = stream_state

                stream_state['buffer'].append(record)
                if len(stream_state['buffer']) >= parquet_batch_size:
                    _flush_parquet_stream(stream_state)

            # if not o['stream'] + '.json' in blob_names:
            #     append_blob_service.create_blob(blob_container_name, filename)

            # append_blob_service.append_blob_from_text(blob_container_name, filename, json.dumps(o['record']) + ',')

            state = None
        elif t == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']

            # if currently_syncing == NONE upload file
            if not state.get('currently_syncing') and os.path.exists(parent_dir):
                if output_format == 'parquet':
                    _flush_all_parquet_streams(parquet_streams)
                    _close_all_parquet_writers(parquet_streams)
                _upload_local_files(
                    block_blob_service,
                    blob_container_name,
                    parent_dir,
                    _content_type_for_format(output_format)
                )

        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o['key_properties']
        elif t == 'ACTIVATE_VERSION':
            logger.debug("Type {} in message {}"
                         .format(o['type'], o))
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

    if output_format == 'parquet':
        _flush_all_parquet_streams(parquet_streams)
        _close_all_parquet_writers(parquet_streams)

    return state


def send_usage_stats():
    try:
        version = pkg_resources.get_distribution(
            'target-azureblobstorage').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-azureblobstorage',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    if not config.get('disable_collection', False):
        logger.info('Sending version information to singer.io. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=send_usage_stats).start()

    block_blob_service = BlockBlobService(config.get(
        'account_name', None), config.get('account_key', None))

    append_blob_service = AppendBlobService(config.get(
        'account_name', None), config.get('account_key', None))

    # TODO: Create container/ prefix if missing
    blob_container_name = config.get('container_name', None)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(block_blob_service,
                          append_blob_service, blob_container_name, input, config)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
