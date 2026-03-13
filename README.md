# target-azureblobstorage

This is a [Singer](https://singer.io) target that reads JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

Install the package:

```bash
pip install -e .
```

See the [Running a Singer Tap with a Singer Target](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target) section of the singer docs for instructions on running a Tap with your Target

## 2ICT Fork

This fork patches two critical issues with the original `target-azureblobstorage` loader to ensure compatibility with modern Python environments and full table syncs:

1. **pkg_resources Import Error**: The original plugin imports `pkg_resources` from the `setuptools` package, which was deprecated and removed in `setuptools` 82.0.0 (released Feb 2026). This causes a `ModuleNotFoundError` during plugin execution. This fork adds `"setuptools<82"` to the `install_requires` list in `setup.py`, pinning the dependency to a compatible version and preventing the error.

2. **KeyError on 'currently_syncing'**: During full table syncs, the Singer state dictionary may not include the `'currently_syncing'` key (as it's primarily used for incremental syncs). The original code uses `state['currently_syncing']`, which throws a `KeyError` if the key is missing. This fork updates line 126 in `target_azureblobstorage.py` from `state['currently_syncing']` to `state.get('currently_syncing')`, safely returning `None` if the key is absent, allowing full table syncs to proceed without crashing.

These patches ensure the plugin works reliably with Meltano pipelines, including full data imports to Azure Blob Storage. If you encounter issues, ensure your environment uses the pinned `setuptools` version.

## Output Format

The target now supports writing local files as CSV (default) or Parquet before upload to Azure Blob Storage.

### Config options

- `output_format`: `csv` (default) or `parquet`
- `parquet_batch_size`: Number of records buffered before flushing a Parquet row group (default: `10000`)
- `parquet_compression`: Parquet compression codec (default: `snappy`)

### Notes for large MSSQL tables (100GB+)

- Use `output_format: parquet` to reduce upload size and improve downstream read performance.
- Keep `parquet_batch_size` moderate (for example `5000` to `20000`) to bound memory usage.
- The implementation flushes buffered batches incrementally, so it does not need to keep full tables in memory.
