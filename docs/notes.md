# notes

- when creating the project's virtual environments, set up two separate ones - `3.14t` (free-threaded) and `3.14` (regular). the reason is that `pysradb` depends on `lxml`, which does not yet support the free-threaded python build, so scripts using pysradb must run under the GIL-enabled environment. for creating venv for `pysradb`, use the following commands:
    ```
    uv venv .venv-pysradb --python 3.14
    uv pip install --python .venv-pysradb/bin/python pysradb
    uv run --python .venv-pysradb/bin/python <pysradb-related-script>
    ```
