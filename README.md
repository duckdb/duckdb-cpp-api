> Work In Progress

C++ API for making DuckDB extensions. The API is based on the stable C API instead of linking to the internal DuckDB APIs. This has several important advantages:

* Extensions developed with this API are not deeply linked to a specific DuckDB version. These extensions don't need to be re-compiled for every single DuckDB release, including patch releases.
* When using a stable version of the API, the extension only needs to be built once, after which it can be loaded by any future versions of DuckDB that support that version of the stable API.
* Extensions developed with this API are significantly smaller as they don't pull in parts of DuckDB.
* Extensions developed with this API compile significantly faster, as we don't need to re-compile DuckDB as part of development. This allows for faster development and iteration.


