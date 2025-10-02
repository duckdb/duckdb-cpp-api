//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"
#include <algorithm>
#include <vector>
#include "duckdb_extension.h"

DUCKDB_EXTENSION_EXTERN

#define DUCKDB_EXTENSION_CPP_ENTRYPOINT(NAME)                                                                          \
	class NAME##Loader : public ExtensionLoader {                                                                      \
	public:                                                                                                            \
		NAME##Loader(duckdb_connection con, duckdb_extension_info info, struct duckdb_extension_access *access)        \
		    : ExtensionLoader(con, info, access) {                                                                     \
		}                                                                                                              \
                                                                                                                       \
	protected:                                                                                                         \
		void Load() override;                                                                                          \
	};                                                                                                                 \
	DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection con, duckdb_extension_info info,                                     \
	                            struct duckdb_extension_access *access) {                                              \
		NAME##Loader loader(con, info, access);                                                                        \
		return loader.LoadExtension();                                                                                 \
	}                                                                                                                  \
	void NAME##Loader::Load()
