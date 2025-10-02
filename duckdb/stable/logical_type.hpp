//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/logical_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"

namespace duckdb_stable {

class LogicalType {
public:
	LogicalType(duckdb_logical_type type_p) : type(type_p) {
	}
	LogicalType(duckdb_type ctype) {
		type = duckdb_create_logical_type(ctype);
	}
	~LogicalType() {
		if (type) {
			duckdb_destroy_logical_type(&type);
			type = nullptr;
		}
	}
	// disable copy constructors
	LogicalType(const LogicalType &other) = delete;
	LogicalType &operator=(const LogicalType &) = delete;
	//! enable move constructors
	LogicalType(LogicalType &&other) noexcept : type(nullptr) {
		std::swap(type, other.type);
	}
	LogicalType &operator=(LogicalType &&other) noexcept {
		std::swap(type, other.type);
		return *this;
	}

public:
	duckdb_logical_type c_type() {
		return type;
	}

	duckdb_type id() {
		return duckdb_get_type_id(type);
	}

	void SetAlias(const char *name) {
		duckdb_logical_type_set_alias(type, name);
	}

public:
	static LogicalType BOOLEAN() {
		return LogicalType(DUCKDB_TYPE_BOOLEAN);
	}
	static LogicalType VARCHAR() {
		return LogicalType(DUCKDB_TYPE_VARCHAR);
	}
	static LogicalType UTINYINT() {
		return LogicalType(DUCKDB_TYPE_UTINYINT);
	}
	static LogicalType USMALLINT() {
		return LogicalType(DUCKDB_TYPE_USMALLINT);
	}
	static LogicalType HUGEINT() {
		return LogicalType(DUCKDB_TYPE_HUGEINT);
	}
	static LogicalType STRUCT(LogicalType *child_types, const char **child_names, idx_t n) {
		return LogicalType(
		    duckdb_create_struct_type(reinterpret_cast<duckdb_logical_type *>(child_types), child_names, n));
	}

private:
	duckdb_logical_type type;
};

} // namespace duckdb_stable
