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
	LogicalType(duckdb_logical_type logical_type_p) : logical_type(logical_type_p) {
	}
	LogicalType(duckdb_type type_p) {
        logical_type = duckdb_create_logical_type(type_p);
	}
	~LogicalType() {
		if (logical_type) {
			duckdb_destroy_logical_type(&logical_type);
		}
	}

	//! Disable copy constructors.
	LogicalType(const LogicalType &other) = delete;
	LogicalType &operator=(const LogicalType &) = delete;

	//! Enable move constructors.
	LogicalType(LogicalType &&other) noexcept : type(nullptr) {
		std::swap(logical_type, other.logical_type);
	}
	LogicalType &operator=(LogicalType &&other) noexcept {
		std::swap(logical_type, other.logical_type);
		return *this;
	}

public:
	void SetAlias(const char *name) {
		duckdb_logical_type_set_alias(logical_type, name);
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
        auto c_child_types = reinterpret_cast<duckdb_logical_type *>(child_types);
		return LogicalType(duckdb_create_struct_type(c_child_types, child_names, n));
	}

public:
    duckdb_logical_type c_logical_type() {
        return logical_type;
    }

    duckdb_type c_type() {
        return duckdb_get_type_id(logical_type);
    }

private:
	duckdb_logical_type logical_type;
};

} // namespace duckdb_stable
