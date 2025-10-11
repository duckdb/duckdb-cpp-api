//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"
#include "duckdb/stable/exception.hpp"
#include "duckdb/stable/logical_type.hpp"

namespace duckdb_stable {

class Vector {
public:
	Vector(duckdb_vector vec_p, const bool owning_p = false) : vec(vec_p), owning(owning_p) {
	}
	~Vector() {
		if (vec && owning) {
			duckdb_destroy_vector(&vec);
		}
	}

	//! Disable copy constructors.
	Vector(const Vector &other) = delete;
	Vector &operator=(const Vector &) = delete;

	//! Enable move constructors.
	Vector(Vector &&other) noexcept : vec(nullptr), owning(false) {
		std::swap(vec, other.vec);
		std::swap(owning, other.owning);
	}
	Vector &operator=(Vector &&other) noexcept {
		std::swap(vec, other.vec);
		std::swap(owning, other.owning);
		return *this;
	}

public:
	Vector GetChild(const idx_t index) {
		auto logical_type = GetLogicalType();
		if (logical_type.c_type() == DUCKDB_TYPE_STRUCT) {
			return Vector(duckdb_struct_vector_get_child(c_vector(), index));
		}
        if (logical_type.c_type() == DUCKDB_TYPE_LIST) {
			if (index != 0) {
				throw Exception("LIST has one child at index 0");
			}
			return Vector(duckdb_list_vector_get_child(c_vector()));
		}
        throw Exception("not a nested type");
	}

	LogicalType GetLogicalType() {
		return LogicalType(duckdb_vector_get_column_type(c_vector()));
	}

public:
	duckdb_vector c_vector() {
		return vec;
	}

private:
	duckdb_vector vec;
	bool owning;
};

} // namespace duckdb_stable
