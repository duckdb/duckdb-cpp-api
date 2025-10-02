//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"
#include "duckdb/stable/logical_type.hpp"

namespace duckdb_stable {

class Vector {
public:
	Vector(duckdb_vector vec_p, bool owning = false) : vec(vec_p), owning(owning) {
	}
	~Vector() {
		if (vec && owning) {
			duckdb_destroy_vector(&vec);
		}
	}
	// disable copy constructors
	Vector(const Vector &other) = delete;
	Vector &operator=(const Vector &) = delete;
	//! enable move constructors
	Vector(Vector &&other) noexcept : vec(nullptr), owning(false) {
		std::swap(vec, other.vec);
		std::swap(owning, other.owning);
	}
	Vector &operator=(Vector &&other) noexcept {
		std::swap(vec, other.vec);
		std::swap(owning, other.owning);
		return *this;
	}

	Vector GetChild(idx_t index) {
		auto type = GetType();
		if (type.id() == DUCKDB_TYPE_STRUCT) {
			return Vector(duckdb_struct_vector_get_child(c_vec(), index));
		} else if (type.id() == DUCKDB_TYPE_LIST) {
			if (index != 0) {
				throw std::runtime_error("List only has a single child");
			}
			return Vector(duckdb_list_vector_get_child(c_vec()));
		} else {
			throw std::runtime_error("Not a nested type");
		}
	}

	LogicalType GetType() {
		return LogicalType(duckdb_vector_get_column_type(c_vec()));
	}

	duckdb_vector c_vec() {
		return vec;
	}

private:
	duckdb_vector vec;
	bool owning;
};

} // namespace duckdb_stable
