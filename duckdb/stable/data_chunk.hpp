//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/data_chunk.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/vector.hpp"

namespace duckdb_stable {

class DataChunk {
public:
	DataChunk(duckdb_data_chunk chunk_p, const bool owning_p = false) : chunk(chunk_p), owning(owning_p) {
	}
	~DataChunk() {
		if (chunk && owning) {
			duckdb_destroy_data_chunk(&chunk);
		}
	}

	//! Disable copy constructors.
	DataChunk(const DataChunk &other) = delete;
	DataChunk &operator=(const DataChunk &) = delete;

	//! Enable move constructors.
	DataChunk(DataChunk &&other) noexcept : chunk(nullptr), owning(false) {
		std::swap(chunk, other.chunk);
		std::swap(owning, other.owning);
	}
	DataChunk &operator=(DataChunk &&other) noexcept {
		std::swap(chunk, other.chunk);
		std::swap(owning, other.owning);
		return *this;
	}

public:
	Vector GetVector(const idx_t index) {
		return Vector(duckdb_data_chunk_get_vector(chunk, index));
	}

	idx_t Size() const {
		return duckdb_data_chunk_get_size(chunk);
	}

	idx_t ColumnCount() const {
		return duckdb_data_chunk_get_column_count(chunk);
	}

public:
	duckdb_data_chunk c_data_chunk() {
		return chunk;
	}

private:
	duckdb_data_chunk chunk;
	bool owning;
};

} // namespace duckdb_stable
