
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
	DataChunk(duckdb_data_chunk chunk_p, bool owning = false) : chunk(chunk_p), owning(owning) {
	}
	~DataChunk() {
		if (chunk && owning) {
			duckdb_destroy_data_chunk(&chunk);
		}
	}
	// disable copy constructors
	DataChunk(const DataChunk &other) = delete;
	DataChunk &operator=(const DataChunk &) = delete;
	//! enable move constructors
	DataChunk(DataChunk &&other) noexcept : chunk(nullptr), owning(false) {
		std::swap(chunk, other.chunk);
		std::swap(owning, other.owning);
	}
	DataChunk &operator=(DataChunk &&other) noexcept {
		std::swap(chunk, other.chunk);
		std::swap(owning, other.owning);
		return *this;
	}

	Vector GetVector(idx_t index) {
		return Vector(duckdb_data_chunk_get_vector(chunk, index));
	}

	idx_t Size() {
		return duckdb_data_chunk_get_size(chunk);
	}

	idx_t ColumnCount() {
		return duckdb_data_chunk_get_column_count(chunk);
	}

	duckdb_data_chunk c_chunk() {
		return chunk;
	}

private:
	duckdb_data_chunk chunk;
	bool owning;
};

} // namespace duckdb_stable
