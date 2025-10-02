//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/string_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"

namespace duckdb_stable {

class string_t {
public:
	static constexpr idx_t PREFIX_LENGTH = 4;
	static constexpr idx_t INLINE_LENGTH = 12;

public:
	string_t() = default;
	string_t(const char *data, uint32_t len) {
		string.value.inlined.length = len;
		if (IsInlined()) {
			// zero initialize the prefix first
			// this makes sure that strings with length smaller than 4 still have an equal prefix
			memset(string.value.inlined.inlined, 0, INLINE_LENGTH);
			if (GetSize() == 0) {
				return;
			}
			// small string: inlined
			memcpy(string.value.inlined.inlined, data, GetSize());
		} else {
			// large string: store pointer
			memcpy(string.value.pointer.prefix, data, PREFIX_LENGTH);
			string.value.pointer.ptr = (char *)data; // NOLINT
		}
	}
	string_t(const char *str) : string_t(str, strlen(str)) {
	}

	const char *GetData() const {
		return IsInlined() ? string.value.inlined.inlined : string.value.pointer.ptr;
	}
	uint32_t GetSize() const {
		return string.value.inlined.length;
	}
	bool IsInlined() const {
		return GetSize() <= INLINE_LENGTH;
	}

private:
	duckdb_string_t string;
};
} // namespace duckdb_stable
