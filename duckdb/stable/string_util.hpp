//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/string_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"
#include <limits>
#include <string>
#include <vector>

namespace duckdb_stable {

class StringUtil {
public:
	static uint64_t ToUnsigned(const char *str, idx_t len, idx_t &pos) {
		uint64_t result = 0;
		while (pos < len && isdigit(str[pos])) {
			result = result * 10 + str[pos] - '0';
			pos++;
		}
		return result;
	}
	static int64_t ToSigned(const char *str, idx_t len, idx_t &pos) {
		if (len == 0) {
			return 0;
		}
		bool negative = false;
		if (*str == '-') {
			negative = true;
			pos++;
		}
		uint64_t result = ToUnsigned(str, len, pos);
		if (result > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
			return 0;
		}
		return negative ? static_cast<int64_t>(-result) : static_cast<int64_t>(result);
	}
	static uint64_t FromHex(const char *str, idx_t len, idx_t &pos) {
		if (len == 0) {
			return 0;
		}
		uint64_t result = 0;
		for (; pos < len; pos++) {
			auto c = str[pos];
			uint64_t digit;
			if (c >= '0' && c <= '9') {
				digit = c - '0';
			} else if (c >= 'a' && c <= 'f') {
				digit = 10 + (c - 'a');
			} else if (c >= 'A' && c <= 'F') {
				digit = 10 + (c - 'A');
			} else {
				break;
			}
			result = result * 16 + digit;
		}
		return result;
	}
};

} // namespace duckdb_stable
