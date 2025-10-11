//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"

#include <stdexcept>
#include <string>
#include <vector>

namespace duckdb_stable {

struct FormatValue {
	FormatValue(const double dbl_val) : str_val(std::to_string(dbl_val)) {} // NOLINT: allow implicit conversion.
	FormatValue(const int64_t int_val) : str_val(std::to_string(int_val)) {} // NOLINT: allow implicit conversion.
	FormatValue(const uint64_t uint_val) : str_val(std::to_string(uint_val)) {} // NOLINT: allow implicit conversion.
	FormatValue(const std::string str_val_p): str_val(std::move(str_val_p)) {} // NOLINT: allow implicit conversion.

	template<class T>
	static FormatValue CreateFormatValue(const T val) {
		return FormatValue(val);
	}

	std::string str_val;
};

class FormatUtil {
public:
	static std::string Format(const char *format, const std::vector<FormatValue> &format_values) {
		if (format_values.empty()) {
			return format;
		}

		idx_t format_idx = 0;
		std::string result;
		for (idx_t i = 0; format[i]; i++) {
			if (format[i] == '{' && format[i + 1] == '}') {
				if (format_idx >= format_values.size()) {
					throw std::runtime_error(std::string("FormatUtil::Format out of range while formatting string ") + format);
				}
				result += format_values[format_idx].str_val;
				i++;
				format_idx++;
				continue;
			}
			result += format[i];
		}
		return result;
	}
};

} // namespace duckdb_stable

