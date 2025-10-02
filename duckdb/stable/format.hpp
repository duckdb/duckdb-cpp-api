//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"
#include "duckdb/stable/hugeint.hpp"
#include "duckdb/stable/uhugeint.hpp"
#include <string>
#include <vector>

namespace duckdb_stable {

struct FormatValue {
	FormatValue(double dbl_val) : str_val(std::to_string(dbl_val)) {}
	FormatValue(int64_t int_val) : str_val(std::to_string(int_val)) {}
	FormatValue(idx_t uint_val) : str_val(std::to_string(uint_val)) {}
	FormatValue(std::string str_val_p): str_val(std::move(str_val_p)) {}

	template<class T>
	static FormatValue CreateFormatValue(T val) {
		return FormatValue(val);
	}

	template<>
	FormatValue CreateFormatValue(hugeint_t val) {
		if (val.upper() == 0) {
			return FormatValue(val.lower());
		}
		// FIXME: format big numbers
		return FormatValue("HUGEINT");
	}

	template<>
	FormatValue CreateFormatValue(uhugeint_t val) {
		if (val.upper() == 0) {
			return FormatValue(val.lower());
		}
		// FIXME: format big numbers
		return FormatValue("UHUGEINT");
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
		for(idx_t i = 0; format[i]; i++) {
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

}

