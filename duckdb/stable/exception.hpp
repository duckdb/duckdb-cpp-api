//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/format.hpp"

namespace duckdb_stable {


class Exception : public std::runtime_error {
public:
	Exception(const std::string &message) : std::runtime_error(message) {
	}
	template <typename... ARGS>
	static std::string ConstructMessage(const std::string &msg, ARGS... params) {
		const std::size_t num_args = sizeof...(ARGS);
		if (num_args == 0) {
			return msg;
		}
		std::vector<FormatValue> values;
		return ConstructMessageRecursive(msg, values, params...);
	}

	static std::string ConstructMessageRecursive(const std::string &msg, std::vector<FormatValue> &values) {
		return FormatUtil::Format(msg.c_str(), values);
	}

	template <class T, typename... ARGS>
	static std::string ConstructMessageRecursive(const std::string &msg, std::vector<FormatValue> &values, T param,
											ARGS... params) {
		values.push_back(FormatValue::CreateFormatValue<T>(param));
		return ConstructMessageRecursive(msg, values, params...);
	}
};

class OutOfRangeException : public Exception {
public:
	explicit OutOfRangeException(const std::string &msg) : Exception("Out of Range Error: " + msg) {}
	template <typename... ARGS>
	explicit OutOfRangeException(const std::string &msg, ARGS... params)
		: OutOfRangeException(ConstructMessage(msg, params...)) {
	}
};

}

