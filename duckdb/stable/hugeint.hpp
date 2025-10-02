//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/hugeint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"
#include "duckdb/stable/exception.hpp"
#include <stdexcept>
#include <limits>

namespace duckdb_stable {

class hugeint_t {
public:
	hugeint_t() = default;
	hugeint_t(duckdb_hugeint value_p) : value(value_p) { // NOLINT: allow implicit conversion
	}
	hugeint_t(const hugeint_t &other) : value(other.value) {
	}
	hugeint_t(int64_t upper, uint64_t lower) {
		value.lower = lower;
		value.upper = upper;
	}
	hugeint_t(int64_t input) { // NOLINT: allow implicit conversion from smaller integers
		value.lower = (uint64_t)input;
		value.upper = (input < 0) * -1;
	}

	int64_t upper() const {
		return value.upper;
	}
	uint64_t lower() const {
		return value.lower;
	}
	duckdb_hugeint c_val() const {
		return value;
	}

	bool try_negate(hugeint_t &result) const {
		if (value.upper == std::numeric_limits<int64_t>::min() && value.lower == 0) {
			return false;
		}
		result.value.lower = UINT64_MAX - value.lower + 1ull;
		result.value.upper = -1 - value.upper + (value.lower == 0);
		return true;
	}

	hugeint_t negate() const {
		hugeint_t result;
		if (!try_negate(result)) {
			throw Exception("Failed to negate hugeint: out of range");
		}
		return result;
	}
	static bool try_add_in_place(hugeint_t &lhs, hugeint_t rhs) {
		int overflow = lhs.value.lower + rhs.value.lower < lhs.value.lower;
		if (rhs.value.upper >= 0) {
			// RHS is positive: check for overflow
			if (lhs.value.upper > (std::numeric_limits<int64_t>::max() - rhs.value.upper - overflow)) {
				return false;
			}
			lhs.value.upper = lhs.value.upper + overflow + rhs.value.upper;
		} else {
			// RHS is negative: check for underflow
			if (lhs.value.upper < std::numeric_limits<int64_t>::min() - rhs.value.upper - overflow) {
				return false;
			}
			lhs.value.upper = lhs.value.upper + (overflow + rhs.value.upper);
		}
		lhs.value.lower += rhs.value.lower;
		return true;
	}
	hugeint_t add(hugeint_t rhs) const {
		hugeint_t result = *this;
		if (!try_add_in_place(result, rhs)) {
			throw Exception("Failed to add hugeint: out of range");
		}
		return result;
	}
	static bool try_subtract_in_place(hugeint_t &lhs, hugeint_t rhs) {
		// underflow
		int underflow = lhs.value.lower - rhs.value.lower > lhs.value.lower;
		if (rhs.value.upper >= 0) {
			// RHS is positive: check for underflow
			if (lhs.value.upper < (std::numeric_limits<int64_t>::min() + rhs.value.upper + underflow)) {
				return false;
			}
			lhs.value.upper = (lhs.value.upper - rhs.value.upper) - underflow;
		} else {
			// RHS is negative: check for overflow
			if (lhs.value.upper > std::numeric_limits<int64_t>::min() &&
			    lhs.value.upper - 1 >= (std::numeric_limits<int64_t>::max() + rhs.value.upper + underflow)) {
				return false;
			}
			lhs.value.upper = lhs.value.upper - (rhs.value.upper + underflow);
		}
		lhs.value.lower -= rhs.value.lower;
		return true;
	}
	hugeint_t subtract(hugeint_t rhs) const {
		hugeint_t result = *this;
		if (!try_subtract_in_place(result, rhs)) {
			throw Exception("Failed to subtract hugeint: out of range");
		}
		return result;
	}

	bool operator==(const hugeint_t &rhs) const {
		return value.lower == rhs.value.lower && value.upper == rhs.value.upper;
	}
	bool operator!=(const hugeint_t &rhs) const {
		return !(*this == rhs);
	}

	bool operator>(const hugeint_t &rhs) const {
		bool upper_bigger = value.upper > rhs.value.upper;
		bool upper_equal = value.upper == rhs.value.upper;
		bool lower_bigger = value.lower > rhs.value.lower;
		return upper_bigger || (upper_equal && lower_bigger);
	}
	bool operator>=(const hugeint_t &rhs) const {
		bool upper_bigger = value.upper > rhs.value.upper;
		bool upper_equal = value.upper == rhs.value.upper;
		bool lower_bigger_equal = value.lower >= rhs.value.lower;
		return upper_bigger || (upper_equal && lower_bigger_equal);
	}
	bool operator<(const hugeint_t &rhs) const {
		return !(rhs >= *this);
	}
	bool operator<=(const hugeint_t &rhs) const {
		return !(rhs > *this);
	}
	hugeint_t operator+(const hugeint_t &rhs) const {
		return hugeint_t(value.upper + rhs.value.upper + ((value.lower + rhs.value.lower) < value.lower),
		                 value.lower + rhs.value.lower);
	}
	hugeint_t operator-(const hugeint_t &rhs) const {
		return hugeint_t(value.upper - rhs.value.upper - ((value.lower - rhs.value.lower) > value.lower),
		                 value.lower - rhs.value.lower);
	}

private:
	duckdb_hugeint value;
};

template<>
inline FormatValue FormatValue::CreateFormatValue(hugeint_t val) {
	if (val.upper() == 0) {
		return FormatValue(val.lower());
	}
	// FIXME: format big numbers
	return FormatValue("HUGEINT");
}

} // namespace duckdb_stable
