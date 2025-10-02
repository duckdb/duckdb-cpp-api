//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/uhugeint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"

namespace duckdb_stable {

class uhugeint_t {
public:
	uhugeint_t() = default;
	uhugeint_t(duckdb_uhugeint value_p) : value(value_p) {
	}
	uhugeint_t(const uhugeint_t &other) : value(other.value) {
	}
	uhugeint_t(uint64_t upper, uint64_t lower) {
		value.lower = lower;
		value.upper = upper;
	}
	uhugeint_t(uint64_t input) { // NOLINT: allow implicit conversion from smaller unsigned integers
		value.lower = (uint64_t)input;
		value.upper = 0;
	}

	uint64_t upper() const {
		return value.upper;
	}
	uint64_t lower() const {
		return value.lower;
	}
	duckdb_uhugeint c_val() const {
		return value;
	}

	static bool try_add_in_place(uhugeint_t &lhs_v, uhugeint_t rhs_v) {
		auto &lhs = lhs_v.value;
		auto &rhs = rhs_v.value;

		uint64_t new_upper = lhs.upper + rhs.upper;
		bool no_overflow = !(new_upper < lhs.upper || new_upper < rhs.upper);
		new_upper += (lhs.lower + rhs.lower) < lhs.lower;
		if (new_upper < lhs.upper || new_upper < rhs.upper) {
			no_overflow = false;
		}
		lhs.upper = new_upper;
		lhs.lower += rhs.lower;
		return no_overflow;
	}
	uhugeint_t add(uhugeint_t rhs) const {
		uhugeint_t result = *this;
		if (!try_add_in_place(result, rhs)) {
			throw std::runtime_error("Out of Range Error: Overflow in addition");
		}
		return result;
	}
	static bool try_subtract_in_place(uhugeint_t &lhs_v, uhugeint_t rhs_v) {
		auto &lhs = lhs_v.value;
		auto &rhs = rhs_v.value;
		uint64_t new_upper = lhs.upper - rhs.upper - ((lhs.lower - rhs.lower) > lhs.lower);
		bool no_overflow = !(new_upper > lhs.upper);
		lhs.lower -= rhs.lower;
		lhs.upper = new_upper;
		return no_overflow;
	}
	uhugeint_t subtract(uhugeint_t rhs) const {
		uhugeint_t result = *this;
		if (!try_subtract_in_place(result, rhs)) {
			throw std::runtime_error("Out of Range Error: Overflow in subtraction");
		}
		return result;
	}
	static bool try_from_hugeint(duckdb_hugeint val, uhugeint_t &result) {
		if (val.upper < 0) {
			return false;
		}
		result.value.lower = val.lower;
		result.value.upper = static_cast<uint64_t>(val.upper);
		return true;
	}
	static uhugeint_t from_hugeint(duckdb_hugeint val) {
		uhugeint_t result;
		if (!try_from_hugeint(val, result)) {
			throw std::runtime_error("Failed to convert hugeint to uhugeint: out of range");
		}
		return result;
	}

	bool operator==(const uhugeint_t &rhs) const {
		return value.lower == rhs.value.lower && value.upper == rhs.value.upper;
	}
	bool operator!=(const uhugeint_t &rhs) const {
		return !(*this == rhs);
	}

	bool operator>(const uhugeint_t &rhs) const {
		bool upper_bigger = value.upper > rhs.value.upper;
		bool upper_equal = value.upper == rhs.value.upper;
		bool lower_bigger = value.lower > rhs.value.lower;
		return upper_bigger || (upper_equal && lower_bigger);
	}
	bool operator>=(const uhugeint_t &rhs) const {
		bool upper_bigger = value.upper > rhs.value.upper;
		bool upper_equal = value.upper == rhs.value.upper;
		bool lower_bigger_equal = value.lower >= rhs.value.lower;
		return upper_bigger || (upper_equal && lower_bigger_equal);
	}
	bool operator<(const uhugeint_t &rhs) const {
		return !(rhs >= *this);
	}
	bool operator<=(const uhugeint_t &rhs) const {
		return !(rhs > *this);
	}
	uhugeint_t operator+(const uhugeint_t &rhs) const {
		return uhugeint_t(value.upper + rhs.value.upper + ((value.lower + rhs.value.lower) < value.lower),
		                  value.lower + rhs.value.lower);
	}
	uhugeint_t operator-(const uhugeint_t &rhs) const {
		return uhugeint_t(value.upper - rhs.value.upper - ((value.lower - rhs.value.lower) > value.lower),
		                  value.lower - rhs.value.lower);
	}

private:
	duckdb_uhugeint value;
};

} // namespace duckdb_stable
