#pragma once



#include "duckdb.h"
#include <algorithm>
#include <vector>
#include "duckdb_extension.h"

DUCKDB_EXTENSION_EXTERN

#define DUCKDB_EXTENSION_CPP_ENTRYPOINT(NAME)                                                                          \
	class NAME##Loader : public ExtensionLoader {                                                                      \
	public:                                                                                                            \
		NAME##Loader(duckdb_connection con, duckdb_extension_info info, struct duckdb_extension_access *access)        \
		    : ExtensionLoader(con, info, access) {                                                                     \
		}                                                                                                              \
                                                                                                                       \
	protected:                                                                                                         \
		void Load() override;                                                                                          \
	};                                                                                                                 \
	DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection con, duckdb_extension_info info,                                     \
	                            struct duckdb_extension_access *access) {                                              \
		NAME##Loader loader(con, info, access);                                                                        \
		return loader.LoadExtension();                                                                                 \
	}                                                                                                                  \
	void NAME##Loader::Load()

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



namespace duckdb_stable {

class LogicalType {
public:
	LogicalType(duckdb_logical_type type_p) : type(type_p) {
	}
	LogicalType(duckdb_type ctype) {
		type = duckdb_create_logical_type(ctype);
	}
	~LogicalType() {
		if (type) {
			duckdb_destroy_logical_type(&type);
			type = nullptr;
		}
	}
	// disable copy constructors
	LogicalType(const LogicalType &other) = delete;
	LogicalType &operator=(const LogicalType &) = delete;
	//! enable move constructors
	LogicalType(LogicalType &&other) noexcept : type(nullptr) {
		std::swap(type, other.type);
	}
	LogicalType &operator=(LogicalType &&other) noexcept {
		std::swap(type, other.type);
		return *this;
	}

public:
	duckdb_logical_type c_type() {
		return type;
	}

	duckdb_type id() {
		return duckdb_get_type_id(type);
	}

	void SetAlias(const char *name) {
		duckdb_logical_type_set_alias(type, name);
	}

public:
	static LogicalType BOOLEAN() {
		return LogicalType(DUCKDB_TYPE_BOOLEAN);
	}
	static LogicalType VARCHAR() {
		return LogicalType(DUCKDB_TYPE_VARCHAR);
	}
	static LogicalType UTINYINT() {
		return LogicalType(DUCKDB_TYPE_UTINYINT);
	}
	static LogicalType USMALLINT() {
		return LogicalType(DUCKDB_TYPE_USMALLINT);
	}
	static LogicalType HUGEINT() {
		return LogicalType(DUCKDB_TYPE_HUGEINT);
	}
	static LogicalType STRUCT(LogicalType *child_types, const char **child_names, idx_t n) {
		return LogicalType(
		    duckdb_create_struct_type(reinterpret_cast<duckdb_logical_type *>(child_types), child_names, n));
	}

private:
	duckdb_logical_type type;
};

} // namespace duckdb_stable

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


#include <limits>

namespace duckdb_stable {

class hugeint_t {
public:
	hugeint_t() = default;
	hugeint_t(duckdb_hugeint value_p) : value(value_p) {
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
			throw std::runtime_error("Failed to negate hugeint: out of range");
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
			throw std::runtime_error("Failed to add hugeint: out of range");
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
			throw std::runtime_error("Failed to subtract hugeint: out of range");
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

} // namespace duckdb_stable

namespace duckdb_stable {

template <class INPUT_TYPE>
struct PrimitiveTypeState {
	INPUT_TYPE *data = nullptr;
	uint64_t *validity = nullptr;

	void PrepareVector(Vector &input, idx_t count) {
		data = reinterpret_cast<INPUT_TYPE *>(duckdb_vector_get_data(input.c_vec()));
		validity = duckdb_vector_get_validity(input.c_vec());
	}
};

struct AssignResult {
	template<class T>
	static void Assign(Vector &result, idx_t r, T result_val) {
		auto result_data = reinterpret_cast<T *>(duckdb_vector_get_data(result.c_vec()));
		result_data[r] = result_val;
	}

	template<>
	void Assign(Vector &result, idx_t r, string_t result_val) {
		duckdb_vector_assign_string_element_len(result.c_vec(), r, result_val.GetData(), result_val.GetSize());
	}
};

template <class INPUT_TYPE>
struct PrimitiveType {
	PrimitiveType() = default;
	PrimitiveType(INPUT_TYPE val) : val(val) { // NOLINT: allow implicit cast
	}

	INPUT_TYPE val;

	using ARG_TYPE = INPUT_TYPE;
	using STRUCT_STATE = PrimitiveTypeState<INPUT_TYPE>;

	static void ConstructType(STRUCT_STATE &state, idx_t r, ARG_TYPE &output) {
		output = state.data[r];
	}
	static void SetNull(Vector &result, STRUCT_STATE &result_state, idx_t i) {
		if (!result_state.validity) {
			duckdb_vector_ensure_validity_writable(result.c_vec());
			result_state.validity = duckdb_vector_get_validity(result.c_vec());
		}
		duckdb_validity_set_row_invalid(result_state.validity, i);
	}
	static void AssignResult(Vector &result, idx_t r, ARG_TYPE result_val) {
		AssignResult::Assign<INPUT_TYPE>(result, r, result_val);
	}
};

template <class A_TYPE, class B_TYPE, class C_TYPE>
struct StructTypeStateTernary {
	typename A_TYPE::STRUCT_STATE a_state;
	typename B_TYPE::STRUCT_STATE b_state;
	typename C_TYPE::STRUCT_STATE c_state;
	uint64_t *validity = nullptr;

	void PrepareVector(Vector &input, idx_t count) {
		Vector a_vector(duckdb_struct_vector_get_child(input.c_vec(), 0));
		a_state.PrepareVector(a_vector, count);

		Vector b_vector(duckdb_struct_vector_get_child(input.c_vec(), 1));
		b_state.PrepareVector(b_vector, count);

		Vector c_vector(duckdb_struct_vector_get_child(input.c_vec(), 2));
		c_state.PrepareVector(c_vector, count);

		validity = duckdb_vector_get_validity(input.c_vec());
	}
};

template <class A_TYPE, class B_TYPE, class C_TYPE>
struct StructTypeTernary {
	typename A_TYPE::ARG_TYPE a_val;
	typename B_TYPE::ARG_TYPE b_val;
	typename C_TYPE::ARG_TYPE c_val;

	using ARG_TYPE = StructTypeTernary<A_TYPE, B_TYPE, C_TYPE>;
	using STRUCT_STATE = StructTypeStateTernary<A_TYPE, B_TYPE, C_TYPE>;

	static void ConstructType(STRUCT_STATE &state, idx_t r, ARG_TYPE &output) {
		A_TYPE::ConstructType(state.a_state, r, output.a_val);
		B_TYPE::ConstructType(state.b_state, r, output.b_val);
		C_TYPE::ConstructType(state.c_state, r, output.c_val);
	}
	static void SetNull(Vector &result, STRUCT_STATE &result_state, idx_t r) {
		if (!result_state.validity) {
			duckdb_vector_ensure_validity_writable(result.c_vec());
			result_state.validity = duckdb_vector_get_validity(result.c_vec());
		}
		duckdb_validity_set_row_invalid(result_state.validity, r);

		auto a_child = result.GetChild(0);
		auto b_child = result.GetChild(1);
		auto c_child = result.GetChild(2);
		A_TYPE::SetNull(a_child, result_state.a_state, r);
		B_TYPE::SetNull(b_child, result_state.b_state, r);
		C_TYPE::SetNull(c_child, result_state.c_state, r);
	}
	static void AssignResult(Vector &result, idx_t r, ARG_TYPE result_val) {
		auto a_child = result.GetChild(0);
		A_TYPE::AssignResult(a_child, r, result_val.a_val);

		auto b_child = result.GetChild(1);
		B_TYPE::AssignResult(b_child, r, result_val.b_val);

		auto c_child = result.GetChild(2);
		C_TYPE::AssignResult(c_child, r, result_val.c_val);
	}
};

struct TemplateToType {
	template<class T>
	static LogicalType Convert() {
		static_assert(false, "Missing type in TemplateToType");
	}

	template <>
	LogicalType Convert<string_t>() {
		return LogicalType::VARCHAR();
	}

	template <>
	LogicalType Convert<PrimitiveType<bool>>() {
		return LogicalType::BOOLEAN();
	}

	template <>
	LogicalType Convert<PrimitiveType<string_t>>() {
		return LogicalType::VARCHAR();
	}

	template <>
	LogicalType Convert<PrimitiveType<uint8_t>>() {
		return LogicalType::UTINYINT();
	}

	template <>
	LogicalType Convert<PrimitiveType<hugeint_t>>() {
		return LogicalType::HUGEINT();
	}

};

} // namespace duckdb_stable



#include <functional>

namespace duckdb_stable {
class ExpressionState;

template <class T>
struct ResultValue {
	ResultValue() = default;
	ResultValue(T val_p) : val(val_p), is_null(false) {
	} // NOLINT: allow implicit conversion
	ResultValue(nullptr_t) : is_null(true) {
	} // NOLINT: allow implicit conversion

	T val;
	bool is_null = false;
};

class Executor {
public:
	template <class A_TYPE, class RESULT_TYPE, class FUNC>
	void ExecuteUnary(Vector &input, Vector &result, idx_t count, FUNC fun) {
		typename A_TYPE::STRUCT_STATE a_state;
		a_state.PrepareVector(input, count);

		typename RESULT_TYPE::STRUCT_STATE result_state;
		for (idx_t r = 0; r < count; r++) {
			if (!duckdb_validity_row_is_valid(a_state.validity, r)) {
				RESULT_TYPE::SetNull(result, result_state, r);
				continue;
			}
			typename A_TYPE::ARG_TYPE a_val;
			A_TYPE::ConstructType(a_state, r, a_val);

			ResultValue<typename RESULT_TYPE::ARG_TYPE> result_value;
			try {
				result_value = fun(a_val);
			} catch (std::exception &ex) {
				if (!SetError(ex.what(), r, result)) {
					return;
				}
				continue;
			}
			if (result_value.is_null) {
				RESULT_TYPE::SetNull(result, result_state, r);
				continue;
			}
			RESULT_TYPE::AssignResult(result, r, result_value.val);
		}
	}

	template <class A_TYPE, class B_TYPE, class RESULT_TYPE, class FUNC>
	void ExecuteBinary(Vector &a, Vector &b, Vector &result, idx_t count, FUNC fun) {
		typename A_TYPE::STRUCT_STATE a_state;
		typename B_TYPE::STRUCT_STATE b_state;

		a_state.PrepareVector(a, count);
		b_state.PrepareVector(b, count);

		typename RESULT_TYPE::STRUCT_STATE result_state;
		for (idx_t r = 0; r < count; r++) {
			if (!duckdb_validity_row_is_valid(a_state.validity, r) ||
			    !duckdb_validity_row_is_valid(b_state.validity, r)) {
				RESULT_TYPE::SetNull(result, result_state, r);
				continue;
			}
			typename A_TYPE::ARG_TYPE a_val;
			typename B_TYPE::ARG_TYPE b_val;
			A_TYPE::ConstructType(a_state, r, a_val);
			B_TYPE::ConstructType(b_state, r, b_val);

			ResultValue<typename RESULT_TYPE::ARG_TYPE> result_value;
			try {
				result_value = fun(a_val, b_val);
			} catch (std::exception &ex) {
				if (!SetError(ex.what(), r, result)) {
					return;
				}
				continue;
			}
			if (result_value.is_null) {
				RESULT_TYPE::SetNull(result, result_state, r);
				continue;
			}
			RESULT_TYPE::AssignResult(result, r, result_value.val);
		}
	}

	virtual bool Success() {
		return true;
	}

protected:
	virtual bool SetError(const char *error_message, idx_t r, Vector &result) = 0;
};

class CastExecutor : public Executor {
public:
	CastExecutor(duckdb_function_info info_p) : info(info_p) {
		cast_mode = duckdb_cast_function_get_cast_mode(info);
	}

public:
	bool Success() override {
		return success;
	}

protected:
	bool SetError(const char *error_message, idx_t r, Vector &result) override {
		duckdb_cast_function_set_row_error(info, error_message, r, result.c_vec());
		if (cast_mode == DUCKDB_CAST_TRY) {
			return true;
		}
		success = false;
		return false;
	}

private:
	duckdb_function_info info;
	duckdb_cast_mode cast_mode;
	bool success = true;
};

class FunctionExecutor : public Executor {
public:
	FunctionExecutor(duckdb_function_info info_p) : info(info_p) {
	}

public:
	bool Success() override {
		return success;
	}

	duckdb_function_info c_info() {
		return info;
	}

protected:
	bool SetError(const char *error_message, idx_t r, Vector &result) override {
		duckdb_scalar_function_set_error(info, error_message);
		success = false;
		return false;
	}

private:
	duckdb_function_info info;
	bool success = true;
};

} // namespace duckdb_stable

namespace duckdb_stable {

class CastFunction {
public:
	virtual LogicalType SourceType() = 0;
	virtual LogicalType TargetType() = 0;
	virtual int64_t ImplicitCastCost() = 0;
	virtual duckdb_cast_function_t GetFunction() = 0;
};

template <class OP, class SOURCE_T, class TARGET_T, class STATIC_T = void>
class StandardCastFunction : public CastFunction {
public:
	using SOURCE_TYPE = SOURCE_T;
	using TARGET_TYPE = TARGET_T;
	using STATIC_DATA = STATIC_T;

	LogicalType SourceType() override {
		return TemplateToType::Convert<SOURCE_TYPE>();
	}

	LogicalType TargetType() override {
		return TemplateToType::Convert<TARGET_TYPE>();
	}

	template <class STATIC_DATA_TYPE>
	static bool TemplatedCastFunction(duckdb_function_info info, idx_t count, duckdb_vector input,
	                                  duckdb_vector output) {
		CastExecutor executor(info);
		Vector input_vec(input);
		Vector output_vec(output);

		STATIC_DATA static_data;
		executor.ExecuteUnary<SOURCE_TYPE, TARGET_TYPE>(
		    input_vec, output_vec, count,
		    [&](const typename SOURCE_TYPE::ARG_TYPE &input) { return OP::Cast(input, static_data); });
		return executor.Success();
	}

	template <>
	bool TemplatedCastFunction<void>(duckdb_function_info info, idx_t count, duckdb_vector input,
	                                 duckdb_vector output) {
		CastExecutor executor(info);
		Vector input_vec(input);
		Vector output_vec(output);

		executor.ExecuteUnary<SOURCE_TYPE, TARGET_TYPE>(
		    input_vec, output_vec, count, [&](const typename SOURCE_TYPE::ARG_TYPE &input) { return OP::Cast(input); });
		return executor.Success();
	}

	duckdb_cast_function_t GetFunction() override {
		return TemplatedCastFunction<STATIC_DATA>;
	}
};

} // namespace duckdb_stable


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


#include <string>
#include <vector>

namespace duckdb_stable {
class LogicalType;
class DataChunk;
class ExpressionState;
class Vector;

class CScalarFunction {
public:
	CScalarFunction(duckdb_scalar_function function) : function(function) {
	}
	~CScalarFunction() {
		if (function) {
			duckdb_destroy_scalar_function(&function);
		}
	}
	// disable copy constructors
	CScalarFunction(const CScalarFunction &other) = delete;
	CScalarFunction &operator=(const CScalarFunction &) = delete;
	//! enable move constructors
	CScalarFunction(CScalarFunction &&other) noexcept : function(nullptr) {
		std::swap(function, other.function);
	}
	CScalarFunction &operator=(CScalarFunction &&other) noexcept {
		std::swap(function, other.function);
		return *this;
	}

	duckdb_scalar_function c_function() {
		return function;
	}

private:
	duckdb_scalar_function function;
};

class ScalarFunction {
public:
	virtual const char *Name() const {
		throw std::runtime_error("ScalarFunction does not have a name defined - it can only be added to a set");
	}
	virtual LogicalType ReturnType() const = 0;
	virtual std::vector<LogicalType> Arguments() const = 0;
	virtual duckdb_scalar_function_t GetFunction() const = 0;

	CScalarFunction CreateFunction(const char *name_override = nullptr) {
		auto scalar_function = duckdb_create_scalar_function();
		duckdb_scalar_function_set_name(scalar_function, name_override ? name_override : Name());
		for (auto &arg : Arguments()) {
			duckdb_scalar_function_add_parameter(scalar_function, arg.c_type());
		}
		duckdb_scalar_function_set_return_type(scalar_function, ReturnType().c_type());
		duckdb_scalar_function_set_function(scalar_function, GetFunction());
		return CScalarFunction(scalar_function);
	}
};

class ScalarFunctionSet {
public:
	ScalarFunctionSet(const char *name_p) : name(name_p) {
		set = duckdb_create_scalar_function_set(name_p);
	}
	~ScalarFunctionSet() {
		if (set) {
			duckdb_destroy_scalar_function_set(&set);
		}
	}

	void AddFunction(ScalarFunction &function) {
		auto scalar_function = function.CreateFunction(name.c_str());
		duckdb_add_scalar_function_to_set(set, scalar_function.c_function());
	}

	duckdb_scalar_function_set c_set() {
		return set;
	}

private:
	std::string name;
	duckdb_scalar_function_set set;
};

template <class OP, class INPUT_TYPE_T, class RETURN_TYPE_T, class STATIC_T = void>
class UnaryFunction : public ScalarFunction {
public:
	using INPUT_TYPE = INPUT_TYPE_T;
	using RESULT_TYPE = RETURN_TYPE_T;
	using STATIC_DATA = STATIC_T;

	LogicalType ReturnType() const override {
		return TemplateToType::Convert<RESULT_TYPE>();
	}
	std::vector<LogicalType> Arguments() const override {
		std::vector<LogicalType> arguments;
		arguments.push_back(TemplateToType::Convert<INPUT_TYPE>());
		return arguments;
	}
	template <class STATIC_DATA_TYPE>
	static void ExecuteUnary(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
		FunctionExecutor executor(info);
		DataChunk chunk(input);
		auto input_vec = chunk.GetVector(0);
		Vector output_vec(output);
		auto count = chunk.Size();

		typename OP::STATIC_DATA static_data;
		executor.ExecuteUnary<INPUT_TYPE, RESULT_TYPE>(
		    input_vec, output_vec, count,
		    [&](const typename INPUT_TYPE::ARG_TYPE &input) { return OP::Operation(input, static_data); });
	}

	template <>
	void ExecuteUnary<void>(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
		FunctionExecutor executor(info);
		DataChunk chunk(input);
		auto input_vec = chunk.GetVector(0);
		Vector output_vec(output);
		auto count = chunk.Size();

		executor.ExecuteUnary<INPUT_TYPE, RESULT_TYPE>(
		    input_vec, output_vec, count,
		    [&](const typename INPUT_TYPE::ARG_TYPE &input) { return OP::Operation(input); });
	}

	duckdb_scalar_function_t GetFunction() const override {
		return ExecuteUnary<STATIC_DATA>;
	}
};

template <class OP, class A_TYPE_T, class B_TYPE_T, class RETURN_TYPE_T, class STATIC_T = void>
class BinaryFunction : public ScalarFunction {
public:
	using A_TYPE = A_TYPE_T;
	using B_TYPE = B_TYPE_T;
	using RESULT_TYPE = RETURN_TYPE_T;
	using STATIC_DATA = STATIC_T;

	LogicalType ReturnType() const override {
		return TemplateToType::Convert<RESULT_TYPE>();
	}
	std::vector<LogicalType> Arguments() const override {
		std::vector<LogicalType> arguments;
		arguments.push_back(TemplateToType::Convert<A_TYPE>());
		arguments.push_back(TemplateToType::Convert<B_TYPE>());
		return arguments;
	}

	template <class STATIC_DATA_TYPE>
	static void ExecuteBinary(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
		FunctionExecutor executor(info);
		DataChunk chunk(input);
		auto a_vec = chunk.GetVector(0);
		auto b_vec = chunk.GetVector(1);
		Vector output_vec(output);
		auto count = chunk.Size();

		typename OP::STATIC_DATA static_data;
		executor.ExecuteBinary<A_TYPE, B_TYPE, RESULT_TYPE>(
		    a_vec, b_vec, output_vec, count,
		    [&](const typename A_TYPE::ARG_TYPE &a_val, const typename B_TYPE::ARG_TYPE &b_val) {
			    return OP::Operation(a_val, b_val, static_data);
		    });
	}

	template <>
	void ExecuteBinary<void>(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
		FunctionExecutor executor(info);
		DataChunk chunk(input);
		auto a_vec = chunk.GetVector(0);
		auto b_vec = chunk.GetVector(1);
		Vector output_vec(output);
		auto count = chunk.Size();

		executor.ExecuteBinary<A_TYPE, B_TYPE, RESULT_TYPE>(
		    a_vec, b_vec, output_vec, count,
		    [&](const typename A_TYPE::ARG_TYPE &a_val, const typename B_TYPE::ARG_TYPE &b_val) {
			    return OP::Operation(a_val, b_val);
		    });
	}

	duckdb_scalar_function_t GetFunction() const override {
		return ExecuteBinary<STATIC_DATA>;
	}
};

} // namespace duckdb_stable

#include <string>

namespace duckdb_stable {

class ExtensionLoader {
public:
	ExtensionLoader(duckdb_connection con, duckdb_extension_info info, struct duckdb_extension_access *access)
	    : connection(con), info(info), access(access) {
	}

public:
	bool LoadExtension() {
		try {
			Load();
		} catch (std::exception &ex) {
			std::string error = std::string("Failed to load extension: ") + ex.what();
			access->set_error(info, error.c_str());
			return false;
		}
		return true;
	}

protected:
	virtual void Load() = 0;

	void Register(LogicalType &type) {
		// Register the type
		auto success = duckdb_register_logical_type(connection, type.c_type(), nullptr) == DuckDBSuccess;
		if (!success) {
			throw std::runtime_error("Failed to register type");
		}
	}

	void Register(CastFunction &cast) {
		auto cast_function = duckdb_create_cast_function();
		auto source_type = cast.SourceType();
		auto target_type = cast.TargetType();
		duckdb_cast_function_set_implicit_cast_cost(cast_function, cast.ImplicitCastCost());
		duckdb_cast_function_set_source_type(cast_function, source_type.c_type());
		duckdb_cast_function_set_target_type(cast_function, target_type.c_type());
		duckdb_cast_function_set_function(cast_function, cast.GetFunction());

		auto success = duckdb_register_cast_function(connection, cast_function) == DuckDBSuccess;

		duckdb_destroy_cast_function(&cast_function);
		if (!success) {
			throw std::runtime_error("Failed to register cast function");
		}
	}

	void Register(ScalarFunction &function) {
		auto scalar_function = function.CreateFunction();
		auto success = duckdb_register_scalar_function(connection, scalar_function.c_function()) == DuckDBSuccess;
		if (!success) {
			throw std::runtime_error(std::string("Failed to register scalar function ") + function.Name());
		}
	}

	void Register(ScalarFunctionSet &function_set) {
		auto success = duckdb_register_scalar_function_set(connection, function_set.c_set()) == DuckDBSuccess;
		if (!success) {
			throw std::runtime_error("Failed to register scalar function set");
		}
	}

protected:
	duckdb_connection connection;
	duckdb_extension_info info;
	struct duckdb_extension_access *access;
};

} // namespace duckdb_stable


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

