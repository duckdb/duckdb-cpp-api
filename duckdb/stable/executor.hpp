//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/generic_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/executor_types.hpp"
#include <functional>
#include <cstddef>

namespace duckdb_stable {

class ExpressionState;

template <class T>
struct ResultValue {
	ResultValue() = default;
	ResultValue(T val_p) : val(val_p), is_null(false) {  // NOLINT: allow implicit conversion.
	}
	ResultValue(std::nullptr_t ) : is_null(true) { // NOLINT: allow implicit conversion.
	}

	T val;
	bool is_null = false;
};

class Executor {
public:
	template <class A_TYPE, class RESULT_TYPE, class FUNC>
	void ExecuteUnary(Vector &input, Vector &result, const idx_t count, FUNC fun) {
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
	void ExecuteBinary(Vector &a, Vector &b, Vector &result, const idx_t count, FUNC fun) {
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
	virtual bool SetError(const char *error_message, const idx_t r, Vector &result) = 0;
};

class CastExecutor : public Executor {
public:
	CastExecutor(duckdb_function_info info_p) : info(info_p), success(true) {
		cast_mode = duckdb_cast_function_get_cast_mode(info);
	}

public:
	bool Success() override {
		return success;
	}

protected:
	bool SetError(const char *error_message, const idx_t r, Vector &result) override {
		duckdb_cast_function_set_row_error(info, error_message, r, result.c_vector());
		if (cast_mode == DUCKDB_CAST_TRY) {
			return true;
		}
		success = false;
		return false;
	}

private:
	duckdb_function_info info;
	duckdb_cast_mode cast_mode;
	bool success;
};

class FunctionExecutor : public Executor {
public:
	FunctionExecutor(duckdb_function_info info_p) : info(info_p), success(true) {
	}

public:
	bool Success() override {
		return success;
	}

	duckdb_function_info c_function_info() {
		return info;
	}

protected:
	bool SetError(const char *error_message, const idx_t r, Vector &result) override {
		duckdb_scalar_function_set_error(info, error_message);
		success = false;
		return false;
	}

private:
	duckdb_function_info info;
	bool success;
};

} // namespace duckdb_stable
