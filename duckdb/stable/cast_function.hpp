//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/cast_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"
#include "duckdb/stable/logical_type.hpp"
#include "duckdb/stable/executor.hpp"
#include "duckdb/stable/vector.hpp"

namespace duckdb_stable {

class CastFunction {
public:
	virtual ~CastFunction() = default;

	virtual LogicalType SourceType() = 0;
	virtual LogicalType TargetType() = 0;
	virtual int64_t ImplicitCastCost() = 0;
	virtual duckdb_cast_function_t GetFunction() = 0;
};

template <class SOURCE_TYPE, class TARGET_TYPE>
class BaseCastFunction : public CastFunction {
public:
	LogicalType SourceType() override {
		return TemplateToType::Convert<SOURCE_TYPE>();
	}

	LogicalType TargetType() override {
		return TemplateToType::Convert<TARGET_TYPE>();
	}
};

template <class OP, class SOURCE_T, class TARGET_T>
class StandardCastFunction : public BaseCastFunction<SOURCE_T, TARGET_T> {
public:
	using SOURCE_TYPE = SOURCE_T;
	using TARGET_TYPE = TARGET_T;

	static bool CastFunc(duckdb_function_info info, idx_t count, duckdb_vector input,
	                                  duckdb_vector output) {
		CastExecutor executor(info);
		Vector input_vec(input);
		Vector output_vec(output);

		executor.ExecuteUnary<SOURCE_TYPE, TARGET_TYPE>(
		    input_vec, output_vec, count,
		    [&](const typename SOURCE_TYPE::ARG_TYPE &input_val) { return OP::Cast(input_val); });
		return executor.Success();
	}

	duckdb_cast_function_t GetFunction() override {
		return CastFunc;
	}
};

template <class OP, class SOURCE_T, class TARGET_T, class STATIC_T>
class StandardCastFunctionExt : public BaseCastFunction<SOURCE_T, TARGET_T> {
public:
	using SOURCE_TYPE = SOURCE_T;
	using TARGET_TYPE = TARGET_T;
	using STATIC_DATA = STATIC_T;

	static bool CastFunc(duckdb_function_info info, idx_t count, duckdb_vector input,
					  duckdb_vector output) {
		CastExecutor executor(info);
		Vector input_vec(input);
		Vector output_vec(output);

		STATIC_DATA static_data;
		executor.ExecuteUnary<SOURCE_TYPE, TARGET_TYPE>(
		    input_vec, output_vec, count,
		    [&](const typename SOURCE_TYPE::ARG_TYPE &input_val) { return OP::Cast(input_val, static_data); });
		return executor.Success();
	}

	duckdb_cast_function_t GetFunction() override {
		return CastFunc;
	}
};

} // namespace duckdb_stable
