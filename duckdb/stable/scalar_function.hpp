//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/scalar_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"
#include "duckdb/stable/logical_type.hpp"
#include "duckdb/stable/executor_types.hpp"
#include "duckdb/stable/executor.hpp"
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
