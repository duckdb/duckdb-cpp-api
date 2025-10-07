//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/executor_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"
#include "duckdb/stable/data_chunk.hpp"
#include "duckdb/stable/hugeint.hpp"
#include "duckdb/stable/string_type.hpp"
#include "duckdb/stable/vector.hpp"

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
};

template<>
inline void AssignResult::Assign(Vector &result, idx_t r, string_t result_val) {
	duckdb_vector_assign_string_element_len(result.c_vec(), r, result_val.GetData(), result_val.GetSize());
}

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

template<class T>
struct AlwaysFalse {
	static constexpr bool value = false;
};

struct TemplateToType {
	template<class T>
	static LogicalType Convert() {
		static_assert(AlwaysFalse<T>::value, "Missing type in TemplateToType");
		throw std::runtime_error("Missing type in TemplateType");
	}
};

template <>
inline LogicalType TemplateToType::Convert<string_t>() {
	return LogicalType::VARCHAR();
}

template <>
inline LogicalType TemplateToType::Convert<PrimitiveType<bool>>() {
	return LogicalType::BOOLEAN();
}

template <>
inline LogicalType TemplateToType::Convert<PrimitiveType<string_t>>() {
	return LogicalType::VARCHAR();
}

template <>
inline LogicalType TemplateToType::Convert<PrimitiveType<uint8_t>>() {
	return LogicalType::UTINYINT();
}

template <>
inline LogicalType TemplateToType::Convert<PrimitiveType<hugeint_t>>() {
	return LogicalType::HUGEINT();
}


} // namespace duckdb_stable
