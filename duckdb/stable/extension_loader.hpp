//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/logical_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"
#include "duckdb/stable/cast_function.hpp"
#include "duckdb/stable/logical_type.hpp"
#include "duckdb/stable/scalar_function.hpp"
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
