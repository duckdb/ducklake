#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

//! Binding fails, so the error is raised however the macro uses the parameter
static unique_ptr<FunctionData> UnreadableMacroDefaultBind(BindScalarFunctionInput &input) {
	auto macro_name = input.GetNonNullConstant(0).GetValue<string>();
	auto parameter_name = input.GetNonNullConstant(1).GetValue<string>();
	throw BinderException("The default of parameter \"%s\" of macro \"%s\" was stored by DuckLake 1.0 in a form that "
	                      "cannot be read back, pass the parameter or create the macro again",
	                      parameter_name, macro_name);
}

static void UnreadableMacroDefaultFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	throw InternalException("ducklake_unreadable_macro_default is never bound");
}

ScalarFunction DuckLakeUnreadableMacroDefaultFunction() {
	return ScalarFunction("ducklake_unreadable_macro_default", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                      LogicalType::SQLNULL, UnreadableMacroDefaultFunction, UnreadableMacroDefaultBind);
}

} // namespace duckdb
