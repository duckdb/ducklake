#include "storage/ducklake_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

static void WouldRetryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto &message_vec = args.data[0];
	if (args.ColumnCount() == 1) {
		UnaryExecutor::Execute<string_t, bool>(message_vec, result, count, [&](string_t message) {
			return DuckLakeTransaction::RetryOnError(message.GetString());
		});
		return;
	}
	auto &type_vec = args.data[1];
	BinaryExecutor::Execute<string_t, string_t, bool>(
	    message_vec, type_vec, result, count, [&](string_t message, string_t type_name) {
		    auto type = Exception::StringToExceptionType(type_name.GetString());
		    return DuckLakeTransaction::RetryOnError(message.GetString(), type);
	    });
}

ScalarFunctionSet DuckLakeWouldRetryFunction() {
	ScalarFunctionSet set("ducklake_would_retry");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN, WouldRetryFunction));
	set.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN, WouldRetryFunction));
	return set;
}

} // namespace duckdb
