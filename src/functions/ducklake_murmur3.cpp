#include "common/ducklake_murmur3.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

static int32_t HashDouble(double value) {
	return DuckLakeMurmur3::HashValue(value == 0.0 ? 0.0 : value);
}

static int32_t HashString(string_t value) {
	return DuckLakeMurmur3::Hash(const_data_ptr_cast(value.GetData()), value.GetSize());
}

static void Murmur3ScalarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	auto count = args.size();
	switch (input.GetType().InternalType()) {
	case PhysicalType::BOOL:
		UnaryExecutor::Execute<bool, int32_t>(input, result, count, DuckLakeMurmur3::HashValue<int64_t>);
		break;
	case PhysicalType::INT8:
		UnaryExecutor::Execute<int8_t, int32_t>(input, result, count, DuckLakeMurmur3::HashValue<int64_t>);
		break;
	case PhysicalType::INT16:
		UnaryExecutor::Execute<int16_t, int32_t>(input, result, count, DuckLakeMurmur3::HashValue<int64_t>);
		break;
	case PhysicalType::INT32:
		UnaryExecutor::Execute<int32_t, int32_t>(input, result, count, DuckLakeMurmur3::HashValue<int64_t>);
		break;
	case PhysicalType::INT64:
		UnaryExecutor::Execute<int64_t, int32_t>(input, result, count, DuckLakeMurmur3::HashValue<int64_t>);
		break;
	case PhysicalType::FLOAT:
		UnaryExecutor::Execute<float, int32_t>(input, result, count, HashDouble);
		break;
	case PhysicalType::DOUBLE:
		UnaryExecutor::Execute<double, int32_t>(input, result, count, HashDouble);
		break;
	case PhysicalType::VARCHAR:
		UnaryExecutor::Execute<string_t, int32_t>(input, result, count, HashString);
		break;
	default: {
		Vector varchar_input(LogicalType::VARCHAR);
		VectorOperations::DefaultCast(input, varchar_input, count);
		UnaryExecutor::Execute<string_t, int32_t>(varchar_input, result, count, HashString);
		if (args.AllConstant()) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		break;
	}
	}
}

ScalarFunction DuckLakeMurmur3Function() {
	auto func = ScalarFunction("murmur3_32", {LogicalType::ANY}, LogicalType::INTEGER, Murmur3ScalarFunction);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return func;
}

} // namespace duckdb
