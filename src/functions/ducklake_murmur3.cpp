#include "common/ducklake_murmur3.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <cstring>

namespace duckdb {

static void Murmur3ScalarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	auto count = args.size();

	UnifiedVectorFormat input_data;
	input.ToUnifiedFormat(count, input_data);

	auto result_data = FlatVector::GetDataMutable<int32_t>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto &type = input.GetType();
	for (idx_t i = 0; i < count; i++) {
		auto idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		switch (type.InternalType()) {
		case PhysicalType::BOOL: {
			// Iceberg: boolean -> int (0 or 1) -> hash as long
			int64_t val = UnifiedVectorFormat::GetData<bool>(input_data)[idx] ? 1 : 0;
			result_data[i] = DuckLakeMurmur3::HashValue<int64_t>(val);
			break;
		}
		case PhysicalType::INT8: {
			// Iceberg: integers are sign-extended to long and hashed as 8 bytes
			int64_t val = UnifiedVectorFormat::GetData<int8_t>(input_data)[idx];
			result_data[i] = DuckLakeMurmur3::HashValue<int64_t>(val);
			break;
		}
		case PhysicalType::INT16: {
			int64_t val = UnifiedVectorFormat::GetData<int16_t>(input_data)[idx];
			result_data[i] = DuckLakeMurmur3::HashValue<int64_t>(val);
			break;
		}
		case PhysicalType::INT32: {
			// Iceberg: int -> hashLong(value)
			int64_t val = UnifiedVectorFormat::GetData<int32_t>(input_data)[idx];
			result_data[i] = DuckLakeMurmur3::HashValue<int64_t>(val);
			break;
		}
		case PhysicalType::INT64: {
			auto val = UnifiedVectorFormat::GetData<int64_t>(input_data)[idx];
			result_data[i] = DuckLakeMurmur3::HashValue<int64_t>(val);
			break;
		}
		case PhysicalType::FLOAT: {
			// Iceberg: float -> double -> doubleToLongBits -> hashLong
			// Normalize -0.0 to +0.0
			auto fval = UnifiedVectorFormat::GetData<float>(input_data)[idx];
			double dval = static_cast<double>(fval);
			if (dval == 0.0) {
				dval = 0.0; // normalize -0.0
			}
			int64_t bits;
			memcpy(&bits, &dval, sizeof(int64_t));
			result_data[i] = DuckLakeMurmur3::HashValue<int64_t>(bits);
			break;
		}
		case PhysicalType::DOUBLE: {
			// Iceberg: doubleToLongBits -> hashLong, normalize -0.0
			auto dval = UnifiedVectorFormat::GetData<double>(input_data)[idx];
			if (dval == 0.0) {
				dval = 0.0; // normalize -0.0
			}
			int64_t bits;
			memcpy(&bits, &dval, sizeof(int64_t));
			result_data[i] = DuckLakeMurmur3::HashValue<int64_t>(bits);
			break;
		}
		case PhysicalType::VARCHAR: {
			// Iceberg: hash UTF-8 bytes directly
			auto val = UnifiedVectorFormat::GetData<string_t>(input_data)[idx];
			result_data[i] = DuckLakeMurmur3::Hash(reinterpret_cast<const uint8_t *>(val.GetData()),
			                                       static_cast<idx_t>(val.GetSize()));
			break;
		}
		default: {
			// For complex types (STRUCT, LIST, etc.) hash the string representation
			auto str_val = input.GetValue(idx).ToString();
			result_data[i] = DuckLakeMurmur3::Hash(reinterpret_cast<const uint8_t *>(str_val.data()),
			                                       static_cast<idx_t>(str_val.size()));
			break;
		}
		}
	}
}

ScalarFunction DuckLakeMurmur3Function() {
	auto func = ScalarFunction("murmur3_32", {LogicalType::ANY}, LogicalType::INTEGER, Murmur3ScalarFunction);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return func;
}

} // namespace duckdb
