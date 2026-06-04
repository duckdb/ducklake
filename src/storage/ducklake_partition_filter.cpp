#include "storage/ducklake_partition_filter.hpp"

#include "storage/ducklake_partition_data.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

struct PartitionFilterKey {
	idx_t partition_key_index;
	LogicalType type;
};

static unordered_map<string, PartitionFilterKey> GetPartitionKeyMap(DuckLakeTableEntry &table) {
	auto partition_data = table.GetPartitionData();
	if (!partition_data) {
		throw InvalidInputException("partition_values requires a partitioned table");
	}
	auto partition_sql_exprs = table.GetPartitionSQLExpressions();
	unordered_map<string, PartitionFilterKey> result;
	case_insensitive_set_t used_names;
	for (auto &field : partition_data->fields) {
		auto &column = table.GetColumnByFieldId(field.field_id);
		auto field_name = column.GetName();
		auto partition_key_name =
		    DuckLakePartitionUtils::GetPartitionKeyName(field.transform.type, field_name, used_names);
		used_names.insert(partition_key_name);

		PartitionFilterKey key {field.partition_key_index,
		                        DuckLakePartitionUtils::GetPartitionKeyType(field.transform.type, column.GetType())};
		result[StringUtil::Lower(partition_key_name)] = key;
		result[StringUtil::Lower(field_name)] = key;
		if (field.partition_key_index < partition_sql_exprs.size()) {
			result[StringUtil::Lower(partition_sql_exprs[field.partition_key_index])] = key;
		}
	}
	return result;
}

static DuckLakePartitionFilterValue CastPartitionValue(const Value &input_value, const LogicalType &type,
                                                       const string &partition_key_name) {
	DuckLakePartitionFilterValue result;
	result.is_null = input_value.IsNull();
	if (result.is_null) {
		return result;
	}
	Value cast_value;
	string error;
	if (!input_value.DefaultTryCastAs(type, cast_value, &error)) {
		throw InvalidInputException("Failed to cast partition value for key \"%s\" to %s: %s", partition_key_name,
		                            type.ToString(), error);
	}
	result.value = cast_value.ToString();
	return result;
}

DuckLakePartitionFilter DuckLakePartitionFilter::Parse(DuckLakeTableEntry &table, const Value &filter) {
	if (filter.IsNull()) {
		throw InvalidInputException("partition_values cannot be NULL");
	}
	if (filter.type().id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("partition_values must be a STRUCT");
	}
	auto partition_data = table.GetPartitionData();
	if (!partition_data) {
		throw InvalidInputException("partition_values requires a partitioned table");
	}

	auto partition_keys = GetPartitionKeyMap(table);
	unordered_map<idx_t, DuckLakePartitionFilterValue> parsed_values;
	auto &children = StructValue::GetChildren(filter);
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto partition_key_name = StructType::GetChildName(filter.type(), child_idx);
		auto partition_key = partition_keys.find(StringUtil::Lower(partition_key_name));
		if (partition_key == partition_keys.end()) {
			throw InvalidInputException("Unknown partition key \"%s\" for table \"%s\"", partition_key_name,
			                            table.name);
		}
		parsed_values[partition_key->second.partition_key_index] =
		    CastPartitionValue(children[child_idx], partition_key->second.type, partition_key_name);
	}
	if (parsed_values.empty()) {
		throw InvalidInputException("partition_values cannot be empty");
	}

	DuckLakePartitionFilter result;
	result.partition_id = partition_data->partition_id;
	for (auto &entry : parsed_values) {
		result.values.push_back({entry.first, std::move(entry.second)});
	}
	return result;
}

static bool PartitionValueMatches(const DuckLakePartitionFilterValue &expected, const Value &actual) {
	if (expected.is_null || actual.IsNull()) {
		return expected.is_null && actual.IsNull();
	}
	return expected.value == actual.ToString();
}

bool DuckLakePartitionFilter::Matches(optional_idx file_partition_id,
                                      const vector<DuckLakeFilePartition> &file_values) const {
	if (partition_id.IsValid() && file_partition_id.IsValid() && partition_id != file_partition_id) {
		return false;
	}
	for (auto &entry : values) {
		bool found = false;
		for (auto &file_value : file_values) {
			if (file_value.partition_column_idx != entry.partition_key_index) {
				continue;
			}
			found = true;
			if (!PartitionValueMatches(entry.value, file_value.partition_value)) {
				return false;
			}
			break;
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
