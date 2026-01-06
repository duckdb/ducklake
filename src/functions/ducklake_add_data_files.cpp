#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_insert.hpp"
#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class HivePartitioningType { AUTOMATIC, YES, NO };

struct DuckLakeAddDataFilesData : public TableFunctionData {
	DuckLakeAddDataFilesData(Catalog &catalog, DuckLakeTableEntry &table) : catalog(catalog), table(table) {
	}

	Catalog &catalog;
	DuckLakeTableEntry &table;
	vector<string> globs;
	bool allow_missing = false;
	bool ignore_extra_columns = false;
	HivePartitioningType hive_partitioning = HivePartitioningType::AUTOMATIC;
};

static unique_ptr<FunctionData> DuckLakeAddDataFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = BaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	string schema_name;
	if (input.inputs[1].IsNull()) {
		throw InvalidInputException("Table name cannot be NULL");
	}
	if (input.named_parameters.find("schema") != input.named_parameters.end()) {
		schema_name = StringValue::Get(input.named_parameters["schema"]);
	}
	const auto table_name = StringValue::Get(input.inputs[1]);

	auto entry =
	    catalog.GetEntry<TableCatalogEntry>(context, schema_name, table_name, OnEntryNotFound::THROW_EXCEPTION);
	auto &table = entry->Cast<DuckLakeTableEntry>();

	auto result = make_uniq<DuckLakeAddDataFilesData>(catalog, table);
	if (input.inputs[2].IsNull()) {
		throw InvalidInputException("File list cannot be NULL");
	}
	result->globs.push_back(StringValue::Get(input.inputs[2]));
	for (auto &entry : input.named_parameters) {
		auto lower = StringUtil::Lower(entry.first);
		if (lower == "allow_missing") {
			result->allow_missing = BooleanValue::Get(entry.second);
		} else if (lower == "ignore_extra_columns") {
			result->ignore_extra_columns = BooleanValue::Get(entry.second);
		} else if (lower == "hive_partitioning") {
			result->hive_partitioning =
			    BooleanValue::Get(entry.second) ? HivePartitioningType::YES : HivePartitioningType::NO;
		} else if (lower != "schema") {
			throw InternalException("Unknown named parameter %s for add_files", entry.first);
		}
	}

	names.emplace_back("filename");
	return_types.emplace_back(LogicalType::VARCHAR);
	return std::move(result);
}

struct DuckLakeAddDataFilesState : public GlobalTableFunctionState {
	DuckLakeAddDataFilesState() {
	}

	bool finished = false;
};

static unique_ptr<GlobalTableFunctionState> DuckLakeAddDataFilesInit(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	return make_uniq<DuckLakeAddDataFilesState>();
}

struct ParquetColumn {
	idx_t column_id;
	string name;
	string type;
	string converted_type;
	optional_idx scale;
	optional_idx precision;
	optional_idx field_id;
	string logical_type;
	vector<DuckLakeColumnStats> column_stats;

	vector<unique_ptr<ParquetColumn>> child_columns;
};

struct HivePartition {
	FieldIndex field_index;
	LogicalType field_type;
	Value hive_value;
};

struct ParquetFileMetadata {
	string filename;
	vector<unique_ptr<ParquetColumn>> columns;
	unordered_map<idx_t, reference<ParquetColumn>> column_id_map;
	optional_idx row_count;
	optional_idx file_size_bytes;
	optional_idx footer_size;

	// Store the column mapping entries once they are computed
	vector<unique_ptr<DuckLakeNameMapEntry>> map_entries;
	// Map from parquet column to the corresponding field ID and type
	unordered_map<idx_t, pair<FieldIndex, LogicalType>> column_id_to_field_map;
	// Map from field ID to hive partition statistics (for partition columns)
	vector<HivePartition> hive_partition_values;
};

struct DuckLakeFileProcessor {
public:
	DuckLakeFileProcessor(DuckLakeTransaction &transaction, const DuckLakeAddDataFilesData &bind_data)
	    : transaction(transaction), table(bind_data.table), allow_missing(bind_data.allow_missing),
	      ignore_extra_columns(bind_data.ignore_extra_columns), hive_partitioning(bind_data.hive_partitioning) {
	}

	vector<DuckLakeDataFile> AddFiles(const vector<string> &globs);

private:
	void ReadParquetSchema(const string &glob);
	void ReadParquetStats(const string &glob);
	void ReadParquetFileMetadata(const string &glob);
	DuckLakeDataFile AddFileToTable(ParquetFileMetadata &file);
	unique_ptr<DuckLakeNameMapEntry> MapColumn(ParquetFileMetadata &file_metadata, ParquetColumn &column,
	                                           const DuckLakeFieldId &field_id, string prefix);
	vector<unique_ptr<DuckLakeNameMapEntry>> MapColumns(ParquetFileMetadata &file,
	                                                    vector<unique_ptr<ParquetColumn>> &parquet_columns,
	                                                    const vector<unique_ptr<DuckLakeFieldId>> &field_ids,
	                                                    const string &prefix = string());
	void MapColumnStats(ParquetFileMetadata &file_metadata, DuckLakeDataFile &result);
	unique_ptr<DuckLakeNameMapEntry> MapHiveColumn(ParquetFileMetadata &file_metadata, const DuckLakeFieldId &field_id,
	                                               const Value &hive_value);
	void DetermineMapping(ParquetFileMetadata &file);
	void MapPartitionColumns(ParquetFileMetadata &file);

	void CheckMatchingType(const LogicalType &type, ParquetColumn &column);

private:
	DuckLakeTransaction &transaction;
	DuckLakeTableEntry &table;
	bool allow_missing;
	bool ignore_extra_columns;
	map<string, string> hive_partitions;
	HivePartitioningType hive_partitioning;
	unordered_map<string, unique_ptr<ParquetFileMetadata>> parquet_files;
};

void DuckLakeFileProcessor::ReadParquetSchema(const string &glob) {
	auto result = transaction.Query(StringUtil::Format(R"(
WITH base AS (
  SELECT file_name, name, type, num_children, converted_type, scale, precision, field_id, logical_type
  FROM parquet_schema(%s)
),
ordered AS (SELECT *, row_number() OVER () AS rn FROM base),
partitioned AS (SELECT * EXCLUDE (rn), row_number() OVER (PARTITION BY file_name ORDER BY rn) - 1 AS flattened_column_id FROM ordered)
SELECT * EXCLUDE (flattened_column_id)
FROM partitioned
ORDER BY file_name, flattened_column_id;
)",
	                                                   SQLString(glob)));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to add data files to DuckLake: ");
	}
	unique_ptr<ParquetFileMetadata> file;
	vector<reference<ParquetColumn>> current_column;
	vector<idx_t> child_counts;
	idx_t column_id = 0;
	for (auto &row : *result) {
		auto filename = row.GetValue<string>(0);
		auto child_count = row.IsNull(3) ? 0 : row.GetValue<idx_t>(3);
		if (!file || file->filename != filename) {
			if (!child_counts.empty()) {
				throw InvalidInputException("child_counts provided by parquet_schema are unaligned");
			}
			if (file) {
				DetermineMapping(*file);
				auto &filename = file->filename;
				parquet_files.emplace(filename, std::move(file));
			}
			file = make_uniq<ParquetFileMetadata>();
			file->filename = filename;
			child_counts.push_back(child_count);
			column_id = 0;
			continue;
		}
		if (child_counts.empty()) {
			throw InvalidInputException("child_counts provided by parquet_schema are unaligned");
		}
		auto column = make_uniq<ParquetColumn>();
		column->name = row.GetValue<string>(1);
		column->type = row.GetValue<string>(2);
		if (!row.IsNull(4)) {
			column->converted_type = row.GetValue<string>(4);
		}
		if (!row.IsNull(5)) {
			column->scale = row.GetValue<idx_t>(5);
		}
		if (!row.IsNull(6)) {
			column->precision = row.GetValue<idx_t>(6);
		}
		if (!row.IsNull(7)) {
			column->field_id = row.GetValue<idx_t>(7);
		}
		if (!row.IsNull(8)) {
			column->logical_type = row.GetValue<string>(8);
		}

		// Only assign column_id to leaf columns (those without children)
		if (child_count == 0) {
			column->column_id = column_id++;
		} else {
			column->column_id = DConstants::INVALID_INDEX; // Mark as non-leaf
		}

		auto &column_ref = *column;

		if (current_column.empty()) {
			// add to root
			file->columns.push_back(std::move(column));
		} else {
			// add as child to last column
			current_column.back().get().child_columns.push_back(std::move(column));
		}

		// Only add leaf columns to column_id_map (those that will have statistics)
		if (column_ref.column_id != DConstants::INVALID_INDEX) {
			file->column_id_map.emplace(column_ref.column_id, column_ref);
		}

		// reduce the child count by one
		child_counts.back()--;
		if (child_counts.back() == 0) {
			// we exhausted all children at this layer - pop back child counts
			if (!current_column.empty()) {
				current_column.pop_back();
			}
			child_counts.pop_back();
		}
		if (child_count > 0) {
			// nested column: push back the child count and the current column and start reading children for this
			// column
			current_column.push_back(column_ref);
			child_counts.push_back(child_count);
		}
	}
	if (file) {
		DetermineMapping(*file);
		auto &filename = file->filename;
		parquet_files.emplace(filename, std::move(file));
	}
}

void DuckLakeFileProcessor::ReadParquetStats(const string &glob) {
	auto result = transaction.Query(StringUtil::Format(R"(
SELECT file_name, column_id, coalesce(stats_min, stats_min_value), coalesce(stats_max, stats_max_value), stats_null_count, total_compressed_size, geo_bbox, geo_types
FROM parquet_metadata(%s)
)",
	                                                   SQLString(glob)));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to add data files to DuckLake: ");
	}
	for (auto &row : *result) {
		auto filename = row.GetValue<string>(0);
		auto entry = parquet_files.find(filename);
		if (entry == parquet_files.end()) {
			throw InvalidInputException("Parquet file was returned by parquet_metadata, but not returned by "
			                            "parquet_schema - did a Parquet file get added to a glob while processing?");
		}
		auto &parquet_file = entry->second;
		auto column_id = row.GetValue<idx_t>(1);
		// find the column this belongs to
		auto column_entry = parquet_file->column_id_map.find(column_id);
		if (column_entry == parquet_file->column_id_map.end()) {
			throw InvalidInputException("Column id not found in Parquet map?");
		}
		auto &column = column_entry->second.get();
		const auto &column_field_entry = parquet_file->column_id_to_field_map.find(column_id);
		if (column_field_entry == parquet_file->column_id_to_field_map.end()) {
			// We've already verified that this file has a mapping for all existing columns in the table, if this one
			// doesn't
			//  have a mapping then it must not correspond to any table column so we can skip it
			continue;
		}
		auto &column_field = column_field_entry->second;
		DuckLakeColumnStats stats(column_field.second);

		if (!row.IsNull(2)) {
			stats.has_min = true;
			stats.min = row.GetValue<string>(2);
		}
		if (!row.IsNull(3)) {
			stats.has_max = true;
			stats.max = row.GetValue<string>(3);
		}
		if (!row.IsNull(4)) {
			stats.has_null_count = true;
			stats.null_count = StringUtil::ToUnsigned(row.GetValue<string>(4));
		}
		if (!row.IsNull(5)) {
			stats.column_size_bytes = StringUtil::ToUnsigned(row.GetValue<string>(5));
		}
		if (!row.IsNull(6)) {
			// Split the bbox struct into individual entries
			auto bbox_value = row.iterator.chunk->GetValue(6, row.row);
			auto &bbox_type = bbox_value.type();

			auto &bbox_child_types = StructType::GetChildTypes(bbox_type);
			auto &bbox_child_values = StructValue::GetChildren(bbox_value);

			for (idx_t child_idx = 0; child_idx < bbox_child_types.size(); child_idx++) {
				auto &name = bbox_child_types[child_idx].first;
				auto &value = bbox_child_values[child_idx];
				if (!value.IsNull()) {
					auto &geo_stats = stats.extra_stats->Cast<DuckLakeColumnGeoStats>();
					if (name == "xmax") {
						geo_stats.xmax = value.DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
					} else if (name == "xmin") {
						geo_stats.xmin = value.DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
					} else if (name == "ymax") {
						geo_stats.ymax = value.DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
					} else if (name == "ymin") {
						geo_stats.ymin = value.DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
					} else if (name == "zmax") {
						geo_stats.zmax = value.DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
					} else if (name == "zmin") {
						geo_stats.zmin = value.DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
					} else if (name == "mmax") {
						geo_stats.mmax = value.DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
					} else if (name == "mmin") {
						geo_stats.mmin = value.DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
					} else {
						throw InternalException("Unknown bbox child name %s", name);
					}
				}
			}
		}
		if (!row.IsNull(7)) {
			auto list_value = row.iterator.chunk->GetValue(7, row.row);
			auto &geo_stats = stats.extra_stats->Cast<DuckLakeColumnGeoStats>();
			for (const auto &child : ListValue::GetChildren(list_value)) {
				geo_stats.geo_types.insert(StringValue::Get(child));
			}
		}

		column.column_stats.push_back(std::move(stats));
	}
}

void DuckLakeFileProcessor::ReadParquetFileMetadata(const string &glob) {

	auto result = transaction.Query(StringUtil::Format(R"(
SELECT file_name, num_rows, footer_size, file_size_bytes
FROM parquet_file_metadata(%s)
)",
	                                                   SQLString(glob)));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to add data files to DuckLake: ");
	}
	for (auto &row : *result) {
		auto filename = row.GetValue<string>(0);
		auto entry = parquet_files.find(filename);
		if (entry == parquet_files.end()) {
			throw InvalidInputException("Parquet file was returned by parquet_metadata, but not returned by "
			                            "parquet_schema - did a Parquet file get added to a glob while processing?");
		}
		entry->second->row_count = row.GetValue<idx_t>(1);
		entry->second->footer_size = row.GetValue<idx_t>(2);
		entry->second->file_size_bytes = row.GetValue<idx_t>(3);
	}
}

class DuckLakeParquetTypeChecker {
public:
	DuckLakeParquetTypeChecker(DuckLakeTableEntry &table, ParquetFileMetadata &file_metadata, const LogicalType &type,
	                           ParquetColumn &column_p, const string &prefix_p)
	    : table(table), file_metadata(file_metadata), source_type(DeriveLogicalType(column_p)), type(type),
	      column(column_p), prefix(prefix_p) {
	}

	DuckLakeTableEntry &table;
	ParquetFileMetadata &file_metadata;
	LogicalType source_type;
	const LogicalType &type;
	ParquetColumn &column;
	const string &prefix;

public:
	void CheckMatchingType();

private:
	void CheckSignedInteger();
	void CheckUnsignedInteger();
	void CheckFloatingPoints();
	void CheckTimestamp();
	void CheckDecimal();

	//! Called when a check fails
	void Fail();

private:
	bool CheckType(const LogicalType &type);
	//! Verify type is equivalent to one of the accepted types
	bool CheckTypes(const vector<LogicalType> &types);

	static LogicalType DeriveLogicalType(const ParquetColumn &column);

private:
	vector<string> failures;
};

LogicalType DuckLakeParquetTypeChecker::DeriveLogicalType(const ParquetColumn &s_ele) {
	// FIXME: this is more or less copied from DeriveLogicalType in DuckDB's Parquet reader
	//  we should just emit DuckDB's type in parquet_schema and remove this method
	if (!s_ele.child_columns.empty()) {
		// nested types
		if (s_ele.converted_type == "LIST") {
			return LogicalTypeId::LIST;
		} else if (s_ele.converted_type == "MAP") {
			return LogicalTypeId::MAP;
		}
		return LogicalTypeId::STRUCT;
	}
	if (!s_ele.logical_type.empty()) {
		if (s_ele.logical_type ==
		    "TimeType(isAdjustedToUTC=0, unit=TimeUnit(MILLIS=<null>, MICROS=MicroSeconds(), NANOS=<null>))") {
			return LogicalType::TIME;
		} else if (s_ele.logical_type == "TimestampType(isAdjustedToUTC=0, unit=TimeUnit(MILLIS=<null>, "
		                                 "MICROS=MicroSeconds(), NANOS=<null>))") {
			return LogicalType::TIMESTAMP;
		} else if (s_ele.logical_type == "TimestampType(isAdjustedToUTC=0, unit=TimeUnit(MILLIS=MilliSeconds(), "
		                                 "MICROS=<null>, NANOS=<null>))") {
			return LogicalType::TIMESTAMP_MS;
		} else if (s_ele.logical_type == "TimestampType(isAdjustedToUTC=0, unit=TimeUnit(MILLIS=<null>, MICROS=<null>, "
		                                 "NANOS=NanoSeconds()))") {
			return LogicalType::TIMESTAMP_NS;
		} else if (StringUtil::StartsWith(s_ele.logical_type, "TimestampType(isAdjustedToUTC=1")) {
			return LogicalType::TIMESTAMP_TZ;
		} else if (StringUtil::StartsWith(s_ele.logical_type, "UUIDType()")) {
			return LogicalType::UUID;
		} else if (StringUtil::StartsWith(s_ele.logical_type, "Geometry")) {
			LogicalType geo_type(LogicalTypeId::BLOB);
			geo_type.SetAlias("GEOMETRY");
			return geo_type;
		}
	}
	if (!s_ele.converted_type.empty()) {
		// Legacy NULL type, does no longer exist, but files are still around of course
		if (s_ele.converted_type == "INT_8") {
			return LogicalType::TINYINT;
		} else if (s_ele.converted_type == "INT_16") {
			return LogicalType::SMALLINT;
		} else if (s_ele.converted_type == "INT_32") {
			return LogicalType::INTEGER;
		} else if (s_ele.converted_type == "INT_64") {
			return LogicalType::BIGINT;
		} else if (s_ele.converted_type == "UINT_8") {
			return LogicalType::UTINYINT;
		} else if (s_ele.converted_type == "UINT_16") {
			return LogicalType::USMALLINT;
		} else if (s_ele.converted_type == "UINT_32") {
			return LogicalType::UINTEGER;
		} else if (s_ele.converted_type == "UINT_64") {
			return LogicalType::UBIGINT;
		} else if (s_ele.converted_type == "DATE") {
			return LogicalType::DATE;
		} else if (s_ele.converted_type == "TIMESTAMP_MICROS") {
			return LogicalType::TIMESTAMP;
		} else if (s_ele.converted_type == "TIMESTAMP_MILLIS") {
			return LogicalType::TIMESTAMP;
		} else if (s_ele.converted_type == "DECIMAL") {
			if (!s_ele.scale.IsValid() || !s_ele.precision.IsValid()) {
				throw InvalidInputException("DECIMAL requires valid precision/scale");
			}
			return LogicalType::DECIMAL(s_ele.precision.GetIndex(), s_ele.scale.GetIndex());
		} else if (s_ele.converted_type == "UTF8") {
			return LogicalType::VARCHAR;
		} else if (s_ele.converted_type == "ENUM") {
			return LogicalType::VARCHAR;
		} else if (s_ele.converted_type == "TIME_MILLIS") {
			return LogicalType::TIME;
		} else if (s_ele.converted_type == "TIME_MICROS") {
			return LogicalType::TIME;
		} else if (s_ele.converted_type == "INTERVAL") {
			return LogicalType::INTERVAL;
		} else if (s_ele.converted_type == "JSON") {
			return LogicalType::JSON();
		}
	}
	// no converted type set
	// use default type for each physical type
	if (s_ele.type == "BOOLEAN") {
		return LogicalType::BOOLEAN;
	} else if (s_ele.type == "INT32") {
		return LogicalType::INTEGER;
	} else if (s_ele.type == "INT64") {
		return LogicalType::BIGINT;
	} else if (s_ele.type == "INT96") {
		return LogicalType::TIMESTAMP;
	} else if (s_ele.type == "FLOAT") {
		return LogicalType::FLOAT;
	} else if (s_ele.type == "DOUBLE") {
		return LogicalType::DOUBLE;
	} else if (s_ele.type == "BYTE_ARRAY") {
		return LogicalType::BLOB;
	} else if (s_ele.type == "FIXED_LEN_BYTE_ARRAY") {
		return LogicalType::BLOB;
	}
	throw InvalidInputException("Unrecognized type %s for parquet file", s_ele.type);
}

static string FormatExpectedError(const vector<LogicalType> &expected) {
	string error;
	for (auto &type : expected) {
		if (!error.empty()) {
			error += ", ";
		}
		error += type.ToString();
	}
	return expected.size() > 1 ? "one of " + error : error;
}

bool DuckLakeParquetTypeChecker::CheckType(const LogicalType &type) {
	vector<LogicalType> types;
	types.push_back(type);
	return CheckTypes(types);
}

bool DuckLakeParquetTypeChecker::CheckTypes(const vector<LogicalType> &types) {
	for (auto &type : types) {
		if (source_type == type) {
			return true;
		}
	}
	failures.push_back(StringUtil::Format("Expected %s, found type %s", FormatExpectedError(types), source_type));
	return false;
}

void DuckLakeParquetTypeChecker::Fail() {
	string error_message =
	    StringUtil::Format("Failed to map column \"%s%s\" from file \"%s\" to the column in table \"%s\"",
	                       prefix.empty() ? prefix : prefix + ".", column.name, file_metadata.filename, table.name);
	for (auto &failure : failures) {
		error_message += "\n* " + failure;
	}
	throw InvalidInputException(error_message);
}

void DuckLakeParquetTypeChecker::CheckSignedInteger() {
	vector<LogicalType> accepted_types;

	switch (type.id()) {
	case LogicalTypeId::BIGINT:
		accepted_types.push_back(LogicalType::BIGINT);
		accepted_types.push_back(LogicalType::UINTEGER);
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case LogicalTypeId::INTEGER:
		accepted_types.push_back(LogicalType::INTEGER);
		accepted_types.push_back(LogicalType::USMALLINT);
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case LogicalTypeId::SMALLINT:
		accepted_types.push_back(LogicalType::SMALLINT);
		accepted_types.push_back(LogicalType::UTINYINT);
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case LogicalTypeId::TINYINT:
		accepted_types.push_back(LogicalType::TINYINT);
		break;
	default:
		throw InternalException("Unknown signed type");
	}
	if (!CheckTypes(accepted_types)) {
		Fail();
	}
}

void DuckLakeParquetTypeChecker::CheckUnsignedInteger() {
	vector<LogicalType> accepted_types;

	switch (type.id()) {
	case LogicalTypeId::UBIGINT:
		accepted_types.push_back(LogicalType::UBIGINT);
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case LogicalTypeId::UINTEGER:
		accepted_types.push_back(LogicalType::UINTEGER);
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case LogicalTypeId::USMALLINT:
		accepted_types.push_back(LogicalType::USMALLINT);
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case LogicalTypeId::UTINYINT:
		accepted_types.push_back(LogicalType::UTINYINT);
		break;
	default:
		throw InternalException("Unknown unsigned type");
	}
	if (!CheckTypes(accepted_types)) {
		Fail();
	}
}

void DuckLakeParquetTypeChecker::CheckFloatingPoints() {
	vector<LogicalType> accepted_types;

	switch (type.id()) {
	case LogicalTypeId::DOUBLE:
		accepted_types.push_back(LogicalType::DOUBLE);
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case LogicalTypeId::FLOAT:
		accepted_types.push_back(LogicalType::FLOAT);
		break;
	default:
		throw InternalException("Unknown float type");
	}
	if (!CheckTypes(accepted_types)) {
		Fail();
	}
}

void DuckLakeParquetTypeChecker::CheckTimestamp() {
	vector<LogicalType> accepted_types;

	if (type.id() == LogicalTypeId::TIMESTAMP || type.id() == LogicalTypeId::TIMESTAMP_NS) {
		accepted_types.push_back(LogicalTypeId::TIMESTAMP_NS);
	}
	accepted_types.push_back(LogicalTypeId::TIMESTAMP);
	accepted_types.push_back(LogicalTypeId::TIMESTAMP_MS);
	accepted_types.push_back(LogicalTypeId::TIMESTAMP_SEC);
	if (!CheckTypes(accepted_types)) {
		Fail();
	}
}

void DuckLakeParquetTypeChecker::CheckDecimal() {
	if (source_type.id() != LogicalTypeId::DECIMAL) {
		failures.push_back(StringUtil::Format("Expected type \"DECIMAL\" but found type \"%s\"", source_type));
		Fail();
	}
	auto source_scale = DecimalType::GetScale(source_type);
	auto source_precision = DecimalType::GetWidth(source_type);
	auto target_scale = DecimalType::GetScale(type);
	auto target_precision = DecimalType::GetWidth(type);

	if (source_scale > target_scale || source_precision > target_precision) {
		failures.push_back(StringUtil::Format("Incompatible decimal precision/scale - found precision %d, scale %d - "
		                                      "but table is defined with precision %d, scale %d",
		                                      source_precision, source_scale, target_precision, target_scale));
		Fail();
	}
}

void DuckLakeParquetTypeChecker::CheckMatchingType() {
	if (type.IsJSONType()) {
		if (!source_type.IsJSONType()) {
			failures.push_back(StringUtil::Format("Expected type \"JSON\" but found type \"%s\"", source_type));
			Fail();
		}
		return;
	}

	if (DuckLakeTypes::IsGeoType(type)) {
		if (!DuckLakeTypes::IsGeoType(source_type)) {
			failures.push_back(StringUtil::Format(
			    "Expected type \"GEOMETRY\" but found type \"%s\". Is this a GeoParquet v1.*.* file? DuckLake only "
			    "supports GEOMETRY types stored in native Parquet(V3) format, not GeoParquet(v1.*.*)",
			    source_type));
			Fail();
		}
		return;
	}

	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		CheckSignedInteger();
		break;
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		CheckUnsignedInteger();
		break;
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		CheckFloatingPoints();
		break;
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		if (source_type.id() != type.id()) {
			failures.push_back(StringUtil::Format("Expected type \"%s\" but found type \"%s\"", type.ToString(),
			                                      source_type.ToString()));
			Fail();
		}
		break;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		CheckTimestamp();
		break;
	case LogicalTypeId::DECIMAL:
		CheckDecimal();
		break;
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP_TZ:
	default:
		// by default just verify that the type matches exactly
		if (!CheckType(type)) {
			Fail();
		}
		break;
	}
}

unique_ptr<DuckLakeNameMapEntry> DuckLakeFileProcessor::MapColumn(ParquetFileMetadata &file_metadata,
                                                                  ParquetColumn &column,
                                                                  const DuckLakeFieldId &field_id, string prefix) {
	// check if types of the columns are compatible
	DuckLakeParquetTypeChecker type_checker(table, file_metadata, field_id.Type(), column, prefix);
	type_checker.CheckMatchingType();

	if (!prefix.empty()) {
		prefix += ".";
	}
	prefix += column.name;

	auto map_entry = make_uniq<DuckLakeNameMapEntry>();
	map_entry->source_name = column.name;
	map_entry->target_field_id = field_id.GetFieldIndex();

	// Store the mapping from column to field for later statistics processing
	file_metadata.column_id_to_field_map.emplace(column.column_id,
	                                             make_pair(field_id.GetFieldIndex(), field_id.Type()));

	// recursively remap children (if any)
	if (field_id.HasChildren()) {
		auto &field_children = field_id.Children();
		switch (field_id.Type().id()) {
		case LogicalTypeId::STRUCT:
			map_entry->child_entries = MapColumns(file_metadata, column.child_columns, field_id.Children(), prefix);
			break;
		case LogicalTypeId::LIST:
			// for lists we don't need to do any name mapping - the child element always maps to each other
			// (1) Parquet has an extra element in between the list and its child ("REPEATED") - strip it
			// (2) Parquet has a different convention on how to name list children - rename them to "list" here
			column.child_columns[0]->child_columns[0]->name = "list";
			map_entry->child_entries.push_back(
			    MapColumn(file_metadata, *column.child_columns[0]->child_columns[0], *field_children[0], prefix));
			break;
		case LogicalTypeId::MAP:
			// for maps we don't need to do any name mapping - the child elements are always key/value
			// (1) Parquet has an extra element in between the list and its child ("REPEATED") - strip it
			map_entry->child_entries =
			    MapColumns(file_metadata, column.child_columns[0]->child_columns, field_id.Children(), prefix);
			break;
		default:
			throw InvalidInputException("Unsupported nested type %s for add files", field_id.Type());
		}
	}

	return map_entry;
}

static bool SupportsHivePartitioning(const LogicalType &type) {
	if (type.IsNested()) {
		return false;
	}
	return true;
}

unique_ptr<DuckLakeNameMapEntry> DuckLakeFileProcessor::MapHiveColumn(ParquetFileMetadata &file_metadata,
                                                                      const DuckLakeFieldId &field_id,
                                                                      const Value &hive_value) {
	auto &target_type = field_id.Type();
	auto target_field_id = field_id.GetFieldIndex();

	if (!SupportsHivePartitioning(target_type)) {
		throw InvalidInputException("Type \"%s\" is not supported for hive partitioning", target_type);
	}

	string error;
	Value cast_result;
	if (!hive_value.DefaultTryCastAs(target_type, cast_result, &error)) {
		throw InvalidInputException("Column \"%s\" exists as a hive partition with value \"%s\", but this value cannot "
		                            "be cast to the column type \"%s\"",
		                            field_id.Name(), hive_value.ToString(), field_id.Type());
	}

	// Store the hive partition information for later statistics processing
	file_metadata.hive_partition_values.emplace_back(HivePartition {target_field_id, field_id.Type(), hive_value});

	// return the map - the name is empty on purpose to signal this comes from a partition
	auto result = make_uniq<DuckLakeNameMapEntry>();
	result->source_name = field_id.Name();
	result->target_field_id = target_field_id;
	result->hive_partition = true;
	return result;
}

void DuckLakeFileProcessor::MapColumnStats(ParquetFileMetadata &file_metadata, DuckLakeDataFile &result) {
	// Process statistics for regular parquet columns
	for (auto &entry : file_metadata.column_id_to_field_map) {
		auto column_id = entry.first;
		const auto &column_entry = file_metadata.column_id_map.find(column_id);
		if (column_entry == file_metadata.column_id_map.end()) {
			// Column not found because it's either not mapped to any table column or it's a nested column
			continue;
		}
		auto &column = column_entry->second.get();
		auto field_index = entry.second.first;

		if (!column.column_stats.empty()) {
			auto base_stats = column.column_stats[0];
			for (idx_t i = 1; i < column.column_stats.size(); i++) {
				base_stats.MergeStats(column.column_stats[i]);
			}
			result.column_stats.emplace(field_index, std::move(base_stats));
		}
	}

	// Process statistics for hive partition columns
	for (auto &entry : file_metadata.hive_partition_values) {
		auto field_index = entry.field_index;
		auto &field_type = entry.field_type;
		auto &hive_value = entry.hive_value;

		DuckLakeColumnStats column_stats(field_type);
		column_stats.min = column_stats.max = hive_value.ToString();
		column_stats.has_min = column_stats.has_max = true;
		column_stats.has_null_count = true;

		result.column_stats.emplace(field_index, std::move(column_stats));
	}
}

vector<unique_ptr<DuckLakeNameMapEntry>>
DuckLakeFileProcessor::MapColumns(ParquetFileMetadata &file_metadata,
                                  vector<unique_ptr<ParquetColumn>> &parquet_columns,
                                  const vector<unique_ptr<DuckLakeFieldId>> &field_ids, const string &prefix) {
	// create a top-level map of columns
	case_insensitive_map_t<const_reference<DuckLakeFieldId>> field_id_map;
	for (auto &field_id : field_ids) {
		field_id_map.emplace(field_id->Name(), *field_id);
	}
	vector<unique_ptr<DuckLakeNameMapEntry>> column_maps;
	for (auto &col : parquet_columns) {
		// find the top-level column to map to
		auto entry = field_id_map.find(col->name);
		if (entry == field_id_map.end()) {
			if (ignore_extra_columns) {
				continue;
			}
			throw InvalidInputException("Column \"%s%s\" exists in file \"%s\" but was not found in table \"%s\"\n* "
			                            "Set ignore_extra_columns => true to add the file anyway",
			                            prefix.empty() ? prefix : prefix + ".", col->name, file_metadata.filename,
			                            table.name);
		}
		column_maps.push_back(MapColumn(file_metadata, *col, entry->second.get(), prefix));
		field_id_map.erase(entry);
	}
	for (auto &entry : field_id_map) {
		auto &field_id = entry.second.get();
		// column does not exist in the file - check hive partitions
		auto hive_entry = hive_partitions.find(field_id.Name());
		if (hive_entry != hive_partitions.end()) {
			// the column exists in the hive partitions - check if the type matches
			column_maps.push_back(MapHiveColumn(file_metadata, field_id, Value(hive_entry->second)));
			continue;
		}
		// column does not exist - check if we are ignoring missing columns
		if (!allow_missing) {
			throw InvalidInputException(
			    "Column \"%s%s\" exists in table \"%s\" but was not found in file \"%s\"\n* Set "
			    "allow_missing => true to allow missing fields and columns",
			    prefix.empty() ? prefix : prefix + ".", entry.second.get().Name(), table.name, file_metadata.filename);
		}
	}
	return column_maps;
}

void DuckLakeFileProcessor::MapPartitionColumns(ParquetFileMetadata &file) {
	auto partition_data = table.GetPartitionData();
	if (!partition_data) {
		return;
	}

	const auto &field_data = table.GetFieldData();
	case_insensitive_set_t used_names;

	for (const auto &partition_field : partition_data->fields) {
		if (partition_field.transform.type == DuckLakeTransformType::IDENTITY) {
			// handled by MapColumns via MapHiveColumn
			continue;
		}

		auto field_id = field_data.GetByFieldIndex(partition_field.field_id);
		if (!field_id) {
			continue;
		}

		string partition_key_name =
		    DuckLakePartitionUtils::GetPartitionKeyName(partition_field.transform.type, field_id->Name(), used_names);
		used_names.insert(partition_key_name);

		auto hive_entry = hive_partitions.find(partition_key_name);
		if (hive_entry == hive_partitions.end()) {
			// key not found in the file path
			continue;
		}

		file.hive_partition_values.emplace_back(
		    HivePartition {partition_field.field_id, field_id->Type(), Value(hive_entry->second)});
	}
}

void DuckLakeFileProcessor::DetermineMapping(ParquetFileMetadata &file) {
	if (hive_partitioning != HivePartitioningType::NO) {
		// we are mapping hive partitions - check if there are any hive partitioned columns
		hive_partitions = HivePartitioning::Parse(file.filename);
	}

	MapPartitionColumns(file);

	file.map_entries = MapColumns(file, file.columns, table.GetFieldData().GetFieldIds());
}

DuckLakeDataFile DuckLakeFileProcessor::AddFileToTable(ParquetFileMetadata &file) {
	DuckLakeDataFile result;
	result.file_name = file.filename;
	result.row_count = file.row_count.GetIndex();
	result.file_size_bytes = file.file_size_bytes.GetIndex();
	result.footer_size = file.footer_size.GetIndex();

	auto name_map = make_uniq<DuckLakeNameMap>();
	name_map->table_id = table.GetTableId();
	MapColumnStats(file, result);
	name_map->column_maps = std::move(file.map_entries);

	// we successfully mapped this file - register the name map and refer to it in the file
	result.mapping_id = transaction.AddNameMap(std::move(name_map));

	const auto partition_data = table.GetPartitionData().get();
	if (partition_data) {
		bool invalid_partition = false;
		if (file.hive_partition_values.size() != partition_data->fields.size()) {
			invalid_partition = true;
		} else {
			for (idx_t i = 0; i < partition_data->fields.size(); i++) {
				if (file.hive_partition_values[i].field_index.index != partition_data->fields[i].field_id.index) {
					invalid_partition = true;
					break;
				}
			}
		}
		if (invalid_partition) {
			throw InvalidInputException("File \"%s\" contains an invalid partition value for the table configuration.",
			                            file.filename);
		}
		unordered_map<idx_t, idx_t> field_partition_key_map;
		for (auto &partition_fields : partition_data->fields) {
			field_partition_key_map[partition_fields.field_id.index] = partition_fields.partition_key_index;
		}
		for (auto &hive_partition : file.hive_partition_values) {
			result.partition_values.push_back({field_partition_key_map[hive_partition.field_index.index],
			                                   hive_partition.hive_value.GetValue<string>()});
		}
		result.partition_id = partition_data->partition_id;
	}
	return result;
}

vector<DuckLakeDataFile> DuckLakeFileProcessor::AddFiles(const vector<string> &globs) {
	// fetch the metadata, stats and columns from the various files
	for (auto &glob : globs) {
		// query the parquet_schema to figure out the schema for each of the columns
		ReadParquetSchema(glob);
		// query the parquet_metadata to get the stats for each of the columns
		ReadParquetStats(glob);
		// read parquet file metadata
		ReadParquetFileMetadata(glob);
	}

	// now we have obtained a list of files to add together with the relevant information (statistics, file size, ...)
	// we need to create a mapping from the columns in the file to the columns in the table
	vector<DuckLakeDataFile> written_files;
	for (auto &entry : parquet_files) {
		auto file = AddFileToTable(*entry.second);
		// File being called by 'add files' is not created by ducklake
		file.created_by_ducklake = false;
		if (file.row_count == 0) {
			// skip adding empty files
			continue;
		}
		written_files.push_back(std::move(file));
	}
	return written_files;
}

static void DuckLakeAddDataFilesExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DuckLakeAddDataFilesState>();
	auto &bind_data = data_p.bind_data->Cast<DuckLakeAddDataFilesData>();
	auto &transaction = DuckLakeTransaction::Get(context, bind_data.catalog);

	if (state.finished) {
		return;
	}
	DuckLakeFileProcessor processor(transaction, bind_data);
	auto files_to_add = processor.AddFiles(bind_data.globs);
	// add the files
	transaction.AppendFiles(bind_data.table.GetTableId(), std::move(files_to_add));
	state.finished = true;
}

DuckLakeAddDataFilesFunction::DuckLakeAddDataFilesFunction()
    : TableFunction("ducklake_add_data_files", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    DuckLakeAddDataFilesExecute, DuckLakeAddDataFilesBind, DuckLakeAddDataFilesInit) {
	named_parameters["allow_missing"] = LogicalType::BOOLEAN;
	named_parameters["ignore_extra_columns"] = LogicalType::BOOLEAN;
	named_parameters["hive_partitioning"] = LogicalType::BOOLEAN;
	named_parameters["schema"] = LogicalType::VARCHAR;
}

} // namespace duckdb
