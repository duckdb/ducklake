#include "functions/ducklake_table_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "common/parquet_file_scanner.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_catalog.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/vector.hpp"
#include "storage/ducklake_geo_stats.hpp"
#include <unordered_set>

namespace duckdb {

enum class HivePartitioningType { AUTOMATIC, YES, NO };

struct DuckLakeAddDataFilesData : public TableFunctionData {
	DuckLakeAddDataFilesData(DuckLakeCatalog &catalog, DuckLakeTableEntry &table) : catalog(catalog), table(table) {
	}

	DuckLakeCatalog &catalog;
	DuckLakeTableEntry &table;
	vector<string> globs;
	bool allow_missing = false;
	bool ignore_extra_columns = false;
	HivePartitioningType hive_partitioning = HivePartitioningType::AUTOMATIC;
};

static unique_ptr<FunctionData> DuckLakeAddDataFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	catalog.VerifyWritable();
	if (input.inputs[1].IsNull()) {
		throw InvalidInputException("Table name cannot be NULL");
	}
	auto schema_name = DuckLakeTableFunctionUtil::GetStringOption(input, "schema");
	const auto table_name = StringValue::Get(input.inputs[1]);
	auto &table = DuckLakeBaseMetadataFunction::GetTableEntry(context, catalog, schema_name, table_name);
	if (catalog.IsEncrypted()) {
		// every data file of an encrypted catalog is read with its own key, which an added file does not have
		throw NotImplementedException("ducklake_add_data_files is not supported for an encrypted DuckLake catalog");
	}

	auto result = make_uniq<DuckLakeAddDataFilesData>(catalog, table);
	auto &file_list = input.inputs[2];
	if (file_list.IsNull()) {
		throw InvalidInputException("File list cannot be NULL");
	}
	if (file_list.type() == LogicalType::VARCHAR) {
		result->globs.push_back(StringValue::Get(file_list));
	} else if (file_list.type() == LogicalType::LIST(LogicalType::VARCHAR)) {
		auto paths = ListValue::GetChildren(file_list);
		for (const auto &path : paths) {
			if (path.IsNull()) {
				throw InvalidInputException("File list cannot contain NULL");
			}
			result->globs.push_back(StringValue::Get(path));
		}
	} else {
		throw InvalidInputException("File list must be a string or a list of strings");
	}
	Value option;
	if (DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "allow_missing", "boolean", option)) {
		result->allow_missing = BooleanValue::Get(option);
	}
	if (DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "ignore_extra_columns", "boolean", option)) {
		result->ignore_extra_columns = BooleanValue::Get(option);
	}
	if (DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "hive_partitioning", "boolean", option)) {
		result->hive_partitioning = BooleanValue::Get(option) ? HivePartitioningType::YES : HivePartitioningType::NO;
	}

	names.emplace_back("filename");
	return_types.emplace_back(LogicalType::VARCHAR);
	return std::move(result);
}

struct ParquetColumn {
	idx_t column_id;
	string name;
	//! The name in the Parquet schema, which the mapping can rename
	string parquet_name;
	string converted_type;
	optional_idx field_id;
	string logical_type;
	bool repeated = false;
	//! The type that the Parquet reader reads the column as
	LogicalType duckdb_type;
	//! Set when the field mapping is made - see the skip_stats_columns option
	bool skip_bounds = false;
	//! Set when the field mapping is made if the bounds are in a type that the reader casts to the field type
	LogicalType bounds_type;
	//! The dotted path of the column, set when the field mapping is made
	string path;
	//! A root group that the Parquet reader reads as VARIANT
	bool is_variant = false;
	vector<DuckLakeColumnStats> column_stats;

	vector<unique_ptr<ParquetColumn>> child_columns;
};

struct HivePartition {
	FieldIndex field_index;
	LogicalType field_type;
	Value hive_value;
	DuckLakeTransform transform;
	optional_idx partition_key_index;
};

static bool IsValidTransformedHivePartitionValue(const HivePartition &hive_partition,
                                                 const DuckLakePartitionField &partition_field) {
	if (partition_field.transform.type != DuckLakeTransformType::BUCKET) {
		return true;
	}
	if (hive_partition.hive_value.IsNull()) {
		return true;
	}
	auto bucket_value = hive_partition.hive_value.GetValue<int32_t>();
	if (bucket_value < 0) {
		return false;
	}
	return NumericCast<idx_t>(bucket_value) < partition_field.transform.bucket_count;
}

struct ParquetFileMetadata {
	string filepath;
	vector<unique_ptr<ParquetColumn>> columns;
	unordered_map<idx_t, reference<ParquetColumn>> column_id_map;
	optional_idx row_count;
	optional_idx file_size_bytes;
	optional_idx footer_size;
	optional_idx row_group_count;

	// Store the column mapping entries once they are computed
	vector<unique_ptr<DuckLakeNameMapEntry>> map_entries;
	// Map from parquet column to the corresponding field ID and type
	unordered_map<idx_t, pair<FieldIndex, LogicalType>> column_id_to_field_map;
	// Map from field ID to hive partition statistics (for partition columns)
	vector<HivePartition> hive_partition_values;
	// Columns absent from the file and the value they read as
	vector<DuckLakeMissingField> missing_columns;
	// VARIANT root columns, by field index
	vector<pair<FieldIndex, reference<ParquetColumn>>> variant_columns;
};

struct DuckLakeFileProcessor {
public:
	DuckLakeFileProcessor(DuckLakeTransaction &transaction, ClientContext &context,
	                      const DuckLakeAddDataFilesData &bind_data)
	    : transaction(transaction), context(context), table(bind_data.table), allow_missing(bind_data.allow_missing),
	      ignore_extra_columns(bind_data.ignore_extra_columns), hive_partitioning(bind_data.hive_partitioning),
	      skipped_fields(bind_data.table.GetSkippedStatsFields()) {
		// the constraint is only enforced for the root columns, like it is for an insert
		auto not_null_fields = table.GetNotNullFields();
		for (auto &field_id : table.GetFieldData().GetFieldIds()) {
			if (not_null_fields.count(field_id->Name())) {
				not_null_columns.emplace(field_id->GetFieldIndex().index, field_id->Name());
			}
		}
		auto partition_data = table.GetPartitionData();
		if (partition_data) {
			partition_key_names = DuckLakePartitionUtils::GetPartitionKeyNames(*partition_data, table.GetFieldData());
			for (idx_t field_idx = 0; field_idx < partition_data->fields.size(); field_idx++) {
				auto &partition_field = partition_data->fields[field_idx];
				if (partition_field.transform.type == DuckLakeTransformType::IDENTITY) {
					identity_hive_keys.emplace(partition_field.field_id.index, partition_key_names[field_idx]);
				}
			}
		}
	}

	vector<DuckLakeDataFile> AddFiles(const vector<string> &globs);

private:
	void ReadParquetFullMetadata(const string &glob, vector<DuckLakeDataFile> &result);
	DuckLakeDataFile AddFileToTable(ParquetFileMetadata &file);
	void CheckNotNullStats(FieldIndex field_index, const DuckLakeColumnStats &stats) const;
	void CheckNotNullValues(const ParquetFileMetadata &file_metadata, const ParquetColumn &column,
	                        FieldIndex field_index) const;
	unique_ptr<DuckLakeNameMapEntry> MapColumn(ParquetFileMetadata &file_metadata, ParquetColumn &column,
	                                           const DuckLakeFieldId &field_id, string prefix, bool repeated);
	vector<unique_ptr<DuckLakeNameMapEntry>> MapColumns(ParquetFileMetadata &file,
	                                                    vector<unique_ptr<ParquetColumn>> &parquet_columns,
	                                                    const vector<unique_ptr<DuckLakeFieldId>> &field_ids,
	                                                    const string &prefix = string(), bool repeated = false);
	void CollectLiveFieldIds(const vector<unique_ptr<DuckLakeFieldId>> &field_ids, unordered_set<idx_t> &result);
	void ValidateParquetFieldIds(const ParquetFileMetadata &file, const vector<unique_ptr<ParquetColumn>> &columns,
	                             const unordered_set<idx_t> &live_field_ids, bool foreign_field_ids,
	                             const string &prefix = string());
	void MapColumnStats(ParquetFileMetadata &file_metadata, DuckLakeDataFile &result);
	DuckLakeColumnStats ConstantColumnStats(const ParquetFileMetadata &file_metadata, FieldIndex field_index,
	                                        const LogicalType &field_type, const Value &value) const;
	unique_ptr<DuckLakeNameMapEntry> MapHiveColumn(ParquetFileMetadata &file_metadata, const DuckLakeFieldId &field_id,
	                                               const string &hive_key, const Value &hive_value);
	bool TryMapHiveColumn(ParquetFileMetadata &file_metadata, const string &prefix, const string &name,
	                      const DuckLakeFieldId &field_id, vector<unique_ptr<DuckLakeNameMapEntry>> &column_maps);
	void DetermineMapping(ParquetFileMetadata &file);
	void MapPartitionColumns(ParquetFileMetadata &file);
	void WrapRepeatedColumn(ParquetFileMetadata &file_metadata, ParquetColumn &column);

	void CheckMatchingType(const LogicalType &type, ParquetColumn &column);

private:
	DuckLakeTransaction &transaction;
	ClientContext &context;
	DuckLakeTableEntry &table;
	bool allow_missing;
	bool ignore_extra_columns;
	map<string, string> hive_partitions;
	HivePartitioningType hive_partitioning;
	unordered_set<string> processed_files;
	unordered_set<idx_t> skipped_fields;
	//! The types bound from the DuckDB type strings of the files
	unordered_map<string, LogicalType> bound_types;
	//! The root columns of the table that do not allow NULL values, by field index
	unordered_map<idx_t, string> not_null_columns;
	vector<string> partition_key_names;
	//! The hive keys of the identity partitions by field index
	unordered_map<idx_t, string> identity_hive_keys;
};

static string GetStringOrEmpty(const VectorIterator<string_t>::ValueEntry &entry) {
	return entry.IsValid() ? entry.GetValue().GetString() : string();
}

static optional_idx GetOptionalIndex(const VectorIterator<int64_t>::ValueEntry &entry) {
	return entry.IsValid() ? optional_idx(entry.GetValue()) : optional_idx();
}

// Negative counts signal a parquet reader underflow
static bool TryGetCount(const VectorIterator<int64_t>::ValueEntry &entry, idx_t &result) {
	return entry.IsValid() && TryCast::Operation<int64_t, idx_t>(entry.GetValue(), result);
}

static InvalidInputException MappingError(const string &column_name, const string &file_path,
                                          const DuckLakeTableEntry &table, const string &failures) {
	return InvalidInputException("Failed to map column \"%s\" from file \"%s\" to the column in table \"%s\"\n* %s",
	                             column_name, file_path, table.name.GetIdentifierName(), failures);
}

void DuckLakeFileProcessor::ReadParquetFullMetadata(const string &glob, vector<DuckLakeDataFile> &written_files) {
	auto result = transaction.ExecuteRaw(StringUtil::Format(R"(
SELECT
    list_transform(parquet_file_metadata, lambda x: struct_pack(
        file_name := x.file_name,
        num_rows := x.num_rows,
        file_size_bytes := x.file_size_bytes,
        footer_size := x.footer_size,
        num_row_groups := x.num_row_groups
    )) AS parquet_file_metadata,
    list_transform(parquet_metadata, lambda x: struct_pack(
        column_id := x.column_id,
        stats_min := COALESCE(x.stats_min, x.stats_min_value),
        stats_max := COALESCE(x.stats_max, x.stats_max_value),
        stats_null_count := x.stats_null_count,
		stats_num_values := x.num_values,
        total_compressed_size := x.total_compressed_size,
        geo_bbox := x.geo_bbox,
        geo_types := x.geo_types,
        min_is_exact := x.min_is_exact AND (x.stats_min IS NULL OR x.stats_min = x.stats_min_value),
        max_is_exact := x.max_is_exact AND (x.stats_max IS NULL OR x.stats_max = x.stats_max_value)
    )) AS parquet_metadata,
    list_transform(parquet_schema, lambda x: struct_pack(
        "name" := x."name",
        num_children := x.num_children,
        converted_type := x.converted_type,
        field_id := x.field_id,
        logical_type := x.logical_type,
        repetition_type := x.repetition_type,
        duckdb_type := x.duckdb_type
    )) AS parquet_schema
FROM parquet_full_metadata(%s)
)",
	                                                        SQLString(glob)));
	result->ThrowIfError("Failed to add data files to DuckLake: ");

	using ParquetFileRow = VectorStructType<string_t, int64_t, uint64_t, uint64_t, int64_t>;
	using ParquetBBoxRow = VectorStructType<double, double, double, double, double, double, double, double>;
	using ParquetStatsRow = VectorStructType<int64_t, string_t, string_t, int64_t, int64_t, int64_t, ParquetBBoxRow,
	                                         VectorListType<string_t>, bool, bool>;
	using ParquetSchemaRow = VectorStructType<string_t, int64_t, string_t, int64_t, string_t, string_t, string_t>;
	for (auto &row : *result) {
		auto &chunk = row.GetChunk();
		auto row_idx = row.GetRowInChunk();
		auto file_iter = chunk.data[0].Values<VectorListType<ParquetFileRow>>();
		auto stats_iter = chunk.data[1].Values<VectorListType<ParquetStatsRow>>();
		auto schema_iter = chunk.data[2].Values<VectorListType<ParquetSchemaRow>>();
		auto file_entry = file_iter[row_idx].GetChildValue(0);
		auto filepath = file_entry.GetChildValue<0>().GetValue().GetString();

		// Use canonicalize path to detect duplicate files
		auto &fs = FileSystem::GetFileSystem(context);
		auto canonical_filepath = fs.CanonicalizePath(filepath);

		// Check if we've already processed this file (can happen with overlapping globs)
		if (processed_files.count(canonical_filepath)) {
			// File already processed in a previous glob, skip
			continue;
		}
		processed_files.insert(canonical_filepath);

		// Keep paths inside the DuckLake data directory in the configured path namespace. Canonicalization can rewrite
		// a symlinked prefix (for example /tmp to /private/tmp), while orphan cleanup scans the configured data path.
		// The canonical path remains the deduplication key, but the rebased path is persisted.
		auto persisted_filepath = canonical_filepath;
		auto &data_path = transaction.GetCatalog().DataPath();
		if (!data_path.empty()) {
			auto canonical_data_path = fs.CanonicalizePath(data_path);
			auto path_separator = fs.PathSeparator(data_path);
			if (!StringUtil::EndsWith(canonical_data_path, path_separator)) {
				canonical_data_path += path_separator;
			}
			if (StringUtil::StartsWith(canonical_filepath, canonical_data_path)) {
				persisted_filepath = data_path + canonical_filepath.substr(canonical_data_path.size());
			}
		}

		ParquetFileMetadata file;
		file.filepath = std::move(persisted_filepath);
		file.row_count = file_entry.GetChildValue<1>().GetValue();
		file.file_size_bytes = file_entry.GetChildValue<2>().GetValue();
		file.footer_size = file_entry.GetChildValue<3>().GetValue();
		file.row_group_count = NumericCast<idx_t>(file_entry.GetChildValue<4>().GetValue());

		bool saw_root = false;
		vector<idx_t> child_counts;
		idx_t next_column_id = 0;
		vector<ParquetColumn *> column_stack;
		for (auto schema_entry : schema_iter[row_idx].GetChildValues()) {
			auto num_children = schema_entry.GetChildValue<1>();
			idx_t child_count = num_children.IsValid() ? num_children.GetValue() : 0;

			if (!saw_root) {
				// parquet_full_metadata emits the synthetic root node as the first entry per file.
				saw_root = true;
				child_counts.push_back(child_count);
				continue;
			}
			if (child_counts.empty()) {
				throw InvalidInputException("child_counts provided by parquet_schema are unaligned");
			}

			auto column = make_uniq<ParquetColumn>();
			column->name = schema_entry.GetChildValue<0>().GetValue().GetString();
			column->parquet_name = column->name;
			column->converted_type = GetStringOrEmpty(schema_entry.GetChildValue<2>());
			column->field_id = GetOptionalIndex(schema_entry.GetChildValue<3>());
			column->logical_type = GetStringOrEmpty(schema_entry.GetChildValue<4>());
			column->repeated = GetStringOrEmpty(schema_entry.GetChildValue<5>()) == "REPEATED";
			auto duckdb_type = GetStringOrEmpty(schema_entry.GetChildValue<6>());
			if (!duckdb_type.empty()) {
				auto entry = bound_types.find(duckdb_type);
				if (entry == bound_types.end()) {
					auto bound_type = DuckLakeTypes::FromDuckDBString(duckdb_type, context);
					entry = bound_types.emplace(duckdb_type, std::move(bound_type)).first;
				}
				column->duckdb_type = entry->second;
			}
			column->is_variant = column_stack.empty() && column->duckdb_type.id() == LogicalTypeId::VARIANT;

			if (child_count == 0) {
				column->column_id = next_column_id++;
			} else {
				column->column_id = DConstants::INVALID_INDEX;
			}

			ParquetColumn *column_ptr = nullptr;
			if (column_stack.empty()) {
				file.columns.push_back(std::move(column));
				column_ptr = file.columns.back().get();
			} else {
				column_stack.back()->child_columns.push_back(std::move(column));
				column_ptr = column_stack.back()->child_columns.back().get();
			}

			if (column_ptr->column_id != DConstants::INVALID_INDEX) {
				file.column_id_map.emplace(column_ptr->column_id, reference<ParquetColumn>(*column_ptr));
			}

			child_counts.back()--;
			if (child_counts.back() == 0) {
				if (!column_stack.empty()) {
					column_stack.pop_back();
				}
				child_counts.pop_back();
			}
			if (child_count > 0) {
				column_stack.push_back(column_ptr);
				child_counts.push_back(child_count);
			}
		}

		DetermineMapping(file);

		for (auto stats_entry : stats_iter[row_idx].GetChildValues()) {
			auto column_id_entry = stats_entry.GetChildValue<0>();
			if (!column_id_entry.IsValid()) {
				// LCOV_EXCL_START
				continue;
				// LCOV_EXCL_STOP
			}
			auto column_id = column_id_entry.GetValue();
			auto column_entry = file.column_id_map.find(column_id);
			if (column_entry == file.column_id_map.end()) {
				throw InvalidInputException("Column id not found in Parquet map?");
			}
			const auto &column_field_entry = file.column_id_to_field_map.find(column_id);
			if (column_field_entry == file.column_id_to_field_map.end()) {
				continue;
			}

			auto &column = column_entry->second.get();
			auto &column_field = column_field_entry->second;
			DuckLakeColumnStats stats(column_field.second);
			// a skipped column reads its bounds only to check that they fit the table type
			auto check_bounds = column.bounds_type.id() != LogicalTypeId::INVALID;
			auto read_bounds = !column.skip_bounds || check_bounds;
			auto stats_min = stats_entry.GetChildValue<1>();
			if (read_bounds && stats_min.IsValid()) {
				stats.has_min = true;
				stats.min = stats_min.GetValue().GetString();
				// files without the footer flag conservatively count as truncated
				auto min_is_exact = stats_entry.GetChildValue<8>();
				stats.min_is_exact = min_is_exact.IsValid() && min_is_exact.GetValue();
			}

			auto stats_max = stats_entry.GetChildValue<2>();
			if (read_bounds && stats_max.IsValid()) {
				stats.has_max = true;
				stats.max = stats_max.GetValue().GetString();
				auto max_is_exact = stats_entry.GetChildValue<9>();
				stats.max_is_exact = max_is_exact.IsValid() && max_is_exact.GetValue();
			}
			if (check_bounds) {
				auto dropped_value = stats.CastBoundsFrom(column.bounds_type);
				if (dropped_value) {
					throw MappingError(column.path, file.filepath, table,
					                   StringUtil::Format("The value %s of type %s in the file does not fit type %s",
					                                      *dropped_value, column.bounds_type, stats.type));
				}
			}
			if (column.skip_bounds) {
				stats.ClearBounds();
			}

			stats.has_null_count = TryGetCount(stats_entry.GetChildValue<3>(), stats.null_count);
			stats.has_num_values = TryGetCount(stats_entry.GetChildValue<4>(), stats.num_values);

			auto compressed_size = stats_entry.GetChildValue<5>();
			if (compressed_size.IsValid()) {
				stats.column_size_bytes = compressed_size.GetValue();
			}

			if (stats.extra_stats && stats.extra_stats->GetStatsType() == DuckLakeExtraStatsType::GEOMETRY) {
				auto &geo_stats = stats.extra_stats->Cast<DuckLakeColumnGeoStats>();
				auto geo_bbox = stats_entry.GetChildValue<6>();
				if (geo_bbox.IsValid()) {
					auto &extent = geo_stats.extent;
					double *bounds[] = {&extent.x_min, &extent.x_max, &extent.y_min, &extent.y_max,
					                    &extent.z_min, &extent.z_max, &extent.m_min, &extent.m_max};
					idx_t bound_idx = 0;
					geo_bbox.ForEach([&](const VectorIterator<double>::ValueEntry &bound) {
						if (bound.IsValid()) {
							*bounds[bound_idx] = bound.GetValue();
						}
						bound_idx++;
					});
				} else {
					geo_stats.SetUnknownExtent();
				}
				auto geo_types = stats_entry.GetChildValue<7>();
				if (geo_types.IsValid()) {
					for (auto geo_type : geo_types.GetChildValues()) {
						geo_stats.geo_types.insert(geo_type.GetValue().GetString());
					}
				}
				// an empty list of types means that they are unknown
				if (geo_stats.geo_types.empty()) {
					geo_stats.SetUnknownTypes();
				}
			}

			column.column_stats.push_back(std::move(stats));
		}
		auto data_file = AddFileToTable(file);
		data_file.created_by_ducklake = false;
		if (data_file.row_count > 0) {
			written_files.push_back(std::move(data_file));
		}
	}
}

class DuckLakeParquetTypeChecker {
public:
	DuckLakeParquetTypeChecker(DuckLakeTableEntry &table, ParquetFileMetadata &file_metadata, const LogicalType &type,
	                           ParquetColumn &column_p, const string &prefix_p)
	    : table(table), file_metadata(file_metadata), source_type(column_p.duckdb_type), type(type), column(column_p),
	      prefix(prefix_p) {
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
	void CheckTimestampWithTimeZone();
	void CheckDecimal();

	//! Called when a check fails
	void Fail();

private:
	bool CheckType(const LogicalType &type);
	//! Verify type is equivalent to one of the accepted types
	bool CheckTypes(const vector<LogicalType> &types);

private:
	vector<string> failures;
};

static string FormatExpectedError(const vector<LogicalType> &expected) {
	auto error = StringUtil::ToString(expected, ", ");
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
	auto column_name = prefix.empty() ? column.name : prefix + "." + column.name;
	throw MappingError(column_name, file_metadata.filepath, table, StringUtil::Join(failures, "\n* "));
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

void DuckLakeParquetTypeChecker::CheckTimestampWithTimeZone() {
	if (!CheckTypes({LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ_NS})) {
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
	if (source_type.id() == LogicalTypeId::VARIANT && !column.is_variant) {
		failures.push_back(StringUtil::Format(
		    "Expected type \"%s\" but found a nested VARIANT column, which DuckLake does not add", type));
		Fail();
	}
	if (type.IsJSONType()) {
		if (!source_type.IsJSONType()) {
			failures.push_back(StringUtil::Format("Expected type \"JSON\" but found type \"%s\"", source_type));
			Fail();
		}
		return;
	}

	if (type.id() == LogicalTypeId::GEOMETRY) {
		if (StringUtil::StartsWith(column.logical_type, "Geography")) {
			// files of GEOGRAPHY columns carry no geometry types, which DuckLake would read as no geometries
			failures.push_back("Expected type \"GEOMETRY\" but found a GEOGRAPHY column, which DuckLake does not "
			                   "support yet");
			Fail();
		}
		// the reader also reads the BLOB columns of GeoParquet v1 files as GEOMETRY
		if (source_type.id() != LogicalTypeId::GEOMETRY || !StringUtil::StartsWith(column.logical_type, "Geometry")) {
			auto found_type = source_type.id() == LogicalTypeId::GEOMETRY ? LogicalType::BLOB : source_type;
			failures.push_back(StringUtil::Format(
			    "Expected type \"GEOMETRY\" but found type \"%s\". Is this a GeoParquet v1.*.* file? DuckLake only "
			    "supports GEOMETRY types stored in native Parquet(V3) format, not GeoParquet(v1.*.*)",
			    found_type));
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
	case LogicalTypeId::TIMESTAMP_TZ:
		CheckTimestampWithTimeZone();
		break;
	case LogicalTypeId::DECIMAL:
		CheckDecimal();
		break;
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	default:
		// by default just verify that the type matches exactly
		if (!CheckType(type)) {
			Fail();
		}
		break;
	}
}

//! The reader reads a repeated group of a single column without a LIST annotation as that column
static void CollapseRepeatedGroup(unique_ptr<ParquetColumn> &element) {
	if (element->repeated && element->converted_type.empty() && element->child_columns.size() == 1) {
		auto child = std::move(element->child_columns[0]);
		child->name = element->name;
		element = std::move(child);
	}
}

//! A repeated column without a LIST annotation is the only element of its own list
void DuckLakeFileProcessor::WrapRepeatedColumn(ParquetFileMetadata &file_metadata, ParquetColumn &column) {
	auto element = make_uniq<ParquetColumn>();
	element->column_id = column.column_id;
	element->name = column.parquet_name;
	element->parquet_name = column.parquet_name;
	element->converted_type = column.converted_type;
	element->field_id = column.field_id;
	element->repeated = column.repeated;
	element->child_columns = std::move(column.child_columns);
	CollapseRepeatedGroup(element);
	if (element->column_id != DConstants::INVALID_INDEX) {
		file_metadata.column_id_map.find(element->column_id)->second = *element;
	}
	column.column_id = DConstants::INVALID_INDEX;
	column.child_columns.clear();
	column.child_columns.push_back(std::move(element));
}

unique_ptr<DuckLakeNameMapEntry> DuckLakeFileProcessor::MapColumn(ParquetFileMetadata &file_metadata,
                                                                  ParquetColumn &column,
                                                                  const DuckLakeFieldId &field_id, string prefix,
                                                                  bool repeated) {
	// check if types of the columns are compatible
	DuckLakeParquetTypeChecker type_checker(table, file_metadata, field_id.Type(), column, prefix);
	type_checker.CheckMatchingType();
	bool wrapped = field_id.Type().id() == LogicalTypeId::LIST && column.repeated && column.converted_type != "LIST";
	if (wrapped) {
		WrapRepeatedColumn(file_metadata, column);
	}

	if (!prefix.empty()) {
		prefix += ".";
	}
	prefix += column.name;
	column.path = prefix;

	auto map_entry = make_uniq<DuckLakeNameMapEntry>();
	map_entry->source_name = column.name;
	map_entry->target_field_id = field_id.GetFieldIndex();

	// Store the mapping from column to field for later statistics processing
	file_metadata.column_id_to_field_map.emplace(column.column_id,
	                                             make_pair(field_id.GetFieldIndex(), field_id.Type()));
	if (column.is_variant) {
		file_metadata.variant_columns.emplace_back(field_id.GetFieldIndex(), column);
	}
	column.skip_bounds = skipped_fields.count(field_id.GetFieldIndex().index) > 0;
	auto &field_type = field_id.Type();
	auto bounds_survive = DuckLakeColumnStats::BoundsSurviveTypePromotion(column.duckdb_type, field_type);
	if (bounds_survive && field_type.id() == LogicalTypeId::DECIMAL) {
		// a decimal with fewer integer digits holds only some values of the file
		bounds_survive = BoundCastExpression::CastIsInvertible(column.duckdb_type, field_type);
	}
	if (!field_id.HasChildren() && column.duckdb_type != field_type && !bounds_survive) {
		column.bounds_type = column.duckdb_type;
	}

	// recursively remap children (if any)
	if (field_id.HasChildren()) {
		auto &field_children = field_id.Children();
		switch (field_id.Type().id()) {
		case LogicalTypeId::STRUCT:
			map_entry->child_entries =
			    MapColumns(file_metadata, column.child_columns, field_id.Children(), prefix, repeated);
			break;
		case LogicalTypeId::LIST: {
			// the element is the repeated child, or its only column, as the reader reads them
			auto &element = column.child_columns[0];
			if (!wrapped) {
				CollapseRepeatedGroup(element);
			}
			element->duckdb_type = ListType::GetChildType(column.duckdb_type);
			map_entry->child_entries.push_back(MapColumn(file_metadata, *element, *field_children[0], prefix, true));
			break;
		}
		case LogicalTypeId::MAP: {
			// skip the repeated element between the map and its children, the first of which is the key
			auto &key_value = column.child_columns[0]->child_columns;
			if (key_value.size() == 2 && StringUtil::CIEquals(key_value[0]->name, field_children[1]->Name()) &&
			    StringUtil::CIEquals(key_value[1]->name, field_children[0]->Name())) {
				key_value[0]->name = field_children[0]->Name();
				key_value[1]->name = field_children[1]->Name();
			}
			map_entry->child_entries = MapColumns(file_metadata, key_value, field_id.Children(), prefix, true);
			break;
		}
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

static optional_idx GetIdentityPartitionKeyIndex(optional_ptr<DuckLakePartition> partition_data,
                                                 FieldIndex field_index) {
	if (!partition_data) {
		return optional_idx();
	}
	for (auto &field : partition_data->fields) {
		if (field.field_id == field_index && field.transform.type == DuckLakeTransformType::IDENTITY) {
			return optional_idx(field.partition_key_index);
		}
	}
	return optional_idx();
}

unique_ptr<DuckLakeNameMapEntry> DuckLakeFileProcessor::MapHiveColumn(ParquetFileMetadata &file_metadata,
                                                                      const DuckLakeFieldId &field_id,
                                                                      const string &hive_key, const Value &hive_value) {
	auto &target_type = field_id.Type();
	auto target_field_id = field_id.GetFieldIndex();

	if (!SupportsHivePartitioning(target_type)) {
		throw InvalidInputException("Type \"%s\" is not supported for hive partitioning", target_type);
	}

	string error;
	auto cast_result = hive_value.DefaultTryCastAs(target_type, &error);
	if (!cast_result) {
		// LCOV_EXCL_START
		throw InvalidInputException("Column \"%s\" exists as a hive partition with value \"%s\", but this value cannot "
		                            "be cast to the column type \"%s\"",
		                            field_id.Name(), hive_value.ToString(), field_id.Type());
		// LCOV_EXCL_STOP
	}

	// Store the hive partition information for later statistics processing
	DuckLakeTransform transform;
	transform.type = DuckLakeTransformType::IDENTITY;
	auto partition_key_index = GetIdentityPartitionKeyIndex(table.GetPartitionData(), target_field_id);
	file_metadata.hive_partition_values.emplace_back(
	    HivePartition {target_field_id, field_id.Type(), hive_value, transform, partition_key_index});

	// the reader takes the value of the hive key in the source name
	auto result = make_uniq<DuckLakeNameMapEntry>();
	result->source_name = hive_key;
	result->target_field_id = target_field_id;
	result->hive_partition = true;
	return result;
}

bool DuckLakeFileProcessor::TryMapHiveColumn(ParquetFileMetadata &file_metadata, const string &prefix,
                                             const string &name, const DuckLakeFieldId &field_id,
                                             vector<unique_ptr<DuckLakeNameMapEntry>> &column_maps) {
	// hive partitions only name top-level columns
	if (!prefix.empty()) {
		return false;
	}
	auto identity_key = identity_hive_keys.find(field_id.GetFieldIndex().index);
	auto &hive_key = identity_key == identity_hive_keys.end() ? name : identity_key->second;
	auto hive_entry = hive_partitions.find(hive_key);
	if (hive_entry == hive_partitions.end()) {
		return false;
	}
	auto hive_value = HivePartitioning::GetValue(context, hive_key, hive_entry->second, field_id.Type());
	column_maps.push_back(MapHiveColumn(file_metadata, field_id, hive_key, hive_value));
	return true;
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
			auto &stats_list = column.column_stats;
			for (auto &stats : stats_list) {
				CheckNotNullStats(field_index, stats);
			}
			auto aggregated = stats_list[0];
			for (idx_t i = 1; i < stats_list.size(); i++) {
				aggregated.MergeStats(stats_list[i]);
			}
			if (!aggregated.has_null_count) {
				CheckNotNullValues(file_metadata, column, field_index);
			}
			result.column_stats.emplace(field_index, std::move(aggregated));
		}
	}

	for (auto &entry : file_metadata.variant_columns) {
		CheckNotNullValues(file_metadata, entry.second.get(), entry.first);
		// the shredded statistics of the file are not read, so the table loses its VARIANT bounds
		result.column_stats.emplace(entry.first, DuckLakeColumnStats(LogicalType::VARIANT()));
	}

	// Process statistics for hive partition columns
	for (auto &entry : file_metadata.hive_partition_values) {
		if (entry.transform.type != DuckLakeTransformType::IDENTITY) {
			// Transformed folder values are not source column values, so no statistics from them
			continue;
		}

		auto hive_stats = ConstantColumnStats(file_metadata, entry.field_index, entry.field_type, entry.hive_value);
		CheckNotNullStats(entry.field_index, hive_stats);
		result.column_stats.emplace(entry.field_index, std::move(hive_stats));
	}

	for (auto &missing : file_metadata.missing_columns) {
		missing.AddStats(file_metadata.row_count.GetIndex(), result.column_stats);
		auto missing_stats = result.column_stats.find(missing.field_index);
		if (missing_stats != result.column_stats.end()) {
			CheckNotNullStats(missing.field_index, missing_stats->second);
		}
	}
}

void DuckLakeFileProcessor::CheckNotNullStats(FieldIndex field_index, const DuckLakeColumnStats &stats) const {
	if (!stats.has_null_count || stats.null_count == 0) {
		return;
	}
	auto column_name = not_null_columns.find(field_index.index);
	if (column_name == not_null_columns.end()) {
		return;
	}
	table.ThrowNotNullViolation(column_name->second);
}

void DuckLakeFileProcessor::CheckNotNullValues(const ParquetFileMetadata &file_metadata, const ParquetColumn &column,
                                               FieldIndex field_index) const {
	auto column_name = not_null_columns.find(field_index.index);
	if (column_name == not_null_columns.end()) {
		return;
	}
	// without a null count in the footer the column itself is read
	DuckLakeFileData file;
	file.path = file_metadata.filepath;
	ParquetFileScanner scanner(context, file);
	auto column_idx = scanner.FindColumn(column.name);
	if (!column_idx.IsValid()) {
		throw InternalException("Column \"%s\" not found in file \"%s\"", column.name, file.path);
	}
	scanner.SetColumnIds({column_idx.GetIndex()});
	DataChunk chunk;
	chunk.Initialize(context, {scanner.GetTypes()[column_idx.GetIndex()]});
	while (scanner.Scan(chunk)) {
		if (VectorOperations::HasNull(chunk.data[0])) {
			table.ThrowNotNullViolation(column_name->second);
		}
	}
}

DuckLakeColumnStats DuckLakeFileProcessor::ConstantColumnStats(const ParquetFileMetadata &file_metadata,
                                                               FieldIndex field_index, const LogicalType &field_type,
                                                               const Value &value) const {
	auto column_stats = DuckLakeColumnStats::FromConstant(field_type, value, file_metadata.row_count.GetIndex());
	if (skipped_fields.count(field_index.index)) {
		// a skipped column records counts but not the value
		column_stats.ClearBounds();
	}
	return column_stats;
}

vector<unique_ptr<DuckLakeNameMapEntry>> DuckLakeFileProcessor::MapColumns(
    ParquetFileMetadata &file_metadata, vector<unique_ptr<ParquetColumn>> &parquet_columns,
    const vector<unique_ptr<DuckLakeFieldId>> &field_ids, const string &prefix, bool repeated) {
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
			                            prefix.empty() ? prefix : prefix + ".", col->name, file_metadata.filepath,
			                            table.name.GetIdentifierName());
		}
		if (!TryMapHiveColumn(file_metadata, prefix, col->name, entry->second.get(), column_maps)) {
			column_maps.push_back(MapColumn(file_metadata, *col, entry->second.get(), prefix, repeated));
		}
		field_id_map.erase(entry);
	}
	for (auto &entry : field_id_map) {
		auto &field_id = entry.second.get();
		// column does not exist in the file - check hive partitions
		if (TryMapHiveColumn(file_metadata, prefix, field_id.Name(), field_id, column_maps)) {
			continue;
		}
		// column does not exist - check if we are ignoring missing columns
		if (!allow_missing) {
			throw InvalidInputException(
			    "Column \"%s%s\" exists in table \"%s\" but was not found in file \"%s\"\n* Set "
			    "allow_missing => true to allow missing fields and columns",
			    prefix.empty() ? prefix : prefix + ".", entry.second.get().Name(), table.name.GetIdentifierName(),
			    file_metadata.filepath);
		}
		DuckLakeMissingField::Collect(field_id, field_id.GetColumnData().initial_default.IsNull(), repeated,
		                              file_metadata.missing_columns);
	}
	return column_maps;
}

static bool IsDuckLakeInternalColumn(const string &name) {
	return name == "duckdb_schema" || StringUtil::StartsWith(name, "_ducklake_internal_");
}

void DuckLakeFileProcessor::CollectLiveFieldIds(const vector<unique_ptr<DuckLakeFieldId>> &field_ids,
                                                unordered_set<idx_t> &result) {
	for (auto &field_id : field_ids) {
		result.insert(field_id->GetFieldIndex().index);
		CollectLiveFieldIds(field_id->Children(), result);
	}
}

//! The reader takes these field ids for the row id and snapshot id columns that DuckLake writes
static bool IsRowLineageFieldId(idx_t field_id) {
	return field_id == idx_t(MultiFileReader::ROW_ID_FIELD_ID) ||
	       field_id == idx_t(MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID);
}

static bool HasFieldIdNotGivenOut(const vector<unique_ptr<ParquetColumn>> &columns, idx_t next_column_id) {
	for (auto &column : columns) {
		if (!IsDuckLakeInternalColumn(column->name) && column->field_id.IsValid()) {
			auto field_id = column->field_id.GetIndex();
			if (field_id >= next_column_id && !IsRowLineageFieldId(field_id)) {
				return true;
			}
		}
		if (HasFieldIdNotGivenOut(column->child_columns, next_column_id)) {
			return true;
		}
	}
	return false;
}

//! Whether another writer such as Iceberg wrote the file: it has a field id the table never gave out, and none of the
//! internal columns that DuckLake writes
static bool HasForeignFieldIds(const vector<unique_ptr<ParquetColumn>> &columns, idx_t next_column_id) {
	for (auto &column : columns) {
		if (StringUtil::StartsWith(column->name, "_ducklake_internal_")) {
			return false;
		}
	}
	return HasFieldIdNotGivenOut(columns, next_column_id);
}

void DuckLakeFileProcessor::ValidateParquetFieldIds(const ParquetFileMetadata &file,
                                                    const vector<unique_ptr<ParquetColumn>> &columns,
                                                    const unordered_set<idx_t> &live_field_ids, bool foreign_field_ids,
                                                    const string &prefix) {
	for (auto &column : columns) {
		const auto full_name = prefix.empty() ? column->name : StringUtil::Format("%s.%s", prefix, column->name);
		if (!IsDuckLakeInternalColumn(column->name) && column->field_id.IsValid()) {
			const auto source_field_id = column->field_id.GetIndex();
			// columns map by name, the ids of another writer's file are not DuckLake's
			if (live_field_ids.find(source_field_id) == live_field_ids.end() &&
			    (!foreign_field_ids || IsRowLineageFieldId(source_field_id))) {
				throw InvalidInputException(
				    "Parquet field ID mismatch for column \"%s\" in file \"%s\": field ID %d is not a live field in "
				    "table \"%s\"",
				    full_name, file.filepath, source_field_id, table.name.GetIdentifierName());
			}
		}
		ValidateParquetFieldIds(file, column->child_columns, live_field_ids, foreign_field_ids, full_name);
	}
}

void DuckLakeFileProcessor::MapPartitionColumns(ParquetFileMetadata &file) {
	auto partition_data = table.GetPartitionData();
	if (!partition_data) {
		return;
	}

	const auto &field_data = table.GetFieldData();
	for (idx_t field_idx = 0; field_idx < partition_data->fields.size(); field_idx++) {
		auto &partition_field = partition_data->fields[field_idx];
		if (partition_field.transform.type == DuckLakeTransformType::IDENTITY) {
			// handled by MapColumns via MapHiveColumn
			continue;
		}
		auto field_id = field_data.GetByFieldIndex(partition_field.field_id);
		auto &partition_key_name = partition_key_names[field_idx];
		auto hive_entry = hive_partitions.find(partition_key_name);
		if (hive_entry == hive_partitions.end()) {
			// key not found in the file path
			continue;
		}
		// Get the correct type for the partition key based on the transform
		// For YEAR/MONTH/DAY/HOUR transforms, the type is BIGINT, not the source column type
		auto partition_key_type =
		    DuckLakePartitionUtils::GetPartitionKeyType(partition_field.transform.type, field_id->Type());
		auto hive_value =
		    HivePartitioning::GetValue(context, partition_key_name, hive_entry->second, partition_key_type);
		file.hive_partition_values.emplace_back(HivePartition {partition_field.field_id, partition_key_type, hive_value,
		                                                       partition_field.transform,
		                                                       optional_idx(partition_field.partition_key_index)});
	}
}

void DuckLakeFileProcessor::DetermineMapping(ParquetFileMetadata &file) {
	if (hive_partitioning != HivePartitioningType::NO) {
		// we are mapping hive partitions - check if there are any hive partitioned columns
		hive_partitions = HivePartitioning::Parse(file.filepath);
	}

	MapPartitionColumns(file);

	// Before we map parquet columns to DuckLake columns, we need to validate that the parquet field ids are live.
	unordered_set<idx_t> live_field_ids;
	CollectLiveFieldIds(table.GetFieldData().GetFieldIds(), live_field_ids);
	// a file this table wrote only has ids it gave out, so one with an id of a dropped column cannot be added
	auto foreign_field_ids = HasForeignFieldIds(file.columns, table.GetNextColumnId(transaction));
	ValidateParquetFieldIds(file, file.columns, live_field_ids, foreign_field_ids);

	file.map_entries = MapColumns(file, file.columns, table.GetFieldData().GetFieldIds());
}

DuckLakeDataFile DuckLakeFileProcessor::AddFileToTable(ParquetFileMetadata &file) {
	DuckLakeDataFile result;
	result.file_name = file.filepath;
	result.row_count = file.row_count.GetIndex();
	result.file_size_bytes = file.file_size_bytes.GetIndex();
	result.footer_size = file.footer_size.GetIndex();
	result.row_group_count = file.row_group_count;

	auto name_map = make_uniq<DuckLakeNameMap>();
	name_map->table_id = table.GetTableId();
	MapColumnStats(file, result);
	name_map->column_maps = std::move(file.map_entries);

	// we successfully mapped this file - register the name map and refer to it in the file
	result.mapping_id = transaction.AddNameMap(std::move(name_map));

	const auto partition_data = table.GetPartitionData().get();
	// RESET PARTITIONED BY leaves a spec without fields until its commit, files added after it are not partitioned
	if (partition_data && !partition_data->fields.empty()) {
		bool invalid_partition = false;
		if (file.hive_partition_values.size() != partition_data->fields.size()) {
			invalid_partition = true;
		} else {
			vector<bool> found_partition_keys(partition_data->fields.size(), false);
			for (const auto &hive_partition_value : file.hive_partition_values) {
				if (!hive_partition_value.partition_key_index.IsValid()) {
					invalid_partition = true;
					break;
				}
				auto partition_key_index = hive_partition_value.partition_key_index.GetIndex();
				if (partition_key_index >= partition_data->fields.size() || found_partition_keys[partition_key_index]) {
					// LCOV_EXCL_START
					invalid_partition = true;
					break;
					// LCOV_EXCL_STOP
				}
				optional_ptr<const DuckLakePartitionField> partition_field;
				for (const auto &field : partition_data->fields) {
					if (field.partition_key_index == partition_key_index) {
						partition_field = &field;
						break;
					}
				}
				if (!partition_field || partition_field->field_id != hive_partition_value.field_index ||
				    !(partition_field->transform == hive_partition_value.transform)) {
					// LCOV_EXCL_START
					invalid_partition = true;
					break;
					// LCOV_EXCL_STOP
				}
				if (!IsValidTransformedHivePartitionValue(hive_partition_value, *partition_field)) {
					invalid_partition = true;
					break;
				}
				found_partition_keys[partition_key_index] = true;
			}
		}
		if (invalid_partition) {
			throw InvalidInputException("File \"%s\" contains an invalid partition value for the table configuration.",
			                            file.filepath);
		}
		for (auto &hive_partition : file.hive_partition_values) {
			result.partition_values.push_back(
			    {hive_partition.partition_key_index.GetIndex(), hive_partition.hive_value});
		}
		result.partition_id = partition_data->partition_id;
	}
	return result;
}

vector<DuckLakeDataFile> DuckLakeFileProcessor::AddFiles(const vector<string> &globs) {
	// Process files directly to DuckLakeDataFile format to minimize peak memory usage
	// Each file's intermediate metadata is discarded immediately after processing
	vector<DuckLakeDataFile> written_files;
	for (auto &glob : globs) {
		ReadParquetFullMetadata(glob, written_files);
	}
	return written_files;
}

static void DuckLakeAddDataFilesExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<DuckLakeAddDataFilesData>();
	auto &transaction = DuckLakeTransaction::Get(context, bind_data.catalog);
	DuckLakeFileProcessor processor(transaction, context, bind_data);
	auto files_to_add = processor.AddFiles(bind_data.globs);
	transaction.AppendFiles(bind_data.table.GetTableId(), std::move(files_to_add));
}

TableFunctionSet DuckLakeAddDataFilesFunction::GetFunctions() {
	TableFunctionSet set("ducklake_add_data_files");
	vector<LogicalType> at_types {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)};
	for (auto &type : at_types) {
		TableFunction function("ducklake_add_data_files",
		                       FunctionSignature()
		                           .AddPositionalOnly("catalog", LogicalType::VARCHAR)
		                           .AddPositionalOnly("table_name", LogicalType::VARCHAR)
		                           .AddPositionalOnly(type.id() == LogicalTypeId::LIST ? "paths" : "path", type),
		                       DuckLakeAddDataFilesExecute, DuckLakeAddDataFilesBind);
		function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
			options.Add("allow_missing", LogicalType::BOOLEAN)
			    .Add("ignore_extra_columns", LogicalType::BOOLEAN)
			    .Add("hive_partitioning", LogicalType::BOOLEAN)
			    .Add("schema", LogicalType::VARCHAR);
		});
		set.AddFunction(function);
	}
	return set;
}

} // namespace duckdb
