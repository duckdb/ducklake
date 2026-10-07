#include "functions/ducklake_table_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
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
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/vector.hpp"
#include "storage/ducklake_geo_stats.hpp"
#include <unordered_set>

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
                                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	string schema_name;
	if (input.inputs[1].IsNull()) {
		throw InvalidInputException("Table name cannot be NULL");
	}
	if (input.named_parameters.find("schema") != input.named_parameters.end()) {
		schema_name = StringValue::Get(input.named_parameters["schema"]);
	}
	const auto table_name = StringValue::Get(input.inputs[1]);

	auto entry = catalog.GetEntry<TableCatalogEntry>(
	    context, catalog.ResolveEntryName(context, schema_name, table_name), OnEntryNotFound::THROW_EXCEPTION);
	auto &table = entry->Cast<DuckLakeTableEntry>();

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
			result->globs.push_back(StringValue::Get(path));
		}
	} else {
		throw InvalidInputException("File list must be a string or a list of strings");
	}
	for (auto &entry : input.named_parameters) {
		if (entry.first == "allow_missing") {
			result->allow_missing = BooleanValue::Get(entry.second);
		} else if (entry.first == "ignore_extra_columns") {
			result->ignore_extra_columns = BooleanValue::Get(entry.second);
		} else if (entry.first == "hive_partitioning") {
			result->hive_partitioning =
			    BooleanValue::Get(entry.second) ? HivePartitioningType::YES : HivePartitioningType::NO;
		}
	}

	names.emplace_back("filename");
	return_types.emplace_back(LogicalType::VARCHAR);
	return std::move(result);
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
	//! Set when the field mapping is made - see the skip_stats_columns option
	bool skip_bounds = false;
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

struct MissingColumn {
	FieldIndex field_index;
	LogicalType field_type;
	bool reads_null;
	//! Whether the column is inside a list or map element, so it does not have one value per row
	bool repeated;
};

static void CollectMissingColumns(const DuckLakeFieldId &field_id, bool reads_null, bool repeated,
                                  vector<MissingColumn> &result) {
	if (!field_id.HasChildren()) {
		result.push_back(MissingColumn {field_id.GetFieldIndex(), field_id.Type(), reads_null, repeated});
		return;
	}
	// the fields of a missing parent read as NULL
	for (auto &child : field_id.Children()) {
		CollectMissingColumns(*child, true, repeated, result);
	}
}

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
	vector<MissingColumn> missing_columns;
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
	                             const unordered_set<idx_t> &live_field_ids, const string &prefix = string());
	void MapColumnStats(ParquetFileMetadata &file_metadata, DuckLakeDataFile &result);
	DuckLakeColumnStats ConstantColumnStats(const ParquetFileMetadata &file_metadata, FieldIndex field_index,
	                                        const LogicalType &field_type, const Value &value) const;
	unique_ptr<DuckLakeNameMapEntry> MapHiveColumn(ParquetFileMetadata &file_metadata, const DuckLakeFieldId &field_id,
	                                               const Value &hive_value);
	void DetermineMapping(ParquetFileMetadata &file);
	void MapPartitionColumns(ParquetFileMetadata &file);

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
	//! The root columns of the table that do not allow NULL values, by field index
	unordered_map<idx_t, string> not_null_columns;
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
        "type" := x."type",
        num_children := x.num_children,
        converted_type := x.converted_type,
        "scale" := x."scale",
        "precision" := x."precision",
        field_id := x.field_id,
        logical_type := x.logical_type
    )) AS parquet_schema
FROM parquet_full_metadata(%s)
)",
	                                                        SQLString(glob)));
	result->ThrowIfError("Failed to add data files to DuckLake: ");

	using ParquetFileRow = VectorStructType<string_t, int64_t, uint64_t, uint64_t, int64_t>;
	using ParquetBBoxRow = VectorStructType<double, double, double, double, double, double, double, double>;
	using ParquetStatsRow = VectorStructType<int64_t, string_t, string_t, int64_t, int64_t, int64_t, ParquetBBoxRow,
	                                         VectorListType<string_t>, bool, bool>;
	using ParquetSchemaRow =
	    VectorStructType<string_t, string_t, int64_t, string_t, int64_t, int64_t, int64_t, string_t>;
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
			auto num_children = schema_entry.GetChildValue<2>();
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
			column->type = GetStringOrEmpty(schema_entry.GetChildValue<1>());
			column->converted_type = GetStringOrEmpty(schema_entry.GetChildValue<3>());
			column->scale = GetOptionalIndex(schema_entry.GetChildValue<4>());
			column->precision = GetOptionalIndex(schema_entry.GetChildValue<5>());
			column->field_id = GetOptionalIndex(schema_entry.GetChildValue<6>());
			column->logical_type = GetStringOrEmpty(schema_entry.GetChildValue<7>());

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
				continue;
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
			// min/max are copied out of the footer here, once per row group, so a skipped column
			// never materializes them - the geo bbox below is untouched, as ClearBounds was too
			auto stats_min = stats_entry.GetChildValue<1>();
			if (!column.skip_bounds && stats_min.IsValid()) {
				stats.has_min = true;
				stats.min = stats_min.GetValue().GetString();
				// files without the footer flag conservatively count as truncated
				auto min_is_exact = stats_entry.GetChildValue<8>();
				stats.min_is_exact = min_is_exact.IsValid() && min_is_exact.GetValue();
			}

			auto stats_max = stats_entry.GetChildValue<2>();
			if (!column.skip_bounds && stats_max.IsValid()) {
				stats.has_max = true;
				stats.max = stats_max.GetValue().GetString();
				auto max_is_exact = stats_entry.GetChildValue<9>();
				stats.max_is_exact = max_is_exact.IsValid() && max_is_exact.GetValue();
			}

			stats.has_null_count = TryGetCount(stats_entry.GetChildValue<3>(), stats.null_count);
			stats.has_num_values = TryGetCount(stats_entry.GetChildValue<4>(), stats.num_values);

			auto compressed_size = stats_entry.GetChildValue<5>();
			if (compressed_size.IsValid()) {
				stats.column_size_bytes = compressed_size.GetValue();
			}

			auto geo_bbox = stats_entry.GetChildValue<6>();
			if (geo_bbox.IsValid() && stats.extra_stats) {
				auto &extent = stats.extra_stats->Cast<DuckLakeColumnGeoStats>().extent;
				double *bounds[] = {&extent.x_min, &extent.x_max, &extent.y_min, &extent.y_max,
				                    &extent.z_min, &extent.z_max, &extent.m_min, &extent.m_max};
				idx_t bound_idx = 0;
				geo_bbox.ForEach([&](const VectorIterator<double>::ValueEntry &bound) {
					if (bound.IsValid()) {
						*bounds[bound_idx] = bound.GetValue();
					}
					bound_idx++;
				});
			}

			auto geo_types = stats_entry.GetChildValue<7>();
			if (geo_types.IsValid() && stats.extra_stats) {
				auto &geo_stats = stats.extra_stats->Cast<DuckLakeColumnGeoStats>();
				for (auto geo_type : geo_types.GetChildValues()) {
					geo_stats.geo_types.insert(geo_type.GetValue().GetString());
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
		} else if (StringUtil::StartsWith(s_ele.logical_type, "NullType()")) {
			return LogicalType::SQLNULL;
		} else if (StringUtil::StartsWith(s_ele.logical_type, "Geometry")) {
			return LogicalType::GEOMETRY();
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
	throw InvalidInputException("Failed to map column \"%s%s\" from file \"%s\" to the column in table \"%s\"\n* %s",
	                            prefix.empty() ? prefix : prefix + ".", column.name, file_metadata.filepath,
	                            table.name.GetIdentifierName(), StringUtil::Join(failures, "\n* "));
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

	if (type.id() == LogicalTypeId::GEOMETRY) {
		if (source_type.id() != LogicalTypeId::GEOMETRY) {
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
                                                                  const DuckLakeFieldId &field_id, string prefix,
                                                                  bool repeated) {
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
	column.skip_bounds = skipped_fields.count(field_id.GetFieldIndex().index) > 0;

	// recursively remap children (if any)
	if (field_id.HasChildren()) {
		auto &field_children = field_id.Children();
		switch (field_id.Type().id()) {
		case LogicalTypeId::STRUCT:
			map_entry->child_entries =
			    MapColumns(file_metadata, column.child_columns, field_id.Children(), prefix, repeated);
			break;
		case LogicalTypeId::LIST:
			if (column.child_columns[0]->name == "array") {
				// With legacy avro list layout, we just access directly
				map_entry->child_entries.push_back(
				    MapColumn(file_metadata, *column.child_columns[0], *field_children[0], prefix, true));
			} else {
				// for lists we don't need to do any name mapping - the child element always maps to each other
				// (1) Parquet has an extra element in between the list and its child ("REPEATED") - strip it
				// (2) Parquet has a different convention on how to name list children - rename them to "list" here
				column.child_columns[0]->child_columns[0]->name = "list";
				map_entry->child_entries.push_back(MapColumn(file_metadata, *column.child_columns[0]->child_columns[0],
				                                             *field_children[0], prefix, true));
			}

			break;
		case LogicalTypeId::MAP:
			// for maps we don't need to do any name mapping - the child elements are always key/value
			// (1) Parquet has an extra element in between the list and its child ("REPEATED") - strip it
			map_entry->child_entries =
			    MapColumns(file_metadata, column.child_columns[0]->child_columns, field_id.Children(), prefix, true);
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
                                                                      const Value &hive_value) {
	auto &target_type = field_id.Type();
	auto target_field_id = field_id.GetFieldIndex();

	if (!SupportsHivePartitioning(target_type)) {
		throw InvalidInputException("Type \"%s\" is not supported for hive partitioning", target_type);
	}

	string error;
	auto cast_result = hive_value.DefaultTryCastAs(target_type, &error);
	if (!cast_result) {
		throw InvalidInputException("Column \"%s\" exists as a hive partition with value \"%s\", but this value cannot "
		                            "be cast to the column type \"%s\"",
		                            field_id.Name(), hive_value.ToString(), field_id.Type());
	}

	// Store the hive partition information for later statistics processing
	DuckLakeTransform transform;
	transform.type = DuckLakeTransformType::IDENTITY;
	auto partition_key_index = GetIdentityPartitionKeyIndex(table.GetPartitionData(), target_field_id);
	file_metadata.hive_partition_values.emplace_back(
	    HivePartition {target_field_id, field_id.Type(), hive_value, transform, partition_key_index});

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
			auto &stats_list = column.column_stats;
			for (auto &stats : stats_list) {
				CheckNotNullStats(field_index, stats);
			}
			auto aggregated = stats_list[0];
			bool numeric_type = aggregated.type.IsNumeric();

			for (idx_t i = 1; i < stats_list.size(); i++) {
				auto &stats = stats_list[i];

				if (aggregated.type != stats.type) {
					aggregated.type = stats.type;
					numeric_type = aggregated.type.IsNumeric();
				}

				if (!stats.has_null_count) {
					aggregated.has_null_count = false;
				} else if (aggregated.has_null_count) {
					aggregated.null_count += stats.null_count;
				}

				if (!stats.has_num_values) {
					aggregated.has_num_values = false;
				} else if (aggregated.has_num_values) {
					aggregated.num_values += stats.num_values;
				}

				aggregated.column_size_bytes += stats.column_size_bytes;

				if (!stats.has_contains_nan) {
					aggregated.has_contains_nan = false;
				} else if (aggregated.has_contains_nan && stats.contains_nan) {
					aggregated.contains_nan = true;
				}

				if (stats.extra_stats) {
					if (aggregated.extra_stats) {
						aggregated.extra_stats->Merge(*stats.extra_stats);
					} else {
						aggregated.extra_stats = stats.extra_stats->Copy();
					}
				}
			}

			Value numeric_min_cache;
			Value numeric_max_cache;
			bool min_cache_valid = false;
			bool max_cache_valid = false;

			if (numeric_type && aggregated.has_min) {
				numeric_min_cache = Value(aggregated.min).DefaultCastAs(aggregated.type);
				min_cache_valid = true;
			}
			if (numeric_type && aggregated.has_max) {
				numeric_max_cache = Value(aggregated.max).DefaultCastAs(aggregated.type);
				max_cache_valid = true;
			}

			if (aggregated.has_min) {
				for (idx_t i = 1; i < stats_list.size(); i++) {
					auto &stats = stats_list[i];
					if (!stats.has_min) {
						aggregated.has_min = false;
						min_cache_valid = false;
						break;
					}
					if (numeric_type) {
						if (!min_cache_valid) {
							numeric_min_cache = Value(aggregated.min).DefaultCastAs(aggregated.type);
							min_cache_valid = true;
						}
						auto stats_min_val = Value(stats.min).DefaultCastAs(aggregated.type);
						if (stats_min_val < numeric_min_cache) {
							aggregated.min = stats.min;
							aggregated.min_is_exact = stats.min_is_exact;
							numeric_min_cache = std::move(stats_min_val);
						} else if (stats_min_val == numeric_min_cache) {
							aggregated.min_is_exact = aggregated.min_is_exact && stats.min_is_exact;
						}
					} else if (stats.min < aggregated.min) {
						aggregated.min = stats.min;
						aggregated.min_is_exact = stats.min_is_exact;
					} else if (stats.min == aggregated.min) {
						aggregated.min_is_exact = aggregated.min_is_exact && stats.min_is_exact;
					}
				}
			}

			if (aggregated.has_max) {
				for (idx_t i = 1; i < stats_list.size(); i++) {
					auto &stats = stats_list[i];
					if (!stats.has_max) {
						aggregated.has_max = false;
						max_cache_valid = false;
						break;
					}
					if (numeric_type) {
						if (!max_cache_valid) {
							numeric_max_cache = Value(aggregated.max).DefaultCastAs(aggregated.type);
							max_cache_valid = true;
						}
						auto stats_max_val = Value(stats.max).DefaultCastAs(aggregated.type);
						if (stats_max_val > numeric_max_cache) {
							aggregated.max = stats.max;
							aggregated.max_is_exact = stats.max_is_exact;
							numeric_max_cache = std::move(stats_max_val);
						} else if (stats_max_val == numeric_max_cache) {
							aggregated.max_is_exact = aggregated.max_is_exact && stats.max_is_exact;
						}
					} else if (stats.max > aggregated.max) {
						aggregated.max = stats.max;
						aggregated.max_is_exact = stats.max_is_exact;
					} else if (stats.max == aggregated.max) {
						aggregated.max_is_exact = aggregated.max_is_exact && stats.max_is_exact;
					}
				}
			}

			if (!aggregated.has_null_count) {
				CheckNotNullValues(file_metadata, column, field_index);
			}
			result.column_stats.emplace(field_index, std::move(aggregated));
		}
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
		if (missing.reads_null) {
			auto column_stats =
			    ConstantColumnStats(file_metadata, missing.field_index, missing.field_type, Value(missing.field_type));
			CheckNotNullStats(missing.field_index, column_stats);
			if (missing.repeated) {
				column_stats.has_num_values = false;
				column_stats.has_null_count = false;
			}
			result.column_stats.emplace(missing.field_index, std::move(column_stats));
			continue;
		}
		// the statistics of a non NULL default are left unknown
		DuckLakeColumnStats unknown_stats(missing.field_type);
		if (unknown_stats.extra_stats) {
			// extra statistics cannot be unknown, so the column gets no statistics
			continue;
		}
		result.column_stats.emplace(missing.field_index, std::move(unknown_stats));
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
		auto hive_entry = hive_partitions.find(col->name);
		if (hive_entry != hive_partitions.end()) {
			auto hive_value =
			    HivePartitioning::GetValue(context, col->name, hive_entry->second, entry->second.get().Type());
			column_maps.push_back(MapHiveColumn(file_metadata, entry->second.get(), hive_value));
			field_id_map.erase(entry);
			continue;
		}
		column_maps.push_back(MapColumn(file_metadata, *col, entry->second.get(), prefix, repeated));
		field_id_map.erase(entry);
	}
	for (auto &entry : field_id_map) {
		auto &field_id = entry.second.get();
		// column does not exist in the file - check hive partitions
		auto hive_entry = hive_partitions.find(field_id.Name());
		if (hive_entry != hive_partitions.end()) {
			// the column exists in the hive partitions - check if the type matches
			auto hive_value = HivePartitioning::GetValue(context, field_id.Name(), hive_entry->second, field_id.Type());
			column_maps.push_back(MapHiveColumn(file_metadata, field_id, hive_value));
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
		CollectMissingColumns(field_id, field_id.GetColumnData().initial_default.IsNull(), repeated,
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

void DuckLakeFileProcessor::ValidateParquetFieldIds(const ParquetFileMetadata &file,
                                                    const vector<unique_ptr<ParquetColumn>> &columns,
                                                    const unordered_set<idx_t> &live_field_ids, const string &prefix) {
	for (auto &column : columns) {
		const auto full_name = prefix.empty() ? column->name : StringUtil::Format("%s.%s", prefix, column->name);
		if (!IsDuckLakeInternalColumn(column->name) && column->field_id.IsValid()) {
			const auto source_field_id = column->field_id.GetIndex();
			if (live_field_ids.find(source_field_id) == live_field_ids.end()) {
				throw InvalidInputException(
				    "Parquet field ID mismatch for column \"%s\" in file \"%s\": field ID %d is not a live field in "
				    "table \"%s\"",
				    full_name, file.filepath, source_field_id, table.name.GetIdentifierName());
			}
		}
		ValidateParquetFieldIds(file, column->child_columns, live_field_ids, full_name);
	}
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
	ValidateParquetFieldIds(file, file.columns, live_field_ids);

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
	if (partition_data) {
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
					invalid_partition = true;
					break;
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
					invalid_partition = true;
					break;
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
