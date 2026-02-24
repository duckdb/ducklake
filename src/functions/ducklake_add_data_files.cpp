#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_insert.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/vector.hpp"
#include "storage/ducklake_geo_stats.hpp"

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
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
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
	void ReadParquetFullMetadata(const string &glob);
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

void DuckLakeFileProcessor::ReadParquetFullMetadata(const string &glob) {
	auto result = transaction.Query(StringUtil::Format(R"(
SELECT 
    list_transform(parquet_file_metadata, x -> struct_pack(
        file_name := x.file_name,
        num_rows := x.num_rows,
        file_size_bytes := x.file_size_bytes,
        footer_size := x.footer_size
    )) AS parquet_file_metadata,
    list_transform(parquet_metadata, x -> struct_pack(
        column_id := x.column_id,
        stats_min := COALESCE(x.stats_min, x.stats_min_value),
        stats_max := COALESCE(x.stats_max, x.stats_max_value),
        stats_null_count := x.stats_null_count,
		stats_num_values := x.num_values,
        total_compressed_size := x.total_compressed_size,
        geo_bbox := x.geo_bbox,
        geo_types := x.geo_types
    )) AS parquet_metadata,
    list_transform(parquet_schema, x -> struct_pack(
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
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to add data files to DuckLake: ");
	}

	for (auto &row : *result) {
		auto &chunk = row.GetChunk();
		idx_t row_idx = row.GetRowInChunk();

		auto &file_metadata_vec = chunk.data[0];
		auto &parquet_metadata_vec = chunk.data[1];
		auto &parquet_schema_vec = chunk.data[2];

		// Access the underlying list data directly
		auto &file_metadata_list_entries = ListVector::GetEntry(file_metadata_vec);
		auto &parquet_metadata_list_entries = ListVector::GetEntry(parquet_metadata_vec);
		auto &parquet_schema_list_entries = ListVector::GetEntry(parquet_schema_vec);
		auto file_metadata_list_data = ListVector::GetData(file_metadata_vec);
		auto parquet_metadata_list_data = ListVector::GetData(parquet_metadata_vec);
		auto parquet_schema_list_data = ListVector::GetData(parquet_schema_vec);

		auto &file_metadata_entry = file_metadata_list_data[row_idx];
		auto file_metadata_offset = file_metadata_entry.offset;

		auto &parquet_metadata_entry = parquet_metadata_list_data[row_idx];
		auto parquet_metadata_offset = parquet_metadata_entry.offset;
		auto parquet_metadata_length = parquet_metadata_entry.length;

		auto &parquet_schema_entry = parquet_schema_list_data[row_idx];
		auto parquet_schema_offset = parquet_schema_entry.offset;
		auto parquet_schema_length = parquet_schema_entry.length;

		// Extract filename from the file metadata struct
		auto &struct_children = StructVector::GetEntries(file_metadata_list_entries);
		idx_t struct_idx = file_metadata_offset;

		auto filename =
		    FlatVector::GetData<string_t>(*struct_children[0])[struct_idx].GetString(); // struct field: file_name

		// Check if we've already processed this file (can happen with overlapping globs)
		auto &file_metadata_ptr = parquet_files[filename];
		if (file_metadata_ptr) {
			// File already processed in a previous glob, skip
			continue;
		}

		// Initialize new file metadata entry
		file_metadata_ptr = make_uniq<ParquetFileMetadata>();
		auto &file = *file_metadata_ptr;
		file.filename = filename;

		file.row_count = FlatVector::GetData<int64_t>(*struct_children[1])[struct_idx]; // struct field: num_rows
		file.file_size_bytes =
		    FlatVector::GetData<uint64_t>(*struct_children[2])[struct_idx]; // struct field: file_size_bytes
		file.footer_size = FlatVector::GetData<uint64_t>(*struct_children[3])[struct_idx]; // struct field: footer_size

		bool saw_root = false;
		vector<idx_t> child_counts;
		idx_t next_column_id = 0;
		vector<ParquetColumn *> column_stack;

		// Get schema struct child vectors once
		auto &schema_struct_children = StructVector::GetEntries(parquet_schema_list_entries);

		// Extract child vectors
		auto &name_vec = *schema_struct_children[0];
		auto &type_vec = *schema_struct_children[1];
		auto &num_children_vec = *schema_struct_children[2];
		auto &converted_type_vec = *schema_struct_children[3];
		auto &scale_vec = *schema_struct_children[4];
		auto &precision_vec = *schema_struct_children[5];
		auto &field_id_vec = *schema_struct_children[6];
		auto &logical_type_vec = *schema_struct_children[7];

		// Get data pointers
		auto name_data = FlatVector::GetData<string_t>(name_vec);
		auto type_data = FlatVector::GetData<string_t>(type_vec);
		auto num_children_data = FlatVector::GetData<int64_t>(num_children_vec);
		auto converted_type_data = FlatVector::GetData<string_t>(converted_type_vec);
		auto scale_data = FlatVector::GetData<int64_t>(scale_vec);
		auto precision_data = FlatVector::GetData<int64_t>(precision_vec);
		auto field_id_data = FlatVector::GetData<int64_t>(field_id_vec);
		auto logical_type_data = FlatVector::GetData<string_t>(logical_type_vec);

		// Get validity masks
		auto &num_children_validity = FlatVector::Validity(num_children_vec);
		auto &type_validity = FlatVector::Validity(type_vec);
		auto &converted_type_validity = FlatVector::Validity(converted_type_vec);
		auto &scale_validity = FlatVector::Validity(scale_vec);
		auto &precision_validity = FlatVector::Validity(precision_vec);
		auto &field_id_validity = FlatVector::Validity(field_id_vec);
		auto &logical_type_validity = FlatVector::Validity(logical_type_vec);

		for (idx_t schema_idx = parquet_schema_offset; schema_idx < parquet_schema_offset + parquet_schema_length;
		     schema_idx++) {
			idx_t child_count = 0;
			if (num_children_validity.RowIsValid(schema_idx)) {
				child_count = num_children_data[schema_idx];
			}

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
			column->name = name_data[schema_idx].GetString();
			if (type_validity.RowIsValid(schema_idx)) {
				column->type = type_data[schema_idx].GetString();
			}
			if (converted_type_validity.RowIsValid(schema_idx)) {
				column->converted_type = converted_type_data[schema_idx].GetString();
			}
			if (scale_validity.RowIsValid(schema_idx)) {
				column->scale = scale_data[schema_idx];
			}
			if (precision_validity.RowIsValid(schema_idx)) {
				column->precision = precision_data[schema_idx];
			}
			if (field_id_validity.RowIsValid(schema_idx)) {
				column->field_id = field_id_data[schema_idx];
			}
			if (logical_type_validity.RowIsValid(schema_idx)) {
				column->logical_type = logical_type_data[schema_idx].GetString();
			}

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

		auto &metadata_struct_children = StructVector::GetEntries(parquet_metadata_list_entries);
		auto &column_id_vec = *metadata_struct_children[0];
		auto &stats_min_vec = *metadata_struct_children[1];
		auto &stats_max_vec = *metadata_struct_children[2];
		auto &stats_null_count_vec = *metadata_struct_children[3];
		auto &stats_num_values_vec = *metadata_struct_children[4];
		auto &total_compressed_size_vec = *metadata_struct_children[5];
		auto &geo_bbox_vec = *metadata_struct_children[6];
		auto &geo_types_vec = *metadata_struct_children[7];

		auto column_id_data = FlatVector::GetData<int64_t>(column_id_vec);
		auto stats_min_data = FlatVector::GetData<string_t>(stats_min_vec);
		auto stats_max_data = FlatVector::GetData<string_t>(stats_max_vec);
		auto stats_null_count_data = FlatVector::GetData<int64_t>(stats_null_count_vec);
		auto stats_num_values_data = FlatVector::GetData<int64_t>(stats_num_values_vec);
		auto total_compressed_size_data = FlatVector::GetData<int64_t>(total_compressed_size_vec);

		auto &column_id_validity = FlatVector::Validity(column_id_vec);
		auto &stats_min_validity = FlatVector::Validity(stats_min_vec);
		auto &stats_max_validity = FlatVector::Validity(stats_max_vec);
		auto &stats_null_count_validity = FlatVector::Validity(stats_null_count_vec);
		auto &stats_num_values_validity = FlatVector::Validity(stats_num_values_vec);
		auto &total_compressed_size_validity = FlatVector::Validity(total_compressed_size_vec);
		auto &geo_bbox_validity = FlatVector::Validity(geo_bbox_vec);
		auto &geo_types_validity = FlatVector::Validity(geo_types_vec);

		for (idx_t metadata_idx = parquet_metadata_offset;
		     metadata_idx < parquet_metadata_offset + parquet_metadata_length; metadata_idx++) {
			if (!column_id_validity.RowIsValid(metadata_idx)) {
				continue;
			}
			auto column_id = column_id_data[metadata_idx];
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

			if (stats_min_validity.RowIsValid(metadata_idx)) {
				stats.has_min = true;
				stats.min = stats_min_data[metadata_idx].GetString();
			}

			if (stats_max_validity.RowIsValid(metadata_idx)) {
				stats.has_max = true;
				stats.max = stats_max_data[metadata_idx].GetString();
			}

			if (stats_null_count_validity.RowIsValid(metadata_idx)) {
				auto null_count = stats_null_count_data[metadata_idx];
				// Guard against negative values (indicates an underflow in parquet reader)
				if (null_count >= 0) {
					stats.has_null_count = true;
					stats.null_count = static_cast<idx_t>(null_count);
				}
			}

			if (stats_num_values_validity.RowIsValid(metadata_idx)) {
				auto num_values = stats_num_values_data[metadata_idx];
				// Guard against negative values (indicates an underflow in parquet reader)
				if (num_values >= 0) {
					stats.has_num_values = true;
					stats.num_values = static_cast<idx_t>(num_values);
				}
			}

			if (total_compressed_size_validity.RowIsValid(metadata_idx)) {
				stats.column_size_bytes = total_compressed_size_data[metadata_idx];
			}

			if (geo_bbox_validity.RowIsValid(metadata_idx) && stats.extra_stats) {
				// Access geo_bbox struct fields directly
				auto &bbox_struct_children = StructVector::GetEntries(geo_bbox_vec);
				auto bbox_xmin_data = FlatVector::GetData<double>(*bbox_struct_children[0]);
				auto bbox_xmax_data = FlatVector::GetData<double>(*bbox_struct_children[1]);
				auto bbox_ymin_data = FlatVector::GetData<double>(*bbox_struct_children[2]);
				auto bbox_ymax_data = FlatVector::GetData<double>(*bbox_struct_children[3]);
				auto bbox_zmin_data = FlatVector::GetData<double>(*bbox_struct_children[4]);
				auto bbox_zmax_data = FlatVector::GetData<double>(*bbox_struct_children[5]);
				auto bbox_mmin_data = FlatVector::GetData<double>(*bbox_struct_children[6]);
				auto bbox_mmax_data = FlatVector::GetData<double>(*bbox_struct_children[7]);

				auto &bbox_xmin_validity = FlatVector::Validity(*bbox_struct_children[0]);
				auto &bbox_xmax_validity = FlatVector::Validity(*bbox_struct_children[1]);
				auto &bbox_ymin_validity = FlatVector::Validity(*bbox_struct_children[2]);
				auto &bbox_ymax_validity = FlatVector::Validity(*bbox_struct_children[3]);
				auto &bbox_zmin_validity = FlatVector::Validity(*bbox_struct_children[4]);
				auto &bbox_zmax_validity = FlatVector::Validity(*bbox_struct_children[5]);
				auto &bbox_mmin_validity = FlatVector::Validity(*bbox_struct_children[6]);
				auto &bbox_mmax_validity = FlatVector::Validity(*bbox_struct_children[7]);
				auto &geo_stats = stats.extra_stats->Cast<DuckLakeColumnGeoStats>();
				if (bbox_xmin_validity.RowIsValid(metadata_idx))
					geo_stats.xmin = bbox_xmin_data[metadata_idx];
				if (bbox_xmax_validity.RowIsValid(metadata_idx))
					geo_stats.xmax = bbox_xmax_data[metadata_idx];
				if (bbox_ymin_validity.RowIsValid(metadata_idx))
					geo_stats.ymin = bbox_ymin_data[metadata_idx];
				if (bbox_ymax_validity.RowIsValid(metadata_idx))
					geo_stats.ymax = bbox_ymax_data[metadata_idx];
				if (bbox_zmin_validity.RowIsValid(metadata_idx))
					geo_stats.zmin = bbox_zmin_data[metadata_idx];
				if (bbox_zmax_validity.RowIsValid(metadata_idx))
					geo_stats.zmax = bbox_zmax_data[metadata_idx];
				if (bbox_mmin_validity.RowIsValid(metadata_idx))
					geo_stats.mmin = bbox_mmin_data[metadata_idx];
				if (bbox_mmax_validity.RowIsValid(metadata_idx))
					geo_stats.mmax = bbox_mmax_data[metadata_idx];
			}

			if (geo_types_validity.RowIsValid(metadata_idx) && stats.extra_stats) {
				auto &geo_stats = stats.extra_stats->Cast<DuckLakeColumnGeoStats>();
				// geo_types is a list of strings, need to access it differently
				auto geo_types_value = geo_types_vec.GetValue(metadata_idx);
				for (const auto &child : ListValue::GetChildren(geo_types_value)) {
					geo_stats.geo_types.insert(StringValue::Get(child));
				}
			}

			column.column_stats.push_back(std::move(stats));
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
			if (column.child_columns[0]->name == "array") {
				// With legacy avro list layout, we just access directly
				map_entry->child_entries.push_back(
				    MapColumn(file_metadata, *column.child_columns[0], *field_children[0], prefix));
			} else {
				// for lists we don't need to do any name mapping - the child element always maps to each other
				// (1) Parquet has an extra element in between the list and its child ("REPEATED") - strip it
				// (2) Parquet has a different convention on how to name list children - rename them to "list" here
				column.child_columns[0]->child_columns[0]->name = "list";
				map_entry->child_entries.push_back(
				    MapColumn(file_metadata, *column.child_columns[0]->child_columns[0], *field_children[0], prefix));
			}

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
			auto &stats_list = column.column_stats;
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

			numeric_type = aggregated.type.IsNumeric();
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
							numeric_min_cache = std::move(stats_min_val);
						}
					} else if (stats.min < aggregated.min) {
						aggregated.min = stats.min;
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
							numeric_max_cache = std::move(stats_max_val);
						}
					} else if (stats.max > aggregated.max) {
						aggregated.max = stats.max;
					}
				}
			}

			result.column_stats.emplace(field_index, std::move(aggregated));
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
			for (const auto &hive_partition_value : file.hive_partition_values) {
				bool found_field = false;
				for (const auto &field : partition_data->fields) {
					if (field.field_id.index == hive_partition_value.field_index.index) {
						found_field = true;
						break;
					}
				}
				if (!found_field) {
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
		ReadParquetFullMetadata(glob);
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

TableFunctionSet DuckLakeAddDataFilesFunction::GetFunctions() {
	TableFunctionSet set("ducklake_add_data_files");
	vector<LogicalType> at_types {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)};
	for (auto &type : at_types) {
		TableFunction function("ducklake_add_data_files", {LogicalType::VARCHAR, LogicalType::VARCHAR, type},
		                       DuckLakeAddDataFilesExecute, DuckLakeAddDataFilesBind, DuckLakeAddDataFilesInit);
		function.named_parameters["allow_missing"] = LogicalType::BOOLEAN;
		function.named_parameters["ignore_extra_columns"] = LogicalType::BOOLEAN;
		function.named_parameters["hive_partitioning"] = LogicalType::BOOLEAN;
		function.named_parameters["schema"] = LogicalType::VARCHAR;
		set.AddFunction(function);
	}
	return set;
}

} // namespace duckdb
