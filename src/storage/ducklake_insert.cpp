#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_field_data.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_inline_data.hpp"
#include "storage/ducklake_geo_stats.hpp"
#include "common/ducklake_types.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "storage/ducklake_variant_stats.hpp"

namespace duckdb {

DuckLakeInsert::DuckLakeInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types, DuckLakeTableEntry &table,
                               optional_idx partition_id, string encryption_key_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 1), table(&table), schema(nullptr),
      partition_id(partition_id), encryption_key(std::move(encryption_key_p)) {
}

DuckLakeInsert::DuckLakeInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types,
                               SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info, string table_uuid_p,
                               string table_data_path_p, string encryption_key_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 1), table(nullptr), schema(&schema),
      info(std::move(info)), table_uuid(std::move(table_uuid_p)), table_data_path(std::move(table_data_path_p)),
      encryption_key(std::move(encryption_key_p)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
DuckLakeInsertGlobalState::DuckLakeInsertGlobalState(DuckLakeTableEntry &table)
    : table(table), total_insert_count(0), not_null_fields(table.GetNotNullFields()) {
}

unique_ptr<GlobalSinkState> DuckLakeInsert::GetGlobalSinkState(ClientContext &context) const {
	optional_ptr<DuckLakeTableEntry> table_ptr;
	if (info) {
		// CREATE TABLE AS - create the table
		auto &catalog = schema->catalog;
		auto &ducklake_schema = schema.get_mutable()->Cast<DuckLakeSchemaEntry>();
		auto transaction = catalog.GetCatalogTransaction(context);
		table_ptr = &ducklake_schema.CreateTableExtended(transaction, *info, table_uuid, table_data_path)
		                 ->Cast<DuckLakeTableEntry>();
	} else {
		// INSERT INTO
		table_ptr = table;
	}
	return make_uniq<DuckLakeInsertGlobalState>(*table_ptr);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
DuckLakeColumnStats DuckLakeInsert::ParseColumnStats(const LogicalType &type, const vector<Value> &col_stats) {
	DuckLakeColumnStats column_stats(type);
	for (idx_t stats_idx = 0; stats_idx < col_stats.size(); stats_idx++) {
		auto &stats_children = StructValue::GetChildren(col_stats[stats_idx]);
		auto &stats_name = StringValue::Get(stats_children[0]);
		if (stats_name == "min") {
			D_ASSERT(!column_stats.has_min);
			column_stats.min = StringValue::Get(stats_children[1]);
			column_stats.has_min = true;
		} else if (stats_name == "max") {
			D_ASSERT(!column_stats.has_max);
			column_stats.max = StringValue::Get(stats_children[1]);
			column_stats.has_max = true;
		} else if (stats_name == "null_count") {
			D_ASSERT(!column_stats.has_null_count);
			column_stats.has_null_count = true;
			column_stats.null_count = StringUtil::ToUnsigned(StringValue::Get(stats_children[1]));
		} else if (stats_name == "num_values") {
			D_ASSERT(!column_stats.has_num_values);
			column_stats.has_num_values = true;
			column_stats.num_values = StringUtil::ToUnsigned(StringValue::Get(stats_children[1]));
		} else if (stats_name == "column_size_bytes") {
			column_stats.column_size_bytes = StringUtil::ToUnsigned(StringValue::Get(stats_children[1]));
		} else if (stats_name == "has_nan") {
			column_stats.has_contains_nan = true;
			column_stats.contains_nan = StringValue::Get(stats_children[1]) == "true";
		} else if (column_stats.extra_stats && column_stats.extra_stats->ParseStats(stats_name, stats_children)) {
			// handled by extra stats
			continue;
		} else {
			throw NotImplementedException("Unsupported stats type \"%s\" in DuckLakeInsert::Sink()", stats_name);
		}
	}
	return column_stats;
}

void DuckLakeInsert::AddWrittenFiles(DuckLakeInsertGlobalState &global_state, DataChunk &chunk,
                                     const string &encryption_key, optional_idx partition_id, bool set_snapshot_id) {
	for (idx_t r = 0; r < chunk.size(); r++) {
		DuckLakeDataFile data_file;
		data_file.file_name = chunk.GetValue(0, r).GetValue<string>();
		data_file.row_count = chunk.GetValue(1, r).GetValue<idx_t>();
		data_file.file_size_bytes = chunk.GetValue(2, r).GetValue<idx_t>();
		data_file.footer_size = chunk.GetValue(3, r).GetValue<idx_t>();
		data_file.encryption_key = encryption_key;
		if (partition_id.IsValid()) {
			data_file.partition_id = partition_id.GetIndex();
		}

		// extract the column stats
		auto column_stats = chunk.GetValue(4, r);
		auto &map_children = MapValue::GetChildren(column_stats);
		auto &table = global_state.table;
		map<FieldIndex, PartialVariantStats> variant_stats;
		for (idx_t col_idx = 0; col_idx < map_children.size(); col_idx++) {
			auto &struct_children = StructValue::GetChildren(map_children[col_idx]);
			auto &col_name = StringValue::Get(struct_children[0]);
			auto &col_stats = MapValue::GetChildren(struct_children[1]);
			auto column_names = DuckLakeUtil::ParseQuotedList(col_name, '.');
			// FIXME: this should be checked differently
			if (column_names[0] == "_ducklake_internal_snapshot_id") {
				if (set_snapshot_id) {
					// set start snapshot id based on the minimum written to the file
					auto snapshot_stats = ParseColumnStats(LogicalType::UBIGINT, col_stats);
					if (snapshot_stats.has_min) {
						data_file.begin_snapshot = StringUtil::ToUnsigned(snapshot_stats.min);
					}
					if (snapshot_stats.has_max) {
						data_file.max_partial_file_snapshot = StringUtil::ToUnsigned(snapshot_stats.max);
					}
				}
				continue;
			}
			if (column_names[0] == "_ducklake_internal_row_id") {
				if (set_snapshot_id) {
					// extract the min row_id so flushed files preserve the original row_id_start
					auto row_id_stats = ParseColumnStats(LogicalType::BIGINT, col_stats);
					if (row_id_stats.has_min) {
						data_file.flush_row_id_start = StringUtil::ToUnsigned(row_id_stats.min);
					}
				}
				continue;
			}

			optional_idx name_offset;
			auto &field_id = table.GetFieldId(column_names, &name_offset);
			if (name_offset.IsValid()) {
				if (field_id.Type().id() != LogicalTypeId::VARIANT) {
					throw InternalException("name_offset can only be set for variant columns");
				}
				// variant stats are constructed iteratively as they are provided per-field
				auto entry = variant_stats.find(field_id.GetFieldIndex());
				if (entry == variant_stats.end()) {
					// insert empty stats for variants if this is the first stats we encounter for variants
					auto insert_entry =
					    variant_stats.insert(make_pair(field_id.GetFieldIndex(), PartialVariantStats()));
					entry = insert_entry.first;
				}
				entry->second.ParseVariantStats(column_names, name_offset.GetIndex(), col_stats);
				continue;
			}
			if (field_id.Type().id() == LogicalTypeId::VARIANT) {
				throw InvalidInputException("Top-level variant cannot have stats");
			}
			auto column_stats = ParseColumnStats(field_id.Type(), col_stats);
			if (column_stats.null_count > 0 && column_names.size() == 1) {
				// we wrote NULL values to a base column - verify NOT NULL constraint
				if (global_state.not_null_fields.count(column_names[0])) {
					throw ConstraintException("NOT NULL constraint failed: %s.%s", table.name, column_names[0]);
				}
			}

			data_file.column_stats.insert(make_pair(field_id.GetFieldIndex(), std::move(column_stats)));
		}
		// finalize variant stats
		for (auto &entry : variant_stats) {
			// FIXME: verify NOT NULL constraints for variants
			data_file.column_stats.insert(make_pair(entry.first, entry.second.Finalize()));
		}
		// extract the partition info
		auto partition_info = chunk.GetValue(5, r);
		if (!partition_info.IsNull()) {
			auto &partition_children = MapValue::GetChildren(partition_info);
			for (idx_t col_idx = 0; col_idx < partition_children.size(); col_idx++) {
				auto &struct_children = StructValue::GetChildren(partition_children[col_idx]);
				auto &part_value = StringValue::Get(struct_children[1]);

				DuckLakeFilePartition file_partition_info;
				file_partition_info.partition_column_idx = col_idx;
				file_partition_info.partition_value = part_value;
				data_file.partition_values.push_back(std::move(file_partition_info));
			}
		}
		if (set_snapshot_id && !data_file.begin_snapshot.IsValid()) {
			throw InvalidInputException("Did not find written snapshot id - but operation requires it to be set");
		}

		global_state.written_files.push_back(std::move(data_file));
	}
}

SinkResultType DuckLakeInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeInsertGlobalState>();
	AddWrittenFiles(global_state, chunk, encryption_key, partition_id);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType DuckLakeInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<DuckLakeInsertGlobalState>();
	auto value = Value::BIGINT(NumericCast<int64_t>(global_state.total_insert_count));
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, value);
	return SourceResultType::FINISHED;
}
//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType DuckLakeInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeInsertGlobalState>();

	for (auto &data_file : global_state.written_files) {
		global_state.total_insert_count += data_file.row_count;
	}
	auto &transaction = DuckLakeTransaction::Get(context, global_state.table.catalog);
	transaction.AppendFiles(global_state.table.GetTableId(), std::move(global_state.written_files));

	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string DuckLakeInsert::GetName() const {
	return table ? "DUCKLAKE_INSERT" : "DUCKLAKE_CREATE_TABLE_AS";
}

InsertionOrderPreservingMap<string> DuckLakeInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table ? table->name : info->Base().table;
	return result;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
CopyFunctionCatalogEntry &DuckLakeFunctions::GetCopyFunction(ClientContext &context, const string &name) {
	// Logic is partially duplicated from Catalog::AutoLoadExtensionByCatalogEntry(db, CatalogType::COPY_FUNCTION_ENTRY,
	// name), but that do not offer enough control
	auto &db = *context.db;
	string extension_name = ExtensionHelper::FindExtensionInEntries(name, EXTENSION_COPY_FUNCTIONS);
	if (!extension_name.empty() && Settings::Get<AutoloadKnownExtensionsSetting>(context) &&
	    ExtensionHelper::CanAutoloadExtension(extension_name)) {
		// This will either succeed or throw
		ExtensionHelper::AutoLoadExtension(db, extension_name);
	}
	D_ASSERT(!name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);

	auto entry =
	    system_catalog.GetEntry<CopyFunctionCatalogEntry>(context, DEFAULT_SCHEMA, name, OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		throw MissingExtensionException(
		    "Could not load the copy function for \"%s\". Try explicitly loading the \"%s\" extension", name, name);
	}
	return *entry;
}

static Value GetFieldIdValue(const DuckLakeFieldId &field_id) {
	auto field_id_value = Value::BIGINT(NumericCast<int64_t>(field_id.GetFieldIndex().index));
	if (!field_id.HasChildren()) {
		// primitive type - return the field-id directly
		return field_id_value;
	}
	// nested type - generate a struct and recurse into children
	child_list_t<Value> values;
	values.emplace_back("__duckdb_field_id", std::move(field_id_value));
	for (auto &child : field_id.Children()) {
		values.emplace_back(child->Name(), GetFieldIdValue(*child));
	}
	return Value::STRUCT(std::move(values));
}

static bool WriteRowId(InsertVirtualColumns virtual_columns) {
	return virtual_columns == InsertVirtualColumns::WRITE_ROW_ID ||
	       virtual_columns == InsertVirtualColumns::WRITE_ROW_ID_AND_SNAPSHOT_ID;
}

static bool WriteSnapshotId(InsertVirtualColumns virtual_columns) {
	return virtual_columns == InsertVirtualColumns::WRITE_SNAPSHOT_ID ||
	       virtual_columns == InsertVirtualColumns::WRITE_ROW_ID_AND_SNAPSHOT_ID;
}

static Value WrittenFieldIds(DuckLakeFieldData &field_data, InsertVirtualColumns virtual_columns) {
	child_list_t<Value> values;
	for (idx_t c_idx = 0; c_idx < field_data.GetColumnCount(); c_idx++) {
		auto &field_id = field_data.GetByRootIndex(PhysicalIndex(c_idx));
		values.emplace_back(field_id.Name(), GetFieldIdValue(field_id));
	}
	if (WriteRowId(virtual_columns)) {
		values.emplace_back("_ducklake_internal_row_id", Value::BIGINT(MultiFileReader::ROW_ID_FIELD_ID));
	}
	if (WriteSnapshotId(virtual_columns)) {
		values.emplace_back("_ducklake_internal_snapshot_id",
		                    Value::BIGINT(MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID));
	}
	return Value::STRUCT(std::move(values));
}

DuckLakeCopyOptions::DuckLakeCopyOptions(unique_ptr<CopyInfo> info_p, CopyFunction copy_function_p)
    : info(std::move(info_p)), copy_function(std::move(copy_function_p)) {
}

DuckLakeCopyInput::DuckLakeCopyInput(ClientContext &context, DuckLakeTableEntry &table, const string &hive_partition)
    : catalog(table.ParentCatalog().Cast<DuckLakeCatalog>()), columns(table.GetColumns()),
      data_path(table.DataPath() + hive_partition) {
	partition_data = table.GetPartitionData();
	field_data = table.GetFieldData();
	schema_id = table.ParentSchema().Cast<DuckLakeSchemaEntry>().GetSchemaId();
	table_id = table.GetTableId();
	encryption_key = catalog.GenerateEncryptionKey(context);
}

DuckLakeCopyInput::DuckLakeCopyInput(ClientContext &context, DuckLakeSchemaEntry &schema, const ColumnList &columns,
                                     const string &data_path_p)
    : catalog(schema.ParentCatalog().Cast<DuckLakeCatalog>()), columns(columns), data_path(data_path_p) {
	schema_id = schema.GetSchemaId();
	encryption_key = catalog.GenerateEncryptionKey(context);
}

static void StripTrailingSeparator(FileSystem &fs, string &path) {
	auto sep = fs.PathSeparator(path);
	if (!StringUtil::EndsWith(path, sep)) {
		return;
	}
	path = path.substr(0, path.size() - sep.size());
}

const DuckLakeFieldId &DuckLakeInsert::GetTopLevelColumn(DuckLakeCopyInput &copy_input, FieldIndex field_id,
                                                         optional_idx &index) {
	if (!copy_input.field_data) {
		throw InvalidInputException("Partitioning requires field ids");
	}
	auto entry = copy_input.field_data->GetByFieldIndex(field_id);
	if (!entry) {
		throw InvalidInputException("Partitioned column not found");
	}
	auto &top_level_field_ids = copy_input.field_data->GetFieldIds();
	for (idx_t col_idx = 0; col_idx < top_level_field_ids.size(); col_idx++) {
		if (top_level_field_ids[col_idx].get() == entry.get()) {
			index = col_idx;
			return *entry;
		}
	}
	throw InvalidInputException("Partitioning is only supported on top-level columns");
}

static unique_ptr<Expression> CreateColumnReference(DuckLakeCopyInput &copy_input, const LogicalType &type,
                                                    idx_t column_index) {
	if (copy_input.get_table_index.IsValid()) {
		// logical plan generation: generate a bound column ref
		ColumnBinding column_binding(copy_input.get_table_index.GetIndex(), column_index);
		return make_uniq<BoundColumnRefExpression>(type, column_binding);
	}
	// physical plan generation: generate a reference directly
	return make_uniq<BoundReferenceExpression>(type, column_index);
}

static unique_ptr<Expression> GetColumnReference(DuckLakeCopyInput &copy_input, FieldIndex field_id) {
	optional_idx index;
	auto &column_field_id = DuckLakeInsert::GetTopLevelColumn(copy_input, field_id, index);
	return CreateColumnReference(copy_input, column_field_id.Type(), index.GetIndex());
}

static unique_ptr<Expression> GetFunction(ClientContext &context, DuckLakeCopyInput &copy_input,
                                          const string &function_name, FieldIndex field_id) {
	vector<unique_ptr<Expression>> children;
	children.push_back(GetColumnReference(copy_input, field_id));

	ErrorData error;
	FunctionBinder binder(context);
	auto function = binder.BindScalarFunction(DEFAULT_SCHEMA, function_name, std::move(children), error, false);
	if (!function) {
		error.Throw();
	}
	return function;
}

static unique_ptr<Expression> GetPartitionExpression(ClientContext &context, DuckLakeCopyInput &copy_input,
                                                     const DuckLakePartitionField &field) {
	switch (field.transform.type) {
	case DuckLakeTransformType::IDENTITY:
		return GetColumnReference(copy_input, field.field_id);
	case DuckLakeTransformType::YEAR:
		return GetFunction(context, copy_input, "year", field.field_id);
	case DuckLakeTransformType::MONTH:
		return GetFunction(context, copy_input, "month", field.field_id);
	case DuckLakeTransformType::DAY:
		return GetFunction(context, copy_input, "day", field.field_id);
	case DuckLakeTransformType::HOUR:
		return GetFunction(context, copy_input, "hour", field.field_id);
	default:
		throw NotImplementedException("Unsupported partition transform type in GetPartitionExpression");
	}
}

static string GetPartitionExpressionName(DuckLakeCopyInput &copy_input, const DuckLakePartitionField &field,
                                         case_insensitive_set_t &names) {
	auto field_id = copy_input.field_data->GetByFieldIndex(field.field_id);
	return DuckLakePartitionUtils::GetPartitionKeyName(field.transform.type, field_id->Name(), names);
}

static void GeneratePartitionExpressions(ClientContext &context, DuckLakeCopyInput &copy_input,
                                         DuckLakeCopyOptions &copy_options) {
	bool all_identity = true;
	for (auto &field : copy_input.partition_data->fields) {
		if (field.transform.type != DuckLakeTransformType::IDENTITY) {
			all_identity = false;
			break;
		}
	}
	if (all_identity) {
		// all transforms are identity transforms - we can partition on the columns directly
		// just set up the correct references to the partition columns
		for (auto &field : copy_input.partition_data->fields) {
			optional_idx col_idx;
			DuckLakeInsert::GetTopLevelColumn(copy_input, field.field_id, col_idx);
			copy_options.partition_columns.push_back(col_idx.GetIndex());
		}
		return;
	}
	idx_t virtual_column_count;
	switch (copy_input.virtual_columns) {
	case InsertVirtualColumns::WRITE_ROW_ID:
	case InsertVirtualColumns::WRITE_SNAPSHOT_ID:
		virtual_column_count = 1;
		break;
	case InsertVirtualColumns::WRITE_ROW_ID_AND_SNAPSHOT_ID:
		virtual_column_count = 2;
		break;
	default:
		virtual_column_count = 0;
		break;
	}
	// if we have partition columns that are NOT identity, we need to compute them separately, and NOT write them
	idx_t partition_column_start = copy_input.columns.PhysicalColumnCount() + virtual_column_count;
	for (idx_t part_idx = 0; part_idx < copy_input.partition_data->fields.size(); part_idx++) {
		copy_options.partition_columns.push_back(partition_column_start++);
	}
	copy_options.write_partition_columns = false;

	idx_t col_idx = 0;
	for (auto &col : copy_input.columns.Physical()) {
		copy_options.projection_list.push_back(CreateColumnReference(copy_input, col.Type(), col_idx++));
	}
	// push any projected virtual columns
	for (idx_t i = 0; i < virtual_column_count; i++) {
		copy_options.projection_list.push_back(CreateColumnReference(copy_input, LogicalType::BIGINT, col_idx++));
	}
	// push the partition expressions
	case_insensitive_set_t names;
	for (auto &field : copy_input.partition_data->fields) {
		auto expr = GetPartitionExpression(context, copy_input, field);
		copy_options.names.push_back(GetPartitionExpressionName(copy_input, field, names));
		names.insert(copy_options.names.back());
		copy_options.expected_types.push_back(expr->return_type);
		copy_options.projection_list.push_back(std::move(expr));
	}
}

DuckLakeCopyOptions DuckLakeInsert::GetCopyOptions(ClientContext &context, DuckLakeCopyInput &copy_input) {
	auto info = make_uniq<CopyInfo>();
	auto &catalog = copy_input.catalog;
	info->file_path = copy_input.data_path;
	info->format = "parquet";
	info->is_from = false;
	// generate the field ids to be written by the parquet writer
	shared_ptr<DuckLakeFieldData> generated_ids;
	if (!copy_input.field_data) {
		// CTAS - generate new ids from columns
		generated_ids = DuckLakeFieldData::FromColumns(copy_input.columns);
	}
	auto &field_ids = copy_input.field_data ? *copy_input.field_data : *generated_ids;
	vector<Value> field_input;
	field_input.push_back(WrittenFieldIds(field_ids, copy_input.virtual_columns));
	info->options["field_ids"] = std::move(field_input);
	if (!copy_input.encryption_key.empty()) {
		child_list_t<Value> values;
		values.emplace_back("footer_key_value", Value::BLOB_RAW(copy_input.encryption_key));
		vector<Value> encryption_input;
		encryption_input.push_back(Value::STRUCT(std::move(values)));
		info->options["encryption_config"] = std::move(encryption_input);
	}
	auto &schema_id = copy_input.schema_id;
	auto &table_id = copy_input.table_id;
	string parquet_compression;
	if (catalog.TryGetConfigOption("parquet_compression", parquet_compression, schema_id, table_id)) {
		info->options["compression"].emplace_back(parquet_compression);
	}
	string parquet_version;
	if (catalog.TryGetConfigOption("parquet_version", parquet_version, schema_id, table_id)) {
		info->options["parquet_version"].emplace_back(parquet_version);
	}
	string parquet_compression_level;
	if (catalog.TryGetConfigOption("parquet_compression_level", parquet_compression_level, schema_id, table_id)) {
		info->options["compression_level"].emplace_back(parquet_compression_level);
	}
	string row_group_size;
	if (catalog.TryGetConfigOption("parquet_row_group_size", row_group_size, schema_id, table_id)) {
		info->options["row_group_size"].emplace_back(row_group_size);
	}
	string row_group_size_bytes;
	if (catalog.TryGetConfigOption("parquet_row_group_size_bytes", row_group_size_bytes, schema_id, table_id)) {
		info->options["row_group_size_bytes"].emplace_back(row_group_size_bytes + " bytes");
	}
	string per_thread_output_str;
	bool per_thread_output = false;
	if (catalog.TryGetConfigOption("per_thread_output", per_thread_output_str, schema_id, table_id)) {
		per_thread_output = per_thread_output_str == "true";
	}
	idx_t target_file_size = catalog.GetConfigOption<idx_t>("target_file_size", schema_id, table_id,
	                                                        DuckLakeCatalog::DEFAULT_TARGET_FILE_SIZE);

	// Always use native parquet geometry for writing
	info->options["geoparquet_version"].emplace_back("NONE");

	// Get Parquet Copy function
	auto &copy_fun = DuckLakeFunctions::GetCopyFunction(context, "parquet");

	auto &fs = FileSystem::GetFileSystem(context);
	if (!fs.IsRemoteFile(copy_input.data_path)) {
		// create data path if it does not yet exist
		try {
			fs.CreateDirectoriesRecursive(copy_input.data_path);
		} catch (...) {
		}
	}

	// Bind Copy Function
	CopyFunctionBindInput bind_input(*info);

	auto names_to_write = copy_input.columns.GetColumnNames();
	auto types_to_write = copy_input.columns.GetColumnTypes();
	if (WriteRowId(copy_input.virtual_columns)) {
		names_to_write.push_back("_ducklake_internal_row_id");
		types_to_write.push_back(LogicalType::BIGINT);
	}
	if (WriteSnapshotId(copy_input.virtual_columns)) {
		names_to_write.push_back("_ducklake_internal_snapshot_id");
		types_to_write.push_back(LogicalType::BIGINT);
	}

	vector<LogicalType> casted_types;
	for (const auto &type : types_to_write) {
		if (DuckLakeTypes::RequiresCast(type)) {
			casted_types.push_back(DuckLakeTypes::GetCastedType(type));
		} else {
			casted_types.push_back(type);
		}
	}

	auto function_data = copy_fun.function.copy_to_bind(context, bind_input, names_to_write, casted_types);

	DuckLakeCopyOptions result(std::move(info), copy_fun.function);
	result.bind_data = std::move(function_data);

	result.use_tmp_file = false;
	if (copy_input.partition_data) {
		result.filename_pattern.SetFilenamePattern("ducklake-{uuidv7}");
		result.partition_output = true;
		result.write_empty_file = true;
		result.rotate = false;
	} else {
		result.filename_pattern.SetFilenamePattern("ducklake-{uuidv7}");
		result.partition_output = false;
		result.write_empty_file = false;
		// file_size_bytes is currently only supported for unpartitioned writes
		result.file_size_bytes = target_file_size;
		result.rotate = true;
	}
	result.file_path = copy_input.data_path;
	StripTrailingSeparator(fs, result.file_path);
	result.file_extension = "parquet";
	result.overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
	result.per_thread_output = per_thread_output;
	result.write_partition_columns = true;
	result.return_type = CopyFunctionReturnType::WRITTEN_FILE_STATISTICS;
	result.names = names_to_write;
	result.expected_types = types_to_write;

	if (copy_input.partition_data) {
		// we are partitioning - generate partition expressions (if any)
		GeneratePartitionExpressions(context, copy_input, result);
	}
	return result;
}

static void GenerateProjection(ClientContext &context, PhysicalPlanGenerator &planner,
                               vector<unique_ptr<Expression>> &expressions, optional_ptr<PhysicalOperator> &plan) {
	// push the projection
	vector<LogicalType> types;
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
	auto &proj =
	    planner.Make<PhysicalProjection>(std::move(types), std::move(expressions), plan->estimated_cardinality);
	proj.children.push_back(*plan);
	plan = proj;
}

void DuckLakeInsert::InsertCasts(const vector<LogicalType> &types, ClientContext &context,
                                 PhysicalPlanGenerator &planner, optional_ptr<PhysicalOperator> &plan) {
	vector<unique_ptr<Expression>> expressions;
	idx_t col_idx = 0;
	for (auto &expected_type : types) {
		auto expr = make_uniq<BoundReferenceExpression>(expected_type, col_idx++);
		if (DuckLakeTypes::RequiresCast(expected_type)) {
			auto new_type = DuckLakeTypes::GetCastedType(expected_type);
			expressions.push_back(BoundCastExpression::AddCastToType(context, std::move(expr), new_type));
		} else {
			expressions.push_back(std::move(expr));
		}
	}
	GenerateProjection(context, planner, expressions, plan);
}

unique_ptr<LogicalOperator> DuckLakeInsert::InsertCasts(Binder &binder, unique_ptr<LogicalOperator> &plan) {
	vector<unique_ptr<Expression>> cast_expressions;

	auto &types = plan->types;
	auto bindings = plan->GetColumnBindings();

	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		auto &type = types[col_idx];
		auto &binding = bindings[col_idx];
		auto ref_expr = make_uniq<BoundColumnRefExpression>(type, binding);
		if (DuckLakeTypes::RequiresCast(type)) {
			auto new_type = DuckLakeTypes::GetCastedType(type);
			cast_expressions.push_back(
			    BoundCastExpression::AddCastToType(binder.context, std::move(ref_expr), new_type));
		} else {
			cast_expressions.push_back(std::move(ref_expr));
		}
	}

	auto result = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(cast_expressions));
	result->children.push_back(std::move(plan));

	return std::move(result);
}

PhysicalOperator &DuckLakeInsert::PlanCopyForInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    DuckLakeCopyInput &copy_input,
                                                    optional_ptr<PhysicalOperator> plan) {
	bool is_encrypted = !copy_input.encryption_key.empty();
	auto copy_options = GetCopyOptions(context, copy_input);
	if (!copy_options.projection_list.empty() && plan) {
		// generate a projection
		GenerateProjection(context, planner, copy_options.projection_list, plan);
	}

	if (DuckLakeTypes::RequiresCast(copy_options.expected_types)) {
		// Insert a cast projection
		if (plan) {
			InsertCasts(copy_options.expected_types, context, planner, plan);
			// Update the expected types to match the cast types
			copy_options.expected_types = plan->types;
		} else {
			// Still update types. If there is no child-plan node, we expect that whoever inserts chunks (e.g.
			// DuckLakeUpdate, DuckLakeMergeInsert) directly into the physical copy operator will pre-cast the data.
			for (auto &type : copy_options.expected_types) {
				if (DuckLakeTypes::RequiresCast(type)) {
					type = DuckLakeTypes::GetCastedType(type);
				}
			}
		}
	}

	auto copy_return_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	auto &physical_copy = planner
	                          .Make<PhysicalCopyToFile>(copy_return_types, std::move(copy_options.copy_function),
	                                                    std::move(copy_options.bind_data), 1)
	                          .Cast<PhysicalCopyToFile>();

	physical_copy.file_path = std::move(copy_options.file_path);
	physical_copy.use_tmp_file = copy_options.use_tmp_file;
	physical_copy.filename_pattern = std::move(copy_options.filename_pattern);
	physical_copy.file_extension = std::move(copy_options.file_extension);
	physical_copy.overwrite_mode = copy_options.overwrite_mode;
	physical_copy.per_thread_output = copy_options.per_thread_output;
	physical_copy.file_size_bytes = copy_options.file_size_bytes;
	physical_copy.rotate = copy_options.rotate;
	physical_copy.return_type = copy_options.return_type;

	physical_copy.partition_output = copy_options.partition_output;
	physical_copy.write_partition_columns = copy_options.write_partition_columns;
	physical_copy.write_empty_file = copy_options.write_empty_file;
	physical_copy.partition_columns = std::move(copy_options.partition_columns);
	physical_copy.names = std::move(copy_options.names);
	physical_copy.expected_types = std::move(copy_options.expected_types);
	physical_copy.parallel = true;
	physical_copy.hive_file_pattern =
	    copy_input.catalog.UseHiveFilePattern(!is_encrypted, copy_input.schema_id, copy_input.table_id);
	if (plan) {
		physical_copy.children.push_back(*plan);
	}

	return physical_copy;
}

PhysicalOperator &DuckLakeInsert::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                             DuckLakeTableEntry &table, string encryption_key) {
	auto partition_data = table.GetPartitionData();
	optional_idx partition_id;
	if (partition_data) {
		partition_id = partition_data->partition_id;
	}
	vector<LogicalType> return_types;
	return_types.emplace_back(LogicalType::BIGINT);
	return planner.Make<DuckLakeInsert>(return_types, table, partition_id, std::move(encryption_key));
}

string DuckLakeCatalog::GenerateEncryptionKey(ClientContext &context) const {
	if (Encryption() != DuckLakeEncryption::ENCRYPTED) {
		// not encrypted
		return string();
	}
	// generate an encryption key
	auto &engine = RandomEngine::Get(context);
	static constexpr const idx_t ENCRYPTION_KEY_SIZE = 16;
	data_t bytes[ENCRYPTION_KEY_SIZE];
	for (idx_t i = 0; i < ENCRYPTION_KEY_SIZE; i += 4) {
		*reinterpret_cast<uint32_t *>(bytes + i) = engine.NextRandomInteger();
	}
	return string(char_ptr_cast(bytes), ENCRYPTION_KEY_SIZE);
}

PhysicalOperator &DuckLakeCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                              optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for insertion into DuckLake table");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for insertion into DuckLake table");
	}
	if (!op.column_index_map.empty()) {
		plan = planner.ResolveDefaultsProjection(op, *plan);
	}
	auto &ducklake_table = op.table.Cast<DuckLakeTableEntry>();
	auto &ducklake_schema = ducklake_table.ParentSchema().Cast<DuckLakeSchemaEntry>();
	optional_ptr<DuckLakeInlineData> inline_data;

	idx_t data_inlining_row_limit = DataInliningRowLimit(ducklake_schema.GetSchemaId(), ducklake_table.GetTableId());
	// FIXME: we are skipping columns that have conflicting names, we should resolve this
	auto &duck_transaction = DuckLakeTransaction::Get(context, *this);
	auto &metadata_manager = duck_transaction.GetMetadataManager();
	if (data_inlining_row_limit > 0 && !DuckLakeUtil::HasInlinedSystemColumnConflict(ducklake_table.GetColumns()) &&
	    metadata_manager.SupportsInliningTypes(plan->types)) {
		plan = planner.Make<DuckLakeInlineData>(*plan, data_inlining_row_limit);
		inline_data = plan->Cast<DuckLakeInlineData>();
	}
	DuckLakeCopyInput copy_input(context, ducklake_table);
	auto &physical_copy = DuckLakeInsert::PlanCopyForInsert(context, planner, copy_input, plan);
	auto &insert = DuckLakeInsert::PlanInsert(context, planner, ducklake_table, std::move(copy_input.encryption_key));
	if (inline_data) {
		inline_data->insert = insert.Cast<DuckLakeInsert>();
	}
	insert.children.push_back(physical_copy);
	return insert;
}

PhysicalOperator &DuckLakeCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                     LogicalCreateTable &op, PhysicalOperator &plan) {
	auto &create_info = op.info->Base();
	auto &columns = create_info.columns;
	auto &duck_transaction = DuckLakeTransaction::Get(context, *this);
	auto &duck_schema = op.schema.Cast<DuckLakeSchemaEntry>();
	// FIXME: if table already exists and we are doing CREATE IF NOT EXISTS - skip
	reference<PhysicalOperator> root = plan;
	optional_ptr<DuckLakeInlineData> inline_data;
	idx_t data_inlining_row_limit = DataInliningRowLimit(duck_schema.GetSchemaId(), TableIndex());
	auto &metadata_manager = duck_transaction.GetMetadataManager();
	if (data_inlining_row_limit > 0 && !DuckLakeUtil::HasInlinedSystemColumnConflict(columns) &&
	    metadata_manager.SupportsInliningTypes(plan.types)) {
		root = planner.Make<DuckLakeInlineData>(root.get(), data_inlining_row_limit);
		inline_data = root.get().Cast<DuckLakeInlineData>();
	}
	for (auto &col : op.info->Base().columns.Logical()) {
		DuckLakeTypes::CheckSupportedType(col.Type());
	}
	auto table_uuid = duck_transaction.GenerateUUID();
	auto table_data_path =
	    duck_schema.DataPath() + DuckLakeCatalog::GeneratePathFromName(table_uuid, create_info.table);

	DuckLakeCopyInput copy_input(context, duck_schema, columns, table_data_path);
	auto &physical_copy = DuckLakeInsert::PlanCopyForInsert(context, planner, copy_input, root.get());
	auto &insert = planner.Make<DuckLakeInsert>(op.types, op.schema, std::move(op.info), std::move(table_uuid),
	                                            std::move(table_data_path), std::move(copy_input.encryption_key));
	if (inline_data) {
		inline_data->insert = insert.Cast<DuckLakeInsert>();
	}
	insert.children.push_back(physical_copy);
	return insert;
}

} // namespace duckdb
