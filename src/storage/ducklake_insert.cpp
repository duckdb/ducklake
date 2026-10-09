#include "storage/ducklake_catalog.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/encryption_functions.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_field_data.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "common/parquet_file_scanner.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_inline_data.hpp"
#include "storage/ducklake_geo_stats.hpp"
#include "common/ducklake_types.hpp"
#include "functions/ducklake_compaction_functions.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/bind_helpers.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "storage/ducklake_variant_stats.hpp"

namespace duckdb {

//! The partition id that data files written under this spec carry (invalid when the table is not partitioned)
static optional_idx GetPartitionId(optional_ptr<DuckLakePartition> partition_data) {
	optional_idx result;
	if (partition_data) {
		result = partition_data->partition_id;
	}
	return result;
}

DuckLakeInsert::DuckLakeInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types, DuckLakeTableEntry &table,
                               optional_idx partition_id_p, string encryption_key_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 1), table(&table), schema(nullptr),
      partition_id(partition_id_p), encryption_key(std::move(encryption_key_p)) {
}

DuckLakeInsert::DuckLakeInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types,
                               SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info, string table_uuid_p,
                               string table_data_path_p, unique_ptr<DuckLakePartition> ctas_partition_data_p,
                               unique_ptr<DuckLakeSort> ctas_sort_data_p, map<string, string> ctas_table_options_p,
                               string encryption_key_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 1), table(nullptr), schema(&schema),
      info(std::move(info)), table_uuid(std::move(table_uuid_p)), table_data_path(std::move(table_data_path_p)),
      ctas_partition_data(std::move(ctas_partition_data_p)), ctas_sort_data(std::move(ctas_sort_data_p)),
      ctas_table_options(std::move(ctas_table_options_p)), partition_id(GetPartitionId(ctas_partition_data)),
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
		// CTAS: materialize the table entry, cloning the prebuilt spec so the operator's copy survives re-execution.
		auto partition_clone = ctas_partition_data ? make_uniq<DuckLakePartition>(*ctas_partition_data) : nullptr;
		auto sort_clone = ctas_sort_data ? make_uniq<DuckLakeSort>(*ctas_sort_data) : nullptr;

		auto &catalog = schema->catalog;
		auto &ducklake_schema = schema.get_mutable()->Cast<DuckLakeSchemaEntry>();
		auto transaction = catalog.GetCatalogTransaction(context);
		auto created =
		    ducklake_schema.CreateTableExtended(transaction, *info, table_uuid, table_data_path,
		                                        std::move(partition_clone), std::move(sort_clone), ctas_table_options);
		table_ptr = &created->Cast<DuckLakeTableEntry>();
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
		} else if (stats_name == "min_is_exact") {
			column_stats.min_is_exact = StringValue::Get(stats_children[1]) == "true";
		} else if (stats_name == "max_is_exact") {
			column_stats.max_is_exact = StringValue::Get(stats_children[1]) == "true";
		} else if (stats_name == "nan_count") {
			// NaN count of floating point columns; contains_nan is already set from has_nan
			continue;
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
	auto skipped_fields = global_state.table.GetSkippedStatsFields();
	for (idx_t r = 0; r < chunk.size(); r++) {
		DuckLakeDataFile data_file;
		data_file.file_name = chunk.GetValue(0, r).GetValue<string>();
		data_file.row_count = chunk.GetValue(1, r).GetValue<idx_t>();
		data_file.file_size_bytes = chunk.GetValue(2, r).GetValue<idx_t>();
		data_file.footer_size = chunk.GetValue(3, r).GetValue<idx_t>();
		if (chunk.ColumnCount() > 6) {
			auto extra_info = chunk.GetValue(6, r);
			if (!extra_info.IsNull()) {
				for (auto &extra_entry : MapValue::GetChildren(extra_info)) {
					auto &entry_children = StructValue::GetChildren(extra_entry);
					if (StringValue::Get(entry_children[0]) == "row_group_count" && !entry_children[1].IsNull()) {
						data_file.row_group_count =
						    entry_children[1].DefaultCastAs(LogicalType::UBIGINT).GetValue<idx_t>();
					}
				}
			}
		}
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
			auto &field_id = table.GetFieldId(StringsToIdentifiers(column_names), &name_offset);
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
					table.ThrowNotNullViolation(column_names[0]);
				}
			}

			if (skipped_fields.count(field_id.GetFieldIndex().index)) {
				column_stats.ClearBounds();
			}
			data_file.column_stats.insert(make_pair(field_id.GetFieldIndex(), std::move(column_stats)));
		}
		// finalize variant stats
		for (auto &entry : variant_stats) {
			data_file.column_stats.insert(make_pair(entry.first, entry.second.Finalize()));
		}
		// extract the partition info
		auto partition_info = chunk.GetValue(5, r);
		if (!partition_info.IsNull()) {
			auto &partition_children = MapValue::GetChildren(partition_info);
			for (idx_t col_idx = 0; col_idx < partition_children.size(); col_idx++) {
				auto &struct_children = StructValue::GetChildren(partition_children[col_idx]);

				DuckLakeFilePartition file_partition_info;
				file_partition_info.partition_column_idx = col_idx;
				file_partition_info.partition_value = struct_children[1];
				data_file.partition_values.push_back(std::move(file_partition_info));
			}
		}
		if (data_file.row_count == 0) {
			global_state.empty_files.push_back(std::move(data_file.file_name));
			continue;
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
	chunk.data[0].Append(value);
	chunk.SetChildCardinality(1);
	return SourceResultType::FINISHED;
}
//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
void DuckLakeInsert::RemoveEmptyFiles(ClientContext &context, DuckLakeInsertGlobalState &gstate) {
	if (gstate.empty_files.empty()) {
		return;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	fs.RemoveFiles(gstate.empty_files);
	gstate.empty_files.clear();
}

SinkFinalizeType DuckLakeInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeInsertGlobalState>();

	RemoveEmptyFiles(context, global_state);
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
	result["Table Name"] = (table ? table->name : info->Base().GetTableName()).GetIdentifierName();
	return result;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
CopyFunctionCatalogEntry &DuckLakeFunctions::GetCopyFunction(ClientContext &context, const Identifier &name) {
	D_ASSERT(!name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(context);

	auto entry = system_catalog.GetEntry<CopyFunctionCatalogEntry>(
	    context, QualifiedName(system_catalog.GetName(), Identifier::DefaultSchema(), name),
	    OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		throw MissingExtensionException(
		    "Could not load the copy function for %s. Try explicitly loading the %s extension", name, name);
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

DuckLakeCopyInput::DuckLakeCopyInput(ClientContext &context, DuckLakeTableEntry &table, const string &hive_partition)
    : catalog(table.ParentCatalog().Cast<DuckLakeCatalog>()), columns(table.GetColumns()),
      data_path(table.DataPath() + hive_partition) {
	partition_data = table.GetPartitionData();
	field_data = table.GetFieldData();
	schema_id = table.ParentSchema().Cast<DuckLakeSchemaEntry>().GetSchemaId();
	table_id = table.GetTableId();
	table_options = table.GetTableOptions();
	encryption_key = catalog.GenerateEncryptionKey(context);
}

DuckLakeCopyInput::DuckLakeCopyInput(ClientContext &context, DuckLakeSchemaEntry &schema, const ColumnList &columns,
                                     const string &data_path_p, DuckLakeFieldData &field_data_p,
                                     optional_ptr<DuckLakePartition> partition_data_p)
    : catalog(schema.ParentCatalog().Cast<DuckLakeCatalog>()), columns(columns), data_path(data_path_p) {
	field_data = &field_data_p;
	partition_data = partition_data_p;
	schema_id = schema.GetSchemaId();
	encryption_key = catalog.GenerateEncryptionKey(context);
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
		ColumnBinding column_binding(TableIndex(copy_input.get_table_index.GetIndex()), ProjectionIndex(column_index));
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

static unique_ptr<Expression> GetPartitionExpression(ClientContext &context, DuckLakeCopyInput &copy_input,
                                                     const DuckLakePartitionField &field) {
	auto column_expr = GetColumnReference(copy_input, field.field_id);
	return DuckLakePartitionUtils::ApplyPartitionTransform(context, std::move(column_expr), field);
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
			copy_options.copy->partition_columns.push_back(col_idx.GetIndex());
		}
		return;
	}
	idx_t virtual_column_count = static_cast<idx_t>(WriteRowId(copy_input.virtual_columns)) +
	                             static_cast<idx_t>(WriteSnapshotId(copy_input.virtual_columns));
	// if we have partition columns that are NOT identity, we need to compute them separately, and NOT write them
	idx_t partition_column_start = copy_input.columns.PhysicalColumnCount() + virtual_column_count;
	for (idx_t part_idx = 0; part_idx < copy_input.partition_data->fields.size(); part_idx++) {
		copy_options.copy->partition_columns.push_back(partition_column_start++);
	}
	copy_options.copy->write_partition_columns = false;

	idx_t col_idx = 0;
	for (auto &col : copy_input.columns.Physical()) {
		copy_options.projection_list.push_back(CreateColumnReference(copy_input, col.Type(), col_idx++));
	}
	// push any projected virtual columns
	for (idx_t i = 0; i < virtual_column_count; i++) {
		copy_options.projection_list.push_back(CreateColumnReference(copy_input, LogicalType::BIGINT, col_idx++));
	}
	// push the partition expressions
	auto &fields = copy_input.partition_data->fields;
	auto key_names = DuckLakePartitionUtils::GetPartitionKeyNames(*copy_input.partition_data, *copy_input.field_data);
	for (idx_t part_idx = 0; part_idx < fields.size(); part_idx++) {
		auto expr = GetPartitionExpression(context, copy_input, fields[part_idx]);
		copy_options.copy->names.push_back(Identifier(key_names[part_idx]));
		copy_options.copy->expected_types.push_back(expr->GetReturnType());
		copy_options.projection_list.push_back(std::move(expr));
	}
}

unique_ptr<CopyInfo> DuckLakeInsert::GetParquetCopyInfo(const string &file_path, Value field_ids,
                                                        const string &encryption_key) {
	auto info = make_uniq<CopyInfo>();
	info->file_path = file_path;
	info->format = "parquet";
	info->is_from = false;
	info->options["field_ids"].push_back(std::move(field_ids));
	if (!encryption_key.empty()) {
		info->options["encryption_config"].push_back(ParquetFileScanner::EncryptionConfig(encryption_key));
	}
	return info;
}

DuckLakeCopyOptions DuckLakeInsert::GetCopyOptions(ClientContext &context, DuckLakeCopyInput &copy_input) {
	auto &catalog = copy_input.catalog;
	// generate the field ids to be written by the parquet writer
	auto info =
	    GetParquetCopyInfo(copy_input.data_path, WrittenFieldIds(*copy_input.field_data, copy_input.virtual_columns),
	                       copy_input.encryption_key);
	auto &schema_id = copy_input.schema_id;
	auto &table_id = copy_input.table_id;
	static const pair<const char *, const char *> PARQUET_OPTIONS[] = {
	    {"parquet_compression", "compression"},
	    {"parquet_version", "parquet_version"},
	    {"parquet_compression_level", "compression_level"}};
	for (auto &option : PARQUET_OPTIONS) {
		string option_value;
		if (catalog.TryGetConfigOption(option.first, option_value, schema_id, table_id, &copy_input.table_options)) {
			info->options[option.second].emplace_back(option_value);
		}
	}
	string per_thread_output_str;
	bool per_thread_output = false;
	if (catalog.TryGetConfigOption("per_thread_output", per_thread_output_str, schema_id, table_id,
	                               &copy_input.table_options)) {
		per_thread_output = per_thread_output_str == "true";
	}
	idx_t target_file_size = catalog.GetTargetFileSize(context, schema_id, table_id, &copy_input.table_options);

	// Always use native parquet geometry for writing
	info->options["geoparquet_version"].emplace_back("NONE");

	// Get Parquet Copy function
	auto &copy_fun = DuckLakeFunctions::GetCopyFunction(context, "parquet");

	auto &fs = FileSystem::GetFileSystem(context);
	DuckLakeUtil::EnsureDirectoryExists(fs, copy_input.data_path);

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

	auto function_data =
	    copy_fun.function.copy_to_bind(context, bind_input, StringsToIdentifiers(names_to_write), types_to_write);

	DuckLakeCopyOptions result;
	result.copy =
	    make_uniq<LogicalCopyToFile>(copy_fun.function, std::move(function_data), std::move(info), TableIndex());
	auto &copy = *result.copy;
	copy.use_tmp_file = false;
	copy.filename_pattern.SetFilenamePattern("ducklake-{uuidv7}");
	static constexpr idx_t MINIMUM_WRITE_FILE_SIZE = 4096;
	copy.file_size_bytes = MaxValue<idx_t>(target_file_size, MINIMUM_WRITE_FILE_SIZE);
	copy.rotate = true;
	copy.batch_size = catalog.GetConfigOption<idx_t>("parquet_row_group_size", schema_id, table_id,
	                                                 DEFAULT_ROW_GROUP_SIZE, &copy_input.table_options);
	if (copy_input.partition_data) {
		copy.partition_output = true;
		copy.write_empty_file = true;
	} else {
		copy.partition_output = false;
		copy.write_empty_file = false;
	}
	copy.file_path = PhysicalCopyToFile::GetTrimmedPath(context, copy_input.data_path);
	copy.file_extension = "parquet";
	copy.overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
	copy.per_thread_output = per_thread_output;
	copy.write_partition_columns = true;
	copy.return_type = CopyFunctionReturnType::WRITTEN_FILE_STATISTICS;
	copy.hive_file_pattern =
	    catalog.UseHiveFilePattern(copy_input.encryption_key.empty(), schema_id, table_id, &copy_input.table_options);
	copy.names = StringsToIdentifiers(names_to_write);
	copy.expected_types = types_to_write;

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
		types.push_back(expr->GetReturnType());
	}
	auto &proj =
	    planner.Make<PhysicalProjection>(std::move(types), std::move(expressions), plan->estimated_cardinality);
	proj.children.push_back(*plan);
	plan = proj;
}

PhysicalOperator &DuckLakeInsert::PlanCopyForInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    DuckLakeCopyInput &copy_input,
                                                    optional_ptr<PhysicalOperator> plan) {
	auto copy_options = GetCopyOptions(context, copy_input);
	auto &copy = *copy_options.copy;
	if (!copy_options.projection_list.empty()) {
		// generate a projection
		GenerateProjection(context, planner, copy_options.projection_list, plan);
	}

	string row_group_size_bytes;
	if (copy_input.catalog.TryGetConfigOption("parquet_row_group_size_bytes", row_group_size_bytes,
	                                          copy_input.schema_id, copy_input.table_id, &copy_input.table_options)) {
		copy.batch_size_bytes = DBConfig::ParseMemoryLimit(row_group_size_bytes + " bytes");
	}
	copy.SetEstimatedCardinality(1);
	copy.ResolveOperatorTypes();
	auto &physical_copy = planner.CreatePlan(copy, *plan).Cast<PhysicalCopyToFile>();
	if (copy_input.ordered_input && copy.function.execution_mode) {
		// the rows are sorted and the batch copy operator does not rotate files by size
		auto execution_mode = copy.function.execution_mode(true, false);
		physical_copy.parallel = execution_mode == CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
	}
	return physical_copy;
}

PhysicalOperator &DuckLakeInsert::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                             DuckLakeTableEntry &table, string encryption_key) {
	vector<LogicalType> return_types;
	return_types.emplace_back(LogicalType::BIGINT);
	return planner.Make<DuckLakeInsert>(return_types, table, GetPartitionId(table.GetPartitionData()),
	                                    std::move(encryption_key));
}

string DuckLakeCatalog::GenerateEncryptionKey(ClientContext &context) const {
	if (Encryption() != DuckLakeEncryption::ENCRYPTED) {
		// not encrypted
		return string();
	}
	// 32 bytes = AES-256; generation only, existing keys are read back at whatever length they were
	// written, so no migration is implied
	static constexpr const idx_t ENCRYPTION_KEY_SIZE = 32;
	data_t bytes[ENCRYPTION_KEY_SIZE];
	EncryptionEngine::GenerateRandomKey(DatabaseInstance::GetDatabase(context), bytes, ENCRYPTION_KEY_SIZE);
	return string(char_ptr_cast(bytes), ENCRYPTION_KEY_SIZE);
}

//! Wrap the plan in an ORDER BY on the sort keys, usable before the table entry exists (CTAS)
static optional_ptr<PhysicalOperator> PlanInsertSort(ClientContext &context, PhysicalPlanGenerator &planner,
                                                     PhysicalOperator &plan, const ColumnList &columns,
                                                     const Identifier &table_name, const DuckLakeSort &sort_data) {
	auto binder = Binder::CreateBinder(context);
	TableIndex table_index(0);
	auto orders = DuckLakeCompactor::BindSortOrders(*binder, columns, table_name, table_index,
	                                                DuckLakeCompactor::ParseSortOrders(sort_data));
	if (orders.empty()) {
		return nullptr;
	}

	// Convert BoundColumnRefExpression to BoundReferenceExpression for physical plan
	auto bindings = LogicalOperator::GenerateColumnBindings(table_index, plan.GetTypes().size());
	ColumnBindingResolver resolver(std::move(bindings), plan.GetTypes());
	for (auto &order : orders) {
		resolver.VisitExpression(&order.expression);
	}

	auto projection_map = PhysicalJoin::FillProjectionMap(plan, {});
	auto &order_op = planner.Make<PhysicalOrder>(plan.types, std::move(orders), std::move(projection_map),
	                                             plan.estimated_cardinality);
	order_op.children.push_back(plan);
	return &order_op;
}

DuckLakeInsertPipeline DuckLakeInsert::PlanInsertPipeline(ClientContext &context, PhysicalPlanGenerator &planner,
                                                          PhysicalOperator &plan, const ColumnList &columns,
                                                          const Identifier &table_name,
                                                          optional_ptr<DuckLakeSort> sort_data, bool sort_on_insert,
                                                          idx_t data_inlining_row_limit) {
	DuckLakeInsertPipeline result {plan, nullptr, false};
	if (sort_data && sort_on_insert) {
		auto sorted_plan = PlanInsertSort(context, planner, result.root.get(), columns, table_name, *sort_data);
		if (sorted_plan) {
			result.root = *sorted_plan;
			result.sorted = true;
		}
	}
	if (data_inlining_row_limit > 0) {
		auto &inline_data = planner.Make<DuckLakeInlineData>(result.root.get(), data_inlining_row_limit);
		result.inline_data = inline_data.Cast<DuckLakeInlineData>();
		result.root = inline_data;
		// with sort_on_insert off only the Parquet bound overflow rows are sorted, after inlining
		if (sort_data && !sort_on_insert) {
			auto sorted_plan = PlanInsertSort(context, planner, result.root.get(), columns, table_name, *sort_data);
			if (sorted_plan) {
				result.root = *sorted_plan;
				result.sorted = true;
			}
		}
	}
	return result;
}

PhysicalOperator &DuckLakeInsertPipeline::AttachInsert(PhysicalOperator &insert, PhysicalOperator &copy) {
	if (inline_data) {
		inline_data->insert = insert.Cast<DuckLakeInsert>();
	}
	insert.children.push_back(copy);
	return insert;
}

DuckLakeVerifyNotNull::DuckLakeVerifyNotNull(PhysicalPlan &physical_plan, PhysicalOperator &child,
                                             Identifier table_name, vector<pair<idx_t, Identifier>> columns)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, child.types, child.estimated_cardinality),
      table_name(std::move(table_name)), columns(std::move(columns)) {
	children.push_back(child);
}

PhysicalOperator &DuckLakeVerifyNotNull::Plan(PhysicalPlanGenerator &planner, DuckLakeTableEntry &table,
                                              PhysicalOperator &plan) {
	auto not_null_fields = table.GetNotNullFields();
	vector<pair<idx_t, Identifier>> columns;
	for (auto &col : table.GetColumns().Physical()) {
		// the file statistics of a nested column are per field, so they do not show a NULL value of the column
		if (col.Type().IsNested() && not_null_fields.count(col.Name().GetIdentifierName())) {
			columns.emplace_back(col.Physical().index, col.Name());
		}
	}
	if (columns.empty()) {
		return plan;
	}
	return planner.Make<DuckLakeVerifyNotNull>(plan, table.name, std::move(columns));
}

OperatorResultType DuckLakeVerifyNotNull::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                  GlobalOperatorState &gstate, OperatorState &state) const {
	for (auto &column : columns) {
		if (VectorOperations::HasNull(input.data[column.first])) {
			throw ConstraintException("NOT NULL constraint failed: %s.%s", SQLIdentifier(table_name),
			                          SQLIdentifier(column.second));
		}
	}
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

string DuckLakeVerifyNotNull::GetName() const {
	return "DUCKLAKE_VERIFY_NOT_NULL";
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
	auto &verified_plan = DuckLakeVerifyNotNull::Plan(planner, ducklake_table, *plan);
	auto pipeline = DuckLakeInsert::PlanInsertPipeline(
	    context, planner, verified_plan, ducklake_table.GetColumns(), ducklake_table.name, ducklake_table.GetSortData(),
	    SortOnInsert(ducklake_table), GetInliningLimit(context, ducklake_table));

	DuckLakeCopyInput copy_input(context, ducklake_table);
	copy_input.ordered_input = pipeline.sorted;
	auto &physical_copy = DuckLakeInsert::PlanCopyForInsert(context, planner, copy_input, pipeline.root.get());
	auto &insert = DuckLakeInsert::PlanInsert(context, planner, ducklake_table, std::move(copy_input.encryption_key));
	return pipeline.AttachInsert(insert, physical_copy);
}

PhysicalOperator &DuckLakeCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                     LogicalCreateTable &op, PhysicalOperator &plan) {
	// CTAS bypasses the catalog CreateTable entry point, so the create table gate runs here
	auto supports_create_table = SupportsCreateTable(*op.info);
	if (supports_create_table.HasError()) {
		supports_create_table.Throw();
	}
	auto &create_info = op.info->Base();
	auto &columns = create_info.columns;
	auto &duck_transaction = DuckLakeTransaction::Get(context, *this);
	auto &duck_schema = op.schema.Cast<DuckLakeSchemaEntry>();

	// The table entry doesn't exist yet; build field/partition/sort specs now so the physical write can use them.
	auto field_data = DuckLakeFieldData::FromColumns(columns);
	unique_ptr<DuckLakePartition> partition_data;
	if (!create_info.partition_keys.empty()) {
		partition_data =
		    DuckLakeTableEntry::BuildPartitionData(duck_transaction, columns, *field_data, create_info.partition_keys);
	}
	auto sort_data = DuckLakeTableEntry::BuildSortData(duck_transaction, columns, create_info.sort_keys);
	auto table_options =
	    DuckLakeTableEntry::ParseTableOptions(context, *this, create_info.options, columns, *field_data,
	                                          partition_data.get(), create_info.GetTableName().GetIdentifierName());

	// No table id yet, so the WITH options stand in for the table scope
	auto pipeline = DuckLakeInsert::PlanInsertPipeline(
	    context, planner, plan, columns, create_info.GetTableName(), sort_data.get(),
	    SortOnInsert(duck_schema.GetSchemaId(), TableIndex(), &table_options),
	    GetInliningLimit(context, duck_schema.GetSchemaId(), TableIndex(), columns, &table_options));

	DuckLakeTypes::CheckSupportedTypes(columns, GetDuckLakeVersion());
	auto table_uuid = duck_transaction.GenerateUUID();
	auto table_data_path =
	    duck_schema.GenerateTableDataPath(table_uuid, create_info.GetTableName().GetIdentifierName());

	DuckLakeCopyInput copy_input(context, duck_schema, columns, table_data_path, *field_data, partition_data.get());
	copy_input.table_options = table_options;
	copy_input.ordered_input = pipeline.sorted;
	auto &physical_copy = DuckLakeInsert::PlanCopyForInsert(context, planner, copy_input, pipeline.root.get());

	auto &insert =
	    planner.Make<DuckLakeInsert>(op.types, op.schema, std::move(op.info), std::move(table_uuid),
	                                 std::move(table_data_path), std::move(partition_data), std::move(sort_data),
	                                 std::move(table_options), std::move(copy_input.encryption_key));
	return pipeline.AttachInsert(insert, physical_copy);
}

} // namespace duckdb
