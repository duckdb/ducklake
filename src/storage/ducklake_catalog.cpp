#include "storage/ducklake_catalog.hpp"

#include "common/ducklake_types.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "storage/ducklake_initializer.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_manager.hpp"
#include "storage/ducklake_view_entry.hpp"
#include "duckdb/main/database_path_and_type.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/function/macro_function.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "storage/ducklake_macro_entry.hpp"

namespace duckdb {

DuckLakeCatalog::DuckLakeCatalog(AttachedDatabase &db_p, DuckLakeOptions options_p)
    : Catalog(db_p), options(std::move(options_p)), last_uncommitted_catalog_version(TRANSACTION_ID_START) {
	// figure out the metadata server type
	auto entry = options.metadata_parameters.find("type");
	if (entry != options.metadata_parameters.end()) {
		// metadata type is explicitly provided - fetch it
		metadata_type = entry->second.ToString();
	} else {
		// extract from the connection string
		string path = options.metadata_path;
		DBPathAndType::ExtractExtensionPrefix(path, metadata_type);
	}
}

DuckLakeCatalog::~DuckLakeCatalog() {
}

void DuckLakeCatalog::Initialize(bool load_builtin) {
	throw InternalException("DuckLakeCatalog cannot be initialized without a client context");
}

void DuckLakeCatalog::Initialize(optional_ptr<ClientContext> context, bool load_builtin) {
}

void DuckLakeCatalog::FinalizeLoad(optional_ptr<ClientContext> context) {
	// initialize the metadata database
	unique_ptr<Connection> con;
	if (!context) {
		con = make_uniq<Connection>(GetDatabase());
		con->BeginTransaction();
		context = con->context.get();
	}
	// If data_inlining_row_limit wasn't explicitly set via ATTACH options,
	// use the global DuckDB setting as the default
	if (options.config_options.find("data_inlining_row_limit") == options.config_options.end()) {
		Value setting_val;
		if (context->TryGetCurrentSetting("ducklake_default_data_inlining_row_limit", setting_val)) {
			auto limit = setting_val.GetValue<idx_t>();
			options.config_options["data_inlining_row_limit"] = to_string(limit);
		}
	}
	DuckLakeInitializer initializer(*context, *this, options);
	initializer.Initialize();
	db.tags["data_path"] = DataPath();
	if (con) {
		con->Commit();
	}
	initialized = true;
}

static bool CanGeneratePathFromName(const string &name) {
	for (auto c : name) {
		if (StringUtil::CharacterIsAlphaNumeric(c)) {
			continue;
		}
		if (c == '_' || c == '-') {
			continue;
		}
		return false;
	}
	return true;
}

string DuckLakeCatalog::GeneratePathFromName(const string &uuid, const string &name) {
	// if the name has special characters we fallback to uuid
	if (CanGeneratePathFromName(name)) {
		return name + separator;
	}
	return uuid + separator;
}

optional_ptr<CatalogEntry> DuckLakeCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto schema = GetSchema(transaction, info.schema, OnEntryNotFound::RETURN_NULL);
	if (schema) {
		if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return nullptr;
		}
		if (info.on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			return nullptr;
		}
		// drop the existing entry
		DropInfo drop_info;
		drop_info.type = CatalogType::SCHEMA_ENTRY;
		drop_info.name = info.schema;
		DropSchema(transaction.GetContext(), drop_info);
	}
	auto &duck_transaction = transaction.transaction->Cast<DuckLakeTransaction>();
	//! get a local table-id
	auto schema_id = SchemaIndex(duck_transaction.GetLocalCatalogId());
	auto schema_uuid = duck_transaction.GenerateUUID();
	auto schema_data_path = DataPath() + DuckLakeCatalog::GeneratePathFromName(schema_uuid, info.schema);
	auto schema_entry =
	    make_uniq<DuckLakeSchemaEntry>(*this, info, schema_id, std::move(schema_uuid), std::move(schema_data_path));
	auto result = schema_entry.get();
	duck_transaction.CreateEntry(std::move(schema_entry));
	return result;
}

void DuckLakeCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	auto schema = GetSchema(GetCatalogTransaction(context), info.name, info.if_not_found);
	if (!schema) {
		return;
	}
	auto &transaction = DuckLakeTransaction::Get(context, *this);
	auto &ducklake_schema = schema->Cast<DuckLakeSchemaEntry>();
	ducklake_schema.TryDropSchema(transaction, info.cascade);
	transaction.DropEntry(*schema);
}

void DuckLakeCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	if (!initialized) {
		return;
	}
	auto &duck_transaction = DuckLakeTransaction::Get(context, *this);
	auto set = duck_transaction.GetTransactionLocalSchemas();
	if (set) {
		for (auto &entry : set->GetEntries()) {
			callback(entry.second->Cast<SchemaCatalogEntry>());
		}
	}
	auto snapshot = duck_transaction.GetSnapshot();
	auto &schemas = GetSchemaForSnapshot(duck_transaction, snapshot);
	for (auto &schema : schemas.GetEntries()) {
		auto &schema_entry = schema.second->Cast<SchemaCatalogEntry>();
		if (duck_transaction.IsDeleted(schema_entry)) {
			continue;
		}
		callback(schema_entry);
	}
}

optional_ptr<CatalogEntry> DuckLakeCatalog::GetEntryById(DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot,
                                                         SchemaIndex schema_id) {
	auto local_entry = transaction.GetLocalEntryById(schema_id);
	if (local_entry) {
		return local_entry;
	}
	auto &schema = GetSchemaForSnapshot(transaction, snapshot);
	return schema.GetEntryById(schema_id);
}

optional_ptr<CatalogEntry> DuckLakeCatalog::GetEntryById(DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot,
                                                         TableIndex table_id) {
	auto local_entry = transaction.GetLocalEntryById(table_id);
	if (local_entry) {
		return local_entry;
	}
	auto &schema = GetSchemaForSnapshot(transaction, snapshot);
	return schema.GetEntryById(table_id);
}

idx_t DuckLakeCatalog::GetBeginSnapshotForTable(TableIndex table_id, DuckLakeTransaction &transaction) {
	auto &metadata_manager = transaction.GetMetadataManager();
	return metadata_manager.GetBeginSnapshotForTable(table_id);
}

DuckLakeCatalogSet &DuckLakeCatalog::GetSchemaForSnapshot(DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot) {
	lock_guard<mutex> guard(schemas_lock);
	auto entry = schemas.find(snapshot.schema_version);
	if (entry != schemas.end()) {
		// this schema version is already cached
		return *entry->second;
	}
	// load the schema version from the metadata manager
	auto schema = LoadSchemaForSnapshot(transaction, snapshot);
	auto &result = *schema;
	schemas.insert(make_pair(snapshot.schema_version, std::move(schema)));
	return result;
}

static unique_ptr<DuckLakeFieldId> TransformColumnType(DuckLakeColumnInfo &col) {
	DuckLakeColumnData col_data;
	col_data.id = col.id;
	if (col.children.empty()) {
		auto col_type = DuckLakeTypes::FromString(col.type);
		col_data.initial_default = col.initial_default.DefaultCastAs(col_type);
		if (col.default_value.IsNull()) {
			col_data.default_value = make_uniq<ConstantExpression>(Value());
		} else {
			if (col.default_value_type == "literal") {
				col_data.default_value = make_uniq<ConstantExpression>(col.default_value);
			} else if (col.default_value_type == "expression") {
				auto sql_expr = Parser::ParseExpressionList(col.default_value.GetValue<string>());
				if (sql_expr.size() != 1) {
					throw InternalException("Expected a single expression");
				}
				col_data.default_value = std::move(sql_expr[0]);
			} else {
				throw NotImplementedException("Column type %s is not supported", col.default_value_type);
			}
		}
		return make_uniq<DuckLakeFieldId>(std::move(col_data), col.name, std::move(col_type));
	}
	if (StringUtil::CIEquals(col.type, "struct")) {
		child_list_t<LogicalType> child_types;
		vector<unique_ptr<DuckLakeFieldId>> child_fields;
		for (auto &child_col : col.children) {
			auto child_id = TransformColumnType(child_col);
			child_types.emplace_back(make_pair(std::move(child_col.name), child_id->Type()));
			child_fields.push_back(std::move(child_id));
		}
		return make_uniq<DuckLakeFieldId>(std::move(col_data), col.name, LogicalType::STRUCT(std::move(child_types)),
		                                  std::move(child_fields));
	}
	if (StringUtil::CIEquals(col.type, "list")) {
		if (col.children.size() != 1) {
			throw InvalidInputException("Lists must have a single child entry");
		}
		auto child_id = TransformColumnType(col.children[0]);
		auto child_type = child_id->Type();
		vector<unique_ptr<DuckLakeFieldId>> child_fields;
		child_fields.push_back(std::move(child_id));
		return make_uniq<DuckLakeFieldId>(std::move(col_data), col.name, LogicalType::LIST(child_type),
		                                  std::move(child_fields));
	}
	if (StringUtil::CIEquals(col.type, "map")) {
		if (col.children.size() != 2) {
			throw InvalidInputException("Maps must have two child entries");
		}
		auto key_id = TransformColumnType(col.children[0]);
		auto value_id = TransformColumnType(col.children[1]);
		auto key_type = key_id->Type();
		auto value_type = value_id->Type();
		vector<unique_ptr<DuckLakeFieldId>> child_fields;
		child_fields.push_back(std::move(key_id));
		child_fields.push_back(std::move(value_id));
		return make_uniq<DuckLakeFieldId>(std::move(col_data), col.name,
		                                  LogicalType::MAP(std::move(key_type), std::move(value_type)),
		                                  std::move(child_fields));
	}
	throw InvalidInputException("Unrecognized nested type \"%s\"", col.type);
}

unique_ptr<CreateMacroInfo> CreateMacroInfoFromDucklake(ClientContext &context, DuckLakeMacroInfo &macro,
                                                        string schema_name) {
	CatalogType type;
	if (macro.implementations.front().type == "scalar") {
		type = CatalogType::MACRO_ENTRY;
	} else if (macro.implementations.front().type == "table") {
		type = CatalogType::TABLE_MACRO_ENTRY;
	} else {
		throw NotImplementedException("Macro type %s is not implemented", macro.implementations.front().type);
	}
	auto macro_info = make_uniq<CreateMacroInfo>(type);
	macro_info->name = macro.macro_name;
	macro_info->schema = schema_name;
	macro_info->temporary = false;
	macro_info->internal = false;
	for (auto &impl : macro.implementations) {
		unique_ptr<MacroFunction> macro_function;
		if (impl.type == "scalar") {
			auto sql_expr = Parser::ParseExpressionList(impl.sql);
			if (sql_expr.size() != 1) {
				throw InternalException("Expected a single expression");
			}
			macro_function = make_uniq<ScalarMacroFunction>(std::move(sql_expr[0]));
		} else if (impl.type == "table") {
			Parser parser;
			parser.ParseQuery(impl.sql);
			if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
				throw InternalException("Expected a single select statement");
			}
			auto node = std::move(parser.statements[0]->Cast<SelectStatement>().node);
			macro_function = make_uniq<TableMacroFunction>(std::move(node));
		} else {
			throw InternalException("Unrecognized macro type %s in CreateMacroInfoFromDucklake", impl.type);
		}
		vector<unique_ptr<ParsedExpression>> expr_list;
		for (auto &param : impl.parameters) {
			expr_list = Parser::ParseExpressionList(param.default_value.ToSQLString());
			if (expr_list.size() != 1) {
				throw InternalException("Expected a single expression");
			}
			macro_function->parameters.push_back(make_uniq<ColumnRefExpression>(param.parameter_name));
			auto &expression = expr_list[0]->Cast<ConstantExpression>();
			auto expr_type = DuckLakeTypes::FromString(param.default_value_type);
			if (expr_type.id() != LogicalTypeId::UNKNOWN) {
				expression.value = expression.value.CastAs(context, expr_type);
				macro_function->default_parameters.insert(make_pair(param.parameter_name, std::move(expr_list[0])));
			}
			macro_function->types.push_back(DuckLakeTypes::FromString(param.parameter_type));
		}
		macro_info->macros.push_back(std::move(macro_function));
	}
	return macro_info;
}

unique_ptr<DuckLakeCatalogSet> DuckLakeCatalog::LoadSchemaForSnapshot(DuckLakeTransaction &transaction,
                                                                      DuckLakeSnapshot snapshot) {
	auto &metadata_manager = transaction.GetMetadataManager();
	auto catalog = metadata_manager.GetCatalogForSnapshot(snapshot);
	ducklake_entries_map_t schema_map;
	for (auto &schema : catalog.schemas) {
		CreateSchemaInfo schema_info;
		schema_info.schema = schema.name;
		auto schema_entry = make_uniq<DuckLakeSchemaEntry>(*this, schema_info, schema.id, std::move(schema.uuid),
		                                                   std::move(schema.path));
		schema_map.insert(make_pair(std::move(schema.name), std::move(schema_entry)));
	}

	auto schema_set = make_uniq<DuckLakeCatalogSet>(std::move(schema_map));
	auto &schema_id_map = schema_set->GetSchemaIdMap();
	// load the table entries
	for (auto &table : catalog.tables) {
		// find the schema for the table
		auto entry = schema_id_map.find(table.schema_id);
		if (entry == schema_id_map.end()) {
			throw InvalidInputException(
			    "Failed to load DuckLake - could not find schema that corresponds to the table entry \"%s\"",
			    table.name);
		}
		auto &schema_entry = entry->second.get();
		auto create_table_info = make_uniq<CreateTableInfo>(schema_entry, table.name);
		for (auto &tag : table.tags) {
			if (tag.key == "comment") {
				create_table_info->comment = tag.value;
			} else {
				create_table_info->tags[tag.key] = tag.value;
			}
		}
		// parse the columns
		auto field_data = make_shared_ptr<DuckLakeFieldData>();
		case_insensitive_set_t not_null_columns;
		for (auto &col_info : table.columns) {
			auto field_id = TransformColumnType(col_info);
			if (!col_info.nulls_allowed) {
				not_null_columns.insert(col_info.name);
			}
			ColumnDefinition column(std::move(col_info.name), field_id->Type());
			for (auto &tag : col_info.tags) {
				if (tag.key == "comment") {
					column.SetComment(tag.value);
				} else {
					throw NotImplementedException("Only comment tags are supported for columns currently");
				}
			}
			auto default_val = field_id->GetDefault();
			if (default_val) {
				column.SetDefaultValue(std::move(default_val));
			}
			create_table_info->columns.AddColumn(std::move(column));
			field_data->Add(std::move(field_id));
		}
		// create the NOT NULL constraints
		for (auto &not_null_col : not_null_columns) {
			auto &col = create_table_info->columns.GetColumn(not_null_col);
			create_table_info->constraints.push_back(make_uniq<NotNullConstraint>(col.Logical()));
		}
		// create the table and add it to the schema set
		auto table_entry = make_uniq<DuckLakeTableEntry>(
		    *this, schema_entry, *create_table_info, table.id, std::move(table.uuid), std::move(table.path),
		    std::move(field_data), optional_idx(), std::move(table.inlined_data_tables), LocalChangeType::NONE);
		schema_set->AddEntry(schema_entry, table.id, std::move(table_entry));
	}

	// load the view entries
	for (auto &view : catalog.views) {
		// find the schema for the view
		auto entry = schema_id_map.find(view.schema_id);
		if (entry == schema_id_map.end()) {
			throw InvalidInputException(
			    "Failed to load DuckLake - could not find schema that corresponds to the view entry \"%s\"", view.name);
		}
		auto &schema_entry = entry->second.get();
		auto create_view_info = make_uniq<CreateViewInfo>(schema_entry, view.name);
		create_view_info->aliases = view.column_aliases;
		for (auto &tag : view.tags) {
			if (tag.key == "comment") {
				create_view_info->comment = tag.value;
			} else {
				create_view_info->tags[tag.key] = tag.value;
			}
		}
		auto view_entry =
		    make_uniq<DuckLakeViewEntry>(*this, schema_entry, *create_view_info, view.id, std::move(view.uuid),
		                                 std::move(view.sql), LocalChangeType::NONE);
		schema_set->AddEntry(schema_entry, view.id, std::move(view_entry));
	}

	// load the macros
	for (auto &macro : catalog.macros) {
		auto entry = schema_id_map.find(macro.schema_id);
		if (entry == schema_id_map.end()) {
			throw InvalidInputException(
			    "Failed to load DuckLake - could not find schema that corresponds to the macro entry \"%s\"",
			    macro.macro_name);
		}
		auto &schema_entry = entry->second.get();
		auto create_macro = CreateMacroInfoFromDucklake(*transaction.context.lock(), macro, schema_entry.name);
		if (macro.implementations.front().type == "scalar") {
			auto macro_catalog_entry =
			    make_uniq<DuckLakeScalarMacroEntry>(*this, schema_entry, *create_macro, macro.macro_id);
			schema_set->AddEntry(schema_entry, macro.macro_id, std::move(macro_catalog_entry));
		} else if (macro.implementations.front().type == "table") {
			auto macro_catalog_entry =
			    make_uniq<DuckLakeTableMacroEntry>(*this, schema_entry, *create_macro, macro.macro_id);
			schema_set->AddEntry(schema_entry, macro.macro_id, std::move(macro_catalog_entry));
		} else {
			throw InvalidInputException("Macro type %s is not accepted", macro.implementations.front().type);
		}
	}

	// load the partition entries
	for (auto &entry : catalog.partitions) {
		auto table = schema_set->GetEntryById(entry.table_id);
		if (!table || table->type != CatalogType::TABLE_ENTRY) {
			throw InvalidInputException("Could not find matching table for partition entry");
		}
		auto partition = make_uniq<DuckLakePartition>();
		partition->partition_id = entry.id.GetIndex();
		for (auto &field : entry.fields) {
			DuckLakePartitionField partition_field;
			partition_field.partition_key_index = field.partition_key_index;
			partition_field.field_id = field.field_id;
			if (field.transform == "year") {
				partition_field.transform.type = DuckLakeTransformType::YEAR;
			} else if (field.transform == "month") {
				partition_field.transform.type = DuckLakeTransformType::MONTH;
			} else if (field.transform == "day") {
				partition_field.transform.type = DuckLakeTransformType::DAY;
			} else if (field.transform == "hour") {
				partition_field.transform.type = DuckLakeTransformType::HOUR;
			} else if (field.transform == "identity") {
				partition_field.transform.type = DuckLakeTransformType::IDENTITY;
			} else {
				throw InvalidInputException("Unsupported partition transform %s", field.transform);
			}
			partition->fields.push_back(partition_field);
		}
		auto &ducklake_table = table->Cast<DuckLakeTableEntry>();
		ducklake_table.SetPartitionData(std::move(partition));
	}

	// load the sort entries
	for (auto &entry : catalog.sorts) {
		auto table = schema_set->GetEntryById(entry.table_id);
		if (!table || table->type != CatalogType::TABLE_ENTRY) {
			throw InvalidInputException("Could not find matching table for sort entry");
		}
		auto sort = make_uniq<DuckLakeSort>();
		sort->sort_id = entry.id.GetIndex();
		for (auto &field : entry.fields) {
			DuckLakeSortField sort_field;
			sort_field.sort_key_index = field.sort_key_index;
			sort_field.expression = field.expression;
			sort_field.dialect = field.dialect;
			sort_field.sort_direction = field.sort_direction;
			sort_field.null_order = field.null_order;

			sort->fields.push_back(sort_field);
		}
		auto &ducklake_table = table->Cast<DuckLakeTableEntry>();
		ducklake_table.SetSortData(std::move(sort));
	}

	return schema_set;
}

DuckLakeStats &DuckLakeCatalog::GetStatsForSnapshot(DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot) {
	auto &schema = GetSchemaForSnapshot(transaction, snapshot);
	lock_guard<mutex> guard(schemas_lock);
	auto entry = stats.find(snapshot.next_file_id);
	if (entry != stats.end()) {
		// this stat is already cached
		return *entry->second;
	}
	// load the stats from the metadata manager
	auto table_stats = LoadStatsForSnapshot(transaction, snapshot, schema);
	auto &result = *table_stats;
	stats.insert(make_pair(snapshot.next_file_id, std::move(table_stats)));
	return result;
}

static unique_ptr<DuckLakeNameMap> ConvertNameMap(DuckLakeColumnMappingInfo column_mapping) {
	if (column_mapping.map_type != "map_by_name") {
		throw InvalidInputException("Unsupported column mapping type \"%s\"", column_mapping.map_type);
	}
	auto result = make_uniq<DuckLakeNameMap>();
	result->id = column_mapping.mapping_id;
	result->table_id = column_mapping.table_id;

	// generate the recursive structure from the SQL table that only has parent references
	unordered_map<idx_t, reference<DuckLakeNameMapEntry>> column_id_map;
	for (auto &col : column_mapping.map_columns) {
		// create the entry
		auto map_entry = make_uniq<DuckLakeNameMapEntry>();
		map_entry->source_name = std::move(col.source_name);
		map_entry->target_field_id = col.target_field_id;
		map_entry->hive_partition = col.hive_partition;
		// add the column id -> entry mapping
		column_id_map.emplace(col.column_id, *map_entry);
		if (!col.parent_column.IsValid()) {
			// root-entry, add to parent map directly
			result->column_maps.push_back(std::move(map_entry));
		} else {
			// non-root entry: find parent entry
			auto parent_entry = column_id_map.find(col.parent_column.GetIndex());
			if (parent_entry == column_id_map.end()) {
				throw InvalidInputException("Parent column %d not found when converting name map with id %d",
				                            col.parent_column.GetIndex(), column_mapping.mapping_id.index);
			}
			auto &parent = parent_entry->second.get();
			parent.child_entries.push_back(std::move(map_entry));
		}
	}
	return result;
}

void DuckLakeCatalog::LoadNameMaps(DuckLakeTransaction &transaction) {
	auto snapshot = transaction.GetSnapshot();
	if (loaded_name_map_index.IsValid() && snapshot.next_file_id <= loaded_name_map_index.GetIndex()) {
		// we have already loaded all name maps that could be relevant for this snapshot
		return;
	}
	// name map entry not found - try to load any new ones
	auto &metadata_manager = transaction.GetMetadataManager();
	auto new_name_maps = metadata_manager.GetColumnMappings(loaded_name_map_index);
	for (auto &column_mapping : new_name_maps) {
		auto name_map = ConvertNameMap(std::move(column_mapping));
		name_maps.Add(std::move(name_map));
	}
	loaded_name_map_index = snapshot.next_file_id;
}

optional_ptr<const DuckLakeNameMap> DuckLakeCatalog::TryGetMappingById(DuckLakeTransaction &transaction,
                                                                       MappingIndex mapping_id) {
	lock_guard<mutex> guard(schemas_lock);
	auto entry = name_maps.name_maps.find(mapping_id);
	if (entry != name_maps.name_maps.end()) {
		return entry->second.get();
	}
	LoadNameMaps(transaction);
	// try to fetch the name map again
	entry = name_maps.name_maps.find(mapping_id);
	if (entry != name_maps.name_maps.end()) {
		return entry->second.get();
	}
	// still no success - return nullptr
	return nullptr;
}

MappingIndex DuckLakeCatalog::TryGetCompatibleNameMap(DuckLakeTransaction &transaction,
                                                      const DuckLakeNameMap &name_map) {
	lock_guard<mutex> guard(schemas_lock);
	LoadNameMaps(transaction);
	return name_maps.TryGetCompatibleNameMap(name_map);
}

unique_ptr<DuckLakeStats> DuckLakeCatalog::ConstructStatsMap(vector<DuckLakeGlobalStatsInfo> &global_stats,
                                                             DuckLakeCatalogSet &schema) {
	auto lake_stats = make_uniq<DuckLakeStats>();
	for (auto &stats : global_stats) {
		// find the referenced table entry
		auto table_entry = schema.GetEntryById(stats.table_id);
		if (!table_entry) {
			// failed to find the referenced table entry - this means the table does not exist for this snapshot
			// since the global stats are not versioned this is not an error - just skip
			continue;
		}
		auto table_stats = make_uniq<DuckLakeTableStats>();
		table_stats->record_count = stats.record_count;
		table_stats->next_row_id = stats.next_row_id;
		table_stats->table_size_bytes = stats.table_size_bytes;
		auto &table = table_entry->Cast<DuckLakeTableEntry>();
		for (auto &col_stats : stats.column_stats) {
			auto field = table.GetFieldId(col_stats.column_id);
			if (!field) {
				// column that this field id references was deleted
				continue;
			}
			DuckLakeColumnStats column_stats(field->Type());
			column_stats.has_null_count = col_stats.has_contains_null;
			if (column_stats.has_null_count) {
				column_stats.null_count = col_stats.contains_null ? 1 : 0;
			}
			column_stats.has_contains_nan = col_stats.has_contains_nan;
			if (column_stats.has_contains_nan) {
				column_stats.contains_nan = col_stats.contains_nan;
			}
			column_stats.has_min = col_stats.has_min;
			if (column_stats.has_min) {
				column_stats.min = col_stats.min_val;
			}
			column_stats.has_max = col_stats.has_max;
			if (column_stats.has_max) {
				column_stats.max = col_stats.max_val;
			}
			if (col_stats.has_extra_stats && column_stats.extra_stats) {
				// The extra_stats should already be allocated in the constructor
				// if the logical type requires extra stats.
				column_stats.extra_stats->Deserialize(col_stats.extra_stats);
			}
			table_stats->column_stats.insert(make_pair(col_stats.column_id, std::move(column_stats)));
		}
		lake_stats->table_stats.insert(make_pair(stats.table_id, std::move(table_stats)));
	}
	return lake_stats;
}

unique_ptr<DuckLakeStats> DuckLakeCatalog::LoadStatsForSnapshot(DuckLakeTransaction &transaction,
                                                                DuckLakeSnapshot snapshot, DuckLakeCatalogSet &schema) {
	auto &metadata_manager = transaction.GetMetadataManager();
	auto global_stats = metadata_manager.GetGlobalTableStats(snapshot);
	// construct the stats map
	return ConstructStatsMap(global_stats, schema);
}

optional_ptr<DuckLakeTableStats> DuckLakeCatalog::GetTableStats(DuckLakeTransaction &transaction, TableIndex table_id) {
	return GetTableStats(transaction, transaction.GetSnapshot(), table_id);
}

optional_ptr<DuckLakeTableStats> DuckLakeCatalog::GetTableStats(DuckLakeTransaction &transaction,
                                                                DuckLakeSnapshot snapshot, TableIndex table_id) {
	auto &lake_stats = GetStatsForSnapshot(transaction, snapshot);
	auto entry = lake_stats.table_stats.find(table_id);
	if (entry == lake_stats.table_stats.end()) {
		return nullptr;
	}
	return entry->second.get();
}

optional_ptr<SchemaCatalogEntry> DuckLakeCatalog::LookupSchema(CatalogTransaction transaction,
                                                               const EntryLookupInfo &schema_lookup,
                                                               OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	if (!initialized) {
		if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			throw BinderException("Failed to look-up \"%s\" - DuckLake %s is not yet initialized", schema_name,
			                      GetName());
		}
		return nullptr;
	}
	auto at_clause = schema_lookup.GetAtClause();
	auto &duck_transaction = transaction.transaction->Cast<DuckLakeTransaction>();
	if (!at_clause) {
		// if we have an AT clause we can never read transaction-local changes
		// look for the schema in the set of transaction-local schemas
		auto set = duck_transaction.GetTransactionLocalSchemas();
		if (set) {
			auto entry = set->GetEntry<SchemaCatalogEntry>(schema_name);
			if (entry) {
				return entry;
			}
		}
	}
	auto snapshot = duck_transaction.GetSnapshot(at_clause);
	auto &schemas = GetSchemaForSnapshot(duck_transaction, snapshot);
	auto entry = schemas.GetEntry<SchemaCatalogEntry>(schema_name);
	if (!entry) {
		if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			throw BinderException("Schema \"%s\" not found in DuckLakeCatalog \"%s\"", schema_name, GetName());
		}
		return nullptr;
	}
	if (!at_clause && duck_transaction.IsDeleted(*entry)) {
		return nullptr;
	}
	return entry;
}

void DuckLakeCatalog::SetEncryption(DuckLakeEncryption new_encryption) {
	if (options.encryption == new_encryption) {
		// already set to this value
		return;
	}
	switch (options.encryption) {
	case DuckLakeEncryption::AUTOMATIC:
		// adopt whichever value here
		options.encryption = new_encryption;
		break;
	case DuckLakeEncryption::ENCRYPTED:
		throw InvalidInputException(
		    "Failed to set encryption - the database is not encrypted but we requested an encrypted database");
	case DuckLakeEncryption::UNENCRYPTED:
		throw InvalidInputException(
		    "Failed to set encryption - the database is encrypted but we requested an unencrypted database");
	default:
		throw InternalException("Unsupported encryption type");
	}
}

unique_ptr<LogicalOperator> DuckLakeCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                             TableCatalogEntry &table,
                                                             unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("DuckLake does not support indexes");
}

DatabaseSize DuckLakeCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize database_size;
	auto &transaction = DuckLakeTransaction::Get(context, *this);
	auto &metadata_manager = transaction.GetMetadataManager();
	auto table_sizes = metadata_manager.GetTableSizes(transaction.GetSnapshot());
	for (auto &table_size : table_sizes) {
		database_size.bytes += table_size.file_size_bytes;
		database_size.bytes += table_size.delete_file_size_bytes;
	}
	return database_size;
}

bool DuckLakeCatalog::InMemory() {
	return false;
}

string DuckLakeCatalog::GetDBPath() {
	return options.metadata_path;
}

string DuckLakeCatalog::GetDataPath() {
	return options.data_path;
}

optional_ptr<BoundAtClause> DuckLakeCatalog::CatalogSnapshot() const {
	return options.at_clause.get();
}

void DuckLakeCatalog::OnDetach(ClientContext &context) {
	// detach the metadata database
	auto &db_manager = DatabaseManager::Get(context);
	db_manager.DetachDatabase(context, MetadataDatabaseName(), OnEntryNotFound::RETURN_NULL);
}

optional_idx DuckLakeCatalog::GetCatalogVersion(ClientContext &context) {
	return DuckLakeTransaction::Get(context, *this).GetCatalogVersion();
}

void DuckLakeCatalog::SetConfigOption(const DuckLakeConfigOption &option) {
	lock_guard<mutex> guard(config_lock);
	auto &key = option.option.key;
	auto &value = option.option.value;
	if (option.table_id.IsValid()) {
		// scoped to a table
		options.table_options[option.table_id][key] = value;
		return;
	}
	if (option.schema_id.IsValid()) {
		// scoped to a schema
		options.schema_options[option.schema_id][key] = value;
		return;
	}
	// scoped globally
	options.config_options[key] = value;
}

bool DuckLakeCatalog::TryGetConfigOption(const string &option, string &result, SchemaIndex schema_id,
                                         TableIndex table_id) const {
	lock_guard<mutex> guard(config_lock);
	// search options in-order
	// table scope
	if (table_id.IsValid()) {
		auto table_entry = options.table_options.find(table_id);
		if (table_entry != options.table_options.end()) {
			auto table_options_entry = table_entry->second.find(option);
			if (table_options_entry != table_entry->second.end()) {
				result = table_options_entry->second;
				return true;
			}
		}
	}
	// schema scope
	if (schema_id.IsValid()) {
		auto schema_entry = options.schema_options.find(schema_id);
		if (schema_entry != options.schema_options.end()) {
			auto schema_options_entry = schema_entry->second.find(option);
			if (schema_options_entry != schema_entry->second.end()) {
				result = schema_options_entry->second;
				return true;
			}
		}
	}

	// global scope
	auto entry = options.config_options.find(option);
	if (entry == options.config_options.end()) {
		return false;
	}
	result = entry->second;
	return true;
}

bool DuckLakeCatalog::TryGetConfigOption(const string &option, string &result, DuckLakeTableEntry &table) const {
	auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
	auto schema_id = schema.GetSchemaId();
	auto table_id = table.GetTableId();
	return TryGetConfigOption(option, result, schema_id, table_id);
}

idx_t DuckLakeCatalog::DataInliningRowLimit(SchemaIndex schema_index, TableIndex table_index) const {
	return GetConfigOption<idx_t>("data_inlining_row_limit", schema_index, table_index, 10);
}

unique_ptr<LogicalOperator> DuckLakeCatalog::BindAlterAddIndex(Binder &binder, TableCatalogEntry &table_entry,
                                                               unique_ptr<LogicalOperator> plan,
                                                               unique_ptr<CreateIndexInfo> create_info,
                                                               unique_ptr<AlterTableInfo> alter_info) {
	throw NotImplementedException("Adding indexes or constraints is not supported in DuckLake");
}

} // namespace duckdb
