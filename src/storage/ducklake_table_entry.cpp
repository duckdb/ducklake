#include "common/ducklake_types.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_transaction.hpp"

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "storage/ducklake_multi_file_reader.hpp"

namespace duckdb {
constexpr column_t DuckLakeMultiFileReader::COLUMN_IDENTIFIER_SNAPSHOT_ID;

void DuckLakeTableEntry::CheckSupportedTypes() {
	for (auto &col : columns.Logical()) {
		DuckLakeTypes::CheckSupportedType(col.Type());
	}
}

DuckLakeTableEntry::DuckLakeTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                       TableIndex table_id, string table_uuid_p, string data_path_p,
                                       shared_ptr<DuckLakeFieldData> field_data_p, optional_idx next_column_id_p,
                                       vector<DuckLakeInlinedTableInfo> inlined_data_tables_p, LocalChange local_change)
    : TableCatalogEntry(catalog, schema, info), table_id(table_id), table_uuid(std::move(table_uuid_p)),
      data_path(std::move(data_path_p)), field_data(std::move(field_data_p)), next_column_id(next_column_id_p),
      inlined_data_tables(std::move(inlined_data_tables_p)), local_change(local_change) {
	CheckSupportedTypes();
	for (auto &col : columns.Logical()) {
		if (col.Generated()) {
			throw NotImplementedException("DuckLake does not support generated columns");
		}
		if (col.CompressionType() != CompressionType::COMPRESSION_AUTO) {
			throw NotImplementedException("Defining a compression type for a column is not supported in DuckLake");
		}
	}
	for (auto &constraint : constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL:
			break;
		case ConstraintType::CHECK:
			throw NotImplementedException("CHECK constraints are not supported in DuckLake");
		case ConstraintType::UNIQUE:
			throw NotImplementedException("PRIMARY KEY/UNIQUE constraints are not supported in DuckLake");
		case ConstraintType::FOREIGN_KEY:
			throw NotImplementedException("FOREIGN KEY constraints are not supported in DuckLake");
		default:
			throw NotImplementedException("Unsupported constraint in DuckLake");
		}
	}
}

// ALTER TABLE RENAME/SET COMMENT/ADD COLUMN/DROP COLUMN
DuckLakeTableEntry::DuckLakeTableEntry(DuckLakeTableEntry &parent, CreateTableInfo &info, LocalChange local_change)
    : DuckLakeTableEntry(parent.ParentCatalog(), parent.ParentSchema(), info, parent.GetTableId(),
                         parent.GetTableUUID(), parent.DataPath(), parent.field_data, parent.next_column_id,
                         parent.inlined_data_tables, local_change) {
	if (parent.partition_data) {
		partition_data = make_uniq<DuckLakePartition>(*parent.partition_data);
	}
	if (parent.sort_data) {
		sort_data = make_uniq<DuckLakeSort>(*parent.sort_data);
	}
	CheckSupportedTypes();
	if (local_change.type == LocalChangeType::ADD_COLUMN) {
		LogicalIndex new_col_idx(columns.LogicalColumnCount() - 1);
		auto &new_col = GetColumn(new_col_idx);
		idx_t next_col = next_column_id.GetIndex();
		field_data = DuckLakeFieldData::AddColumn(*field_data, new_col, next_col);
		next_column_id = next_col;
	} else if (local_change.type == LocalChangeType::REMOVE_COLUMN) {
		auto changed_id = local_change.field_index;
		field_data = DuckLakeFieldData::DropColumn(*field_data, changed_id);
	}
}

DuckLakeTableEntry::DuckLakeTableEntry(DuckLakeTableEntry &parent, CreateTableInfo &info,
                                       SetDefaultLocalChange local_change)
    : DuckLakeTableEntry(parent.ParentCatalog(), parent.ParentSchema(), info, parent.GetTableId(),
                         parent.GetTableUUID(), parent.DataPath(), parent.field_data, parent.next_column_id,
                         parent.inlined_data_tables, local_change) {
	if (parent.partition_data) {
		partition_data = make_uniq<DuckLakePartition>(*parent.partition_data);
	}
	CheckSupportedTypes();

	auto changed_id = local_change.field_index;
	field_data = DuckLakeFieldData::SetDefault(*field_data, changed_id, GetColumnByFieldId(changed_id),
	                                           local_change.is_column_new);
}

// ALTER TABLE RENAME COLUMN
DuckLakeTableEntry::DuckLakeTableEntry(DuckLakeTableEntry &parent, CreateTableInfo &info, LocalChange local_change,
                                       const string &new_name)
    : DuckLakeTableEntry(parent, info, local_change) {
	D_ASSERT(local_change.type == LocalChangeType::RENAME_COLUMN);
	field_data = DuckLakeFieldData::RenameColumn(*field_data, local_change.field_index, new_name);
}

// ALTER TABLE DROP COLUMN
DuckLakeTableEntry::DuckLakeTableEntry(DuckLakeTableEntry &parent, CreateTableInfo &info, LocalChange local_change,
                                       unique_ptr<ColumnChangeInfo> changed_fields_p)
    : DuckLakeTableEntry(parent, info, local_change) {
	D_ASSERT(local_change.type == LocalChangeType::REMOVE_COLUMN);
	changed_fields = std::move(changed_fields_p);
}

// ALTER TABLE SET DATA TYPE
DuckLakeTableEntry::DuckLakeTableEntry(DuckLakeTableEntry &parent, CreateTableInfo &info, LocalChange local_change,
                                       unique_ptr<ColumnChangeInfo> changed_fields_p,
                                       shared_ptr<DuckLakeFieldData> new_field_data)
    : DuckLakeTableEntry(parent, info, local_change) {
	CheckSupportedTypes();
	D_ASSERT(local_change.type == LocalChangeType::CHANGE_COLUMN_TYPE);
	changed_fields = std::move(changed_fields_p);
	field_data = std::move(new_field_data);
}

// ALTER TABLE SET PARTITION KEY
DuckLakeTableEntry::DuckLakeTableEntry(DuckLakeTableEntry &parent, CreateTableInfo &info,
                                       unique_ptr<DuckLakePartition> partition_data_p)
    : DuckLakeTableEntry(parent, info, LocalChangeType::SET_PARTITION_KEY) {
	partition_data = std::move(partition_data_p);
}

// ALTER TABLE SET SORT KEY
DuckLakeTableEntry::DuckLakeTableEntry(DuckLakeTableEntry &parent, CreateTableInfo &info,
                                       unique_ptr<DuckLakeSort> sort_data_p)
    : DuckLakeTableEntry(parent, info, LocalChangeType::SET_SORT_KEY) {
	sort_data = std::move(sort_data_p);
}

const DuckLakeFieldId &DuckLakeTableEntry::GetFieldId(PhysicalIndex column_index) const {
	return field_data->GetByRootIndex(column_index);
}

optional_ptr<const DuckLakeFieldId> DuckLakeTableEntry::GetFieldId(FieldIndex field_index) const {
	return field_data->GetByFieldIndex(field_index);
}

const DuckLakeFieldId &DuckLakeTableEntry::GetFieldId(const vector<string> &column_names,
                                                      optional_ptr<optional_idx> name_offset) const {
	auto result = TryGetFieldId(column_names, name_offset);
	if (!result) {
		throw BinderException("Column \"%s\" does not exist", StringUtil::Join(column_names, "."));
	}
	return *result;
}

optional_ptr<const DuckLakeFieldId> DuckLakeTableEntry::TryGetFieldId(const vector<string> &column_names,
                                                                      optional_ptr<optional_idx> name_offset) const {
	if (!columns.ColumnExists(column_names[0])) {
		return nullptr;
	}
	auto &root_col = columns.GetColumn(column_names[0]);
	return field_data->GetByNames(root_col.Physical(), column_names, name_offset);
}

const ColumnDefinition &DuckLakeTableEntry::GetColumnByFieldId(FieldIndex field_index) const {
	auto field_id = GetFieldId(field_index);
	if (!field_id) {
		throw InternalException("Column with field id %d not found", field_index.index);
	}
	return GetColumn(field_id->Name());
}

unique_ptr<BaseStatistics> GetColumnStats(const DuckLakeFieldId &field_id, const DuckLakeTableStats &table_stats) {
	auto &field_children = field_id.Children();
	if (field_children.empty()) {
		// non-nested type - lookup the field id in the stats map
		auto entry = table_stats.column_stats.find(field_id.GetFieldIndex());
		if (entry == table_stats.column_stats.end()) {
			return nullptr;
		}
		return entry->second.ToStats();
	}
	// nested type
	switch (field_id.Type().id()) {
	case LogicalTypeId::STRUCT: {
		auto struct_stats = StructStats::CreateUnknown(field_id.Type());
		for (idx_t child_idx = 0; child_idx < field_children.size(); ++child_idx) {
			auto child_stats = GetColumnStats(*field_children[child_idx], table_stats);
			StructStats::SetChildStats(struct_stats, child_idx, std::move(child_stats));
		}
		return struct_stats.ToUnique();
	}
	case LogicalTypeId::LIST: {
		auto list_stats = ListStats::CreateUnknown(field_id.Type());
		auto child_stats = GetColumnStats(*field_children[0], table_stats);
		ListStats::SetChildStats(list_stats, std::move(child_stats));
		return list_stats.ToUnique();
	}
	default:
		// unsupported nested type
		return nullptr;
	}
}

case_insensitive_set_t DuckLakeTableEntry::GetNotNullFields() const {
	case_insensitive_set_t result;
	for (auto &constraint : GetConstraints()) {
		if (constraint->type != ConstraintType::NOT_NULL) {
			continue;
		}
		auto &not_null = constraint->Cast<NotNullConstraint>();
		auto &col = GetColumn(not_null.index);
		result.insert(col.Name());
	}
	return result;
}

unique_ptr<BaseStatistics> DuckLakeTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	auto table_stats = GetTableStats(context);
	if (!table_stats) {
		return nullptr;
	}
	auto &field_id = field_data->GetByRootIndex(PhysicalIndex(column_id));
	return GetColumnStats(field_id, *table_stats);
}

TableFunction DuckLakeTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	throw InternalException("DuckLakeTableEntry::GetScanFunction called without entry lookup info");
}

unique_ptr<FunctionData> DuckLakeFunctions::BindDuckLakeScan(ClientContext &context, TableFunction &function) {
	vector<Value> inputs {Value("")};
	named_parameter_map_t param_map;
	vector<LogicalType> return_types;
	vector<string> names;
	TableFunctionRef empty_ref;

	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, function, empty_ref);

	return function.bind(context, bind_input, return_types, names);
}

TableFunction DuckLakeTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                  const EntryLookupInfo &lookup_info) {
	auto function = DuckLakeFunctions::GetDuckLakeScanFunction(*context.db);
	auto &transaction = DuckLakeTransaction::Get(context, ParentCatalog());
	auto function_info =
	    make_shared_ptr<DuckLakeFunctionInfo>(*this, transaction, transaction.GetSnapshot(lookup_info.GetAtClause()));
	function_info->table_name = name;
	for (auto &col : columns.Logical()) {
		function_info->column_names.push_back(col.Name());
		function_info->column_types.push_back(col.Type());
	}
	auto table_id = GetTableId();
	function_info->table_id = table_id;
	function.function_info = std::move(function_info);
	auto &dropped_tables = transaction.GetDroppedTables();
	auto &renamed_tables = transaction.GetRenamedTables();
	if (dropped_tables.find(table_id) != dropped_tables.end()) {
		// Table has been dropped, so it doesn't exist anymore
		throw BinderException("Table with name %s does not exist", name);
	}
	if (renamed_tables.find(table_id) != renamed_tables.end()) {
		// Table has been renamed, are we then querying the correct name?
		bool found = false;
		for (auto &catalog_set : transaction.GetNewTables()) {
			auto table = catalog_set.second->GetEntry(name);
			if (table) {
				auto &ducklake_table = table->Cast<DuckLakeTableEntry>();
				if (ducklake_table.GetTableId() == table_id) {
					found = true;
					break;
				}
			}
		}
		if (!found) {
			throw BinderException("Table with name %s does not exist", name);
		}
	}

	bind_data = DuckLakeFunctions::BindDuckLakeScan(context, function);
	auto &multi_file_bind_data = bind_data->Cast<MultiFileBindData>();
	multi_file_bind_data.virtual_columns = GetVirtualColumns();

	return function;
}

virtual_column_map_t DuckLakeTableEntry::GetVirtualColumns() const {
	virtual_column_map_t result;
	result.insert(
	    make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR)));
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	                        TableColumn("file_row_number", LogicalType::BIGINT)));
	result.insert(
	    make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX, TableColumn("file_index", LogicalType::UBIGINT)));
	result.insert(make_pair(COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", LogicalType::BIGINT)));
	result.insert(make_pair(DuckLakeMultiFileReader::COLUMN_IDENTIFIER_SNAPSHOT_ID,
	                        TableColumn("snapshot_id", LogicalType::BIGINT)));
	result.insert(make_pair(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN)));
	return result;
}

vector<column_t> DuckLakeTableEntry::GetRowIdColumns() const {
	vector<column_t> result;
	result.push_back(COLUMN_IDENTIFIER_ROW_ID);
	result.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILENAME);
	result.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX);
	result.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
	return result;
}

void DuckLakeTableEntry::SetPartitionData(unique_ptr<DuckLakePartition> partition_data_p) {
	partition_data = std::move(partition_data_p);
}

void DuckLakeTableEntry::SetSortData(unique_ptr<DuckLakeSort> sort_data_p) {
	sort_data = std::move(sort_data_p);
}

const string &DuckLakeTableEntry::DataPath() const {
	return data_path;
}

optional_ptr<DuckLakeTableStats> DuckLakeTableEntry::GetTableStats(ClientContext &context) {
	auto &transaction = DuckLakeTransaction::Get(context, ParentCatalog());
	return GetTableStats(transaction);
}

optional_ptr<DuckLakeTableStats> DuckLakeTableEntry::GetTableStats(DuckLakeTransaction &transaction) {
	if (IsTransactionLocal()) {
		// no stats for transaction local tables
		return nullptr;
	}
	auto &dl_catalog = catalog.Cast<DuckLakeCatalog>();
	if (transaction.HasTransactionLocalInserts(GetTableId())) {
		// no stats if there are transaction-local inserts
		return nullptr;
	}
	return dl_catalog.GetTableStats(transaction, GetTableId());
}

idx_t DuckLakeTableEntry::GetNetDataFileRowCount(DuckLakeTransaction &transaction) {
	auto &metadata_manager = transaction.GetMetadataManager();
	return metadata_manager.GetNetDataFileRowCount(GetTableId(), transaction.GetSnapshot());
}

idx_t DuckLakeTableEntry::GetNetInlinedRowCount(DuckLakeTransaction &transaction) {
	auto &metadata_manager = transaction.GetMetadataManager();
	auto snapshot = transaction.GetSnapshot();
	idx_t total = 0;
	for (auto &inlined_table : inlined_data_tables) {
		total += metadata_manager.GetNetInlinedRowCount(inlined_table.table_name, snapshot);
	}
	return total;
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::AlterTable(DuckLakeTransaction &transaction, RenameTableInfo &info) {
	auto create_info = GetInfo();
	auto &table_info = create_info->Cast<CreateTableInfo>();
	table_info.table = info.new_table_name;
	// create a complete copy of this table with only the name changed
	return make_uniq<DuckLakeTableEntry>(*this, table_info, LocalChangeType::RENAMED);
}

string GetPartitionColumnName(ColumnRefExpression &colref) {
	if (colref.IsQualified()) {
		throw InvalidInputException("Unexpected qualified column reference - only unqualified columns are supported");
	}
	return colref.GetColumnName();
}

void DuckLakeTableEntry::ValidateSortExpressionColumns(DuckLakeTableEntry &table,
                                                       const vector<OrderByNode> &orders) {
	vector<string> missing_columns;
	for (auto &order : orders) {
		ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(
		    *order.expression, [&](const ColumnRefExpression &colref) {
			    if (colref.IsQualified()) {
				    throw InvalidInputException(
				        "Unexpected qualified column reference - only unqualified columns are supported");
			    }
			    string column_name = colref.GetColumnName();
			    if (!table.ColumnExists(column_name)) {
				    if (std::find(missing_columns.begin(), missing_columns.end(), column_name) ==
				        missing_columns.end()) {
					    missing_columns.push_back(column_name);
				    }
			    }
		    });
	}
	if (!missing_columns.empty()) {
		string error_string =
		    "Columns in the SET SORTED BY statement were not found in the DuckLake table. Unmatched columns were: ";
		for (idx_t i = 0; i < missing_columns.size(); i++) {
			if (i > 0) {
				error_string += ", ";
			}
			error_string += missing_columns[i];
		}
		throw BinderException(error_string);
	}
}

DuckLakePartitionField GetPartitionField(DuckLakeTableEntry &table, ParsedExpression &expr) {
	string column_name;
	DuckLakeTransformType transform_type;
	switch (expr.type) {
	case ExpressionType::COLUMN_REF: {
		auto &colref = expr.Cast<ColumnRefExpression>();
		column_name = GetPartitionColumnName(colref);
		transform_type = DuckLakeTransformType::IDENTITY;
		break;
	}
	case ExpressionType::FUNCTION: {
		auto &function = expr.Cast<FunctionExpression>();
		auto name = StringUtil::Lower(function.function_name);
		if (name == "year") {
			transform_type = DuckLakeTransformType::YEAR;
		} else if (name == "month") {
			transform_type = DuckLakeTransformType::MONTH;
		} else if (name == "day") {
			transform_type = DuckLakeTransformType::DAY;
		} else if (name == "hour") {
			transform_type = DuckLakeTransformType::HOUR;
		} else {
			throw NotImplementedException(
			    "Unsupported partition function %s - only year, month, day, hour are supported", name);
		}
		if (function.children.size() != 1 || function.children[0]->type != ExpressionType::COLUMN_REF) {
			throw NotImplementedException("Expected %s(column), but got %s", name, expr.ToString());
		}
		auto &colref = function.children[0]->Cast<ColumnRefExpression>();
		column_name = GetPartitionColumnName(colref);
		break;
	}
	default:
		throw NotImplementedException(
		    "Unsupported partition key %s - only identity columns and year/month/day/hour are supported",
		    expr.ToString());
	}
	DuckLakePartitionField field;
	if (!table.ColumnExists(column_name)) {
		throw CatalogException("Unexpected partition key - column \"%s\" does not exist", column_name);
	}
	auto &col = table.GetColumn(column_name);
	PhysicalIndex column_index(col.StorageOid());
	auto &field_id = table.GetFieldData().GetByRootIndex(column_index);
	field.field_id = field_id.GetFieldIndex();
	field.transform.type = transform_type;
	return field;
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::AlterTable(DuckLakeTransaction &transaction, SetPartitionedByInfo &info) {
	auto create_info = GetInfo();
	auto &table_info = create_info->Cast<CreateTableInfo>();
	// create a complete copy of this table with the partition info added
	auto partition_data = make_uniq<DuckLakePartition>();
	partition_data->partition_id = transaction.GetLocalCatalogId();
	for (idx_t expr_idx = 0; expr_idx < info.partition_keys.size(); expr_idx++) {
		auto &expr = *info.partition_keys[expr_idx];
		auto partition_field = GetPartitionField(*this, expr);
		partition_field.partition_key_index = expr_idx;
		partition_data->fields.push_back(partition_field);
	}

	auto new_entry = make_uniq<DuckLakeTableEntry>(*this, table_info, std::move(partition_data));
	return std::move(new_entry);
}

optional_idx FindNotNullConstraint(CreateTableInfo &table_info, LogicalIndex index) {
	for (idx_t constraint_idx = 0; constraint_idx < table_info.constraints.size(); constraint_idx++) {
		auto &constraint = table_info.constraints[constraint_idx];
		if (constraint->type != ConstraintType::NOT_NULL) {
			continue;
		}
		auto &not_null_constraint = constraint->Cast<NotNullConstraint>();
		if (not_null_constraint.index == index) {
			return constraint_idx;
		}
	}
	return optional_idx();
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::AlterTable(DuckLakeTransaction &transaction, SetNotNullInfo &info) {
	auto create_info = GetInfo();
	auto &table_info = create_info->Cast<CreateTableInfo>();
	if (!table_info.columns.ColumnExists(info.column_name)) {
		throw CatalogException("Failed to alter column - column %s does not exist", info.column_name);
	}
	auto &col = table_info.columns.GetColumn(info.column_name);
	auto &field_id = GetFieldId(col.Physical());

	// verify the column has no NULL values currently by looking at the stats
	auto stats = GetTableStats(transaction);
	if (!stats) {
		throw CatalogException(
		    "Cannot SET NULL on table %s - the table has transaction-local changes or no stats are available", name);
	}

	auto column_stats = stats->column_stats.find(field_id.GetFieldIndex());
	if (column_stats == stats->column_stats.end()) {
		throw CatalogException("Cannot SET NULL on table %s - no column stats are available", name);
	}
	auto &col_stats = column_stats->second;
	if (col_stats.has_null_count && col_stats.null_count > 0) {
		throw CatalogException("Cannot SET NULL on column %s - the column has NULL values", col.GetName());
	}

	// check if there is an existing constraint
	auto existing_idx = FindNotNullConstraint(table_info, col.Logical());
	if (existing_idx.IsValid()) {
		throw CatalogException("Cannot SET NULL on column %s - it already has a NOT NULL constraint", col.GetName());
	}
	table_info.constraints.push_back(make_uniq<NotNullConstraint>(col.Logical()));

	auto new_entry = make_uniq<DuckLakeTableEntry>(*this, table_info, LocalChange::SetNull(field_id.GetFieldIndex()));
	return std::move(new_entry);
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::AlterTable(DuckLakeTransaction &transaction, DropNotNullInfo &info) {
	auto create_info = GetInfo();
	auto &table_info = create_info->Cast<CreateTableInfo>();
	if (!table_info.columns.ColumnExists(info.column_name)) {
		throw CatalogException("Failed to alter column - column %s does not exist", info.column_name);
	}
	auto &col = table_info.columns.GetColumn(info.column_name);
	auto &field_id = GetFieldId(col.Physical());

	// find the existing index
	auto existing_idx = FindNotNullConstraint(table_info, col.Logical());
	if (!existing_idx.IsValid()) {
		throw CatalogException("Cannot DROP NULL on column %s - it has no NOT NULL constraint defined", col.GetName());
	}
	table_info.constraints.erase_at(existing_idx.GetIndex());

	auto new_entry = make_uniq<DuckLakeTableEntry>(*this, table_info, LocalChange::DropNull(field_id.GetFieldIndex()));
	return std::move(new_entry);
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::AlterTable(DuckLakeTransaction &transaction, RenameColumnInfo &info) {
	auto create_info = GetInfo();
	auto &table_info = create_info->Cast<CreateTableInfo>();
	if (!table_info.columns.ColumnExists(info.old_name)) {
		throw CatalogException("Failed to rename column - column %s does not exist", info.old_name);
	}
	auto &col = table_info.columns.GetColumn(info.old_name);
	auto &field_id = GetFieldId(col.Physical());

	// create a new list with the renamed column
	ColumnList new_columns;
	for (auto &col : columns.Logical()) {
		auto copy = col.Copy();
		if (copy.Name() == info.old_name) {
			copy.SetName(info.new_name);
		}
		new_columns.AddColumn(std::move(copy));
	}
	table_info.columns = std::move(new_columns);

	auto new_entry = make_uniq<DuckLakeTableEntry>(*this, table_info,
	                                               LocalChange::RenameColumn(field_id.GetFieldIndex()), info.new_name);
	return std::move(new_entry);
}

void DuckLakeTableEntry::RequireNextColumnId(DuckLakeTransaction &transaction) {
	if (next_column_id.IsValid()) {
		return;
	}
	// we need to fetch the next column id from the catalog
	// you might think we can look at the columns of the table itself - but that is not true in case there are dropped
	// columns the column id HAS to be unique globally
	auto &metadata_manager = transaction.GetMetadataManager();
	next_column_id = metadata_manager.GetNextColumnId(GetTableId());
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::AlterTable(DuckLakeTransaction &transaction, AddColumnInfo &info) {
	auto create_info = GetInfo();
	auto &table_info = create_info->Cast<CreateTableInfo>();
	if (info.if_column_not_exists && ColumnExists(info.new_column.Name())) {
		return nullptr;
	}

	table_info.columns.AddColumn(info.new_column.Copy());

	RequireNextColumnId(transaction);
	auto new_entry = make_uniq<DuckLakeTableEntry>(*this, table_info, LocalChangeType::ADD_COLUMN);

	if (transaction.HasTransactionInlinedData(GetTableId())) {
		auto &new_table = new_entry->Cast<DuckLakeTableEntry>();
		LogicalIndex new_col_idx(new_table.columns.LogicalColumnCount() - 1);
		auto &new_col = new_table.GetColumn(new_col_idx);
		auto &field_id = new_table.GetFieldData().GetByRootIndex(new_col.Physical());
		FieldIndex new_field_index = field_id.GetFieldIndex();

		// Get default value if it's a constant literal
		Value default_value;
		if (info.new_column.HasDefaultValue()) {
			auto &default_expr = info.new_column.DefaultValue();
			if (default_expr.type == ExpressionType::VALUE_CONSTANT) {
				auto &constant_expr = default_expr.Cast<ConstantExpression>();
				default_value = constant_expr.value.DefaultCastAs(new_col.Type());
			}
		}

		transaction.AddColumnToLocalInlinedData(GetTableId(), new_col.Type(), new_field_index, default_value);
	}

	return std::move(new_entry);
}

void ColumnChangeInfo::DropField(const DuckLakeFieldId &field_id) {
	dropped_fields.push_back(field_id.GetFieldIndex());
	for (auto &child_id : field_id.Children()) {
		DropField(*child_id);
	}
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::AlterTable(DuckLakeTransaction &transaction, RemoveColumnInfo &info) {
	auto create_info = GetInfo();
	auto &table_info = create_info->Cast<CreateTableInfo>();
	if (!ColumnExists(info.removed_column)) {
		if (info.if_column_exists) {
			return nullptr;
		}
		throw BinderException("Table \"%s\" does not have a column with name \"%s\"", name, info.removed_column);
	}

	auto &col = table_info.columns.GetColumn(info.removed_column);
	auto &field_id = GetFieldId(col.Physical());
	if (columns.LogicalColumnCount() == 1) {
		throw CatalogException("Cannot drop column: table only has one column remaining!");
	}
	// check if we are partitioning on this column
	if (partition_data) {
		for (auto &partition_field : partition_data->fields) {
			if (field_id.GetFieldIndex() == partition_field.field_id) {
				throw CatalogException("Cannot drop column \"%s\" - the table is partitioned by this column. Reset or "
				                       "change the partitioning on this table in order to drop this column",
				                       col.Name());
			}
		}
	}
	if (transaction.HasTransactionInlinedData(GetTableId())) {
		transaction.RemoveColumnFromLocalInlinedData(GetTableId(), col.Logical(), field_id);
	}

	auto removed_index = col.Logical();
	for (idx_t c_idx = 0; c_idx < table_info.constraints.size(); c_idx++) {
		auto &constraint = table_info.constraints[c_idx];
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = constraint->Cast<NotNullConstraint>();
			if (not_null.index == removed_index) {
				// this index belongs to the removed column - remove it
				table_info.constraints.erase_at(c_idx);
				c_idx--;
				continue;
			}
			if (not_null.index.index > removed_index.index) {
				// this index belongs to a column after the removed column - shift the index
				not_null.index.index--;
			}
		}
	}
	// remove the column from the column list
	ColumnList new_columns;
	for (auto &col : columns.Logical()) {
		auto copy = col.Copy();
		if (StringUtil::CIEquals(copy.Name(), info.removed_column)) {
			continue;
		}
		new_columns.AddColumn(std::move(copy));
	}
	table_info.columns = std::move(new_columns);

	auto change_info = make_uniq<ColumnChangeInfo>();
	change_info->DropField(field_id);

	auto new_entry = make_uniq<DuckLakeTableEntry>(
	    *this, table_info, LocalChange::RemoveColumn(field_id.GetFieldIndex()), std::move(change_info));
	return std::move(new_entry);
}

static bool TypePromotionIsAllowedTinyint(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return true;
	default:
		return false;
	}
}

static bool TypePromotionIsAllowedSmallint(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return true;
	default:
		return false;
	}
}

static bool TypePromotionIsAllowedUTinyint(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return true;
	default:
		return false;
	}
}

static bool TypePromotionIsAllowedUSmallint(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return true;
	default:
		return false;
	}
}
bool TypePromotionIsAllowed(const LogicalType &source, const LogicalType &target) {
	// FIXME: Rework to use DUCKDB_API static LogicalType MaxLogicalType
	switch (source.id()) {
	case LogicalTypeId::TINYINT:
		return TypePromotionIsAllowedTinyint(target);
	case LogicalTypeId::SMALLINT:
		return TypePromotionIsAllowedSmallint(target);
	case LogicalTypeId::INTEGER:
		return target.id() == LogicalTypeId::BIGINT;
	case LogicalTypeId::BIGINT:
		return false;
	case LogicalTypeId::UTINYINT:
		return TypePromotionIsAllowedUTinyint(target);
	case LogicalTypeId::USMALLINT:
		return TypePromotionIsAllowedUSmallint(target);
	case LogicalTypeId::UINTEGER:
		return target.id() == LogicalTypeId::UBIGINT;
	case LogicalTypeId::UBIGINT:
		return false;
	case LogicalTypeId::FLOAT:
		return target.id() == LogicalTypeId::DOUBLE;
	case LogicalTypeId::TIMESTAMP:
		return target.id() == LogicalTypeId::TIMESTAMP_TZ;
	default:
		return false;
	}
}

bool IsSimpleCast(const ParsedExpression &expr) {
	if (expr.type != ExpressionType::OPERATOR_CAST) {
		return false;
	}
	auto &cast = expr.Cast<CastExpression>();
	if (cast.child->type != ExpressionType::COLUMN_REF) {
		return false;
	}
	return true;
}

idx_t GetNestedChildCount(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
		return 1;
	case LogicalTypeId::MAP:
		return 2;
	case LogicalTypeId::STRUCT:
		return StructType::GetChildTypes(type).size();
	default:
		throw NotImplementedException("Unimplemented nested type %s for DuckLake type evolution", type);
	}
}

string GetNestedChildName(const LogicalType &type, idx_t index) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
		return "element";
	case LogicalTypeId::MAP:
		return index == 0 ? "key" : "value";
	case LogicalTypeId::STRUCT:
		return StructType::GetChildTypes(type)[index].first;
	default:
		throw NotImplementedException("Unimplemented nested type %s for DuckLake type evolution", type);
	}
}
const LogicalType &GetNestedChildType(const LogicalType &type, idx_t index) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
		return ListType::GetChildType(type);
	case LogicalTypeId::MAP:
		return index == 0 ? MapType::KeyType(type) : MapType::ValueType(type);
	case LogicalTypeId::STRUCT:
		return StructType::GetChildTypes(type)[index].second;
	default:
		throw NotImplementedException("Unimplemented nested type %s for DuckLake type evolution", type);
	}
}

unique_ptr<DuckLakeFieldId> DuckLakeTableEntry::GetNestedEvolution(const DuckLakeFieldId &source_id,
                                                                   const LogicalType &target, ColumnChangeInfo &result,
                                                                   optional_idx parent_idx) {
	auto &source_type = source_id.Type();
	if (source_type.id() != target.id()) {
		throw NotImplementedException("Type evolution is not supported from type %s to type %s", source_type, target);
	}

	case_insensitive_map_t<idx_t> source_type_map;
	for (idx_t source_idx = 0; source_idx < GetNestedChildCount(source_type); ++source_idx) {
		source_type_map[GetNestedChildName(source_type, source_idx)] = source_idx;
	}
	auto &source_children = source_id.Children();
	DuckLakeColumnData column_data;
	column_data.id = source_id.GetFieldIndex();

	vector<unique_ptr<DuckLakeFieldId>> children;
	// for each type in target_types, check if it is in source types
	for (idx_t target_idx = 0; target_idx < GetNestedChildCount(target); ++target_idx) {
		auto target_name = GetNestedChildName(target, target_idx);
		auto &target_type = GetNestedChildType(target, target_idx);
		auto entry = source_type_map.find(target_name);
		if (entry == source_type_map.end()) {
			// type not found - this is a new entry
			// first construct a new field id for this entry
			idx_t next_col = next_column_id.GetIndex();
			auto field_id = DuckLakeFieldId::FieldIdFromType(target_name, target_type, nullptr, next_col, false);
			next_column_id = next_col;

			// add the column to the list of "to-be-added" columns
			DuckLakeNewColumn new_col;
			new_col.column_info = ConvertColumn(target_name, target_type, *field_id);
			new_col.parent_idx = column_data.id.index;
			result.new_fields.push_back(std::move(new_col));
			children.push_back(std::move(field_id));
			continue;
		}
		auto source_idx = entry->second;

		// the name exists in both the source and target
		// recursively perform type promotion
		auto new_child_id = TypePromotion(*source_children[source_idx], target_type, result, column_data.id.index);

		children.push_back(std::move(new_child_id));
		// erase from the source map to indicate this field has been handled
		source_type_map.erase(target_name);
	}
	for (auto &entry : source_type_map) {
		auto source_idx = entry.second;
		auto &source_field = *source_children[source_idx];
		result.DropField(source_field);
	}
	return make_uniq<DuckLakeFieldId>(std::move(column_data), source_id.Name(), target, std::move(children));
}

unique_ptr<DuckLakeFieldId> DuckLakeTableEntry::TypePromotion(const DuckLakeFieldId &source_id,
                                                              const LogicalType &target, ColumnChangeInfo &result,
                                                              optional_idx parent_idx) {
	if (!source_id.Children().empty()) {
		return GetNestedEvolution(source_id, target, result, parent_idx);
	}
	auto &source_type = source_id.Type();
	if (source_type == target) {
		// type is unchanged - return field id directly
		return source_id.Copy();
	}
	// primitive type promotion
	// only widening type promotions are allowed
	if (!TypePromotionIsAllowed(source_type, target)) {
		throw CatalogException(
		    "Cannot change type of column %s from %s to %s - only widening type promotions are allowed",
		    source_id.Name(), source_type, target);
	}
	// field id is unchanged - but the column is changed
	// we need to drop and recreate the column
	// drop the field
	result.DropField(source_id);

	// re-create with the new type
	DuckLakeColumnData column_data;
	column_data.id = source_id.GetFieldIndex();
	DuckLakeNewColumn new_col;
	if (!parent_idx.IsValid()) {
		// root column - get the info from the table directly
		new_col.column_info = GetColumnInfo(column_data.id);
	} else {
		// nested column - generate the info here
		new_col.column_info.id = column_data.id;
		new_col.column_info.name = source_id.Name();
	}
	new_col.column_info.type = DuckLakeTypes::ToString(target);
	new_col.parent_idx = parent_idx;
	result.new_fields.push_back(std::move(new_col));

	return make_uniq<DuckLakeFieldId>(std::move(column_data), source_id.Name(), target);
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::AlterTable(DuckLakeTransaction &transaction, ChangeColumnTypeInfo &info) {
	auto create_info = GetInfo();
	auto &table_info = create_info->Cast<CreateTableInfo>();
	if (!ColumnExists(info.column_name)) {
		throw BinderException("Table \"%s\" does not have a column with name \"%s\"", name, info.column_name);
	}
	auto &col = table_info.columns.GetColumn(info.column_name);
	auto &field_id = GetFieldId(col.Physical());
	if (!IsSimpleCast(*info.expression)) {
		throw NotImplementedException("Column type cannot be modified using an expression");
	}
	auto change_info = make_uniq<ColumnChangeInfo>();
	if (info.target_type.IsNested()) {
		RequireNextColumnId(transaction);
	}
	auto new_field_id = TypePromotion(field_id, info.target_type, *change_info, optional_idx());

	// generate a new column list with the modified type
	ColumnList new_columns;
	for (auto &col : columns.Logical()) {
		auto copy = col.Copy();
		if (copy.Name() == info.column_name) {
			copy.SetType(info.target_type);
		}
		new_columns.AddColumn(std::move(copy));
	}
	table_info.columns = std::move(new_columns);

	// generate the new field ids for the table
	auto &current_field_ids = field_data->GetFieldIds();
	auto new_field_ids = make_shared_ptr<DuckLakeFieldData>();
	for (auto &field_id : current_field_ids) {
		if (new_field_id && field_id->Name() == info.column_name) {
			new_field_ids->Add(std::move(new_field_id));
			new_field_id.reset();
		} else {
			new_field_ids->Add(field_id->Copy());
		}
	}

	auto new_entry = make_uniq<DuckLakeTableEntry>(*this, table_info, LocalChangeType::CHANGE_COLUMN_TYPE,
	                                               std::move(change_info), std::move(new_field_ids));
	return std::move(new_entry);
}

static void ExtractDefaultValue(const DuckLakeColumnData &col_data, DuckLakeColumnInfo &info) {
	info.initial_default = col_data.initial_default;
	if (col_data.default_value) {
		if (col_data.default_value->type == ExpressionType::VALUE_CONSTANT) {
			auto &constant_value = col_data.default_value->Cast<ConstantExpression>();
			info.default_value = constant_value.value;
			info.default_value_type = "literal";
		} else {
			info.default_value = col_data.default_value->ToString();
			info.default_value_type = "expression";
		}
	} else {
		info.default_value = Value(LogicalTypeId::VARCHAR);
		info.default_value_type = "literal";
	}
}

void AddNewColumns(const DuckLakeFieldId &field_id, vector<DuckLakeNewColumn> &new_fields, FieldIndex parent_idx) {
	auto &col_data = field_id.GetColumnData();

	DuckLakeNewColumn new_col;
	new_col.column_info.id = col_data.id;
	new_col.column_info.name = field_id.Name();
	new_col.column_info.type = DuckLakeTypes::ToString(field_id.Type());

	ExtractDefaultValue(col_data, new_col.column_info);
	new_col.parent_idx = parent_idx.index;
	new_fields.push_back(std::move(new_col));
	for (auto &child : field_id.Children()) {
		AddNewColumns(*child, new_fields, field_id.GetFieldIndex());
	}
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::AlterTable(DuckLakeTransaction &transaction, AddFieldInfo &info) {
	auto create_info = GetInfo();
	auto &table_info = create_info->Cast<CreateTableInfo>();
	auto change_info = make_uniq<ColumnChangeInfo>();
	RequireNextColumnId(transaction);

	auto &parent_id = GetFieldId(info.column_path);
	if (parent_id.Type().id() != LogicalTypeId::STRUCT) {
		throw CatalogException("Fields can only be added to structs - %s is a %s", parent_id.Name(), parent_id.Type());
	}
	for (auto &child : StructType::GetChildTypes(parent_id.Type())) {
		if (StringUtil::CIEquals(child.first, info.new_field.Name())) {
			if (info.if_field_not_exists) {
				return nullptr;
			}
			throw CatalogException("Failed to add field - field \"%s\" already exists in column \"%s\"",
			                       info.new_field.Name(), info.column_path.back());
		}
	}

	// generate a new field id for the column
	auto next_field_id = next_column_id.GetIndex();
	auto child_field_id = DuckLakeFieldId::FieldIdFromColumn(info.new_field, next_field_id);
	next_column_id = next_field_id;

	// generate the new to-be-inserted columns
	AddNewColumns(*child_field_id, change_info->new_fields, parent_id.GetFieldIndex());

	// generate the new field ids for the table
	auto &current_field_ids = field_data->GetFieldIds();
	auto new_field_ids = make_shared_ptr<DuckLakeFieldData>();
	for (idx_t col_idx = 0; col_idx < current_field_ids.size(); col_idx++) {
		auto &field_id = current_field_ids[col_idx];
		if (child_field_id && StringUtil::CIEquals(field_id->Name(), info.column_path[0])) {
			auto new_field_id = field_id->AddField(info.column_path, std::move(child_field_id));
			auto &col = table_info.columns.GetColumnMutable(PhysicalIndex(col_idx));
			col.SetType(new_field_id->Type());
			new_field_ids->Add(std::move(new_field_id));
			child_field_id.reset();
		} else {
			new_field_ids->Add(field_id->Copy());
		}
	}

	auto new_entry = make_uniq<DuckLakeTableEntry>(*this, table_info, LocalChangeType::CHANGE_COLUMN_TYPE,
	                                               std::move(change_info), std::move(new_field_ids));
	return std::move(new_entry);
}

void RemoveColumns(const DuckLakeFieldId &field_id, vector<FieldIndex> &dropped_fields) {
	dropped_fields.push_back(field_id.GetFieldIndex());
	for (auto &child : field_id.Children()) {
		RemoveColumns(*child, dropped_fields);
	}
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::AlterTable(DuckLakeTransaction &transaction, RemoveFieldInfo &info) {
	auto create_info = GetInfo();
	auto &table_info = create_info->Cast<CreateTableInfo>();

	if (info.if_column_exists) {
		if (!ColumnExists(info.column_path.front())) {
			return nullptr;
		}
	}
	optional_ptr<const DuckLakeFieldId> removed_id_ptr;
	if (info.if_column_exists) {
		removed_id_ptr = TryGetFieldId(info.column_path);
	} else {
		removed_id_ptr = GetFieldId(info.column_path);
	}
	if (!removed_id_ptr) {
		return nullptr;
	}
	auto &removed_id = *removed_id_ptr;

	// generate the removed column info
	auto change_info = make_uniq<ColumnChangeInfo>();
	RemoveColumns(removed_id, change_info->dropped_fields);

	// generate the new field ids for the table
	auto &current_field_ids = field_data->GetFieldIds();
	auto new_field_ids = make_shared_ptr<DuckLakeFieldData>();
	for (idx_t col_idx = 0; col_idx < current_field_ids.size(); col_idx++) {
		auto &field_id = current_field_ids[col_idx];
		if (StringUtil::CIEquals(field_id->Name(), info.column_path[0])) {
			auto new_field_id = field_id->RemoveField(info.column_path);
			auto &col = table_info.columns.GetColumnMutable(PhysicalIndex(col_idx));
			col.SetType(new_field_id->Type());
			new_field_ids->Add(std::move(new_field_id));
		} else {
			new_field_ids->Add(field_id->Copy());
		}
	}

	auto new_entry = make_uniq<DuckLakeTableEntry>(*this, table_info, LocalChangeType::CHANGE_COLUMN_TYPE,
	                                               std::move(change_info), std::move(new_field_ids));
	return std::move(new_entry);
}

void RenameField(const DuckLakeFieldId &field_id, const DuckLakeFieldId &parent_id, string new_name,
                 ColumnChangeInfo &change_info) {
	// drop the current field
	change_info.dropped_fields.push_back(field_id.GetFieldIndex());
	// re-add the field with a different name
	DuckLakeNewColumn renamed_field;
	renamed_field.column_info.id = field_id.GetFieldIndex();
	renamed_field.column_info.name = std::move(new_name);
	renamed_field.column_info.type = DuckLakeTypes::ToString(field_id.Type());
	renamed_field.parent_idx = parent_id.GetFieldIndex().index;
	change_info.new_fields.push_back(std::move(renamed_field));
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::AlterTable(DuckLakeTransaction &transaction, RenameFieldInfo &info) {
	auto create_info = GetInfo();
	auto &table_info = create_info->Cast<CreateTableInfo>();

	auto &renamed_id = GetFieldId(info.column_path);
	auto parent_path = info.column_path;
	parent_path.pop_back();
	auto &parent_id = GetFieldId(parent_path);
	if (parent_id.Type().id() != LogicalTypeId::STRUCT) {
		throw NotImplementedException("Only rename on top-level struct fields is supported currently");
	}
	for (auto &child : StructType::GetChildTypes(parent_id.Type())) {
		if (StringUtil::CIEquals(child.first, info.new_name)) {
			throw CatalogException(
			    "Failed to rename field \"%s\" to \"%s\" - field with this name already exists in column \"%s\"",
			    info.column_path.back(), info.new_name, StringUtil::Join(parent_path, "."));
		}
	}

	// generate the removed column info
	auto change_info = make_uniq<ColumnChangeInfo>();
	RenameField(renamed_id, parent_id, info.new_name, *change_info);

	// generate the new field ids for the table
	auto &current_field_ids = field_data->GetFieldIds();
	auto new_field_ids = make_shared_ptr<DuckLakeFieldData>();
	for (idx_t col_idx = 0; col_idx < current_field_ids.size(); col_idx++) {
		auto &field_id = current_field_ids[col_idx];
		if (StringUtil::CIEquals(field_id->Name(), info.column_path[0])) {
			auto new_field_id = field_id->RenameField(info.column_path, info.new_name);
			auto &col = table_info.columns.GetColumnMutable(PhysicalIndex(col_idx));
			col.SetType(new_field_id->Type());
			new_field_ids->Add(std::move(new_field_id));
		} else {
			new_field_ids->Add(field_id->Copy());
		}
	}

	auto new_entry = make_uniq<DuckLakeTableEntry>(*this, table_info, LocalChangeType::CHANGE_COLUMN_TYPE,
	                                               std::move(change_info), std::move(new_field_ids));
	return std::move(new_entry);
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::AlterTable(DuckLakeTransaction &transaction, SetDefaultInfo &info) {
	auto create_info = GetInfo();
	auto &table_info = create_info->Cast<CreateTableInfo>();
	if (!ColumnExists(info.column_name)) {
		throw BinderException("Table \"%s\" does not have a column with name \"%s\"", name, info.column_name);
	}
	auto &col = table_info.columns.GetColumnMutable(info.column_name);
	auto &field_id = GetFieldId(col.Physical());
	col.SetDefaultValue(std::move(info.expression));
	bool new_column = !transaction.GetMetadataManager().IsColumnCreatedWithTable(table_info.table, col.GetName());

	auto new_entry = make_uniq<DuckLakeTableEntry>(
	    *this, table_info, SetDefaultLocalChange::SetDefault(field_id.GetFieldIndex(), new_column));
	return std::move(new_entry);
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::AlterTable(DuckLakeTransaction &transaction, SetSortedByInfo &info) {
	auto create_info = GetInfo();
	auto &table_info = create_info->Cast<CreateTableInfo>();

	if (info.orders.empty()) {
		// RESET SORTED BY - clear sort data
		auto new_entry = make_uniq<DuckLakeTableEntry>(*this, table_info, unique_ptr<DuckLakeSort>());
		return std::move(new_entry);
	}

	// Validate all column references in all sort expressions
	ValidateSortExpressionColumns(*this, info.orders);

	auto sort_data = make_uniq<DuckLakeSort>();
	sort_data->sort_id = transaction.GetLocalCatalogId();
	for (idx_t order_node_idx = 0; order_node_idx < info.orders.size(); order_node_idx++) {
		auto &order_node = info.orders[order_node_idx];

		DuckLakeSortField sort_field;
		sort_field.sort_key_index = order_node_idx;
		sort_field.expression = order_node.expression->ToString();
		sort_field.dialect = "duckdb";
		sort_field.sort_direction = order_node.type;
		sort_field.null_order = order_node.null_order;
		sort_data->fields.push_back(sort_field);
	}

	auto new_entry = make_uniq<DuckLakeTableEntry>(*this, table_info, std::move(sort_data));
	return std::move(new_entry);
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::Alter(DuckLakeTransaction &transaction, AlterTableInfo &info) {
	if (transaction.HasTransactionInlinedData(GetTableId())) {
		if (info.alter_table_type != AlterTableType::ADD_COLUMN &&
		    info.alter_table_type != AlterTableType::REMOVE_COLUMN &&
		    info.alter_table_type != AlterTableType::RENAME_TABLE &&
		    info.alter_table_type != AlterTableType::RENAME_COLUMN &&
		    info.alter_table_type != AlterTableType::ALTER_COLUMN_TYPE &&
		    info.alter_table_type != AlterTableType::SET_NOT_NULL &&
		    info.alter_table_type != AlterTableType::DROP_NOT_NULL) {
			throw NotImplementedException("ALTER on a table with transaction-local inlined data is not supported %s",
			                              EnumUtil::ToString(info.alter_table_type));
		}
	}
	switch (info.alter_table_type) {
	case AlterTableType::RENAME_TABLE:
		return AlterTable(transaction, info.Cast<RenameTableInfo>());
	case AlterTableType::SET_PARTITIONED_BY:
		return AlterTable(transaction, info.Cast<SetPartitionedByInfo>());
	case AlterTableType::SET_NOT_NULL:
		return AlterTable(transaction, info.Cast<SetNotNullInfo>());
	case AlterTableType::DROP_NOT_NULL:
		return AlterTable(transaction, info.Cast<DropNotNullInfo>());
	case AlterTableType::RENAME_COLUMN:
		return AlterTable(transaction, info.Cast<RenameColumnInfo>());
	case AlterTableType::ADD_COLUMN:
		return AlterTable(transaction, info.Cast<AddColumnInfo>());
	case AlterTableType::REMOVE_COLUMN:
		return AlterTable(transaction, info.Cast<RemoveColumnInfo>());
	case AlterTableType::ALTER_COLUMN_TYPE:
		return AlterTable(transaction, info.Cast<ChangeColumnTypeInfo>());
	case AlterTableType::ADD_FIELD:
		return AlterTable(transaction, info.Cast<AddFieldInfo>());
	case AlterTableType::REMOVE_FIELD:
		return AlterTable(transaction, info.Cast<RemoveFieldInfo>());
	case AlterTableType::RENAME_FIELD:
		return AlterTable(transaction, info.Cast<RenameFieldInfo>());
	case AlterTableType::SET_DEFAULT:
		return AlterTable(transaction, info.Cast<SetDefaultInfo>());
	case AlterTableType::SET_SORTED_BY:
		return AlterTable(transaction, info.Cast<SetSortedByInfo>());
	default:
		throw BinderException("Unsupported ALTER TABLE type in DuckLake");
	}
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::Alter(DuckLakeTransaction &transaction, SetCommentInfo &info) {
	auto create_info = GetInfo();
	create_info->comment = info.comment_value;
	auto &table_info = create_info->Cast<CreateTableInfo>();

	auto new_entry = make_uniq<DuckLakeTableEntry>(*this, table_info, LocalChangeType::SET_COMMENT);
	return std::move(new_entry);
}

unique_ptr<CatalogEntry> DuckLakeTableEntry::Alter(DuckLakeTransaction &transaction, SetColumnCommentInfo &info) {
	auto create_info = GetInfo();
	auto &table_info = create_info->Cast<CreateTableInfo>();
	auto &col = table_info.columns.GetColumnMutable(info.column_name);
	col.SetComment(info.comment_value);
	auto &field_id = GetFieldId(col.Physical());

	auto new_entry =
	    make_uniq<DuckLakeTableEntry>(*this, table_info, LocalChange::SetColumnComment(field_id.GetFieldIndex()));
	return std::move(new_entry);
}

DuckLakeColumnInfo DuckLakeTableEntry::GetColumnInfo(FieldIndex field_index) const {
	auto field_id = GetFieldId(field_index);
	if (!field_id) {
		throw InternalException("Field id not found in table");
	}
	auto &col = GetColumn(field_id->Name());
	auto &col_data = field_id->GetColumnData();

	DuckLakeColumnInfo result;
	result.id = field_index;
	result.name = col.Name();
	result.type = DuckLakeTypes::ToString(col.Type());
	ExtractDefaultValue(col_data, result);
	result.nulls_allowed = GetNotNullFields().count(col.Name()) == 0;
	return result;
}

DuckLakeColumnInfo DuckLakeTableEntry::ConvertColumn(const string &name, const LogicalType &type,
                                                     const DuckLakeFieldId &field_id) {
	DuckLakeColumnInfo column_entry;
	column_entry.id = field_id.GetFieldIndex();
	column_entry.name = name;
	column_entry.nulls_allowed = true;
	column_entry.type = DuckLakeTypes::ToString(type);
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		auto &struct_children = StructType::GetChildTypes(type);
		for (idx_t child_idx = 0; child_idx < struct_children.size(); ++child_idx) {
			auto &child = struct_children[child_idx];
			auto &child_id = field_id.GetChildByIndex(child_idx);
			column_entry.children.push_back(ConvertColumn(child.first, child.second, child_id));
		}
		break;
	}
	case LogicalTypeId::LIST: {
		auto &child_id = field_id.GetChildByIndex(0);
		column_entry.children.push_back(ConvertColumn("element", ListType::GetChildType(type), child_id));
		break;
	}
	case LogicalTypeId::ARRAY: {
		auto &child_id = field_id.GetChildByIndex(0);
		column_entry.children.push_back(ConvertColumn("element", ArrayType::GetChildType(type), child_id));
		break;
	}
	case LogicalTypeId::MAP: {
		auto &key_id = field_id.GetChildByIndex(0);
		auto &value_id = field_id.GetChildByIndex(1);
		column_entry.children.push_back(ConvertColumn("key", MapType::KeyType(type), key_id));
		column_entry.children.push_back(ConvertColumn("value", MapType::ValueType(type), value_id));
		break;
	}
	default: {
		auto &column_data = field_id.GetColumnData();
		ExtractDefaultValue(column_data, column_entry);
		break;
	}
	}
	return column_entry;
}

DuckLakeColumnInfo DuckLakeTableEntry::GetAddColumnInfo() const {
	// the column that is added is always the last column
	LogicalIndex new_col_idx(columns.LogicalColumnCount() - 1);
	auto &new_col = GetColumn(new_col_idx);

	auto &field_id = field_data->GetByRootIndex(new_col.Physical());
	return ConvertColumn(new_col.Name(), new_col.Type(), field_id);
}

TableStorageInfo DuckLakeTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo storage_info;
	auto table_stats = GetTableStats(context);
	if (table_stats) {
		storage_info.cardinality = table_stats->record_count;
	}
	return storage_info;
}

} // namespace duckdb
