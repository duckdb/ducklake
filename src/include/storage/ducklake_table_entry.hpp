//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_partition_data.hpp"
#include "storage/ducklake_sort_data.hpp"
#include "common/index.hpp"
#include "storage/ducklake_field_data.hpp"
#include "common/local_change.hpp"
#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {
struct AlterTableInfo;
struct DuckLakeColumnInfo;
struct SetPartitionedByInfo;
struct SetCommentInfo;
class DuckLakeTransaction;

struct ColumnChangeInfo {
	vector<DuckLakeNewColumn> new_fields;
	vector<FieldIndex> dropped_fields;

	void DropField(const DuckLakeFieldId &field_id);
};

class DuckLakeTableEntry : public TableCatalogEntry {
public:
	DuckLakeTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, TableIndex table_id,
	                   string table_uuid, string data_path, shared_ptr<DuckLakeFieldData> field_data,
	                   optional_idx next_column_id, vector<DuckLakeInlinedTableInfo> inlined_data_tables,
	                   LocalChange local_change);

public:
	TableIndex GetTableId() const {
		return table_id;
	}
	const string &GetTableUUID() const {
		return table_uuid;
	}
	bool IsTransactionLocal() const {
		return local_change.type != LocalChangeType::NONE;
	}
	LocalChange GetLocalChange() const {
		return local_change;
	}
	optional_ptr<DuckLakePartition> GetPartitionData() {
		return partition_data.get();
	}
	optional_ptr<DuckLakeSort> GetSortData() {
		return sort_data.get();
	}
	DuckLakeFieldData &GetFieldData() {
		return *field_data;
	}
	const ColumnChangeInfo &GetChangedFields() const {
		return *changed_fields;
	}
	const vector<DuckLakeInlinedTableInfo> &GetInlinedDataTables() const {
		return inlined_data_tables;
	}
	const ColumnDefinition &GetColumnByFieldId(FieldIndex field_index) const;
	//! Returns the root field id of a column
	const DuckLakeFieldId &GetFieldId(PhysicalIndex column_index) const;
	//! Returns the field id of a column by a column path.
	// If name_offset is provided and column_names points to a field **within** the variant, the variant column is
	// returned and the offset in the column_names vector where the variant is located
	const DuckLakeFieldId &GetFieldId(const vector<string> &column_names,
	                                  optional_ptr<optional_idx> name_offset = nullptr) const;
	//! Returns the field id of a column by a column path if it exists (and nullptr otherwise)
	optional_ptr<const DuckLakeFieldId> TryGetFieldId(const vector<string> &column_names,
	                                                  optional_ptr<optional_idx> name_offset = nullptr) const;
	//! Returns the field id of a column by a field index
	optional_ptr<const DuckLakeFieldId> GetFieldId(FieldIndex field_index) const;
	void SetPartitionData(unique_ptr<DuckLakePartition> partition_data);
	void SetSortData(unique_ptr<DuckLakeSort> sort_data);
	optional_ptr<DuckLakeTableStats> GetTableStats(ClientContext &context);
	optional_ptr<DuckLakeTableStats> GetTableStats(DuckLakeTransaction &transaction);
	idx_t GetNetDataFileRowCount(DuckLakeTransaction &transaction);
	idx_t GetNetInlinedRowCount(DuckLakeTransaction &transaction);

	//! Gets the top-level not-null fields
	case_insensitive_set_t GetNotNullFields() const;

	DuckLakeTableInfo GetTableInfo() const;
	vector<DuckLakeColumnInfo> GetTableColumns() const;

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup_info) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	unique_ptr<CatalogEntry> Alter(DuckLakeTransaction &transaction, AlterTableInfo &info);
	unique_ptr<CatalogEntry> Alter(DuckLakeTransaction &transaction, SetCommentInfo &info);
	unique_ptr<CatalogEntry> Alter(DuckLakeTransaction &transaction, SetColumnCommentInfo &info);

	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

	DuckLakeColumnInfo GetColumnInfo(FieldIndex field_index) const;
	DuckLakeColumnInfo GetAddColumnInfo() const;

	static DuckLakeColumnInfo ConvertColumn(const string &name, const LogicalType &type,
	                                        const DuckLakeFieldId &field_id);
	void RequireNextColumnId(DuckLakeTransaction &transaction);

	const string &DataPath() const;
	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;

	//! Validates that all column references in sort expressions exist in the table
	static void ValidateSortExpressionColumns(DuckLakeTableEntry &table,
	                                          const vector<reference<ParsedExpression>> &expressions);

private:
	unique_ptr<CatalogEntry> AlterTable(DuckLakeTransaction &transaction, RenameTableInfo &info);
	unique_ptr<CatalogEntry> AlterTable(DuckLakeTransaction &transaction, SetPartitionedByInfo &info);
	unique_ptr<CatalogEntry> AlterTable(DuckLakeTransaction &transaction, SetNotNullInfo &info);
	unique_ptr<CatalogEntry> AlterTable(DuckLakeTransaction &transaction, DropNotNullInfo &info);
	unique_ptr<CatalogEntry> AlterTable(DuckLakeTransaction &transaction, RenameColumnInfo &info);
	unique_ptr<CatalogEntry> AlterTable(DuckLakeTransaction &transaction, AddColumnInfo &info);
	unique_ptr<CatalogEntry> AlterTable(DuckLakeTransaction &transaction, RemoveColumnInfo &info);
	unique_ptr<CatalogEntry> AlterTable(DuckLakeTransaction &transaction, ChangeColumnTypeInfo &info);
	unique_ptr<CatalogEntry> AlterTable(DuckLakeTransaction &transaction, AddFieldInfo &info);
	unique_ptr<CatalogEntry> AlterTable(DuckLakeTransaction &transaction, RemoveFieldInfo &info);
	unique_ptr<CatalogEntry> AlterTable(DuckLakeTransaction &transaction, RenameFieldInfo &info);
	unique_ptr<CatalogEntry> AlterTable(DuckLakeTransaction &transaction, SetDefaultInfo &info);
	unique_ptr<CatalogEntry> AlterTable(DuckLakeTransaction &transaction, SetSortedByInfo &info);

	unique_ptr<DuckLakeFieldId> GetNestedEvolution(const DuckLakeFieldId &source_id, const LogicalType &target,
	                                               ColumnChangeInfo &result, optional_idx parent_idx);
	unique_ptr<DuckLakeFieldId> TypePromotion(const DuckLakeFieldId &source_id, const LogicalType &target,
	                                          ColumnChangeInfo &result, optional_idx parent_idx);

	void CheckSupportedTypes();

public:
	// ! Create a DuckLakeTableEntry from an ALTER
	DuckLakeTableEntry(DuckLakeTableEntry &parent, CreateTableInfo &info, LocalChange local_change);
	DuckLakeTableEntry(DuckLakeTableEntry &parent, CreateTableInfo &info, SetDefaultLocalChange local_change);

	// ! Create a DuckLakeTableEntry from a RENAME COLUMN
	DuckLakeTableEntry(DuckLakeTableEntry &parent, CreateTableInfo &info, LocalChange local_change,
	                   const string &new_name);
	// ! Create a DuckLakeTableEntry from a REMOVE COLUMN
	DuckLakeTableEntry(DuckLakeTableEntry &parent, CreateTableInfo &info, LocalChange local_change,
	                   unique_ptr<ColumnChangeInfo> changed_fields);
	// ! Create a DuckLakeTableEntry from a CHANGE COLUMN TYPE
	DuckLakeTableEntry(DuckLakeTableEntry &parent, CreateTableInfo &info, LocalChange local_change,
	                   unique_ptr<ColumnChangeInfo> changed_fields, shared_ptr<DuckLakeFieldData> new_field_data);
	// ! Create a DuckLakeTableEntry from a SET PARTITION KEY
	DuckLakeTableEntry(DuckLakeTableEntry &parent, CreateTableInfo &info, unique_ptr<DuckLakePartition> partition_data);
	// ! Create a DuckLakeTableEntry from a SET SORT KEY
	DuckLakeTableEntry(DuckLakeTableEntry &parent, CreateTableInfo &info, unique_ptr<DuckLakeSort> sort_data);

private:
	TableIndex table_id;
	string table_uuid;
	string data_path;
	shared_ptr<DuckLakeFieldData> field_data;
	optional_idx next_column_id;
	vector<DuckLakeInlinedTableInfo> inlined_data_tables;
	LocalChange local_change;
	unique_ptr<DuckLakePartition> partition_data;
	unique_ptr<DuckLakeSort> sort_data;
	// only set for REMOVED_COLUMN
	unique_ptr<ColumnChangeInfo> changed_fields;
};

} // namespace duckdb
