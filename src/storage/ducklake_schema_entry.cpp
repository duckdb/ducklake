#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_view_entry.hpp"
#include "storage/ducklake_catalog.hpp"

#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"

namespace duckdb {

DuckLakeSchemaEntry::DuckLakeSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, SchemaIndex schema_id,
                                         string schema_uuid, string data_path_p)
    : SchemaCatalogEntry(catalog, info), schema_id(schema_id), schema_uuid(std::move(schema_uuid)),
      data_path(std::move(data_path_p)) {
}

bool DuckLakeSchemaEntry::HandleCreateConflict(CatalogTransaction transaction, CatalogType catalog_type,
                                               const string &entry_name, OnCreateConflict on_conflict) {
	auto existing_entry = GetEntry(transaction, catalog_type, entry_name);
	if (!existing_entry) {
		// no conflict
		return true;
	}
	switch (on_conflict) {
	case OnCreateConflict::ERROR_ON_CONFLICT:
		throw CatalogException("%s with name \"%s\" already exists", CatalogTypeToString(existing_entry->type),
		                       entry_name);
	case OnCreateConflict::IGNORE_ON_CONFLICT:
		// ignore - skip without throwing an error
		return false;
	case OnCreateConflict::REPLACE_ON_CONFLICT: {
		if (existing_entry->type != catalog_type) {
			throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", entry_name,
			                       CatalogTypeToString(existing_entry->type), CatalogTypeToString(catalog_type));
		}
		// try to drop the entry prior to creating
		DropInfo info;
		info.type = catalog_type;
		info.name = entry_name;
		DropEntry(transaction.GetContext(), info);
		break;
	}
	default:
		throw InternalException("Unsupported conflict type");
	}
	return true;
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::CreateTableExtended(CatalogTransaction transaction,
                                                                    BoundCreateTableInfo &info, string table_uuid,
                                                                    string table_data_path) {
	auto &duck_transaction = transaction.transaction->Cast<DuckLakeTransaction>();
	auto &base_info = info.Base();
	// check if we have an existing entry with this name
	if (!HandleCreateConflict(transaction, CatalogType::TABLE_ENTRY, base_info.table, base_info.on_conflict)) {
		return nullptr;
	}
	//! get a local table-id
	auto table_id = TableIndex(duck_transaction.GetLocalCatalogId());
	// generate field ids based on the column ids
	idx_t column_id = 1;
	auto field_data = DuckLakeFieldData::FromColumns(base_info.columns, column_id);
	vector<DuckLakeInlinedTableInfo> inlined_tables;
	auto table_entry = make_uniq<DuckLakeTableEntry>(ParentCatalog(), *this, base_info, table_id, std::move(table_uuid),
	                                                 std::move(table_data_path), std::move(field_data), column_id,
	                                                 std::move(inlined_tables), LocalChangeType::CREATED);
	auto result = table_entry.get();
	duck_transaction.CreateEntry(std::move(table_entry));
	return result;
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                            BoundCreateTableInfo &info) {
	auto &duck_transaction = transaction.transaction->Cast<DuckLakeTransaction>();
	auto &duck_catalog = catalog.Cast<DuckLakeCatalog>();
	auto &base_info = info.Base();
	auto table_uuid = duck_transaction.GenerateUUID();
	auto table_data_path = DataPath() + duck_catalog.GeneratePathFromName(table_uuid, base_info.table);
	return CreateTableExtended(transaction, info, std::move(table_uuid), std::move(table_data_path));
}

bool DuckLakeSchemaEntry::CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return true;
	default:
		return false;
	}
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                               CreateFunctionInfo &info) {
	throw NotImplementedException("DuckLake does not support functions");
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                            TableCatalogEntry &table) {
	throw NotImplementedException("DuckLake does not support indexes");
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	// check if we have an existing entry with this name
	if (!HandleCreateConflict(transaction, CatalogType::VIEW_ENTRY, info.view_name, info.on_conflict)) {
		return nullptr;
	}
	auto &duck_transaction = transaction.transaction->Cast<DuckLakeTransaction>();
	//! get a local view-id
	auto view_id = TableIndex(duck_transaction.GetLocalCatalogId());
	auto view_uuid = UUID::ToString(UUID::GenerateRandomUUID());

	auto view_entry = make_uniq<DuckLakeViewEntry>(ParentCatalog(), *this, info, view_id, std::move(view_uuid),
	                                               info.query->ToString(), LocalChangeType::CREATED);
	auto result = view_entry.get();
	duck_transaction.CreateEntry(std::move(view_entry));
	return result;
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                               CreateSequenceInfo &info) {
	throw NotImplementedException("DuckLake does not support sequences");
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                    CreateTableFunctionInfo &info) {
	throw NotImplementedException("DuckLake does not support table functions");
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                   CreateCopyFunctionInfo &info) {
	throw NotImplementedException("DuckLake does not support copy functions");
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                     CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("DuckLake does not support pragma functions");
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                                CreateCollationInfo &info) {
	throw NotImplementedException("DuckLake does not support collations");
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("DuckLake does not support user-defined types");
}

void DuckLakeSchemaEntry::Alter(CatalogTransaction catalog_transaction, AlterInfo &info) {
	auto &context = catalog_transaction.GetContext();
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	switch (info.type) {
	case AlterType::ALTER_TABLE: {
		auto &alter = info.Cast<AlterTableInfo>();
		auto table_entry = GetEntry(catalog_transaction, CatalogType::TABLE_ENTRY, alter.name);
		if (table_entry->type != CatalogType::TABLE_ENTRY) {
			throw BinderException("Cannot use ALTER TABLE on entry %s - it is not a table", alter.name);
		}
		auto &table = table_entry->Cast<DuckLakeTableEntry>();
		auto new_table = table.Alter(transaction, alter);
		transaction.AlterEntry(table, std::move(new_table));
		break;
	}
	case AlterType::ALTER_VIEW: {
		auto &alter = info.Cast<AlterViewInfo>();
		auto view_entry = GetEntry(catalog_transaction, CatalogType::VIEW_ENTRY, alter.name);
		if (view_entry->type != CatalogType::VIEW_ENTRY) {
			throw BinderException("Cannot use ALTER VIEW on entry %s - it is not a view", alter.name);
		}
		auto &view = view_entry->Cast<DuckLakeViewEntry>();
		auto new_view = view.AlterEntry(context, alter);
		transaction.AlterEntry(view, std::move(new_view));
		break;
	}
	case AlterType::SET_COMMENT: {
		auto &alter = info.Cast<SetCommentInfo>();
		switch (alter.entry_catalog_type) {
		case CatalogType::TABLE_ENTRY: {
			auto table_entry = GetEntry(catalog_transaction, CatalogType::TABLE_ENTRY, alter.name);
			if (table_entry->type != CatalogType::TABLE_ENTRY) {
				throw BinderException("Cannot use ALTER TABLE on entry %s - it is not a table", alter.name);
			}
			auto &table = table_entry->Cast<DuckLakeTableEntry>();
			auto new_table = table.Alter(transaction, alter);
			transaction.AlterEntry(table, std::move(new_table));
			break;
		}
		case CatalogType::VIEW_ENTRY: {
			auto view_entry = GetEntry(catalog_transaction, CatalogType::VIEW_ENTRY, alter.name);
			if (view_entry->type != CatalogType::VIEW_ENTRY) {
				throw BinderException("Cannot use ALTER VIEW on entry %s - it is not a view", alter.name);
			}
			auto &view = view_entry->Cast<DuckLakeViewEntry>();
			auto new_view = view.AlterEntry(context, alter);
			transaction.AlterEntry(view, std::move(new_view));
			break;
		}
		default:
			throw BinderException("Unsupported catalog type for SET COMMENT in DuckLake");
		}
		break;
	}
	case AlterType::SET_COLUMN_COMMENT: {
		auto &alter = info.Cast<SetColumnCommentInfo>();
		auto table_entry = GetEntry(catalog_transaction, CatalogType::TABLE_ENTRY, alter.name);
		if (table_entry->type != CatalogType::TABLE_ENTRY) {
			throw BinderException("Cannot comment on columns for entry %s - it is not a table", alter.name);
		}
		auto &table = table_entry->Cast<DuckLakeTableEntry>();
		auto new_table = table.Alter(transaction, alter);
		transaction.AlterEntry(table, std::move(new_table));
		break;
	}
	default:
		throw BinderException("Unsupported ALTER type for DuckLake");
	}
}

void DuckLakeSchemaEntry::Scan(ClientContext &context, CatalogType type,
                               const std::function<void(CatalogEntry &)> &callback) {
	if (!CatalogTypeIsSupported(type)) {
		return;
	}
	// scan transaction-local entries
	auto &duck_transaction = DuckLakeTransaction::Get(context, ParentCatalog());
	auto local_set = duck_transaction.GetTransactionLocalEntries(type, name);
	if (local_set) {
		for (auto &entry : local_set->GetEntries()) {
			callback(*entry.second);
		}
	}
	// scan committed entries
	auto &catalog_set = GetCatalogSet(type);
	for (auto &entry : catalog_set.GetEntries()) {
		if (duck_transaction.IsDeleted(*entry.second)) {
			continue;
		}
		if (local_set && local_set->GetEntry(entry.second->name)) {
			// this entry exists in both the local and global set - emit only the transaction-local entry
			continue;
		}
		if (entry.second->type == CatalogType::VIEW_ENTRY) {
			auto view_entry = dynamic_cast<DuckLakeViewEntry *>(entry.second.get());
			if (!view_entry->IsBound()) {
				try {
					view_entry->Bind(context);
				} catch (...) {
					//
				}
			}
		}
		callback(*entry.second);
	}
}

void DuckLakeSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	auto &catalog_set = GetCatalogSet(type);
	for (auto &entry : catalog_set.GetEntries()) {
		callback(*entry.second);
	}
}

void DuckLakeSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.cascade) {
		throw NotImplementedException("Cascade Drop not supported in DuckLake");
	}
	auto catalog_entry = GetEntry(GetCatalogTransaction(context), info.type, info.name);
	if (!catalog_entry) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw InternalException("Failed to drop entry \"%s\" - could not find entry", info.name);
	}
	if (catalog_entry->type != info.type) {
		throw CatalogException("Existing object %s is of type %s, trying to drop type %s", catalog_entry->name,
		                       CatalogTypeToString(catalog_entry->type), CatalogTypeToString(info.type));
	}
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	transaction.DropEntry(*catalog_entry);
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                            const EntryLookupInfo &lookup_info) {
	auto catalog_type = lookup_info.GetCatalogType();
	auto &entry_name = lookup_info.GetEntryName();
	if (catalog_type == CatalogType::TABLE_FUNCTION_ENTRY) {
		auto entry = TryLoadBuiltInFunction(entry_name);
		if (entry) {
			return entry;
		}
	}
	if (!CatalogTypeIsSupported(catalog_type)) {
		return nullptr;
	}
	auto &duck_transaction = transaction.transaction->Cast<DuckLakeTransaction>();
	//! search in transaction local storage first
	auto transaction_entry = duck_transaction.GetTransactionLocalEntry(catalog_type, name, entry_name);
	if (transaction_entry) {
		return transaction_entry;
	}
	auto &catalog_set = GetCatalogSet(catalog_type);
	auto entry = catalog_set.GetEntry(entry_name);
	if (!entry) {
		return nullptr;
	}
	if (duck_transaction.IsDeleted(*entry)) {
		return nullptr;
	}
	return *entry;
}

void DuckLakeSchemaEntry::AddEntry(CatalogType type, unique_ptr<CatalogEntry> entry) {
	auto &catalog_set = GetCatalogSet(type);
	catalog_set.CreateEntry(std::move(entry));
}

void DuckLakeSchemaEntry::TryDropSchema(DuckLakeTransaction &transaction, bool cascade) {
	if (!cascade) {
		// get a list of all dependents
		vector<reference<CatalogEntry>> dependents;
		for (auto &entry : tables.GetEntries()) {
			dependents.push_back(*entry.second);
		}
		if (dependents.empty()) {
			return;
		}
		string error_string = "Cannot drop schema \"" + name + "\" because there are entries that depend on it\n";
		for (auto &dependent : dependents) {
			auto &dep = dependent.get();
			error_string += StringUtil::Format("%s %s depends on %s.\n", CatalogTypeToString(dep.type), dep.name, name);
		}
		error_string += "Use DROP...CASCADE to drop all dependents.";
		throw CatalogException(error_string);
	}
	// drop all dependents
	for (auto &entry : tables.GetEntries()) {
		transaction.DropEntry(*entry.second);
	}
}

DuckLakeCatalogSet &DuckLakeSchemaEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return tables;
	default:
		throw NotImplementedException("Unsupported catalog type %s for DuckLake", CatalogTypeToString(type));
	}
}

} // namespace duckdb
