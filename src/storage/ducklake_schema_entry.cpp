#include "storage/ducklake_schema_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/similar_catalog_entry.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "storage/ducklake_catalog.hpp"
#include "common/ducklake_types.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_view_entry.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "storage/ducklake_macro_entry.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/optimizer/remote_pushdown_optimizer.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"

namespace duckdb {

DuckLakeSchemaEntry::DuckLakeSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, SchemaIndex schema_id,
                                         string schema_uuid, string data_path_p,
                                         optional_ptr<DuckLakeSchemaEntry> parent_schema_p)
    : SchemaCatalogEntry(catalog, info), schema_id(schema_id), schema_uuid(std::move(schema_uuid)),
      data_path(std::move(data_path_p)), parent_schema(parent_schema_p), schema_depth(1) {
	RefreshPathKey();
}

DuckLakeSchemaEntry::~DuckLakeSchemaEntry() {
	vector<unique_ptr<CatalogEntry>> pending;
	child_schemas.MoveEntriesTo(pending);
	while (!pending.empty()) {
		auto entry = std::move(pending.back());
		pending.pop_back();
		if (entry->type != CatalogType::SCHEMA_ENTRY) {
			continue;
		}
		entry->Cast<DuckLakeSchemaEntry>().child_schemas.MoveEntriesTo(pending);
	}
}

optional_ptr<SchemaCatalogEntry> DuckLakeSchemaEntry::GetParentSchema() const {
	return parent_schema.get_mutable();
}

void DuckLakeSchemaEntry::SetParentSchema(DuckLakeSchemaEntry &parent) {
	parent_schema = &parent;
	RefreshPathKey();
}

void DuckLakeSchemaEntry::RefreshPathKey() {
	path_key = ChildPathKey(parent_schema.get(), name.GetIdentifierName());
	schema_depth = parent_schema ? parent_schema->SchemaDepth() + 1 : 1;
}

const string &DuckLakeSchemaEntry::PathKey() const {
	return path_key;
}

string DuckLakeSchemaEntry::ChildPathKey(optional_ptr<const DuckLakeSchemaEntry> parent, const string &name) {
	auto key = SQLQuotedIdentifier::ToString(name);
	if (!parent) {
		return key;
	}
	return parent->PathKey() + "." + key;
}

unique_ptr<CreateInfo> DuckLakeSchemaEntry::GetInfo() const {
	auto result = SchemaCatalogEntry::GetInfo();
	if (parent_schema) {
		result->SetQualifiedName(GetQualifiedName(Identifier()));
	}
	result->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	return result;
}

bool DuckLakeSchemaEntry::HandleCreateConflict(CatalogTransaction transaction, CatalogType catalog_type,
                                               const string &entry_name, OnCreateConflict on_conflict) {
	auto existing_entry = GetEntry(transaction, catalog_type, Identifier(entry_name));
	if (!existing_entry) {
		// no conflict
		return true;
	}
	switch (on_conflict) {
	case OnCreateConflict::ERROR_ON_CONFLICT:
		throw CatalogException("%s with name \"%s\" already exists!", CatalogTypeToString(existing_entry->type),
		                       entry_name);
	case OnCreateConflict::IGNORE_ON_CONFLICT:
		// ignore - skip without throwing an error
		return false;
	case OnCreateConflict::REPLACE_ON_CONFLICT: {
		if (existing_entry->type != catalog_type) {
			throw CatalogException("Existing object %s is of type %s, trying to replace with type %s",
			                       Identifier(entry_name), CatalogTypeToString(existing_entry->type),
			                       CatalogTypeToString(catalog_type));
		}
		// try to drop the entry prior to creating
		DropInfo info;
		info.type = catalog_type;
		info.SetName(Identifier(entry_name));
		DropEntry(transaction.GetContext(), info);
		break;
	}
	default:
		throw InternalException("Unsupported conflict type");
	}
	return true;
}

optional_ptr<CatalogEntry>
DuckLakeSchemaEntry::CreateTableExtended(CatalogTransaction transaction, BoundCreateTableInfo &info, string table_uuid,
                                         string table_data_path, unique_ptr<DuckLakePartition> prebuilt_partition_data,
                                         unique_ptr<DuckLakeSort> prebuilt_sort_data,
                                         map<string, string> prebuilt_table_options) {
	auto &duck_transaction = transaction.transaction->Cast<DuckLakeTransaction>();
	auto &base_info = info.Base();
	// check if we have an existing entry with this name
	if (!HandleCreateConflict(transaction, CatalogType::TABLE_ENTRY, base_info.GetTableName().GetIdentifierName(),
	                          base_info.on_conflict)) {
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
	// CTAS passes prebuilt specs (ids must match the on-disk files); plain CREATE rebuilds from the inline clauses.
	if (prebuilt_partition_data) {
		table_entry->SetPartitionData(std::move(prebuilt_partition_data));
	} else if (!base_info.partition_keys.empty()) {
		table_entry->SetPartitionData(DuckLakeTableEntry::BuildPartitionData(
		    duck_transaction, table_entry->GetColumns(), table_entry->GetFieldData(), base_info.partition_keys));
	}
	if (prebuilt_sort_data) {
		table_entry->SetSortData(std::move(prebuilt_sort_data));
	} else if (!base_info.sort_keys.empty()) {
		table_entry->SetSortData(
		    DuckLakeTableEntry::BuildSortData(duck_transaction, table_entry->GetColumns(), base_info.sort_keys));
	}
	if (!prebuilt_table_options.empty()) {
		table_entry->SetTableOptions(std::move(prebuilt_table_options));
	} else if (!base_info.options.empty()) {
		table_entry->SetTableOptions(DuckLakeTableEntry::ParseTableOptions(
		    transaction.GetContext(), catalog.Cast<DuckLakeCatalog>(), base_info.options, table_entry->GetColumns(),
		    table_entry->GetFieldData(), table_entry->GetPartitionData().get(),
		    base_info.GetTableName().GetIdentifierName()));
	}
	DuckLakeUtil::ValidateNoInlinedSystemColumns(catalog.Cast<DuckLakeCatalog>(), transaction.GetContext(), schema_id,
	                                             table_entry->GetColumns(), &table_entry->GetTableOptions());
	auto result = table_entry.get();
	duck_transaction.CreateEntry(std::move(table_entry));
	return result;
}

string DuckLakeSchemaEntry::GenerateTableDataPath(const string &table_uuid, const string &table_name) const {
	auto &duck_catalog = catalog.Cast<DuckLakeCatalog>();
	return DataPath() + duck_catalog.GeneratePathFromName(table_uuid, table_name);
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                            BoundCreateTableInfo &info) {
	auto &duck_transaction = transaction.transaction->Cast<DuckLakeTransaction>();
	auto &base_info = info.Base();
	auto table_uuid = duck_transaction.GenerateUUID();
	auto table_data_path = GenerateTableDataPath(table_uuid, base_info.GetTableName().GetIdentifierName());
	return CreateTableExtended(transaction, info, std::move(table_uuid), std::move(table_data_path));
}

bool DuckLakeSchemaEntry::CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::SCHEMA_ENTRY:
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
	case CatalogType::SCALAR_FUNCTION_ENTRY:
	case CatalogType::TABLE_FUNCTION_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
	case CatalogType::MACRO_ENTRY:
		return true;
	default:
		return false;
	}
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                               CreateFunctionInfo &info) {
	unique_ptr<CatalogEntry> macro_entry;
	auto &create_macro_info = info.Cast<CreateMacroInfo>();
	auto &ducklake_catalog = ParentCatalog().Cast<DuckLakeCatalog>();
	auto version = ducklake_catalog.GetDuckLakeVersion();
	for (auto &macro : create_macro_info.macros) {
		for (auto &type : macro->types) {
			DuckLakeTypes::CheckSupportedType(type, version);
			if (DuckLakeTypes::IsNested(type) && !ducklake_catalog.SupportsV1_1Metadata()) {
				ThrowUnsupportedByVersion(version, "nested macro parameter types");
			}
		}
		for (auto &entry : macro->default_parameters) {
			Value default_value;
			if (DuckLakeUtil::TryGetLiteralValue(*entry.second, default_value)) {
				DuckLakeTypes::CheckSupportedType(default_value.IsNull() ? LogicalType::SQLNULL : default_value.type(),
				                                  version);
			}
			if (!ducklake_catalog.SupportsV1_1Metadata() &&
			    !DuckLakeUtil::TryGetMacroDefaultLiteral(*entry.second, default_value)) {
				ThrowUnsupportedByVersion(version, "macro parameter defaults that are not scalar constants");
			}
		}
	}
	switch (info.type) {
	case CatalogType::MACRO_ENTRY:
		macro_entry = make_uniq<ScalarMacroCatalogEntry>(ParentCatalog(), *this, create_macro_info);
		break;
	case CatalogType::TABLE_MACRO_ENTRY:
		macro_entry = make_uniq<TableMacroCatalogEntry>(ParentCatalog(), *this, create_macro_info);
		break;
	default:
		throw NotImplementedException("DuckLake does not support %s functions", CatalogTypeToString(info.type));
	}
	// We check if there is a conflict, as multi-macro implementations are only supported if they do not exist yet
	if (!HandleCreateConflict(transaction, info.type, info.GetFunctionName().GetIdentifierName(), info.on_conflict)) {
		return nullptr;
	}
	auto &duck_transaction = transaction.transaction->Cast<DuckLakeTransaction>();
	auto result = macro_entry.get();
	duck_transaction.CreateEntry(std::move(macro_entry));
	return result;
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                            TableCatalogEntry &table) {
	throw NotImplementedException("DuckLake does not support indexes");
}

//! The references of a pivot keep their catalog qualifier after binding, every other one is stripped
static void StripCatalogFromPivots(QueryNode &node, const Identifier &catalog_name);

static void StripCatalogFromPivots(ParsedExpression &expr, const Identifier &catalog_name) {
	RemotePushdownOptimizer::StripCatalogName(expr, catalog_name);
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		StripCatalogFromPivots(*expr.Cast<SubqueryExpression>().SubqueryMutable()->node, catalog_name);
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](unique_ptr<ParsedExpression> &child) { StripCatalogFromPivots(*child, catalog_name); });
}

static void StripCatalogFromPivots(QueryNode &node, const Identifier &catalog_name) {
	auto strip_expression = [&](unique_ptr<ParsedExpression> &expr) {
		StripCatalogFromPivots(*expr, catalog_name);
	};
	auto strip_ref = [&](TableRef &ref) {
		RemotePushdownOptimizer::StripCatalogName(ref, catalog_name);
		if (ref.type != TableReferenceType::PIVOT) {
			return;
		}
		for (auto &pivot : ref.Cast<PivotRef>().pivots) {
			for (auto &expr : pivot.pivot_expressions) {
				StripCatalogFromPivots(*expr, catalog_name);
			}
			for (auto &entry : pivot.entries) {
				if (entry.expr) {
					StripCatalogFromPivots(*entry.expr, catalog_name);
				}
			}
		}
	};
	ParsedExpressionIterator::EnumerateQueryNodeChildren(node, strip_expression, strip_ref);
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	if (info.security_type == ViewSecurityType::SECURE_VIEW) {
		throw NotImplementedException("DuckLake does not support secure views");
	}
	// check if we have an existing entry with this name
	if (!HandleCreateConflict(transaction, CatalogType::VIEW_ENTRY, info.GetViewName().GetIdentifierName(),
	                          info.on_conflict)) {
		return nullptr;
	}
	auto &duck_transaction = transaction.transaction->Cast<DuckLakeTransaction>();
	// get a local view-id
	auto view_id = TableIndex(duck_transaction.GetLocalCatalogId());
	auto view_uuid = UUID::ToString(UUID::GenerateRandomUUID());

	StripCatalogFromPivots(*info.query->node, ParentCatalog().GetName());
	auto query_sql = info.query->ToString();

	auto view_entry = make_uniq<DuckLakeViewEntry>(ParentCatalog(), *this, info, view_id, std::move(view_uuid),
	                                               query_sql, LocalChangeType::CREATED);
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

namespace {

bool TryApplySetColumnCommentToTable(DuckLakeTransaction &transaction, CatalogTransaction catalog_transaction,
                                     DuckLakeSchemaEntry &schema, SetColumnCommentInfo &alter) {
	auto table_entry = schema.GetEntry(catalog_transaction, CatalogType::TABLE_ENTRY, alter.GetQualifiedName().Name());
	if (!table_entry || table_entry->type != CatalogType::TABLE_ENTRY) {
		return false;
	}
	auto &table = table_entry->Cast<DuckLakeTableEntry>();
	auto new_table = table.Alter(transaction, alter);
	transaction.AlterEntry(table, std::move(new_table));
	return true;
}

void ApplySetColumnCommentToTable(DuckLakeTransaction &transaction, CatalogTransaction catalog_transaction,
                                  DuckLakeSchemaEntry &schema, SetColumnCommentInfo &alter,
                                  const char *not_a_table_message) {
	if (!TryApplySetColumnCommentToTable(transaction, catalog_transaction, schema, alter)) {
		throw BinderException(not_a_table_message, alter.GetQualifiedName().Name());
	}
}

void ApplySetColumnCommentToView(DuckLakeTransaction &transaction, CatalogTransaction catalog_transaction,
                                 DuckLakeSchemaEntry &schema, SetColumnCommentInfo &alter,
                                 const char *not_a_view_message) {
	auto view_entry = schema.GetEntry(catalog_transaction, CatalogType::VIEW_ENTRY, alter.GetQualifiedName().Name());
	if (!view_entry || view_entry->type != CatalogType::VIEW_ENTRY) {
		throw BinderException(not_a_view_message, alter.GetQualifiedName().Name());
	}
	auto &view = view_entry->Cast<DuckLakeViewEntry>();
	auto new_view = view.Alter(transaction, alter);
	transaction.AlterEntry(view, std::move(new_view));
}

} // namespace

void DuckLakeSchemaEntry::Alter(CatalogTransaction catalog_transaction, AlterInfo &info) {
	auto &context = catalog_transaction.GetContext();
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	switch (info.type) {
	case AlterType::ALTER_TABLE: {
		auto &alter = info.Cast<AlterTableInfo>();
		auto table_entry = GetEntry(catalog_transaction, CatalogType::TABLE_ENTRY, alter.GetQualifiedName().Name());
		if (table_entry->type != CatalogType::TABLE_ENTRY) {
			throw BinderException("Cannot use ALTER TABLE on entry %s - it is not a table",
			                      alter.GetQualifiedName().Name());
		}
		auto &table = table_entry->Cast<DuckLakeTableEntry>();
		auto new_table = table.Alter(context, transaction, alter);
		if (alter.alter_table_type == AlterTableType::RENAME_TABLE) {
			// We must check if this view name does not yet exist.
			auto existing_table = GetEntry(catalog_transaction, CatalogType::TABLE_ENTRY, new_table->name);
			if (alter.GetQualifiedName().Name() != new_table->name && existing_table) {
				throw BinderException("Cannot rename table %s to %s, since %s already exists.",
				                      alter.GetQualifiedName().Name(), new_table->name, new_table->name);
			}
		}
		transaction.AlterEntry(table, std::move(new_table));
		break;
	}
	case AlterType::ALTER_VIEW: {
		auto &alter = info.Cast<AlterViewInfo>();
		auto view_entry = GetEntry(catalog_transaction, CatalogType::VIEW_ENTRY, alter.GetQualifiedName().Name());
		if (view_entry->type != CatalogType::VIEW_ENTRY) {
			throw BinderException("Cannot use ALTER VIEW on entry %s - it is not a view",
			                      alter.GetQualifiedName().Name());
		}
		auto &view = view_entry->Cast<DuckLakeViewEntry>();
		auto new_view = view.AlterEntry(context, alter);
		if (alter.alter_view_type == AlterViewType::RENAME_VIEW) {
			// We must check if this view name does not yet exist.
			auto existing_view = GetEntry(catalog_transaction, CatalogType::VIEW_ENTRY, new_view->name);
			if (alter.GetQualifiedName().Name() != new_view->name && existing_view) {
				throw CatalogException(
				    "Could not rename view \"%s\" to \"%s\": another entry with this name already exists!",
				    alter.GetQualifiedName().Name().GetIdentifierName(), new_view->name.GetIdentifierName());
			}
		}
		transaction.AlterEntry(view, std::move(new_view));
		break;
	}
	case AlterType::SET_COMMENT: {
		auto &alter = info.Cast<SetCommentInfo>();
		switch (alter.entry_catalog_type) {
		case CatalogType::TABLE_ENTRY: {
			auto table_entry = GetEntry(catalog_transaction, CatalogType::TABLE_ENTRY, alter.GetQualifiedName().Name());
			if (table_entry->type != CatalogType::TABLE_ENTRY) {
				throw BinderException("Cannot use ALTER TABLE on entry %s - it is not a table",
				                      alter.GetQualifiedName().Name());
			}
			auto &table = table_entry->Cast<DuckLakeTableEntry>();
			auto new_table = table.Alter(transaction, alter);
			transaction.AlterEntry(table, std::move(new_table));
			break;
		}
		case CatalogType::VIEW_ENTRY: {
			auto view_entry = GetEntry(catalog_transaction, CatalogType::VIEW_ENTRY, alter.GetQualifiedName().Name());
			if (view_entry->type != CatalogType::VIEW_ENTRY) {
				throw BinderException("Cannot use ALTER VIEW on entry %s - it is not a view",
				                      alter.GetQualifiedName().Name());
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
		if (alter.catalog_entry_type == CatalogType::VIEW_ENTRY) {
			ApplySetColumnCommentToView(transaction, catalog_transaction, *this, alter,
			                            "Cannot comment on columns for entry %s - it is not a view");
		} else if (alter.catalog_entry_type == CatalogType::TABLE_ENTRY) {
			ApplySetColumnCommentToTable(transaction, catalog_transaction, *this, alter,
			                             "Cannot comment on columns for entry %s - it is not a table");
		} else if (!TryApplySetColumnCommentToTable(transaction, catalog_transaction, *this, alter)) {
			ApplySetColumnCommentToView(transaction, catalog_transaction, *this, alter,
			                            "Cannot comment on columns for entry %s - could not find table or view");
		}
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
	auto &duck_transaction = DuckLakeTransaction::Get(context, ParentCatalog());
	Scan(duck_transaction, type, callback);
}

void DuckLakeSchemaEntry::Scan(DuckLakeTransaction &duck_transaction, CatalogType type,
                               const std::function<void(CatalogEntry &)> &callback) {
	if (type == CatalogType::SCHEMA_ENTRY) {
		for (auto &child : duck_transaction.GetTransactionLocalChildSchemas(*this)) {
			callback(child.get());
		}
		for (auto &entry : child_schemas.GetEntries()) {
			if (duck_transaction.IsDeleted(*entry.second)) {
				continue;
			}
			callback(*entry.second);
		}
		return;
	}
	// scan transaction-local entries
	auto local_set = duck_transaction.GetTransactionLocalEntries(type, schema_id);
	if (local_set) {
		for (auto &entry : local_set->GetEntries()) {
			callback(*entry.second);
		}
	}
	// scan committed entries
	auto &catalog_set = GetCatalogSet(type);
	for (auto &entry : catalog_set.GetEntries()) {
		if (duck_transaction.IsDeleted(*entry.second) || duck_transaction.IsRenamed(*entry.second)) {
			continue;
		}
		if (local_set && local_set->GetEntry(entry.second->name.GetIdentifierName())) {
			// this entry exists in both the local and global set - emit only the transaction-local entry
			continue;
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

void DuckLakeSchemaEntry::Scan(CatalogType type, const std::function<void(const CatalogEntry &)> &callback) const {
	auto &catalog_set = GetCatalogSet(type);
	for (auto &entry : catalog_set.GetEntries()) {
		callback(*entry.second);
	}
}

void DuckLakeSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.cascade) {
		throw NotImplementedException("Cascade Drop not supported in DuckLake");
	}
	auto catalog_entry = GetEntry(GetCatalogTransaction(context), info.type, info.GetQualifiedName().Name());
	if (!catalog_entry) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw InternalException("Failed to drop entry \"%s\" - could not find entry",
		                        info.GetQualifiedName().Name().GetIdentifierName());
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
	auto at_clause = lookup_info.GetAtClause();
	if (!at_clause) {
		auto transaction_entry = catalog_type == CatalogType::SCHEMA_ENTRY
		                             ? duck_transaction.GetTransactionLocalSchema(*this, entry_name)
		                             : duck_transaction.GetTransactionLocalEntry(catalog_type, schema_id, entry_name);
		if (transaction_entry) {
			return transaction_entry;
		}
	}
	auto &catalog_set = GetCatalogSet(catalog_type);
	auto entry = catalog_set.GetEntry(entry_name);
	if (!entry) {
		return nullptr;
	}
	if (!at_clause && (duck_transaction.IsDeleted(*entry) || duck_transaction.IsRenamed(*entry))) {
		return nullptr;
	}
	return *entry;
}

SimilarCatalogEntry DuckLakeSchemaEntry::GetSimilarEntry(CatalogTransaction transaction,
                                                         const EntryLookupInfo &lookup_info) {
	SimilarCatalogEntry result;
	auto catalog_type = lookup_info.GetCatalogType();
	auto &entry_name = lookup_info.GetEntryName();
	if (!CatalogTypeIsSupported(catalog_type)) {
		return result;
	}
	auto &duck_transaction = transaction.transaction->Cast<DuckLakeTransaction>();
	// check transaction local first
	auto local_set = duck_transaction.GetTransactionLocalEntries(catalog_type, schema_id);
	if (local_set) {
		for (auto &entry : local_set->GetEntries()) {
			auto entry_score = StringUtil::SimilarityRating(entry.second->name, Identifier(entry_name));
			if (entry_score > result.score) {
				result.score = entry_score;
				result.name = Identifier(entry.second->name.GetIdentifierName());
				result.schema = this;
			}
		}
	}
	// check commited entries, without binding views
	auto &catalog_set = GetCatalogSet(catalog_type);
	for (auto &entry : catalog_set.GetEntries()) {
		if (duck_transaction.IsDeleted(*entry.second) || duck_transaction.IsRenamed(*entry.second)) {
			// this changed
			continue;
		}
		auto entry_score = StringUtil::SimilarityRating(entry.second->name, Identifier(entry_name));
		if (entry_score > result.score) {
			result.score = entry_score;
			result.name = Identifier(entry.second->name.GetIdentifierName());
			result.schema = this;
		}
	}
	return result;
}

void DuckLakeSchemaEntry::AddEntry(CatalogType type, unique_ptr<CatalogEntry> entry) {
	auto &catalog_set = GetCatalogSet(type);
	catalog_set.CreateEntry(std::move(entry));
}

vector<reference<DuckLakeSchemaEntry>> DuckLakeSchemaEntry::GetChildSchemas(DuckLakeTransaction &transaction) {
	vector<reference<DuckLakeSchemaEntry>> result;
	Scan(transaction, CatalogType::SCHEMA_ENTRY,
	     [&](CatalogEntry &entry) { result.emplace_back(entry.Cast<DuckLakeSchemaEntry>()); });
	return result;
}

void DuckLakeSchemaEntry::TryDropSchema(DuckLakeTransaction &transaction, bool cascade) {
	if (!cascade) {
		vector<reference<CatalogEntry>> dependents;
		for (auto type : {CatalogType::SCHEMA_ENTRY, CatalogType::TABLE_ENTRY, CatalogType::MACRO_ENTRY,
		                  CatalogType::TABLE_MACRO_ENTRY}) {
			Scan(transaction, type, [&](CatalogEntry &entry) { dependents.emplace_back(entry); });
		}
		if (dependents.empty()) {
			return;
		}
		string error_string =
		    StringUtil::Format("Cannot drop entry %s because there are entries that depend on it.\n", name);
		for (auto &dependent : dependents) {
			auto &dep = dependent.get();
			error_string += StringUtil::Format(
			    "%s \"%s\" depends on %s \"%s\".\n", StringUtil::Lower(CatalogTypeToString(dep.type)),
			    dep.name.GetIdentifierName(), StringUtil::Lower(CatalogTypeToString(type)), name.GetIdentifierName());
		}
		error_string += "Use DROP...CASCADE to drop all dependents.";
		throw CatalogException(error_string);
	}
	DropSchemaDependents(transaction);
}

void DuckLakeSchemaEntry::DropSchemaDependents(DuckLakeTransaction &transaction) {
	vector<reference<DuckLakeSchemaEntry>> subtree;
	vector<reference<DuckLakeSchemaEntry>> pending;
	pending.emplace_back(*this);
	while (!pending.empty()) {
		auto &current = pending.back().get();
		pending.pop_back();
		subtree.emplace_back(current);
		for (auto &child : current.GetChildSchemas(transaction)) {
			pending.emplace_back(child);
		}
	}
	for (idx_t schema_idx = subtree.size(); schema_idx > 0; schema_idx--) {
		auto &current = subtree[schema_idx - 1].get();
		current.DropSchemaContents(transaction);
		if (&current == this) {
			continue;
		}
		transaction.DropEntry(current);
	}
}

void DuckLakeSchemaEntry::DropSchemaContents(DuckLakeTransaction &transaction) {
	vector<reference<CatalogEntry>> entries;
	for (auto type : {CatalogType::TABLE_ENTRY, CatalogType::MACRO_ENTRY, CatalogType::TABLE_MACRO_ENTRY}) {
		Scan(transaction, type, [&](CatalogEntry &entry) { entries.emplace_back(entry); });
	}
	for (auto &entry : entries) {
		transaction.DropEntry(entry.get());
	}
}

DuckLakeCatalogSet &DuckLakeSchemaEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::SCHEMA_ENTRY:
		return child_schemas;
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return tables;
	case CatalogType::MACRO_ENTRY:
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		return scalar_macros;
	case CatalogType::TABLE_FUNCTION_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
		return table_macros;
	default:
		throw NotImplementedException("Unsupported catalog type %s for DuckLake", CatalogTypeToString(type));
	}
}

const DuckLakeCatalogSet &DuckLakeSchemaEntry::GetCatalogSet(CatalogType type) const {
	switch (type) {
	case CatalogType::SCHEMA_ENTRY:
		return child_schemas;
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return tables;
	case CatalogType::MACRO_ENTRY:
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		return scalar_macros;
	case CatalogType::TABLE_FUNCTION_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
		return table_macros;
	default:
		throw NotImplementedException("Unsupported catalog type %s for DuckLake", CatalogTypeToString(type));
	}
}

} // namespace duckdb
