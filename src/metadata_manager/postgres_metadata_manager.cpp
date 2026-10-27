#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/main/database.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "common/ducklake_types.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"

namespace duckdb {

static bool HasFourDigitDatePrefix(const string &value) {
	return value.size() >= 10 && StringUtil::CharacterIsDigit(value[0]) && StringUtil::CharacterIsDigit(value[1]) &&
	       StringUtil::CharacterIsDigit(value[2]) && StringUtil::CharacterIsDigit(value[3]) && value[4] == '-' &&
	       StringUtil::CharacterIsDigit(value[5]) && StringUtil::CharacterIsDigit(value[6]) && value[7] == '-' &&
	       StringUtil::CharacterIsDigit(value[8]) && StringUtil::CharacterIsDigit(value[9]);
}

static bool IsInfinityLiteral(const string &value) {
	return value == "infinity" || value == "-infinity";
}

static bool IsPostgresTemporalStatsType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
		return true;
	default:
		return false;
	}
}

static bool IsNanosecondTemporalType(const LogicalType &type) {
	return type.id() == LogicalTypeId::TIMESTAMP_NS || type.id() == LogicalTypeId::TIMESTAMP_TZ_NS ||
	       type.id() == LogicalTypeId::TIME_NS;
}

static bool IsTimeOfDayType(const LogicalType &type) {
	return type.id() == LogicalTypeId::TIME || type.id() == LogicalTypeId::TIME_NS;
}

static string GetPostgresStatsType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "BOOLEAN";
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
		return "SMALLINT";
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
		return "INTEGER";
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UINTEGER:
		return "BIGINT";
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
		return "NUMERIC";
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE PRECISION";
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
		return "TIMESTAMP";
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return "TIMESTAMPTZ";
	case LogicalTypeId::DECIMAL:
		return type.ToString();
	default:
		// LCOV_EXCL_START
		return string();
		// LCOV_EXCL_STOP
	}
}

static bool CanCastPostgresStatsForValueComparison(const LogicalType &type) {
	return type.IsNumeric() || type.id() == LogicalTypeId::BOOLEAN || IsPostgresTemporalStatsType(type);
}

static bool CanCastPostgresTemporalValue(const Value &value, const LogicalType &type) {
	auto string_value = value.ToString();
	if (IsInfinityLiteral(string_value)) {
		return true;
	}
	if (!HasFourDigitDatePrefix(string_value)) {
		return false;
	}
	return type.id() != LogicalTypeId::DATE || string_value.size() == 10;
}

string PostgresMetadataManager::CastValueToTarget(const Value &value, const LogicalType &type) {
	if (value.IsNull() || value.ToString().find('\0') != string::npos || type.id() == LogicalTypeId::BLOB) {
		return string();
	}
	if (RequiresValueComparison(type) && (!CanCastPostgresStatsForValueComparison(type) || !ValueIsFinite(value))) {
		return string();
	}
	if (type.id() == LogicalTypeId::UUID) {
		// canonical text sorts like the bytes
		return DuckLakeUtil::WithBinaryCollation(SQLString::ToString(value.ToString()));
	}
	if (!RequiresValueComparison(type) && type.id() != LogicalTypeId::VARCHAR) {
		return string();
	}
	if (IsTimeOfDayType(type)) {
		if (value.type().id() != type.id()) {
			return string();
		}
		// Anchor time to the epoch date so timestamp bounds can widen without wrapping.
		auto micros = value.GetValueUnsafe<int64_t>();
		if (type.id() == LogicalTypeId::TIME_NS) {
			micros /= Interval::NANOS_PER_MICRO;
		}
		return CastValueToTarget(Value::TIMESTAMP(timestamp_t(micros)), LogicalType::TIMESTAMP);
	}
	if (IsNanosecondTemporalType(type) && value.type().id() == type.id()) {
		// postgres keeps microseconds, so floor the constant and widen the stats bounds to match
		auto nanos = value.GetValueUnsafe<int64_t>();
		if (timestamp_t(nanos).IsFinite()) {
			auto micros = nanos / Interval::NANOS_PER_MICRO - (nanos % Interval::NANOS_PER_MICRO < 0 ? 1 : 0);
			auto floored = type.id() == LogicalTypeId::TIMESTAMP_NS ? Value::TIMESTAMP(timestamp_t(micros))
			                                                        : Value::TIMESTAMPTZ(timestamp_tz_t(micros));
			return CastValueToTarget(floored, floored.type());
		}
	}
	if (IsPostgresTemporalStatsType(type) && !CanCastPostgresTemporalValue(value, type)) {
		return string();
	}
	if (type.id() == LogicalTypeId::FLOAT) {
		// a bare number is NUMERIC, which compares with REAL bounds as DOUBLE PRECISION
		return "CAST(" + value.ToString() + " AS " + GetPostgresStatsType(type) + ")";
	}
	if (type.IsNumeric()) {
		return value.ToString();
	}
	auto literal = SQLString::ToString(value.ToString());
	if (type.id() == LogicalTypeId::VARCHAR) {
		return DuckLakeUtil::WithBinaryCollation(literal);
	}
	if (IsPostgresTemporalStatsType(type)) {
		return literal + "::" + GetPostgresStatsType(type);
	}
	if (type.id() == LogicalTypeId::BOOLEAN) {
		return literal + "::BOOLEAN";
	}
	return string();
}

static string PostgresSafeTemporalStatsCast(const string &stats, const LogicalType &type) {
	string regex;
	auto fraction = IsNanosecondTemporalType(type) ? "9" : "6";
	auto value = stats;
	if (IsNanosecondTemporalType(type)) {
		value = StringUtil::Format("regexp_replace(%s, '(\\.[0-9]{6})[0-9]+', '\\1')", stats);
	}
	if (IsTimeOfDayType(type)) {
		regex = StringUtil::Format("'^(([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\\.[0-9]{1,%s})?|24:00:00(\\.0+)?)$'",
		                           fraction);
		return StringUtil::Format("(CASE WHEN %s ~ %s THEN DATE '1970-01-01' + %s::TIME END)", stats, regex, value);
	}
	if (type.id() == LogicalTypeId::DATE) {
		regex = "'^[0-9]{4}-(0[1-9]|1[0-2])-([0][1-9]|[12][0-9]|3[01])$'";
	} else if (type.id() == LogicalTypeId::TIMESTAMP_TZ || type.id() == LogicalTypeId::TIMESTAMP_TZ_NS) {
		regex = StringUtil::Format("'^[0-9]{4}-(0[1-9]|1[0-2])-([0][1-9]|[12][0-9]|3[01]) "
		                           "([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\\.[0-9]{1,%s})?"
		                           "(Z|[+-](0[0-9]|1[0-5])(:[0-5][0-9])?)$'",
		                           fraction);
	} else {
		regex = StringUtil::Format("'^[0-9]{4}-(0[1-9]|1[0-2])-([0][1-9]|[12][0-9]|3[01])"
		                           "( ([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\\.[0-9]{1,%s})?)?$'",
		                           fraction);
	}

	auto year = StringUtil::Format("substring(%s FROM 1 FOR 4)::INTEGER", stats);
	auto month = StringUtil::Format("substring(%s FROM 6 FOR 2)::INTEGER", stats);
	auto day = StringUtil::Format("substring(%s FROM 9 FOR 2)::INTEGER", stats);
	auto max_day = StringUtil::Format(
	    "(CASE WHEN %s = 2 THEN CASE WHEN mod(%s, 4) = 0 AND (mod(%s, 100) <> 0 OR mod(%s, 400) = 0) "
	    "THEN 29 ELSE 28 END WHEN %s IN (4, 6, 9, 11) THEN 30 ELSE 31 END)",
	    month, year, year, year, month);
	auto valid_date = StringUtil::Format("%s > 0 AND %s <= %s", year, day, max_day);
	return StringUtil::Format(
	    "(CASE WHEN %s ~ %s THEN CASE WHEN %s THEN %s::%s END WHEN %s IN ('infinity', '-infinity') THEN %s::%s END)",
	    stats, regex, valid_date, value, GetPostgresStatsType(type), stats, stats, GetPostgresStatsType(type));
}

string PostgresMetadataManager::CastStatsToTarget(const string &stats, const LogicalType &type,
                                                  StatsCastType cast_type) {
	if (IsPostgresTemporalStatsType(type)) {
		auto cast = PostgresSafeTemporalStatsCast(stats, type);
		if (cast_type == StatsCastType::ORDERING) {
			return cast;
		}
		if (IsNanosecondTemporalType(type)) {
			// widen truncated bounds by a microsecond
			cast = StringUtil::Format("(%s %s INTERVAL '1 microsecond')", cast,
			                          cast_type == StatsCastType::MIN ? "-" : "+");
		}
		return BoundOrInfinity(cast, GetPostgresStatsType(type), cast_type);
	}
	if (CanCastPostgresStatsForValueComparison(type)) {
		return stats + "::" + GetPostgresStatsType(type);
	}
	if (type.id() == LogicalTypeId::VARCHAR || type.id() == LogicalTypeId::UUID) {
		return DuckLakeUtil::WithBinaryCollation(stats);
	}
	return string();
}

PostgresMetadataManager::PostgresMetadataManager(DuckLakeTransaction &transaction)
    : DuckLakeMetadataManager(transaction) {
}

bool PostgresMetadataManager::InsertsInlinedRowsAfterBatch(idx_t row_count) const {
	// the copy of a separate insert costs more round trips than parsing the VALUES text of a few rows
	static constexpr idx_t MAX_INLINED_ROWS_IN_BATCH = 100;
	return row_count > MAX_INLINED_ROWS_IN_BATCH;
}

bool PostgresMetadataManager::TypeIsNativelySupported(const LogicalType &type) {
	switch (type.id()) {
	// Unnamed composite types are not supported.
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	// Postgres timestamp/date ranges are narrower than DuckDB's, and its TIME keeps only microseconds
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	// Postgres bytea input format differs from DuckDB's blob text format
	case LogicalTypeId::BLOB:
	// Postgres cannot store null bytes in VARCHAR/TEXT columns
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::VARIANT:
	// If we knew that the Postgres installation has PostGIS installed, we could support GEOMETRY in the future.
	case LogicalTypeId::GEOMETRY:
		return false;
	default:
		return true;
	}
}

static bool ContainsNulByte(Vector &vector, idx_t offset, idx_t count) {
	vector.Flatten();
	auto &validity = FlatVector::Validity(vector);
	switch (vector.GetType().id()) {
	case LogicalTypeId::VARCHAR: {
		auto strings = FlatVector::GetData<string_t>(vector);
		for (idx_t i = offset; i < offset + count; i++) {
			if (validity.RowIsValid(i) && memchr(strings[i].GetData(), '\0', strings[i].GetSize())) {
				return true;
			}
		}
		return false;
	}
	case LogicalTypeId::STRUCT:
		for (auto &child : StructVector::GetEntries(vector)) {
			if (ContainsNulByte(child, offset, count)) {
				return true;
			}
		}
		return false;
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP: {
		auto entries = FlatVector::GetData<list_entry_t>(vector);
		auto &child = ListVector::GetChildMutable(vector);
		for (idx_t i = offset; i < offset + count; i++) {
			if (validity.RowIsValid(i) && ContainsNulByte(child, entries[i].offset, entries[i].length)) {
				return true;
			}
		}
		return false;
	}
	default:
		return false;
	}
}

bool PostgresMetadataManager::CanInlineValues(DataChunk &chunk) {
	// nested values are stored as text, which cannot hold a NUL byte
	for (auto &column : chunk.data) {
		auto &type = column.GetType();
		if (DuckLakeTypes::IsNested(type) && TypeVisitor::Contains(type, LogicalTypeId::VARCHAR) &&
		    ContainsNulByte(column, 0, chunk.size())) {
			return false;
		}
	}
	return true;
}

string PostgresMetadataManager::GetColumnTypeInternal(const LogicalType &column_type) {
	switch (column_type.id()) {
	case LogicalTypeId::DOUBLE:
		return "DOUBLE PRECISION";
	case LogicalTypeId::TINYINT:
		return "SMALLINT";
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::SQLNULL:
		return "INTEGER";
	case LogicalTypeId::UINTEGER:
		return "BIGINT";
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return "BYTEA";
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return "VARCHAR";
	default:
		return column_type.ToString();
	}
}

bool PostgresMetadataManager::IsRetryableCommitError(const string &message) const {
	// the server error follows the failed query, which can contain any text
	auto server_error_start = message.rfind("\": ");
	auto server_error = server_error_start == string::npos ? message : message.substr(server_error_start);
	// constraint names are not translated, so a conflict on a metadata primary key is found in any language
	for (auto end = server_error.find("_pkey"); end != string::npos; end = server_error.find("_pkey", end + 1)) {
		auto start = end;
		while (start > 0 &&
		       (StringUtil::CharacterIsAlphaNumeric(server_error[start - 1]) || server_error[start - 1] == '_')) {
			start--;
		}
		if (StringUtil::StartsWith(server_error.substr(start, end - start), "ducklake_")) {
			return true;
		}
	}
	return false;
}

bool PostgresMetadataManager::InlinedDeletionTableExists(const string &table_name) {
	auto &catalog = transaction.GetCatalog();
	auto remote_query = StringUtil::Format(
	    "SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = %s AND tablename = %s LIMIT 1",
	    SQLString::ToString(catalog.MetadataSchemaName().GetIdentifierName()), SQLString::ToString(table_name));
	auto query =
	    StringUtil::Format("SELECT 1 FROM postgres_query({METADATA_CATALOG_NAME_LITERAL}, %s, use_transaction = true)",
	                       SQLString::ToString(remote_query));
	auto result = DuckLakeMetadataManager::Query(query);
	result->ThrowIfError("Failed to probe for DuckLake inlined-deletion table: ");
	return result->Fetch() != nullptr;
}

void PostgresMetadataManager::MigrateInlinedDataTypes() {
	auto columns = DuckLakeMetadataManager::Query(GetInlinedTableColumnsSql());
	columns->ThrowIfError("Failed to read the columns of inlined-data tables while migrating: ");
	map<string, case_insensitive_map_t<string>> inlined_tables;
	for (auto &row : *columns) {
		inlined_tables[row.GetValue<string>(0)][row.GetValue<string>(1)] = row.GetValue<string>(2);
	}
	for (auto &inlined_table : inlined_tables) {
		auto &table_name = inlined_table.first;
		auto probe = DuckLakeMetadataManager::Query(
		    StringUtil::Format("SELECT * FROM {METADATA_CATALOG}.%s LIMIT 0", SQLIdentifier(table_name)));
		if (probe->HasError() || probe->GetNames().size() < 3) {
			continue;
		}
		// the metadata columns are always the first three columns, user columns follow
		auto &names = probe->GetNames();
		DuckLakeInlinedColNames col_names(false);
		col_names.row_id = names[0].GetIdentifierName();
		col_names.begin_snapshot = names[1].GetIdentifierName();
		col_names.end_snapshot = names[2].GetIdentifierName();
		vector<string> select_list;
		for (idx_t i = 0; i < 3; i++) {
			select_list.push_back(SQLQuotedIdentifier::ToString(names[i].GetIdentifierName()));
		}
		string column_defs;
		bool rewrite = false;
		for (idx_t i = 3; i < names.size(); i++) {
			auto name = names[i].GetIdentifierName();
			auto column_type = inlined_table.second.find(name);
			if (column_type == inlined_table.second.end()) {
				rewrite = false;
				break;
			}
			auto type = DuckLakeTypes::FromString(column_type->second);
			auto native_type = type.HasAlias() ? LogicalType(type.id()) : type;
			// DuckLake 0.3 stored values with the native type of their DuckLake type, other columns are kept
			auto &stored_type = probe->GetTypes()[i];
			string storage_type_name;
			bool convert = false;
			// nested columns are stored as VARCHAR in every version
			if (!DuckLakeTypes::IsNested(type)) {
				DuckLakeColumnInfo column;
				column.type = column_type->second;
				storage_type_name = GetColumnType(column);
				auto storage_type = UnboundType::TryParseAndDefaultBind(storage_type_name);
				convert = stored_type != storage_type && stored_type == native_type;
			}
			auto column_name = SQLQuotedIdentifier::ToString(name);
			column_defs += StringUtil::Format("%s%s %s", column_defs.empty() ? "" : ", ", column_name,
			                                  convert ? storage_type_name : stored_type.ToString());
			select_list.push_back(convert ? DuckLakeUtil::InlinedStorageExpression(*this, column_name, type)
			                              : column_name);
			rewrite = rewrite || convert;
		}
		if (!rewrite) {
			continue;
		}
		auto migrated_name = table_name + "_migrated";
		auto migrate_query = InlinedTableDdlSql(migrated_name, column_defs, col_names);
		migrate_query += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.%s SELECT %s FROM {METADATA_CATALOG}.%s;",
		                                    SQLIdentifier(migrated_name), StringUtil::Join(select_list, ", "),
		                                    SQLIdentifier(table_name));
		migrate_query += StringUtil::Format("DROP TABLE {METADATA_CATALOG}.%s;", SQLIdentifier(table_name));
		migrate_query += StringUtil::Format("ALTER TABLE {METADATA_CATALOG}.%s RENAME TO %s;",
		                                    SQLIdentifier(migrated_name), SQLIdentifier(table_name));
		auto result = DuckLakeMetadataManager::Execute(migrate_query);
		result->ThrowIfError(
		    StringUtil::Format("Failed to migrate the column types of inlined-data table \"%s\": ", table_name));
	}
}

unique_ptr<QueryResult> PostgresMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	SubstituteTransactionPlaceholders(snapshot, query);
	auto &ducklake_catalog = transaction.GetCatalog();
	SubstituteCatalogPlaceholders(query, SQLQuotedIdentifier::ToString(ducklake_catalog.MetadataSchemaName()));
	if (query.find('\0') != string::npos) {
		throw InvalidInputException("A DuckLake catalog on PostgreSQL cannot store text that contains a NUL byte, "
		                            "such as a partition value, a name or a commit message");
	}
	auto catalog_literal = SQLString::ToString(ducklake_catalog.MetadataDatabaseName());
	// casts of float bounds to text keep their exact value
	query = "SET LOCAL extra_float_digits = 3;" + query;
	auto result = transaction.GetConnection().Query(
	    StringUtil::Format("CALL postgres_execute(%s, %s, prepare=FALSE)", catalog_literal, SQLString(query)));
	return std::move(result);
}

unique_ptr<QueryResult> PostgresMetadataManager::ExecuteDropInTransaction(string &query) {
	// DROP TABLE waits for every open transaction that read the table and the readers queued behind it fail, so it
	// gives up instead and leaves the table to a later cleanup
	auto batch = "CALL postgres_execute({METADATA_CATALOG_NAME_LITERAL}, 'SET LOCAL lock_timeout = 1');\n" + query;
	return ExecuteInTransaction(batch);
}

void PostgresMetadataManager::ClearCache() {
	auto result = transaction.ExecuteRaw("CALL postgres_clear_cache();");
	result->ThrowIfError("Failed to clear the PostgreSQL metadata cache: ");
}

static string PostgresQuery(const string &remote_query) {
	return StringUtil::Format("SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL}, %s)",
	                          SQLString(remote_query));
}

string PostgresMetadataManager::GetLatestSnapshotQuery() const {
	auto remote_query = LatestSnapshotQuery();
	SubstituteCatalogPlaceholders(remote_query,
	                              SQLQuotedIdentifier::ToString(transaction.GetCatalog().MetadataSchemaName()));
	return PostgresQuery(remote_query);
}

string PostgresMetadataManager::GetSnapshotAndStatsAndChangesQuery(DuckLakeSnapshot snapshot,
                                                                   bool include_exactness) const {
	// one statement reads the latest snapshot and the table stats from the same state at any isolation level
	auto remote_query = SnapshotAndStatsAndChangesQuery(include_exactness);
	SubstituteCatalogPlaceholders(remote_query,
	                              SQLQuotedIdentifier::ToString(transaction.GetCatalog().MetadataSchemaName()));
	SubstituteSnapshotPlaceholders(snapshot, remote_query);
	return PostgresQuery(remote_query);
}

string PostgresMetadataManager::GenerateFileListQuery(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
                                                      const FilterPushdownInfo *filter_info,
                                                      const vector<DuckLakeFileListDynamicFilter> &dynamic_filters,
                                                      const vector<idx_t> &runtime_filter_stats_columns,
                                                      FileListType file_list_type, const string &) {
	auto schema = SQLQuotedIdentifier::ToString(transaction.GetCatalog().MetadataSchemaName());
	auto remote_query = DuckLakeMetadataManager::GenerateFileListQuery(
	    table, snapshot, filter_info, dynamic_filters, runtime_filter_stats_columns, file_list_type, schema);
	// placeholders inside the literal are not replaced later
	SubstituteSnapshotPlaceholders(snapshot, remote_query);
	return PostgresQuery(remote_query);
}

} // namespace duckdb
