#include "common/ducklake_util.hpp"
#include "common/ducklake_types.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "storage/ducklake_transaction.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/encryption_state.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/file_system.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "storage/ducklake_catalog.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/unordered_map.hpp"

#include <cmath>

namespace duckdb {

string DuckLakeUtil::ParseQuotedValue(const string &input, idx_t &pos) {
	if (pos >= input.size() || input[pos] != '"') {
		throw InvalidInputException("Failed to parse quoted value - expected a quote");
	}
	string result;
	if (!StringUtil::TryParseQuotedString(input, pos, result)) {
		throw InvalidInputException("Failed to parse quoted value - unterminated quote");
	}
	return result;
}

string DuckLakeUtil::ToQuotedList(const vector<string> &input, char list_separator) {
	return StringUtil::Join(input, input.size(), string(1, list_separator),
	                        [](const string &value) { return SQLQuotedIdentifier::ToString(value); });
}

vector<string> DuckLakeUtil::ParseQuotedList(const string &input, char list_separator) {
	vector<string> result;
	if (input.empty()) {
		return result;
	}
	idx_t pos = 0;
	while (true) {
		result.push_back(ParseQuotedValue(input, pos));
		if (pos >= input.size()) {
			break;
		}
		if (input[pos] != list_separator) {
			throw InvalidInputException("Failed to parse list - expected a %s", string(1, list_separator));
		}
		pos++;
	}
	return result;
}

string ParsedCatalogEntry::SchemaKey() const {
	return DuckLakeUtil::ToQuotedList(schema_path, '.');
}

ParsedCatalogEntry DuckLakeUtil::ParseCatalogEntry(const string &input) {
	auto parts = ParseQuotedList(input, '.');
	if (parts.size() < 2) {
		throw InvalidInputException("Failed to parse catalog entry - expected a schema and a name");
	}
	ParsedCatalogEntry result_data;
	result_data.name = std::move(parts.back());
	parts.pop_back();
	result_data.schema_path = std::move(parts);
	return result_data;
}

string DuckLakeUtil::StatsToString(const string &text) {
	if (text.find('\0') != string::npos) {
		return "NULL";
	}
	return SQLString::ToString(text);
}

static string EscapeVarcharForSQL(const string &str_val) {
	string ret;
	bool concat = false;
	for (auto c : str_val) {
		switch (c) {
		case '\0':
			concat = true;
			ret += "', chr(0), '";
			break;
		case '\'':
			ret += "''";
			break;
		default:
			ret += c;
			break;
		}
	}
	if (concat) {
		return "CONCAT('" + ret + "')";
	}
	return "'" + ret + "'";
}

string ToSQLString(DuckLakeMetadataManager &metadata_manager, const Value &value) {
	if (value.IsNull()) {
		return value.ToString();
	}
	bool use_native_type = metadata_manager.TypeIsNativelySupported(value.type());
	string value_type = use_native_type ? metadata_manager.GetColumnTypeInternal(value.type()) : "VARCHAR";
	switch (value.type().id()) {
	case LogicalTypeId::UUID:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::GEOMETRY:
		// ANSI CAST(value AS type) instead of the PostgreSQL-flavored
		// `'value'::type` operator: SQLite's parser rejects `::` outright,
		// which breaks SQLite-backed metadata backends that ship these
		// inlined-INSERT batches directly to SQLite.
		return StringUtil::Format("CAST('%s' AS %s)", value.ToString(), value_type);
	case LogicalTypeId::INTERVAL: {
		auto interval = IntervalValue::Get(value);
		return StringUtil::Format("CAST('%d months %d days %lld microseconds' AS %s)", interval.months, interval.days,
		                          interval.micros, value_type);
	}
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::ENUM:
		return EscapeVarcharForSQL(value.ToString());
	case LogicalTypeId::VARIANT: {
		if (!use_native_type) {
			throw InternalException("VARIANT values must be encoded as Parquet Variant blobs before being "
			                        "inlined in this catalog type");
		}
		return value.ToSQLString();
	}
	case LogicalTypeId::FLOAT: {
		float fval = FloatValue::Get(value);
		if (!Value::FloatIsFinite(fval) || (fval == 0.0f && std::signbit(fval))) {
			return StringUtil::Format("CAST('%s' AS %s)", value.ToString(), value_type);
		}
		return value.ToString();
	}
	case LogicalTypeId::DOUBLE: {
		double val = DoubleValue::Get(value);
		if (!Value::DoubleIsFinite(val) || (val == 0.0 && std::signbit(val))) {
			return StringUtil::Format("CAST('%s' AS %s)", value.ToString(), value_type);
		}
		return value.ToString();
	}
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY:
	case LogicalTypeId::MAP: {
		if (!use_native_type) {
			return value.ToString();
		}
		return Value::NestedToSQLString(value,
		                                [&](const Value &child) { return ToSQLString(metadata_manager, child); });
	}
	default:
		return value.ToString();
	}
}

string ToByteaHexLiteral(const string &raw_bytes) {
	string hex(raw_bytes.size() * 2, '\0');
	CryptoHash::ToHex(const_data_ptr_cast(raw_bytes.data()), raw_bytes.size(), &hex[0]);
	return "'\\x" + hex + "'";
}

string DuckLakeUtil::ValueToSQL(DuckLakeMetadataManager &metadata_manager, ClientContext &context, const Value &val) {
	// FIXME: this should be upstreamed
	if (val.IsNull()) {
		return val.ToString();
	}
	if (val.type().HasAlias()) {
		// extension type: cast to string
		auto str_val = val.CastAs(context, LogicalType::VARCHAR);
		return ValueToSQL(metadata_manager, context, str_val);
	}
	string result;
	switch (val.type().id()) {
	case LogicalTypeId::VARCHAR: {
		auto &str_val = StringValue::Get(val);
		if (!metadata_manager.TypeIsNativelySupported(LogicalType::VARCHAR)) {
			return ToByteaHexLiteral(str_val);
		}
		return EscapeVarcharForSQL(str_val);
	}
	case LogicalTypeId::BLOB: {
		if (!metadata_manager.TypeIsNativelySupported(LogicalType::BLOB)) {
			return ToByteaHexLiteral(StringValue::Get(val));
		}
		result = ToSQLString(metadata_manager, val);
		break;
	}
	default:
		result = ToSQLString(metadata_manager, val);
	}
	if (metadata_manager.TypeIsNativelySupported(val.type()) || !val.type().IsNested()) {
		return result;
	}
	return SQLString::ToString(result);
}

void DuckLakeUtil::EnsureDirectoryExists(FileSystem &fs, const string &data_path) {
	if (!fs.IsRemoteFile(data_path)) {
		try {
			fs.CreateDirectoriesRecursive(data_path);
		} catch (...) {
		}
	}
}

string DuckLakeUtil::JoinPath(FileSystem &fs, const string &a, const string &b) {
	auto sep = fs.PathSeparator(a);
	if (StringUtil::EndsWith(a, sep)) {
		return a + b;
	} else {
		return a + sep + b;
	}
}

unique_ptr<Expression> DuckLakeUtil::MergeFilterExpressions(unique_ptr<Expression> left, unique_ptr<Expression> right) {
	vector<unique_ptr<Expression>> conjuncts;
	conjuncts.push_back(std::move(left));
	conjuncts.push_back(std::move(right));
	LogicalFilter::SplitPredicates(conjuncts);

	vector<unique_ptr<Expression>> merged;
	for (auto &conjunct : conjuncts) {
		bool is_duplicate = false;
		for (auto &existing : merged) {
			if (existing->Equals(*conjunct)) {
				is_duplicate = true;
				break;
			}
		}
		if (!is_duplicate) {
			merged.push_back(std::move(conjunct));
		}
	}
	return BoundConjunctionExpression::Create(ExpressionType::CONJUNCTION_AND, std::move(merged));
}

bool DuckLakeUtil::IsStructExtract(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &func = expr.Cast<BoundFunctionExpression>();
	if (func.GetChildren().empty()) {
		return false;
	}
	// stats are stored against a named field, so an unnamed struct (TUPLE) has nothing to resolve against
	auto &input_type = func.GetChildren()[0]->GetReturnType();
	if (input_type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	idx_t position;
	return TryGetStructExtractChildIndex(func, position) && position < StructType::GetChildCount(input_type);
}

//! Walk to the sub-expressions a filter reads a column through, without descending into them
static void FindFilterSubject(const Expression &expr, optional_ptr<const Expression> &subject, bool &conflict) {
	if (conflict) {
		return;
	}
	if (ExpressionFilter::IsSimpleFilterColumnRef(expr) || DuckLakeUtil::IsStructExtract(expr)) {
		if (subject && !subject->Equals(expr)) {
			conflict = true;
		} else {
			subject = expr;
		}
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { FindFilterSubject(child, subject, conflict); });
}

optional_ptr<const Expression> DuckLakeUtil::GetFilterSubject(const Expression &expr) {
	optional_ptr<const Expression> subject;
	bool conflict = false;
	FindFilterSubject(expr, subject, conflict);
	return conflict ? nullptr : subject;
}

bool DuckLakeUtil::IsInlinedSystemColumn(const string &name, bool prefixed_inlined_columns) {
	if (prefixed_inlined_columns) {
		return StringUtil::CIStartsWith(name, DuckLakeInlinedColNames::PREFIX);
	}
	return DuckLakeInlinedColNames(false).ConflictsWith(name);
}

static void ThrowReservedInlinedColumn(const string &name, bool prefixed_inlined_columns) {
	if (prefixed_inlined_columns) {
		throw BinderException("Column name \"%s\" is reserved by DuckLake for internal use: column names starting "
		                      "with \"%s\" are not allowed.",
		                      name, DuckLakeInlinedColNames::PREFIX);
	}
	throw BinderException(
	    "Column name \"%s\" is reserved by DuckLake for internal use when data inlining is enabled. If "
	    "you must use this column name, disable inlining by calling "
	    "ducklake_set_option('data_inlining_row_limit', 0).",
	    name);
}

void DuckLakeUtil::ValidateInlinedSystemColumn(DuckLakeCatalog &catalog, ClientContext &context, SchemaIndex schema_id,
                                               TableIndex table_id, const string &name,
                                               optional_ptr<const map<string, string>> table_options) {
	bool prefixed_inlined_columns = catalog.SupportsV1_1Metadata();
	if (!prefixed_inlined_columns && catalog.DataInliningRowLimit(context, schema_id, table_id, table_options) == 0) {
		return;
	}
	if (IsInlinedSystemColumn(name, prefixed_inlined_columns)) {
		ThrowReservedInlinedColumn(name, prefixed_inlined_columns);
	}
}

void DuckLakeUtil::ValidateNoInlinedSystemColumns(DuckLakeCatalog &catalog, ClientContext &context,
                                                  SchemaIndex schema_id, const ColumnList &columns,
                                                  optional_ptr<const map<string, string>> table_options) {
	bool prefixed_inlined_columns = catalog.SupportsV1_1Metadata();
	if (!prefixed_inlined_columns &&
	    catalog.DataInliningRowLimit(context, schema_id, TableIndex(), table_options) == 0) {
		return;
	}
	for (auto &col : columns.Logical()) {
		if (IsInlinedSystemColumn(col.Name().GetIdentifierName(), prefixed_inlined_columns)) {
			ThrowReservedInlinedColumn(col.Name().GetIdentifierName(), prefixed_inlined_columns);
		}
	}
}

void DuckLakeUtil::ValidateCanEnableInlining(const ColumnList &columns, bool prefixed_inlined_columns,
                                             const string &table_name) {
	DuckLakeInlinedColNames col_names(prefixed_inlined_columns);
	for (auto &col : columns.Logical()) {
		if (col_names.ConflictsWith(col.Name().GetIdentifierName())) {
			throw BinderException(
			    "Cannot enable data inlining for table \"%s\". Column \"%s\" conflicts with a reserved DuckLake "
			    "internal column name used for inlining. To enable inlining for this table, rename or drop column "
			    "\"%s\".",
			    table_name, col.Name().GetIdentifierName(), col.Name().GetIdentifierName());
		}
	}
}

string DuckLakeUtil::ReplaceSkippingQuotes(const string &sql, const string &from, const string &to) {
	if (from.empty()) {
		return sql;
	}

	auto tokens = Parser::Tokenize(sql);

	// Collect quoted ranges (string constants and double-quoted identifiers) where replacement doesn't happen
	vector<pair<idx_t, idx_t>> no_replace_ranges;
	for (idx_t i = 0; i < tokens.size(); i++) {
		bool is_quoted = tokens[i].type == SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT;
		if (!is_quoted && tokens[i].type == SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER &&
		    tokens[i].start < sql.size() && sql[tokens[i].start] == '"') {
			is_quoted = true;
		}
		if (is_quoted) {
			const idx_t start = tokens[i].start;
			const idx_t end = (i + 1 < tokens.size()) ? tokens[i + 1].start : sql.size();
			no_replace_ranges.push_back({start, end});
		}
	}

	string result;
	result.reserve(sql.size());
	idx_t pos = 0;
	idx_t range_idx = 0;

	while (pos < sql.size()) {
		while (range_idx < no_replace_ranges.size() && pos >= no_replace_ranges[range_idx].second) {
			range_idx++;
		}

		// If inside a quoted range, copy verbatim to its end
		if (range_idx < no_replace_ranges.size() && pos >= no_replace_ranges[range_idx].first) {
			idx_t end = no_replace_ranges[range_idx].second;
			result += sql.substr(pos, end - pos);
			pos = end;
			range_idx++;
			continue;
		}

		// If not inside a quoted range, check for a match of `from`
		if (sql.compare(pos, from.size(), from) == 0) {
			result += to;
			pos += from.size();
			continue;
		}

		// Otherwise, just copy the character at the current position
		result += sql[pos];
		pos++;
	}

	return result;
}

string DuckLakeUtil::OptionalIdxOrNull(const optional_idx &v) {
	return v.IsValid() ? std::to_string(v.GetIndex()) : "NULL";
}

string DuckLakeUtil::MappingIdOrNull(const MappingIndex &m) {
	return m.IsValid() ? std::to_string(m.index) : "NULL";
}

string DuckLakeUtil::EncryptionKeyLiteral(const string &key) {
	if (key.empty()) {
		return "NULL";
	}
	return "'" + Blob::ToBase64(string_t(key)) + "'";
}

const char *DuckLakeUtil::BoolLiteral(bool v) {
	return v ? "true" : "false";
}

string DuckLakeUtil::PartitionValueLiteral(const Value &v) {
	return v.IsNull() ? string("NULL") : SQLString::ToString(v.ToString());
}

static string ChunkRowToSQL(DuckLakeMetadataManager &metadata_manager, ClientContext &context, DataChunk &chunk,
                            idx_t row) {
	string result;
	for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
		if (c > 0) {
			result += ", ";
		}
		result += DuckLakeUtil::ValueToSQL(metadata_manager, context, chunk.GetValue(c, row));
	}
	return result;
}

LogicalType DuckLakeUtil::GetInlinedStorageType(DuckLakeMetadataManager &metadata_manager, const LogicalType &type) {
	if (metadata_manager.TypeIsNativelySupported(LogicalType::VARIANT())) {
		return type;
	}
	return TypeVisitor::VisitReplace(type, [](const LogicalType &child) {
		return child.id() == LogicalTypeId::VARIANT ? LogicalType::BLOB : child;
	});
}

string DuckLakeUtil::InlinedVariantExpression(const string &expression, const LogicalType &type, bool encode,
                                              idx_t depth) {
	if (!TypeVisitor::Contains(type, LogicalTypeId::VARIANT)) {
		return expression;
	}
	switch (type.id()) {
	case LogicalTypeId::VARIANT: {
		if (!encode) {
			return "variant_bytes_to_variant(" + expression + ")";
		}
		auto parquet = "(CAST(variant_to_parquet_variant(" + expression + ") AS STRUCT(metadata BLOB, value BLOB)))";
		return "CASE WHEN " + expression + " IS NULL THEN NULL ELSE " + parquet + ".metadata || " + parquet +
		       ".value END";
	}
	case LogicalTypeId::LIST: {
		auto element = "__ducklake_element_" + to_string(depth);
		return "list_transform(" + expression + ", lambda " + element + ": " +
		       InlinedVariantExpression(element, ListType::GetChildType(type), encode, depth + 1) + ")";
	}
	case LogicalTypeId::STRUCT: {
		vector<string> fields;
		auto &children = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < children.size(); i++) {
			auto &child = children[i];
			auto name =
			    StructType::IsUnnamed(type) ? to_string(i + 1) : SQLString::ToString(child.first.GetIdentifierName());
			auto field = InlinedVariantExpression("struct_extract(" + expression + ", " + name + ")", child.second,
			                                      encode, depth);
			fields.push_back(StructType::IsUnnamed(type) ? field : name + ": " + field);
		}
		auto contents = StringUtil::Join(fields, ", ");
		auto result = StructType::IsUnnamed(type) ? "row(" + contents + ")" : "{" + contents + "}";
		return "CASE WHEN " + expression + " IS NULL THEN NULL ELSE " + result + " END";
	}
	case LogicalTypeId::MAP: {
		auto keys = InlinedVariantExpression("map_keys(" + expression + ")", LogicalType::LIST(MapType::KeyType(type)),
		                                     encode, depth);
		auto values = InlinedVariantExpression("map_values(" + expression + ")",
		                                       LogicalType::LIST(MapType::ValueType(type)), encode, depth);
		return "CASE WHEN " + expression + " IS NULL THEN NULL ELSE map(" + keys + ", " + values + ") END";
	}
	default:
		throw NotImplementedException("Cannot inline VARIANT inside %s", type);
	}
}

string DuckLakeUtil::InlinedStorageExpression(DuckLakeMetadataManager &metadata_manager, string expression,
                                              const LogicalType &type) {
	if (GetInlinedStorageType(metadata_manager, type) != type) {
		expression = InlinedVariantExpression(expression, type, true);
	}
	if (!metadata_manager.TypeIsNativelySupported(type)) {
		if (type.IsNested()) {
			expression = "CAST(" + expression + " AS VARCHAR)";
		} else if (type.id() == LogicalTypeId::VARCHAR) {
			// PostgreSQL stores strings as BYTEA to preserve embedded NUL bytes.
			expression = "encode(" + expression + ")";
		}
	}
	return expression;
}

vector<string> DuckLakeUtil::InlinedDataToSQL(DuckLakeTransaction &transaction, ColumnDataCollection &data) {
	auto &metadata_manager = transaction.GetMetadataManager();
	auto context = transaction.context.lock();
	vector<string> rows;
	rows.reserve(data.Count());
	for (auto &chunk : data.Chunks()) {
		for (idx_t r = 0; r < chunk.size(); r++) {
			rows.push_back(ChunkRowToSQL(metadata_manager, *context, chunk, r));
		}
	}
	return rows;
}

void DuckLakeUtil::CopyExtensionSettings(ClientContext &from, ClientContext &to) {
	auto &db_config = DBConfig::GetConfig(from);
	for (auto &entry : db_config.GetExtensionSettings()) {
		auto &option = entry.second;
		if (!option.setting_index.IsValid()) {
			continue;
		}
		auto setting_index = option.setting_index.GetIndex();
		if (!from.config.user_settings.IsSet(setting_index)) {
			continue;
		}
		Value value;
		from.TryGetCurrentUserSetting(setting_index, value);
		to.config.user_settings.SetUserSetting(setting_index, std::move(value));
	}
}

using config_option_parser_t = string (*)(ClientContext &context, const string &option, const Value &val);

static string ParseParquetCompression(ClientContext &, const string &, const Value &val) {
	auto codec = val.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();
	vector<string> supported_algorithms {"uncompressed", "snappy", "gzip", "zstd", "brotli", "lz4", "lz4_raw"};
	bool found = false;
	for (auto &algorithm : supported_algorithms) {
		if (StringUtil::CIEquals(algorithm, codec)) {
			found = true;
			break;
		}
	}
	if (!found) {
		auto supported = StringUtil::Join(supported_algorithms, ", ");
		throw NotImplementedException("Unsupported codec \"%s\" for parquet, supported options are %s", codec,
		                              supported);
	}
	return StringUtil::Lower(codec);
}

static string ParseParquetVersion(ClientContext &, const string &, const Value &val) {
	auto version = val.DefaultCastAs(LogicalType::UBIGINT).GetValue<idx_t>();
	if (version != 1 && version != 2) {
		throw NotImplementedException("Only Parquet version 1 and 2 are supported");
	}
	return "V" + to_string(version);
}

static string ParseUnsignedInteger(ClientContext &, const string &, const Value &val) {
	return to_string(val.DefaultCastAs(LogicalType::UBIGINT).GetValue<idx_t>());
}

static string ParseRowGroupSize(ClientContext &, const string &, const Value &val) {
	auto row_group_size = val.DefaultCastAs(LogicalType::UBIGINT).GetValue<idx_t>();
	if (row_group_size == 0) {
		throw NotImplementedException("Row group size cannot be 0");
	}
	return to_string(row_group_size);
}

static string ParseRowGroupSizeBytes(ClientContext &, const string &, const Value &val) {
	auto row_group_size_bytes = DBConfig::ParseMemoryLimit(val.ToString());
	if (row_group_size_bytes == 0) {
		throw NotImplementedException("Row group size bytes cannot be 0");
	}
	return to_string(row_group_size_bytes);
}

static string ParseMemorySize(ClientContext &, const string &, const Value &val) {
	return to_string(DBConfig::ParseMemoryLimit(val.ToString()));
}

static string ParseBooleanLiteral(ClientContext &, const string &, const Value &val) {
	return DuckLakeUtil::BoolLiteral(val.GetValue<bool>());
}

static string ParseDeleteThreshold(ClientContext &, const string &, const Value &val) {
	double threshold = val.GetValue<double>();
	if (threshold < 0 || threshold > 1) {
		throw BinderException("The rewrite_delete_threshold must be between 0 and 1");
	}
	return to_string(threshold);
}

static string ParseInterval(ClientContext &, const string &option, const Value &val) {
	auto interval_value = val.ToString();
	if (!interval_value.empty()) {
		interval_t result;
		if (!Interval::FromString(interval_value, result)) {
			throw BinderException("%s is not a valid interval value.", option);
		}
	}
	return interval_value;
}

static string ParseBoolean(ClientContext &context, const string &, const Value &val) {
	return DuckLakeUtil::BoolLiteral(val.CastAs(context, LogicalType::BOOLEAN).GetValue<bool>());
}

static string ParseAutoCompact(ClientContext &context, const string &option, const Value &val) {
	if (val.IsNull()) {
		throw BinderException("The %s option can't be null.", option.c_str());
	}
	return ParseBoolean(context, option, val);
}

//! resolved against the table by the caller
static string ParseSkippedStatsColumns(ClientContext &, const string &, const Value &) {
	return string();
}

static const unordered_map<string, config_option_parser_t> &ConfigOptionParsers() {
	static const unordered_map<string, config_option_parser_t> parsers {
	    {"parquet_compression", ParseParquetCompression},
	    {"parquet_version", ParseParquetVersion},
	    {"parquet_compression_level", ParseUnsignedInteger},
	    {"data_inlining_row_limit", ParseUnsignedInteger},
	    {"parquet_row_group_size", ParseRowGroupSize},
	    {"parquet_row_group_size_bytes", ParseRowGroupSizeBytes},
	    {"target_file_size", ParseMemorySize},
	    {"require_commit_message", ParseBooleanLiteral},
	    {"hive_file_pattern", ParseBooleanLiteral},
	    {"rewrite_delete_threshold", ParseDeleteThreshold},
	    {"delete_older_than", ParseInterval},
	    {"expire_older_than", ParseInterval},
	    {"auto_compact", ParseAutoCompact},
	    {"per_thread_output", ParseBoolean},
	    {"write_deletion_vectors", ParseBoolean},
	    {"sort_on_insert", ParseBoolean},
	    {"skip_stats_columns", ParseSkippedStatsColumns},
	};
	return parsers;
}

void DuckLakeUtil::ValidateConfigOptionName(const string &option) {
	if (!ConfigOptionParsers().count(option)) {
		throw NotImplementedException("Unsupported option %s", option);
	}
}

string DuckLakeUtil::ParseConfigOptionValue(ClientContext &context, const string &option, const Value &val) {
	ValidateConfigOptionName(option);
	return ConfigOptionParsers().at(option)(context, option, val);
}

void DuckLakeUtil::ValidateConfigOptionScope(const string &option, bool has_schema, bool has_table) {
	if ((has_schema || has_table) && (option == "expire_older_than" || option == "delete_older_than")) {
		throw InvalidInputException("The '%s' option can only be set globally, not for a specific schema or table",
		                            option);
	}
	if (option == "skip_stats_columns" && !has_table) {
		throw InvalidInputException("The '%s' option can only be set for a specific table - pass table_name", option);
	}
}

bool DuckLakeUtil::TryGetLiteralValue(const ParsedExpression &expr, Value &result) {
	if (expr.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		result = expr.Cast<ConstantExpression>().GetLiteral().ToValue();
		return true;
	}
	if (expr.GetExpressionType() != ExpressionType::OPERATOR_CAST) {
		return false;
	}
	auto &cast = expr.Cast<CastExpression>();
	if (cast.IsTryCast() || cast.Child().GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
		return false;
	}
	auto target_type = UnboundType::TryDefaultBind(cast.TargetType());
	if (target_type.id() == LogicalTypeId::INVALID || target_type.id() == LogicalTypeId::UNBOUND) {
		return false;
	}
	auto value = cast.Child().Cast<ConstantExpression>().GetLiteral().ToValue().DefaultTryCastAs(target_type);
	if (!value) {
		return false;
	}
	result = std::move(*value);
	return true;
}

bool DuckLakeUtil::TryGetMacroDefaultLiteral(const ParsedExpression &expr, Value &result) {
	if (!TryGetLiteralValue(expr, result)) {
		return false;
	}
	// nested types are stored without their child types and a NULL string would read back as the text NULL
	return !result.type().IsNested() && !(result.IsNull() && DuckLakeTypes::IsStringType(result.type()));
}

} // namespace duckdb
