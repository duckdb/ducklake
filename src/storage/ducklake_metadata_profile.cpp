#include "storage/ducklake_metadata_profile.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/json_document.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "re2/re2.h"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_storage.hpp"

namespace duckdb {

optional_ptr<const DuckLakeCommitErrorRule> DuckLakeMetadataProfile::FindCommitErrorRule(const string &message) const {
	for (auto &rule : commit_errors) {
		if (duckdb_re2::RE2::PartialMatch(message, *rule.pattern)) {
			return &rule;
		}
	}
	return nullptr;
}

const DuckLakeMetadataProfile &DuckLakeMetadataProfile::Default() {
	static const DuckLakeMetadataProfile DEFAULT_PROFILE;
	return DEFAULT_PROFILE;
}

namespace {

//! Reads the fields of a profile from its JSON
class DuckLakeProfileParser {
public:
	explicit DuckLakeProfileParser(DuckLakeMetadataProfile &profile) : profile(profile) {
	}

	void ParseProfile(JSONValue root) {
		root.IterateObject([&](const string &key, JSONValue value) {
			if (key == "base") {
				// resolved before the other fields
				return;
			}
			if (key == "types") {
				ParseTypes(value);
			} else if (key == "max_identifier_length") {
				profile.max_identifier_length = GetCount(key, value);
			} else if (key == "max_column_count") {
				profile.max_column_count = GetCount(key, value);
			} else if (key == "system_column_names") {
				profile.system_column_names.clear();
				IterateStrings(key, value, [&](const string &name) { profile.system_column_names.push_back(name); });
			} else if (key == "dropped_tables_stay_readable") {
				profile.dropped_tables_stay_readable = GetBoolean(key, value);
			} else if (key == "rename_inlined_columns_by_copy") {
				profile.rename_inlined_columns_by_copy = GetBoolean(key, value);
			} else if (key == "supports_appender") {
				profile.supports_appender = GetBoolean(key, value);
			} else if (key == "new_tables_require_cache_clear") {
				profile.new_tables_require_cache_clear = GetBoolean(key, value);
			} else if (key == "materialize_stats_ctes") {
				profile.materialize_stats_ctes = GetBoolean(key, value);
			} else if (key == "max_inlined_rows_in_batch") {
				profile.max_inlined_rows_in_batch = GetCount(key, value);
			} else if (key == "snapshot_time_sql") {
				profile.snapshot_time_sql = GetString(key, value);
			} else if (key == "config_option_lock_sql") {
				profile.config_option_lock_sql = GetString(key, value);
			} else if (key == "shows_concurrent_commits_with_parameter") {
				ExpectObject(key, value);
				profile.shows_concurrent_commits_with_parameters.clear();
				value.IterateObject([&](const string &parameter, JSONValue parameter_value) {
					profile.shows_concurrent_commits_with_parameters[parameter] = GetString(parameter, parameter_value);
				});
			} else if (key == "commit_errors") {
				ParseCommitErrors(value);
			} else if (key == "reset_settings") {
				profile.reset_settings.clear();
				IterateStrings(key, value, [&](const string &name) { profile.reset_settings.push_back(name); });
			} else if (key == "reported_catalog_type") {
				profile.reported_catalog_type = GetString(key, value);
			} else {
				throw InvalidInputException("unknown field \"%s\"", key);
			}
		});
	}

private:
	void ParseTypes(JSONValue types) {
		ExpectObject("types", types);
		types.IterateObject([&](const string &key, JSONValue value) {
			if (key == "natively_unsupported") {
				profile.natively_unsupported_types.clear();
				IterateStrings(key, value,
				               [&](const string &name) { profile.natively_unsupported_types.insert(GetTypeId(name)); });
			} else if (key == "column_type_overrides") {
				ExpectObject(key, value);
				profile.column_type_overrides.clear();
				value.IterateObject([&](const string &name, JSONValue column_type) {
					profile.column_type_overrides[GetTypeId(name)] = GetString(name, column_type);
				});
			} else {
				throw InvalidInputException("unknown field \"types.%s\"", key);
			}
		});
	}

	void ParseCommitErrors(JSONValue rules) {
		if (!rules.IsArray()) {
			throw InvalidInputException("\"commit_errors\" must be an array");
		}
		profile.commit_errors.clear();
		rules.IterateArray([&](JSONValue rule_value) {
			ExpectObject("commit_errors", rule_value);
			DuckLakeCommitErrorRule rule;
			rule_value.IterateObject([&](const string &key, JSONValue value) {
				if (key == "pattern") {
					rule.pattern = CompilePattern(GetString(key, value));
				} else if (key == "retryable") {
					rule.retryable = GetBoolean(key, value);
				} else if (key == "hint") {
					rule.hint = GetString(key, value);
				} else if (key == "hint_unless_parameter") {
					ExpectObject(key, value);
					value.IterateObject([&](const string &parameter, JSONValue parameter_value) {
						rule.hint_unless_parameters[parameter] = GetString(parameter, parameter_value);
					});
				} else {
					throw InvalidInputException("unknown field \"commit_errors.%s\"", key);
				}
			});
			if (!rule.pattern) {
				throw InvalidInputException("a rule of \"commit_errors\" has no \"pattern\"");
			}
			profile.commit_errors.push_back(std::move(rule));
		});
	}

	static shared_ptr<duckdb_re2::RE2> CompilePattern(const string &pattern) {
		duckdb_re2::RE2::Options options;
		options.set_case_sensitive(false);
		options.set_log_errors(false);
		auto result = make_shared_ptr<duckdb_re2::RE2>(pattern, options);
		if (!result->ok()) {
			throw InvalidInputException("invalid pattern \"%s\": %s", pattern, result->error());
		}
		return result;
	}

	static LogicalTypeId GetTypeId(const string &name) {
		try {
			return EnumUtil::FromString<LogicalTypeId>(name);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			throw InvalidInputException("unknown type \"%s\": %s", name, error.RawMessage());
		}
	}

	static void ExpectObject(const string &key, JSONValue value) {
		if (!value.IsObject()) {
			throw InvalidInputException("\"%s\" must be an object", key);
		}
	}

	static string GetString(const string &key, JSONValue value) {
		if (!value.IsString()) {
			throw InvalidInputException("\"%s\" must be a string", key);
		}
		return value.GetString();
	}

	static bool GetBoolean(const string &key, JSONValue value) {
		if (value.GetType() != JSONValueType::BOOLEAN) {
			throw InvalidInputException("\"%s\" must be a boolean", key);
		}
		return value.GetBoolean();
	}

	static idx_t GetCount(const string &key, JSONValue value) {
		if (value.GetType() != JSONValueType::UNSIGNED_INTEGER) {
			throw InvalidInputException("\"%s\" must be a non-negative integer", key);
		}
		return value.GetUnsignedInteger();
	}

	static void IterateStrings(const string &key, JSONValue value,
	                           const std::function<void(const string &)> &callback) {
		if (!value.IsArray()) {
			throw InvalidInputException("\"%s\" must be an array of strings", key);
		}
		value.IterateArray([&](JSONValue element) { callback(GetString(key, element)); });
	}

private:
	DuckLakeMetadataProfile &profile;
};

} // namespace

DuckLakeMetadataProfiles &DuckLakeMetadataProfiles::Get(DatabaseInstance &db) {
	auto extension = StorageExtension::Find(DBConfig::GetConfig(db), "ducklake");
	if (!extension) {
		// LCOV_EXCL_START
		throw InternalException("DuckLake storage extension is not registered");
		// LCOV_EXCL_STOP
	}
	return static_cast<DuckLakeStorageExtension &>(*extension).metadata_profiles;
}

//! The profile of Postgres metadata databases, until the postgres extension registers its own.
//! Postgres has no unnamed composite types, unsigned or 128-bit integers, or GEOMETRY without PostGIS, narrower date
//! and timestamp ranges, microsecond TIME, another bytea text format and no NUL bytes in text, so such values are
//! stored as text or BYTEA. Its NOW() is the start of the metadata transaction, so commits use clock_timestamp().
//! postgres_scanner finds a table it has not cached, Postgres misplans several inlined stats joins, and a commit that
//! changes options updates the version row so that of two concurrent ones the later fails or waits.
const char *const POSTGRES_PROFILE = R"json({
	"types": {
		"natively_unsupported": ["STRUCT", "MAP", "LIST", "UBIGINT", "HUGEINT", "UHUGEINT", "TIME_NS", "DATE",
		                         "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ_NS", "TIMESTAMP_S",
		                         "TIMESTAMP_MS", "TIMESTAMP_NS", "BLOB", "VARCHAR", "VARIANT", "GEOMETRY"],
		"column_type_overrides": {
			"DOUBLE": "DOUBLE PRECISION", "FLOAT": "REAL", "TINYINT": "SMALLINT", "UTINYINT": "INTEGER",
			"USMALLINT": "INTEGER", "NULL": "INTEGER", "UINTEGER": "BIGINT", "BLOB": "BYTEA", "VARCHAR": "BYTEA",
			"UBIGINT": "VARCHAR", "HUGEINT": "VARCHAR", "UHUGEINT": "VARCHAR", "TIME_NS": "VARCHAR",
			"DATE": "VARCHAR", "TIMESTAMP": "VARCHAR", "TIMESTAMP WITH TIME ZONE": "VARCHAR",
			"TIMESTAMPTZ_NS": "VARCHAR", "TIMESTAMP_S": "VARCHAR", "TIMESTAMP_MS": "VARCHAR",
			"TIMESTAMP_NS": "VARCHAR"
		}
	},
	"max_identifier_length": 63,
	"max_column_count": 1600,
	"system_column_names": ["tableoid", "xmin", "cmin", "xmax", "cmax", "ctid"],
	"dropped_tables_stay_readable": false,
	"supports_appender": false,
	"new_tables_require_cache_clear": false,
	"materialize_stats_ctes": true,
	"max_inlined_rows_in_batch": 100,
	"snapshot_time_sql": "clock_timestamp()",
	"config_option_lock_sql": "UPDATE {METADATA_CATALOG}.ducklake_metadata SET value = value WHERE key = 'version' AND scope IS NULL;\n",
	"shows_concurrent_commits_with_parameter": {"isolation_level": "READ COMMITTED"},
	"reset_settings": ["pg_use_text_protocol", "pg_query_mode", "pg_use_binary_copy", "pg_array_as_varchar",
	                   "pg_numeric_as_varchar"]
})json";

DuckLakeMetadataProfiles::DuckLakeMetadataProfiles() {
	RegisterProfile("postgres", POSTGRES_PROFILE, true);
}

void DuckLakeMetadataProfiles::Register(const string &name, const string &json, bool replace) {
	if (DuckLakeMetadataManager::HasBuiltInManager(name)) {
		throw InvalidInputException("Failed to register DuckLake metadata profile \"%s\": DuckLake has a built-in "
		                            "metadata manager of this name",
		                            name);
	}
	RegisterProfile(name, json, replace);
}

void DuckLakeMetadataProfiles::RegisterProfile(const string &name, const string &json, bool replace) {
	try {
		JSONParseError parse_error;
		auto document = JSONDocument::TryParse(json.c_str(), json.size(), parse_error);
		if (!document) {
			throw InvalidInputException("invalid JSON at position %d: %s", parse_error.position, parse_error.message);
		}
		auto root = document->GetRoot();
		if (!root.IsObject()) {
			throw InvalidInputException("the profile must be a JSON object");
		}
		auto profile = make_shared_ptr<DuckLakeMetadataProfile>();
		auto base = root.GetMember("base");
		if (base.IsValid()) {
			if (!base.IsString()) {
				throw InvalidInputException("\"base\" must be a string");
			}
			if (DuckLakeMetadataManager::HasBuiltInManager(base.GetString())) {
				// the manager would not be used for the catalog type of the profile
				throw InvalidInputException("base \"%s\" is the profile of a built-in metadata manager",
				                            base.GetString());
			}
			auto base_profile = Find(base.GetString());
			if (!base_profile) {
				throw InvalidInputException("base \"%s\" is not a registered metadata profile", base.GetString());
			}
			*profile = *base_profile;
		}
		DuckLakeProfileParser(*profile).ParseProfile(root);
		lock_guard<mutex> guard(lock);
		auto &entry = profiles[name];
		if (entry && !replace) {
			return;
		}
		entry = std::move(profile);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() != ExceptionType::INVALID_INPUT) {
			throw;
		}
		throw InvalidInputException("Failed to register DuckLake metadata profile \"%s\": %s", name,
		                            error.RawMessage());
	}
}

shared_ptr<const DuckLakeMetadataProfile> DuckLakeMetadataProfiles::Find(const string &name) {
	lock_guard<mutex> guard(lock);
	auto entry = profiles.find(name);
	if (entry == profiles.end()) {
		return nullptr;
	}
	return entry->second;
}

} // namespace duckdb
