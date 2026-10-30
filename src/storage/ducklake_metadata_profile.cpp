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
			} else if (key == "max_column_count") {
				profile.max_column_count = GetCount(key, value);
			} else if (key == "system_column_names") {
				profile.system_column_names.clear();
				IterateStrings(key, value, [&](const string &name) { profile.system_column_names.push_back(name); });
			} else if (key == "dropped_tables_stay_readable") {
				profile.dropped_tables_stay_readable = GetBoolean(key, value);
			} else if (key == "rename_inlined_columns_by_copy") {
				profile.rename_inlined_columns_by_copy = GetBoolean(key, value);
			} else if (key == "commit_errors") {
				ParseCommitErrors(value);
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

//! The profile of SQLite metadata databases, until the sqlite extension registers its own
const char *const SQLITE_PROFILE = R"({
	"types": {
		"natively_unsupported": ["STRUCT", "MAP", "LIST", "FLOAT", "DOUBLE", "TIMESTAMP WITH TIME ZONE",
		                         "TIMESTAMPTZ_NS", "VARIANT"],
		"column_type_overrides": {"FLOAT": "VARCHAR", "DOUBLE": "VARCHAR", "NULL": "INTEGER"}
	},
	"max_column_count": 2000,
	"system_column_names": ["rowid", "oid", "_rowid_"],
	"dropped_tables_stay_readable": false,
	"rename_inlined_columns_by_copy": true,
	"commit_errors": [
		{
			"pattern": "failed to execute query \"commit\": database is locked",
			"hint": "In its rollback journal mode, a SQLite catalog cannot commit while another transaction is open on it. Attach the catalog with META_JOURNAL_MODE 'WAL' to let open transactions and a commit run at the same time.\n",
			"hint_unless_parameter": {"journal_mode": "wal"}
		},
		{"pattern": "database is locked\\s*$", "retryable": true}
	]
})";

} // namespace

DuckLakeMetadataProfiles::DuckLakeMetadataProfiles() {
	Register("sqlite", SQLITE_PROFILE);
}

DuckLakeMetadataProfiles &DuckLakeMetadataProfiles::Get(DatabaseInstance &db) {
	auto extension = StorageExtension::Find(DBConfig::GetConfig(db), "ducklake");
	if (!extension) {
		// LCOV_EXCL_START
		throw InternalException("DuckLake storage extension is not registered");
		// LCOV_EXCL_STOP
	}
	return static_cast<DuckLakeStorageExtension &>(*extension).metadata_profiles;
}

void DuckLakeMetadataProfiles::Register(const string &name, const string &json) {
	try {
		if (DuckLakeMetadataManager::HasBuiltInManager(name)) {
			throw InvalidInputException("DuckLake has a built-in metadata manager of this name");
		}
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
			auto base_profile = Find(base.GetString());
			if (!base_profile) {
				throw InvalidInputException("base \"%s\" is not a registered metadata manager", base.GetString());
			}
			*profile = *base_profile;
		}
		DuckLakeProfileParser(*profile).ParseProfile(root);
		lock_guard<mutex> guard(lock);
		profiles[name] = std::move(profile);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() != ExceptionType::INVALID_INPUT) {
			throw;
		}
		throw InvalidInputException("Failed to register DuckLake metadata manager \"%s\": %s", name,
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
