//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_metadata_profile.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb_re2 {
class RE2;
} // namespace duckdb_re2

namespace duckdb {
class DatabaseInstance;

//! How a commit error whose message matches the pattern is handled
struct DuckLakeCommitErrorRule {
	shared_ptr<duckdb_re2::RE2> pattern;
	bool retryable = false;
	//! Advice added to the error of a commit that failed for good
	string hint;
	//! Metadata parameters that leave out the hint when they have the given value
	case_insensitive_map_t<string> hint_unless_parameters;
};

//! The behaviour of a metadata database that DuckLake cannot detect, registered as JSON
struct DuckLakeMetadataProfile {
	set<LogicalTypeId> natively_unsupported_types;
	//! The type a column of the metadata tables is created with
	map<LogicalTypeId, string> column_type_overrides;
	idx_t max_column_count = NumericLimits<idx_t>::Maximum();
	vector<string> system_column_names;
	bool dropped_tables_stay_readable = true;
	//! Rename the columns of inlined data tables by copying the tables instead of with ALTER TABLE
	bool rename_inlined_columns_by_copy = false;
	//! The first rule whose pattern is found in the message of a commit error applies
	vector<DuckLakeCommitErrorRule> commit_errors;

	optional_ptr<const DuckLakeCommitErrorRule> FindCommitErrorRule(const string &message) const;

	//! The profile of a metadata database that has none registered
	static const DuckLakeMetadataProfile &Default();
};

//! The profiles registered in a database, by the catalog type of the metadata database they describe
class DuckLakeMetadataProfiles {
public:
	DuckLakeMetadataProfiles();

	static DuckLakeMetadataProfiles &Get(DatabaseInstance &db);

	//! Registers a profile from its JSON, whose fields replace those of its "base" profile
	void Register(const string &name, const string &json);
	shared_ptr<const DuckLakeMetadataProfile> Find(const string &name);

private:
	mutex lock;
	case_insensitive_map_t<shared_ptr<const DuckLakeMetadataProfile>> profiles;
};

} // namespace duckdb
