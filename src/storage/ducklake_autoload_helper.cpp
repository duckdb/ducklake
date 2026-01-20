#include "duckdb/main/extension_entries.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"

#include "storage/ducklake_initializer.hpp"

namespace duckdb {

static string LookupExtensionForPattern(const string &pattern) {
	for (const auto &entry : EXTENSION_FILE_PREFIXES) {
		if (StringUtil::StartsWith(pattern, entry.name)) {
			return entry.extension;
		}
	}
	return "";
}

void DuckLakeInitializer::CheckAndAutoloadedRequiredExtension(const string &pattern) {
	// This functions will:
	//	1. Check if a known extension pattern matches the start of the data_path
	//	2. If so, either load the required extension or throw a relevant error message

	// FIXME: This function is currently a copy of the logic at FileSystem::GlobFiles in duckdb/duckdb (version 1.3.0)
	// repository Proper solution would be offer this functionality as part of DuckDB C++ API, so this file can be
	// simplified reducing the risk of misalignment between the two codebases

	string required_extension = LookupExtensionForPattern(pattern);
	if (!required_extension.empty() && !context.db->ExtensionIsLoaded(required_extension)) {
		auto &dbconfig = DBConfig::GetConfig(context);
		if (!ExtensionHelper::CanAutoloadExtension(required_extension) || !!Settings::Get<AutoloadKnownExtensionsSetting>(context)) {
			auto error_message =
			    "Data path " + pattern + " requires the extension " + required_extension + " to be loaded";
			error_message =
			    ExtensionHelper::AddExtensionInstallHintToErrorMsg(context, error_message, required_extension);
			throw MissingExtensionException(error_message);
		}
		// an extension is required to read this file, but it is not loaded - try to load it
		ExtensionHelper::AutoLoadExtension(context, required_extension);
		// success! glob again
		// check the extension is loaded just in case to prevent an infinite loop here
		if (!context.db->ExtensionIsLoaded(required_extension)) {
			throw InternalException("Extension load \"%s\" did not throw but somehow the extension was not loaded",
			                        required_extension);
		}
	}
}

} // namespace duckdb
