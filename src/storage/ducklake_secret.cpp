#include "storage/ducklake_secret.hpp"

namespace duckdb {

unique_ptr<BaseSecret> DuckLakeSecret::CreateDuckLakeSecretFunction(ClientContext &context, CreateSecretInput &input) {
	// apply any overridden settings
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "ducklake", "config", input.name);
	if (input.options.find("metadata_path") == input.options.end()) {
		throw InvalidInputException("metadata_path must be defined when creating a DuckLake secret");
	}
	for (const auto &named_param : input.options) {
		result->secret_map[named_param.first] = named_param.second;
	}
	return std::move(result);
}

bool DuckLakeSecret::PathIsSecret(const string &path) {
	// secret names can be alphanumeric and have underscores
	for (auto c : path) {
		if (StringUtil::CharacterIsAlphaNumeric(c)) {
			continue;
		}
		if (c == '_') {
			continue;
		}
		return false;
	}
	return true;
}

SecretType DuckLakeSecret::GetSecretType() {
	SecretType secret_type;
	secret_type.name = "ducklake";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	return secret_type;
}

CreateSecretFunction DuckLakeSecret::GetFunction() {
	CreateSecretFunction function = {"ducklake", "config", CreateDuckLakeSecretFunction};
	function.named_parameters["data_path"] = LogicalType::VARCHAR;
	function.named_parameters["metadata_schema"] = LogicalType::VARCHAR;
	function.named_parameters["metadata_catalog"] = LogicalType::VARCHAR;
	function.named_parameters["metadata_path"] = LogicalType::VARCHAR;
	function.named_parameters["metadata_parameters"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	function.named_parameters["encrypted"] = LogicalType::BOOLEAN;
	function.named_parameters["ducklake_version"] = LogicalType::VARCHAR;
	return function;
}

unique_ptr<SecretEntry> DuckLakeSecret::GetSecret(ClientContext &context, const string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	// FIXME: this should be adjusted once the `GetSecretByName` API supports this use case
	auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "memory");
	if (secret_entry) {
		return secret_entry;
	}
	secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "local_file");
	if (secret_entry) {
		return secret_entry;
	}
	return nullptr;
}
} // namespace duckdb
