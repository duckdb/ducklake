#include "storage/ducklake_catalog.hpp"

#include "common/ducklake_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

DuckLakeFileIdentity DuckLakeCatalog::BuildEncryptionIdentity(TableIndex table_id, const string &stored_path,
                                                              bool is_delete_file) const {
	DuckLakeFileIdentity identity;
	identity.lake_id = options.encryption_lake_id;
	identity.table_id = NumericCast<int64_t>(table_id.index);
	identity.is_delete_file = is_delete_file;
	identity.stored_path = stored_path;
	return identity;
}

void DuckLakeCatalog::RefuseWrappedKeyWithoutProvider(const string &file_path, const string &stored_key) const {
	if (encryption_provider) {
		// A lake with a provider unwraps rather than refuses; that is the caller's job.
		return;
	}
	if (!DuckLakeEncryptionProvider::LooksWrapped(stored_key)) {
		return;
	}
	throw IOException("File %s carries a wrapped encryption key, but this DuckLake was attached without "
	                  "encryption_socket - re-attach with the envelope options",
	                  file_path);
}

void DuckLakeCatalog::RefuseMissingEncryptionKey(const string &file_path) const {
	if (!IsEncrypted()) {
		// On an unencrypted lake a row without a key is the ordinary case.
		return;
	}
	throw InvalidInputException("Database is encrypted, but file %s does not have an encryption key", file_path);
}

void DuckLakeCatalog::RefuseUnusableEncryptionKey(const string &file_path, const string &decoded_key) const {
	// The accepted lengths are read off ParquetCrypto::ValidKey, which rejects everything else.
	if (decoded_key.size() == 16 || decoded_key.size() == 24 || decoded_key.size() == 32) {
		return;
	}
	throw InvalidInputException("File %s carries an encryption key of %llu bytes, and AES accepts only 16, 24 or 32 - "
	                            "the stored encryption_key for this row is corrupt",
	                            file_path, static_cast<uint64_t>(decoded_key.size()));
}

void DuckLakeCatalog::RequireEncryptedTempSpill(ClientContext &context) {
	if (!encryption_provider) {
		return;
	}
	if (Settings::Get<TempFileEncryptionSetting>(context)) {
		return;
	}
	auto option = DBConfig::GetOptionByName("temp_file_encryption");
	if (!option) {
		throw InvalidInputException("This DuckDB build has no temp_file_encryption setting, which an encrypted "
		                            "DuckLake requires - an out-of-core query would spill decrypted rows in the clear");
	}
	auto &config = DBConfig::GetConfig(context);
	try {
		config.SetOption(context.db.get(), *option, Value::BOOLEAN(true));
	} catch (std::exception &ex) {
		// DuckDB refuses to encrypt a temp directory that already holds plaintext files.
		throw InvalidInputException("temp_file_encryption must be on before an encrypted DuckLake is attached, and "
		                            "turning it on failed: %s. Attach the lake before running work that spills, or "
		                            "start a fresh process",
		                            string(ex.what()));
	}
}

void DuckLakeCatalog::RefuseUnencryptedTempSpill(const string &what) const {
	if (!encryption_provider) {
		return;
	}
	if (Settings::Get<TempFileEncryptionSetting>(db.GetDatabase())) {
		return;
	}
	throw IOException("Refusing %s on an encrypted DuckLake while temp_file_encryption is off - decrypted rows would "
	                  "spill to temp_directory in the clear. Run SET temp_file_encryption = true",
	                  what);
}

string DuckLakeCatalog::ResolveStoredEncryptionKey(TableIndex table_id, const string &stored_path,
                                                   const string &resolved_path, bool is_delete_file,
                                                   const Value &stored_key_value) const {
	if (stored_key_value.IsNull()) {
		RefuseMissingEncryptionKey(resolved_path);
		return string();
	}
	auto stored_key = stored_key_value.GetValue<string>();
	if (stored_key.empty()) {
		// An empty string says what NULL says, so it gets the same refusal.
		RefuseMissingEncryptionKey(resolved_path);
		return string();
	}
	string decoded_key;
	if (encryption_provider) {
		// Checked before the unwrap: once this returns, the rows this key opens are on their way into
		// the buffer manager and can be evicted to temp_directory.
		RefuseUnencryptedTempSpill("a read");
		decoded_key =
		    encryption_provider->UnwrapKey(BuildEncryptionIdentity(table_id, stored_path, is_delete_file), stored_key);
	} else {
		RefuseWrappedKeyWithoutProvider(resolved_path, stored_key);
		decoded_key = Blob::FromBase64(string_t(stored_key));
	}
	RefuseUnusableEncryptionKey(resolved_path, decoded_key);
	return decoded_key;
}

void DuckLakeCatalog::PrepareFileKeysForCommit(const vector<TableIndex> &table_ids, const vector<string> &stored_paths,
                                               bool is_delete_file, vector<string> &keys) const {
	D_ASSERT(table_ids.size() == keys.size() && stored_paths.size() == keys.size());
	if (!encryption_provider) {
		DuckLakeUtil::EncodeStoredEncryptionKeys(keys);
		return;
	}
	// Checked once per commit rather than per file: a wrap hands back key material this process holds.
	RefuseUnencryptedTempSpill("a commit");
	vector<DuckLakeFileIdentity> wrap_identities;
	vector<string> deks;
	vector<idx_t> positions;
	for (idx_t i = 0; i < keys.size(); i++) {
		if (keys[i].empty()) {
			continue;
		}
		if (DuckLakeEncryptionProvider::LooksWrapped(keys[i])) {
			// Already wrapped: wrapping again would make UnwrapKey return ciphertext rather than the key.
			continue;
		}
		wrap_identities.push_back(BuildEncryptionIdentity(table_ids[i], stored_paths[i], is_delete_file));
		deks.push_back(keys[i]);
		positions.push_back(i);
	}
	if (deks.empty()) {
		return;
	}
	auto wrapped = encryption_provider->WrapKeys(wrap_identities, deks);
	if (wrapped.size() != deks.size()) {
		throw IOException("The encryption provider returned %llu wrapped keys for %llu files",
		                  static_cast<uint64_t>(wrapped.size()), static_cast<uint64_t>(deks.size()));
	}
	for (idx_t i = 0; i < positions.size(); i++) {
		if (wrapped[i].empty()) {
			throw IOException("The encryption provider returned an empty wrapped key for file %s",
			                  wrap_identities[i].stored_path);
		}
		keys[positions[i]] = wrapped[i];
	}
}

} // namespace duckdb
