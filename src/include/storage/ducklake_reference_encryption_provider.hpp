//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_reference_encryption_provider.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "storage/ducklake_encryption_provider.hpp"

namespace duckdb {
class DatabaseInstance;

//! A test fixture, never a KMS. It wraps DEKs with real AES-256-GCM, but under a key-encryption key
//! derived from a seed published in this file's source, and the tests that drive it run with
//! force_mbedtls_unsafe on, which makes GCM nonces come from a non-cryptographic PRNG. Both are
//! disqualifying for production; a deployment supplies its own DuckLakeEncryptionProvider.
//! Each lake_id carries a process-wide active key version so a test can rotate keys and watch
//! ducklake_rewrap_keys move every blob onto the new version. The cache TTL is ignored: nothing is
//! cached.
class DuckLakeReferenceEncryptionProvider : public DuckLakeEncryptionProvider {
public:
	DuckLakeReferenceEncryptionProvider(DatabaseInstance &db, string lake_id);

	vector<string> WrapKeys(const vector<DuckLakeFileIdentity> &identities, const vector<string> &deks) override;
	string UnwrapKey(const DuckLakeFileIdentity &identity, const string &base64_value) override;
	vector<DuckLakeRewrapResult> RewrapKeys(const vector<DuckLakeFileIdentity> &identities,
	                                        const vector<string> &blobs) override;
	string SelfTest() override;
	const string &GetLakeId() const override;

	//! Sets the active key version for a lake id, so the next rewrap sweep has work to do.
	static int64_t SetActiveVersionForTests(const string &lake_id, int64_t version);
	static int64_t GetActiveVersionForTests(const string &lake_id);

private:
	DatabaseInstance &db;
	string lake_id;
};

//! Installs the reference provider as the process-wide encryption provider factory. Idempotent, since
//! the extension is loaded once per DatabaseInstance and a test binary creates many.
void RegisterDuckLakeReferenceEncryptionProviderForTests();

//! `ducklake_reference_provider_set_active_version(lake_id, version) -> BIGINT`, so a .test file can
//! drive a key rotation without new SQL syntax.
ScalarFunction DuckLakeReferenceProviderSetActiveVersionFunction();

} // namespace duckdb
