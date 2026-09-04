#include "storage/ducklake_reference_encryption_provider.hpp"

#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/main/database.hpp"

#include <cstring>

namespace duckdb {

namespace {

constexpr idx_t KEK_BYTES = 32;
constexpr idx_t NONCE_BYTES = 12;
constexpr idx_t TAG_BYTES = 16;
constexpr idx_t HEADER_BYTES = 5;
//! Envelope magic; base64 of these bytes is what DuckLakeEncryptionProvider::LooksWrapped matches.
constexpr char MAGIC[4] = {'D', 'L', 'K', '1'};

//! Published on purpose: the KEKs of a test fixture are not secrets.
constexpr const char *REFERENCE_TEST_SEED = "ducklake-reference-provider-TEST-ONLY-NOT-FOR-PRODUCTION-v1";

mutex g_version_mutex;
unordered_map<string, int64_t> g_active_version; // NOLINT

int64_t GetActiveVersionLocked(const string &lake_id) {
	auto it = g_active_version.find(lake_id);
	if (it == g_active_version.end()) {
		g_active_version[lake_id] = 1;
		return 1;
	}
	return it->second;
}

//! Length-prefixed so the concatenation is injective: no field's contents can imitate a separator and
//! make two different identities serialize the same way.
void AppendField(string &out, const string &field) {
	out += to_string(field.size());
	out += ':';
	out += field;
}

//! SHA-256(seed || lake_id || version), so every connection agrees on the KEK for a given version.
string DeriveKek(EncryptionUtil &util, const string &lake_id, int64_t version) {
	string material;
	AppendField(material, REFERENCE_TEST_SEED);
	AppendField(material, lake_id);
	AppendField(material, to_string(version));
	data_t digest[KEK_BYTES];
	util.Hash(CryptoHashFunction::SHA256, const_data_ptr_cast(material.data()), material.size(), digest);
	return string(const_char_ptr_cast(digest), KEK_BYTES);
}

//! The AAD a wrapped key is bound to. Unwrapping under a different table, path, or file kind fails GCM
//! tag verification rather than handing back a key.
string SerializeIdentity(const DuckLakeFileIdentity &identity) {
	string aad;
	AppendField(aad, identity.lake_id);
	AppendField(aad, to_string(identity.table_id));
	AppendField(aad, identity.is_delete_file ? "D" : "F");
	AppendField(aad, identity.stored_path);
	return aad;
}

shared_ptr<EncryptionState> MakeAesGcmState(EncryptionUtil &util, idx_t key_len) {
	auto metadata =
	    make_uniq<EncryptionStateMetadata>(EncryptionTypes::GCM, key_len, EncryptionTypes::EncryptionVersion::V0_1);
	return util.CreateEncryptionState(std::move(metadata));
}

//! MAGIC(4) || version:uint8(1) || nonce(12) || ciphertext(N) || tag(16)
string EnvelopeEncrypt(EncryptionUtil &util, const string &plaintext, const string &kek, int64_t version,
                       const string &aad) {
	if (version < 0 || version > 255) {
		throw InvalidInputException("ducklake reference encryption provider: key version %lld is outside the test "
		                            "range [0,255]",
		                            static_cast<long long>(version));
	}
	auto aes = MakeAesGcmState(util, kek.size());
	EncryptionNonce nonce;
	D_ASSERT(nonce.size() == NONCE_BYTES);
	aes->GenerateRandomData(nonce.data(), nonce.size());

	aes->InitializeEncryption(nonce, const_data_ptr_cast(kek.data()), const_data_ptr_cast(aad.data()), aad.size());

	string ciphertext(plaintext.size(), '\0');
	auto written = aes->Process(const_data_ptr_cast(plaintext.data()), plaintext.size(), data_ptr_cast(&ciphertext[0]),
	                            ciphertext.size());
	ciphertext.resize(written);

	data_t tail[64];
	data_t tag[TAG_BYTES];
	auto tail_written = aes->Finalize(tail, 0, tag, TAG_BYTES);
	ciphertext.append(const_char_ptr_cast(tail), tail_written);

	string blob;
	blob.append(MAGIC, 4);
	blob.push_back(static_cast<char>(static_cast<uint8_t>(version)));
	blob.append(const_char_ptr_cast(nonce.data()), nonce.size());
	blob.append(ciphertext);
	blob.append(const_char_ptr_cast(tag), TAG_BYTES);
	return blob;
}

//! Inverse of EnvelopeEncrypt. Throws on a malformed blob, and on a tag mismatch, which is what a wrong
//! key version or a tampered identity produces.
string EnvelopeDecrypt(EncryptionUtil &util, const string &blob, const string &lake_id, const string &aad,
                       int64_t *version_out) {
	if (blob.size() < HEADER_BYTES + NONCE_BYTES + TAG_BYTES || std::memcmp(blob.data(), MAGIC, 4) != 0) {
		throw InvalidInputException("ducklake reference encryption provider: the stored value is not a DLK1 envelope");
	}
	int64_t version = static_cast<uint8_t>(blob[4]);
	idx_t ct_len = blob.size() - HEADER_BYTES - NONCE_BYTES - TAG_BYTES;
	auto ciphertext = blob.substr(HEADER_BYTES + NONCE_BYTES, ct_len);
	data_t expected_tag[TAG_BYTES];
	std::memcpy(expected_tag, blob.data() + blob.size() - TAG_BYTES, TAG_BYTES);

	auto kek = DeriveKek(util, lake_id, version);
	auto aes = MakeAesGcmState(util, kek.size());
	EncryptionNonce nonce;
	D_ASSERT(nonce.size() == NONCE_BYTES);
	std::memcpy(nonce.data(), blob.data() + HEADER_BYTES, NONCE_BYTES);

	aes->InitializeDecryption(nonce, const_data_ptr_cast(kek.data()), const_data_ptr_cast(aad.data()), aad.size());

	string plaintext(ct_len, '\0');
	auto written =
	    aes->Process(const_data_ptr_cast(ciphertext.data()), ct_len, data_ptr_cast(&plaintext[0]), plaintext.size());
	plaintext.resize(written);

	data_t tail[64];
	auto tail_written = aes->Finalize(tail, 0, expected_tag, TAG_BYTES);
	plaintext.append(const_char_ptr_cast(tail), tail_written);

	if (version_out) {
		*version_out = version;
	}
	return plaintext;
}

} // namespace

DuckLakeReferenceEncryptionProvider::DuckLakeReferenceEncryptionProvider(DatabaseInstance &db, string lake_id_p)
    : db(db), lake_id(std::move(lake_id_p)) {
}

vector<string> DuckLakeReferenceEncryptionProvider::WrapKeys(const vector<DuckLakeFileIdentity> &identities,
                                                             const vector<string> &deks) {
	if (identities.size() != deks.size()) {
		throw InternalException("ducklake reference encryption provider: WrapKeys identity/key count mismatch");
	}
	auto util = db.GetEncryptionUtil(false);
	vector<string> out;
	out.reserve(deks.size());
	for (idx_t i = 0; i < deks.size(); i++) {
		int64_t version;
		{
			lock_guard<mutex> lock(g_version_mutex);
			version = GetActiveVersionLocked(identities[i].lake_id);
		}
		auto kek = DeriveKek(*util, identities[i].lake_id, version);
		auto blob = EnvelopeEncrypt(*util, deks[i], kek, version, SerializeIdentity(identities[i]));
		out.push_back(Blob::ToBase64(string_t(blob)));
	}
	return out;
}

string DuckLakeReferenceEncryptionProvider::UnwrapKey(const DuckLakeFileIdentity &identity,
                                                      const string &base64_value) {
	auto util = db.GetEncryptionUtil(true);
	auto raw = Blob::FromBase64(base64_value);
	return EnvelopeDecrypt(*util, raw, identity.lake_id, SerializeIdentity(identity), nullptr);
}

vector<DuckLakeRewrapResult>
DuckLakeReferenceEncryptionProvider::RewrapKeys(const vector<DuckLakeFileIdentity> &identities,
                                                const vector<string> &blobs) {
	if (identities.size() != blobs.size()) {
		throw InternalException("ducklake reference encryption provider: RewrapKeys identity/blob count mismatch");
	}
	auto util = db.GetEncryptionUtil(false);
	vector<DuckLakeRewrapResult> out;
	out.reserve(blobs.size());
	for (idx_t i = 0; i < blobs.size(); i++) {
		auto raw = Blob::FromBase64(blobs[i]);
		int64_t stored_version;
		auto aad = SerializeIdentity(identities[i]);
		auto dek = EnvelopeDecrypt(*util, raw, identities[i].lake_id, aad, &stored_version);

		int64_t active_version;
		{
			lock_guard<mutex> lock(g_version_mutex);
			active_version = GetActiveVersionLocked(identities[i].lake_id);
		}
		if (stored_version == active_version) {
			out.push_back(DuckLakeRewrapResult {blobs[i], false});
			continue;
		}
		auto kek = DeriveKek(*util, identities[i].lake_id, active_version);
		auto new_blob = EnvelopeEncrypt(*util, dek, kek, active_version, aad);
		out.push_back(DuckLakeRewrapResult {Blob::ToBase64(string_t(new_blob)), true});
	}
	return out;
}

string DuckLakeReferenceEncryptionProvider::SelfTest() {
	DuckLakeFileIdentity identity;
	identity.lake_id = lake_id;
	identity.table_id = 0;
	identity.is_delete_file = false;
	identity.stored_path = "__ducklake_reference_provider_self_test__";

	string plaintext_dek(32, '\0');
	for (idx_t i = 0; i < plaintext_dek.size(); i++) {
		plaintext_dek[i] = static_cast<char>(i);
	}

	auto wrapped = WrapKeys({identity}, {plaintext_dek});
	auto unwrapped = UnwrapKey(identity, wrapped[0]);
	if (unwrapped != plaintext_dek) {
		throw InternalException("ducklake reference encryption provider: self-test round-trip mismatch");
	}

	// Proves the AAD binding is load-bearing rather than merely computed.
	DuckLakeFileIdentity other = identity;
	other.stored_path = "__ducklake_reference_provider_self_test_other__";
	bool refused = false;
	try {
		UnwrapKey(other, wrapped[0]);
	} catch (const std::exception &) {
		refused = true;
	}
	if (!refused) {
		throw InternalException(
		    "ducklake reference encryption provider: self-test blob was accepted under a different identity");
	}

	int64_t active_version;
	{
		lock_guard<mutex> lock(g_version_mutex);
		active_version = GetActiveVersionLocked(lake_id);
	}
	return StringUtil::Format("ducklake reference encryption provider ok (lake_id=%s, active_version=%lld)", lake_id,
	                          static_cast<long long>(active_version));
}

const string &DuckLakeReferenceEncryptionProvider::GetLakeId() const {
	return lake_id;
}

int64_t DuckLakeReferenceEncryptionProvider::SetActiveVersionForTests(const string &lake_id_p, int64_t version) {
	lock_guard<mutex> lock(g_version_mutex);
	g_active_version[lake_id_p] = version;
	return version;
}

int64_t DuckLakeReferenceEncryptionProvider::GetActiveVersionForTests(const string &lake_id_p) {
	lock_guard<mutex> lock(g_version_mutex);
	return GetActiveVersionLocked(lake_id_p);
}

static void SetActiveVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, int64_t, int64_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t lake_id_val, int64_t version) {
		    return DuckLakeReferenceEncryptionProvider::SetActiveVersionForTests(lake_id_val.GetString(), version);
	    });
}

ScalarFunction DuckLakeReferenceProviderSetActiveVersionFunction() {
	return ScalarFunction("ducklake_reference_provider_set_active_version", {LogicalType::VARCHAR, LogicalType::BIGINT},
	                      LogicalType::BIGINT, SetActiveVersionScalarFun);
}

void RegisterDuckLakeReferenceEncryptionProviderForTests() {
	if (DuckLakeEncryptionProvider::HasFactory()) {
		// LoadInternal runs once per DatabaseInstance, and RegisterFactory refuses to overwrite.
		return;
	}
	DuckLakeEncryptionProvider::RegisterFactory([](DatabaseInstance &db, const string &encryption_socket,
	                                               const string &encryption_lake_id,
	                                               idx_t cache_ttl_seconds) -> unique_ptr<DuckLakeEncryptionProvider> {
		return make_uniq<DuckLakeReferenceEncryptionProvider>(db, encryption_lake_id);
	});
}

} // namespace duckdb
