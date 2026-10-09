//===----------------------------------------------------------------------===//
//                         DuckLake
//
// common/ducklake_key_wrap.hpp
//
// Envelope encryption for the per-file data encryption keys (DEKs) that an
// ENCRYPTED DuckLake stores in the catalog. With a key-encryption key (KEK)
// supplied at ATTACH time (KEY_ENCRYPTION_KEY '...'), the catalog only ever
// holds AES-256-GCM(KEK, DEK); the plaintext DEK exists in process memory only.
//
// Stored format (VARCHAR column `encryption_key`, unchanged schema):
//   raw:     base64(DEK)                                  (legacy, still readable)
//   wrapped: "kek1:" + base64(iv[12] || ciphertext || tag[16])
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/encryption_state.hpp"

namespace duckdb {

class DuckLakeKeyWrap {
public:
	static constexpr const char *PREFIX = "kek1:";
	static constexpr idx_t KEK_SIZE = 32;
	static constexpr idx_t IV_SIZE = 12;
	static constexpr idx_t TAG_SIZE = 16;

	//! Derive a 32-byte KEK from a user passphrase (SHA-256). A 64-hex-char input is taken as the raw key.
	static string DeriveKEK(const string &passphrase);
	//! True if the stored value carries the wrapped-key prefix.
	static bool IsWrapped(const string &stored);
	//! Wrap a DEK for storage. Empty kek -> plain base64 (legacy behaviour).
	static string Encode(EncryptionUtil &util, const string &kek, const string &dek);
	//! Read a stored value back to a DEK. Wrapped values require kek; raw values never do.
	static string Decode(EncryptionUtil &util, const string &kek, const string &stored);
};

//! Value type carrying the KEK (possibly empty) and the cipher provider, so the static SQL-emitting
//! paths shared with the server-side commit can wrap keys without a catalog object.
struct DuckLakeKeyCodec {
	shared_ptr<EncryptionUtil> util;
	string kek;

	bool HasKEK() const {
		return !kek.empty();
	}
	//! Storable form of a DEK (plain base64 when no KEK)
	string Encode(const string &dek) const;
	//! SQL literal of Encode, or NULL for an empty key
	string Literal(const string &dek) const;
	//! DEK from a stored value (raw or wrapped)
	string Decode(const string &stored) const;
};

} // namespace duckdb
