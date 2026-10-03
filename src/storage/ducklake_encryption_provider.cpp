#include "storage/ducklake_encryption_provider.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

namespace {

//! Base64 of a 32-byte key: the longest `encryption_key` a lake without an envelope stores. A
//! wrapped value carries a header and an authentication tag on top of the key, so it is longer.
constexpr idx_t LONGEST_PLAINTEXT_KEY_LENGTH = 44;

//! Registered once for the life of the process so a factory survives ATTACH/DETACH; never freed,
//! hence a raw pointer rather than a global with an exit-time destructor.
DuckLakeEncryptionProvider::Factory *global_factory = nullptr;

} // namespace

// Out-of-line definitions: the constants are ODR-used (e.g. passed by reference into a variadic
// exception constructor), and under C++11 an ODR-used static constexpr member needs a definition.
constexpr int64_t DuckLakeEncryptionProvider::DEFAULT_CACHE_TTL_SECONDS;
constexpr int64_t DuckLakeEncryptionProvider::MAX_CACHE_TTL_SECONDS;

bool DuckLakeEncryptionProvider::LooksWrapped(const string &stored_value) {
	// A wrapped value starts with the fixed header "DLK1", whose first three bytes base64 as the
	// literal prefix "RExL". The length floor is part of the test: without it a plaintext key that
	// happens to start with those three bytes would be misread as wrapped and refused forever.
	if (stored_value.size() <= LONGEST_PLAINTEXT_KEY_LENGTH) {
		return false;
	}
	return stored_value.compare(0, 4, "RExL") == 0;
}

bool DuckLakeEncryptionProvider::IsBase64(const string &value) {
	if (value.empty()) {
		return false;
	}
	for (auto character : value) {
		auto c = static_cast<unsigned char>(character);
		if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '+' || c == '/' ||
		    c == '=') {
			continue;
		}
		return false;
	}
	return true;
}

void DuckLakeEncryptionProvider::RegisterFactory(Factory factory) {
	if (global_factory) {
		throw InvalidInputException("a DuckLake encryption provider factory is already registered - registering a "
		                            "second one would make the provider a lake gets depend on extension load order");
	}
	global_factory = new Factory(std::move(factory));
}

bool DuckLakeEncryptionProvider::HasFactory() {
	return global_factory != nullptr;
}

const DuckLakeEncryptionProvider::Factory &DuckLakeEncryptionProvider::GetFactory() {
	static Factory empty;
	if (!global_factory) {
		return empty;
	}
	return *global_factory;
}

} // namespace duckdb
