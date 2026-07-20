#include "storage/ducklake_deletion_vector.hpp"

#include "duckdb/common/bswap.hpp"

namespace duckdb {

namespace {

class CRC32 {
public:
	CRC32() : crc(0xFFFFFFFF) {
		InitTable();
	}

public:
	static void InitTable() {
		if (table_initialized)
			return;

		for (uint32_t i = 0; i < 256; i++) {
			uint32_t c = i;
			for (int j = 0; j < 8; j++) {
				if (c & 1) {
					c = 0xEDB88320 ^ (c >> 1);
				} else {
					c = c >> 1;
				}
			}
			crc_table[i] = c;
		}
		table_initialized = true;
	}

public:
	void Update(const data_t *data, idx_t length) {
		for (idx_t i = 0; i < length; i++) {
			crc = crc_table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
		}
	}

	uint32_t GetValue() const {
		return crc ^ 0xFFFFFFFF;
	}

private:
	uint32_t crc;
	static uint32_t crc_table[256];
	static bool table_initialized;
};

uint32_t CRC32::crc_table[256];
bool CRC32::table_initialized = false;

} // namespace

unique_ptr<DuckLakeDeletionVectorData> DuckLakeDeletionVectorData::FromBlob(data_ptr_t blob_start, idx_t blob_length) {
	//! https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type

	if (blob_length < 12) {
		throw InvalidInputException("Blob is too small (length of %d bytes) to be a deletion-vector-v1", blob_length);
	}

	auto blob_end = blob_start + blob_length;
	auto vector_size = Load<uint32_t>(blob_start);
	vector_size = BSwap(vector_size);
	blob_start += sizeof(uint32_t);
	if (blob_start >= blob_end) {
		throw InvalidInputException("Deletion vector blob truncated after vector_size field");
	}

	auto checksummed_data_start = blob_start;
	auto memcmp_res = memcmp(DELETION_VECTOR_MAGIC, blob_start, 4);
	blob_start += 4;
	if (vector_size < 4) {
		throw InvalidInputException("Deletion vector vector_size too small for magic bytes");
	}
	vector_size -= 4;
	if (blob_start >= blob_end) {
		throw InvalidInputException("Deletion vector blob truncated after magic bytes");
	}

	if (memcmp_res != 0) {
		throw InvalidInputException("Magic bytes mismatch, deletion vector is corrupt!");
	}

	if (vector_size < sizeof(int64_t)) {
		throw InvalidInputException("Deletion vector vector_size too small for bitmap count");
	}
	int64_t amount_of_bitmaps = Load<int64_t>(blob_start);
	blob_start += sizeof(int64_t);
	vector_size -= sizeof(int64_t);
	if (blob_start >= blob_end) {
		throw InvalidInputException("Deletion vector blob truncated after bitmap count");
	}
	if (amount_of_bitmaps < 0) {
		throw InvalidInputException("Deletion vector has negative bitmap count: %lld", amount_of_bitmaps);
	}

	auto result = make_uniq<DuckLakeDeletionVectorData>();
	result->bitmaps.reserve(NumericCast<idx_t>(amount_of_bitmaps));
	for (int64_t i = 0; i < amount_of_bitmaps; i++) {
		if (vector_size < sizeof(int32_t)) {
			throw InvalidInputException("Deletion vector vector_size too small for bitmap key at index %lld", i);
		}
		auto key = Load<int32_t>(blob_start);
		blob_start += sizeof(int32_t);
		vector_size -= sizeof(int32_t);
		if (blob_start >= blob_end) {
			throw InvalidInputException("Deletion vector blob truncated at bitmap key %lld", i);
		}

		size_t bitmap_size =
		    roaring::api::roaring_bitmap_portable_deserialize_size((const char *)blob_start, vector_size);
		if (bitmap_size > vector_size) {
			throw InvalidInputException("Deletion vector bitmap %lld exceeds remaining data", i);
		}
		auto bitmap = roaring::Roaring::readSafe((const char *)blob_start, bitmap_size);
		blob_start += bitmap_size;
		vector_size -= bitmap_size;
		if (blob_start >= blob_end) {
			throw InvalidInputException("Deletion vector blob truncated after bitmap %lld", i);
		}
		result->bitmaps.emplace(key, std::move(bitmap));
	}

	//! Compute and compare the checksum
	auto checksummed_data_length = blob_start - checksummed_data_start;
	if (blob_start + sizeof(uint32_t) > blob_end) {
		throw InvalidInputException("Deletion vector blob truncated before checksum");
	}
	auto stored_checksum = BSwap(Load<uint32_t>(blob_start));
	blob_start += sizeof(uint32_t);
	if (blob_start != blob_end) {
		throw InvalidInputException("Deletion vector blob has %lld unexpected trailing bytes",
		                            static_cast<int64_t>(blob_end - blob_start));
	}

	CRC32 crc;
	crc.Update(checksummed_data_start, checksummed_data_length);
	uint32_t checksum = crc.GetValue();
	if (checksum != stored_checksum) {
		throw InvalidInputException(
		    "Stored checksum (%d) does not match computed checksum (%d), the DeletionVector is corrupted",
		    stored_checksum, checksum);
	}
	return result;
}

vector<data_t> DuckLakeDeletionVectorData::ToBlob(const set<idx_t> &positions) {
	//! https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type

	// Group row positions by high 32 bits into roaring bitmaps
	unordered_map<int32_t, roaring::Roaring> bitmaps;
	for (auto row_idx : positions) {
		int32_t high_bits = static_cast<int32_t>(static_cast<int64_t>(row_idx) >> 32);
		uint32_t low_bits = static_cast<uint32_t>(row_idx & 0xFFFFFFFF);
		bitmaps[high_bits].add(low_bits);
	}

	// Calculate total size needed
	idx_t total_size = 0;
	total_size += sizeof(uint32_t); // vector_size field
	total_size += sizeof(uint32_t); // magic bytes
	total_size += sizeof(uint64_t); // amount of bitmaps
	for (const auto &entry : bitmaps) {
		total_size += sizeof(int32_t);                   // key
		total_size += entry.second.getSizeInBytes(true); // portable serialized bitmap
	}
	total_size += sizeof(uint32_t); // CRC checksum

	vector<data_t> blob_output;
	blob_output.resize(total_size);
	data_ptr_t blob_ptr = blob_output.data();

	// Write vector_size (total_size - (CRC checksum + vector_size field))
	uint32_t vector_size = BSwap(static_cast<uint32_t>(total_size - sizeof(uint32_t) - sizeof(uint32_t)));
	Store<uint32_t>(vector_size, blob_ptr);
	blob_ptr += sizeof(uint32_t);

	auto checksummed_data_start = blob_ptr;
	memcpy(blob_ptr, DELETION_VECTOR_MAGIC, 4);
	blob_ptr += sizeof(uint32_t);

	// Write bitmap count
	Store<uint64_t>(bitmaps.size(), blob_ptr);
	blob_ptr += sizeof(uint64_t);

	// Write each bitmap
	for (const auto &entry : bitmaps) {
		// Write key (high 32 bits)
		Store<int32_t>(entry.first, blob_ptr);
		blob_ptr += sizeof(int32_t);

		// Write bitmap (portable format)
		size_t bitmap_size = entry.second.write((char *)blob_ptr, true);
		blob_ptr += bitmap_size;
	}

	// Compute and write CRC checksum
	auto checksummed_data_length = blob_ptr - checksummed_data_start;
	CRC32 crc;
	crc.Update(checksummed_data_start, checksummed_data_length);
	uint32_t checksum = crc.GetValue();

	Store<uint32_t>(BSwap(checksum), blob_ptr);
	return blob_output;
}

namespace {

struct RoaringIterateContext {
	set<idx_t> *out;
	idx_t high;
};

} // namespace

void DuckLakeDeletionVectorData::ToSet(set<idx_t> &out) const {
	for (auto &entry : bitmaps) {
		RoaringIterateContext ctx {&out, static_cast<idx_t>(entry.first)};
		auto &bitmap = entry.second;

		bitmap.iterate(
		    [](uint32_t value, void *ptr) -> bool {
			    auto *ctx = static_cast<RoaringIterateContext *>(ptr);
			    idx_t full_value = (ctx->high << 32) | static_cast<idx_t>(value);
			    ctx->out->insert(full_value);
			    return true;
		    },
		    &ctx);
	}
}

} // namespace duckdb
