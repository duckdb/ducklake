#include "storage/ducklake_deletion_vector.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/operator/numeric_cast.hpp"

#include "miniz.hpp"

namespace duckdb {

static constexpr idx_t DELETION_VECTOR_MAGIC_SIZE = sizeof(DuckLakeDeletionVectorData::DELETION_VECTOR_MAGIC);

static uint32_t ComputeChecksum(const_data_ptr_t data, idx_t size) {
	return static_cast<uint32_t>(duckdb_miniz::mz_crc32(MZ_CRC32_INIT, data, size));
}

unique_ptr<DuckLakeDeletionVectorData> DuckLakeDeletionVectorData::FromBlob(data_ptr_t blob_start, idx_t blob_length) {
	//! https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type
	if (blob_length < 12) {
		throw InvalidInputException("Blob is too small (length of %d bytes) to be a deletion-vector-v1", blob_length);
	}
	idx_t vector_size = BSwapIfLE(Load<uint32_t>(blob_start));
	auto checksummed_data = blob_start + sizeof(uint32_t);
	if (vector_size < DELETION_VECTOR_MAGIC_SIZE) {
		throw InvalidInputException("Deletion vector vector_size too small for magic bytes");
	}
	if (memcmp(DELETION_VECTOR_MAGIC, checksummed_data, DELETION_VECTOR_MAGIC_SIZE) != 0) {
		throw InvalidInputException("Magic bytes mismatch, deletion vector is corrupt!");
	}
	auto expected_length = sizeof(uint32_t) + vector_size + sizeof(uint32_t);
	if (blob_length < expected_length) {
		throw InvalidInputException("Deletion vector blob truncated before checksum");
	}
	if (blob_length > expected_length) {
		throw InvalidInputException("Deletion vector blob has %lld unexpected trailing bytes",
		                            NumericCast<int64_t>(blob_length - expected_length));
	}
	auto stored_checksum = BSwapIfLE(Load<uint32_t>(checksummed_data + vector_size));
	auto checksum = ComputeChecksum(checksummed_data, vector_size);
	if (checksum != stored_checksum) {
		throw InvalidInputException(
		    "Stored checksum (%d) does not match computed checksum (%d), the DeletionVector is corrupted",
		    stored_checksum, checksum);
	}

	auto bitmap_data = const_char_ptr_cast(checksummed_data + DELETION_VECTOR_MAGIC_SIZE);
	auto bitmap_size = vector_size - DELETION_VECTOR_MAGIC_SIZE;
	if (bitmap_size < sizeof(uint64_t)) {
		throw InvalidInputException("Deletion vector vector_size too small for bitmap count");
	}
	auto result = make_uniq<DuckLakeDeletionVectorData>();
	try {
		result->bitmap = roaring::Roaring64Map::readSafe(bitmap_data, bitmap_size);
	} catch (std::exception &ex) {
		throw InvalidInputException("Deletion vector bitmap is corrupt: %s", ex.what());
	}
	if (result->bitmap.getSizeInBytes(true) != bitmap_size) {
		throw InvalidInputException("Deletion vector bitmap does not span vector_size");
	}
	return result;
}

vector<data_t> DuckLakeDeletionVectorData::ToBlob(const set<idx_t> &positions) {
	//! https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type
	roaring::Roaring64Map bitmap;
	for (auto position : positions) {
		bitmap.add(static_cast<uint64_t>(position));
	}
	idx_t vector_size = DELETION_VECTOR_MAGIC_SIZE + bitmap.getSizeInBytes(true);
	vector<data_t> blob_output(sizeof(uint32_t) + vector_size + sizeof(uint32_t));

	auto blob_ptr = blob_output.data();
	Store<uint32_t>(BSwapIfLE(NumericCast<uint32_t>(vector_size)), blob_ptr);
	auto checksummed_data = blob_ptr + sizeof(uint32_t);
	memcpy(checksummed_data, DELETION_VECTOR_MAGIC, DELETION_VECTOR_MAGIC_SIZE);
	// Iceberg requires keys in ascending order
	bitmap.write(char_ptr_cast(checksummed_data + DELETION_VECTOR_MAGIC_SIZE), true);
	Store<uint32_t>(BSwapIfLE(ComputeChecksum(checksummed_data, vector_size)), checksummed_data + vector_size);
	return blob_output;
}

} // namespace duckdb
