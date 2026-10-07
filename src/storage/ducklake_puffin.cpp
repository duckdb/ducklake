#include "storage/ducklake_puffin.hpp"

#include "storage/ducklake_deletion_vector.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/json_document.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

// https://iceberg.apache.org/puffin-spec/
// File structure: Magic Blob1 ... BlobN Footer
// Footer structure: Magic | FooterPayload | FooterPayloadSize (4, LE) | Flags (4) | Magic
static constexpr const data_t PUFFIN_MAGIC[4] = {'P', 'F', 'A', '1'};
static constexpr idx_t PUFFIN_MAGIC_SIZE = 4;
static constexpr idx_t PUFFIN_FOOTER_SIZE_FIELD_SIZE = 4;
static constexpr idx_t PUFFIN_FOOTER_FLAGS_SIZE = 4;
// FooterPayloadSize + Flags + trailing Magic
static constexpr idx_t PUFFIN_FOOTER_TAIL_SIZE =
    PUFFIN_FOOTER_SIZE_FIELD_SIZE + PUFFIN_FOOTER_FLAGS_SIZE + PUFFIN_MAGIC_SIZE;
// Full footer overhead excluding the payload
static constexpr idx_t PUFFIN_FOOTER_STRUCT_SIZE = PUFFIN_MAGIC_SIZE + PUFFIN_FOOTER_TAIL_SIZE;
static constexpr idx_t PUFFIN_MIN_FILE_SIZE = PUFFIN_MAGIC_SIZE + PUFFIN_FOOTER_STRUCT_SIZE;
// Flags byte 0, bit 0: FooterPayload is LZ4-compressed
static constexpr uint32_t PUFFIN_FOOTER_COMPRESSED_FLAG = 1;
static constexpr const char *DELETION_VECTOR_BLOB_TYPE = "deletion-vector-v1";
static constexpr const char *DUCKLAKE_SNAPSHOT_PROPERTY = "ducklake-snapshot-id";

//===--------------------------------------------------------------------===//
// Writer
//===--------------------------------------------------------------------===//
static void AppendMagic(vector<data_t> &file_data) {
	file_data.insert(file_data.end(), PUFFIN_MAGIC, PUFFIN_MAGIC + PUFFIN_MAGIC_SIZE);
}

// Blobs written back to back
static vector<DuckLakePuffinBlob> AppendBlobs(vector<data_t> &file_data,
                                              const vector<DuckLakePuffinWriter::BlobInput> &blobs) {
	vector<DuckLakePuffinBlob> blob_infos;
	for (auto &blob : blobs) {
		auto blob_data = DuckLakeDeletionVectorData::ToBlob(*blob.positions);
		DuckLakePuffinBlob info;
		info.snapshot_id = blob.snapshot_id;
		info.offset = file_data.size();
		info.length = blob_data.size();
		blob_infos.push_back(info);
		file_data.insert(file_data.end(), blob_data.begin(), blob_data.end());
	}
	return blob_infos;
}

// BlobMetadata: type, fields, snapshot-id, sequence-number, offset, length, properties
// snapshot-id/sequence-number must be -1 for deletion vectors
static JSONMutableValue CreateBlobMetadata(JSONWriter &writer, const DuckLakePuffinWriter::BlobInput &input,
                                           const DuckLakePuffinBlob &info, const string &data_file_path) {
	auto blob_obj = writer.CreateObject();
	blob_obj.AddString("type", DELETION_VECTOR_BLOB_TYPE);
	blob_obj.Add("fields", writer.CreateArray());
	blob_obj.Add("snapshot-id", writer.CreateSignedInteger(-1));
	blob_obj.Add("sequence-number", writer.CreateSignedInteger(-1));
	blob_obj.Add("offset", writer.CreateUnsignedInteger(info.offset));
	blob_obj.Add("length", writer.CreateUnsignedInteger(info.length));
	auto properties = writer.CreateObject();
	properties.AddString("referenced-data-file", data_file_path);
	properties.AddString("cardinality", to_string(input.positions->size()));
	if (input.snapshot_id.IsValid()) {
		properties.AddString(DUCKLAKE_SNAPSHOT_PROPERTY, to_string(input.snapshot_id.GetIndex()));
	}
	blob_obj.Add("properties", properties);
	return blob_obj;
}

// FileMetadata payload: blobs (required) and properties (optional)
static string WriteFooterPayload(const vector<DuckLakePuffinWriter::BlobInput> &blobs,
                                 const vector<DuckLakePuffinBlob> &blob_infos, const string &data_file_path) {
	JSONWriter writer;
	auto blob_arr = writer.CreateArray();
	for (idx_t blob_idx = 0; blob_idx < blobs.size(); blob_idx++) {
		blob_arr.Append(CreateBlobMetadata(writer, blobs[blob_idx], blob_infos[blob_idx], data_file_path));
	}
	auto file_properties = writer.CreateObject();
	file_properties.AddString("created-by", "ducklake");
	auto root = writer.CreateObject();
	root.Add("blobs", blob_arr);
	root.Add("properties", file_properties);
	writer.SetRoot(root);
	return writer.ToString();
}

// Footer: Magic | FooterPayload | FooterPayloadSize (little-endian) | Flags (all zero) | Magic
static void AppendFooter(vector<data_t> &file_data, const string &payload) {
	AppendMagic(file_data);
	file_data.insert(file_data.end(), payload.begin(), payload.end());
	data_t footer_tail[PUFFIN_FOOTER_SIZE_FIELD_SIZE + PUFFIN_FOOTER_FLAGS_SIZE] = {};
	Store<uint32_t>(NumericCast<uint32_t>(payload.size()), footer_tail);
	file_data.insert(file_data.end(), footer_tail, footer_tail + sizeof(footer_tail));
	AppendMagic(file_data);
}

// A single deletion vector is written as a bare deletion-vector-v1 blob (no puffin container/footer),
// matching how Iceberg stores deletion vectors.
DuckLakePuffinWriteResult DuckLakePuffinWriter::Write(FileSystem &fs, const string &path, const string &data_file_path,
                                                      const vector<BlobInput> &blobs) {
	vector<data_t> file_data;
	DuckLakePuffinWriteResult result;
	if (blobs.size() == 1) {
		// bare blob, no footer
		file_data = DuckLakeDeletionVectorData::ToBlob(*blobs[0].positions);
		result.delete_count = blobs[0].positions->size();
		result.footer_size = 0;
	} else {
		// File structure: Magic Blob1 ... BlobN Footer
		AppendMagic(file_data);
		auto blob_infos = AppendBlobs(file_data, blobs);
		auto payload = WriteFooterPayload(blobs, blob_infos, data_file_path);
		AppendFooter(file_data, payload);
		// blobs are cumulative, the latest blob holds the full deletion vector
		for (auto &blob : blobs) {
			result.delete_count = MaxValue<idx_t>(result.delete_count, blob.positions->size());
		}
		result.footer_size = payload.size() + PUFFIN_FOOTER_STRUCT_SIZE;
	}

	auto file_handle = fs.OpenFile(path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	file_handle->Write(file_data.data(), file_data.size());
	file_handle->Close();

	result.file_size_bytes = file_data.size();
	return result;
}

//===--------------------------------------------------------------------===//
// Reader
//===--------------------------------------------------------------------===//
static idx_t ParseSnapshotProperty(const string &value, const string &path) {
	idx_t parsed;
	if (!TryCast::Operation<string_t, idx_t>(string_t(value), parsed, true)) {
		throw InvalidInputException("Puffin file \"%s\" is corrupt - invalid %s property \"%s\"", path,
		                            DUCKLAKE_SNAPSHOT_PROPERTY, value);
	}
	return parsed;
}

// A bare deletion-vector-v1 blob is a 4-byte length prefix followed by the blob magic
static bool IsBareDeletionVector(data_ptr_t data, idx_t size) {
	auto &magic = DuckLakeDeletionVectorData::DELETION_VECTOR_MAGIC;
	return size >= sizeof(uint32_t) + sizeof(magic) && memcmp(data + sizeof(uint32_t), magic, sizeof(magic)) == 0;
}

// Footer: Magic | FooterPayload | FooterPayloadSize (little-endian) | Flags | Magic
// Returns the offset at which FooterPayload begins
static idx_t ValidateFooter(data_ptr_t data, idx_t size, const string &path) {
	if (memcmp(data + size - PUFFIN_MAGIC_SIZE, PUFFIN_MAGIC, PUFFIN_MAGIC_SIZE) != 0) {
		throw InvalidInputException("Puffin file \"%s\" is corrupt - trailing magic mismatch", path);
	}
	auto flags = Load<uint32_t>(data + size - PUFFIN_MAGIC_SIZE - PUFFIN_FOOTER_FLAGS_SIZE);
	if (flags & PUFFIN_FOOTER_COMPRESSED_FLAG) {
		throw InvalidInputException("Puffin file \"%s\" has a compressed footer, which is not supported", path);
	}
	if (flags != 0) {
		throw InvalidInputException("Puffin file \"%s\" has unsupported footer flags", path);
	}
	idx_t payload_size = Load<uint32_t>(data + size - PUFFIN_FOOTER_TAIL_SIZE);
	if (payload_size > size - PUFFIN_MIN_FILE_SIZE) {
		throw InvalidInputException("Puffin file \"%s\" is corrupt - footer payload size out of range", path);
	}
	auto payload_start = size - PUFFIN_FOOTER_TAIL_SIZE - payload_size;
	if (memcmp(data + payload_start - PUFFIN_MAGIC_SIZE, PUFFIN_MAGIC, PUFFIN_MAGIC_SIZE) != 0) {
		throw InvalidInputException("Puffin file \"%s\" is corrupt - footer magic mismatch", path);
	}
	return payload_start;
}

// BlobMetadata: type, fields, snapshot-id, sequence-number, offset, length, properties
// Returns false for blob types we do not know
static bool TryParseBlobMetadata(const JSONValue &blob_val, idx_t blob_section_end, const string &path,
                                 DuckLakePuffinBlob &blob) {
	auto type_val = blob_val.GetMember("type");
	if (!type_val.IsString()) {
		throw InvalidInputException("Puffin file \"%s\" is corrupt - blob without a type", path);
	}
	if (type_val.GetString() != DELETION_VECTOR_BLOB_TYPE) {
		return false;
	}
	auto offset_val = blob_val.GetMember("offset");
	auto length_val = blob_val.GetMember("length");
	if (!offset_val.IsInteger() || !length_val.IsInteger()) {
		throw InvalidInputException("Puffin file \"%s\" is corrupt - blob without offset/length", path);
	}
	auto raw_offset = offset_val.GetSignedInteger();
	auto raw_length = length_val.GetSignedInteger();
	if (raw_offset < 0 || raw_length < 0) {
		throw InvalidInputException("Puffin file \"%s\" is corrupt - blob offset/length out of range", path);
	}
	blob.offset = NumericCast<idx_t>(raw_offset);
	blob.length = NumericCast<idx_t>(raw_length);
	if (blob.offset < PUFFIN_MAGIC_SIZE || blob.length > blob_section_end ||
	    blob.offset > blob_section_end - blob.length) {
		throw InvalidInputException("Puffin file \"%s\" is corrupt - blob offset/length out of range", path);
	}
	auto snapshot_val = blob_val.GetMember("properties").GetMember(DUCKLAKE_SNAPSHOT_PROPERTY);
	if (snapshot_val.IsString()) {
		blob.snapshot_id = ParseSnapshotProperty(snapshot_val.GetString(), path);
	}
	return true;
}

// FileMetadata payload: blobs (required) and properties (optional)
static vector<DuckLakePuffinBlob> ParseFileMetadata(data_ptr_t payload, idx_t payload_size, idx_t blob_section_end,
                                                    const string &path) {
	JSONParseError error;
	auto doc = JSONDocument::TryParse(const_char_ptr_cast(payload), payload_size, error);
	if (!doc) {
		throw InvalidInputException("Puffin file \"%s\" is corrupt - failed to parse footer payload", path);
	}
	auto blobs_val = doc->GetRoot().GetMember("blobs");
	if (!blobs_val.IsArray()) {
		throw InvalidInputException("Puffin file \"%s\" is corrupt - footer has no \"blobs\" list", path);
	}
	vector<DuckLakePuffinBlob> result;
	blobs_val.IterateArray([&](JSONValue blob_val) {
		DuckLakePuffinBlob blob;
		if (TryParseBlobMetadata(blob_val, blob_section_end, path, blob)) {
			result.push_back(blob);
		}
	});
	return result;
}

DuckLakePuffinReader::DuckLakePuffinReader(data_ptr_t data, idx_t size, const string &path)
    : data(data), size(size), path(path) {
	ParseFooter();
}

// Puffin container: Magic Blob1 ... BlobN Footer, or a single bare deletion-vector-v1 blob
void DuckLakePuffinReader::ParseFooter() {
	if (size >= PUFFIN_MIN_FILE_SIZE && memcmp(data, PUFFIN_MAGIC, PUFFIN_MAGIC_SIZE) == 0) {
		auto payload_start = ValidateFooter(data, size, path);
		auto payload_size = size - PUFFIN_FOOTER_TAIL_SIZE - payload_start;
		auto blob_section_end = payload_start - PUFFIN_MAGIC_SIZE;
		blobs = ParseFileMetadata(data + payload_start, payload_size, blob_section_end, path);
		return;
	}
	if (IsBareDeletionVector(data, size)) {
		// a single snapshot-less blob spanning the whole file, its snapshot is the metadata begin_snapshot
		DuckLakePuffinBlob blob;
		blob.offset = 0;
		blob.length = size;
		blobs.push_back(blob);
		return;
	}
	throw InvalidInputException("File \"%s\" is not a valid deletion vector - magic mismatch", path);
}

unique_ptr<DuckLakeDeletionVectorData> DuckLakePuffinReader::DecodeBlob(const DuckLakePuffinBlob &blob) const {
	return DuckLakeDeletionVectorData::FromBlob(data + blob.offset, blob.length);
}

} // namespace duckdb
