#include "rest/ducklake_rest_arrow_ipc.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"

#include <cstring>

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// FlatBuffers
//===--------------------------------------------------------------------===//
//! Builds a flatbuffer back to front, as the FlatBuffers library does. Only the parts that Arrow messages use
class FlatBufferBuilder {
public:
	//! The position of an object, as its distance to the end of the buffer
	typedef uint32_t Offset;

	idx_t Size() const {
		return used;
	}

	string Finish(Offset root) {
		PreAlign(4, min_align);
		Align(4);
		Push<uint32_t>(Size() - root + 4);
		return string(reinterpret_cast<const char *>(storage.data() + storage.size() - used), used);
	}

	Offset CreateString(const string &value) {
		PreAlign(value.size() + 1, 4);
		Fill(1);
		PushBytes(value.data(), value.size());
		Push<uint32_t>(NumericCast<uint32_t>(value.size()));
		return Size();
	}

	//! A vector of offsets
	Offset CreateOffsetVector(const vector<Offset> &values) {
		StartVector(values.size(), 4, 4);
		for (idx_t i = values.size(); i > 0; i--) {
			Align(4);
			Push<uint32_t>(Size() - values[i - 1] + 4);
		}
		return EndVector(values.size());
	}

	//! A vector of structures of 16 bytes made of two 64 bit integers (FieldNode, Buffer)
	Offset CreatePairVector(const vector<pair<int64_t, int64_t>> &values) {
		StartVector(values.size(), 16, 8);
		for (idx_t i = values.size(); i > 0; i--) {
			Align(8);
			Push<int64_t>(values[i - 1].second);
			Push<int64_t>(values[i - 1].first);
		}
		return EndVector(values.size());
	}

	void StartTable() {
		D_ASSERT(!in_table);
		in_table = true;
		table_start = Size();
		fields.clear();
		max_voffset = 0;
	}

	template <class T>
	void AddScalar(idx_t slot, T value, T default_value) {
		if (value == default_value) {
			return;
		}
		Align(sizeof(T));
		Push<T>(value);
		TrackField(slot);
	}

	void AddOffset(idx_t slot, Offset value) {
		if (!value) {
			return;
		}
		Align(4);
		Push<uint32_t>(Size() - value + 4);
		TrackField(slot);
	}

	Offset EndTable() {
		D_ASSERT(in_table);
		in_table = false;
		Align(4);
		Push<int32_t>(0);
		Offset table_location = Size();
		max_voffset = MaxValue<uint16_t>(NumericCast<uint16_t>(max_voffset + 2), 4);
		Fill(max_voffset);
		auto vtable = storage.data() + storage.size() - used;
		WriteAt<uint16_t>(vtable + 2, NumericCast<uint16_t>(table_location - table_start));
		WriteAt<uint16_t>(vtable, max_voffset);
		for (auto &field : fields) {
			WriteAt<uint16_t>(vtable + field.second, NumericCast<uint16_t>(table_location - field.first));
		}
		auto vtable_location = Size();
		WriteAt<int32_t>(storage.data() + storage.size() - table_location,
		                 NumericCast<int32_t>(vtable_location) - NumericCast<int32_t>(table_location));
		return table_location;
	}

private:
	void StartVector(idx_t count, idx_t element_size, idx_t alignment) {
		PreAlign(count * element_size, 4);
		PreAlign(count * element_size, alignment);
	}

	Offset EndVector(idx_t count) {
		Push<uint32_t>(NumericCast<uint32_t>(count));
		return Size();
	}

	void TrackField(idx_t slot) {
		auto id = NumericCast<uint16_t>(4 + slot * 2);
		fields.emplace_back(Size(), id);
		max_voffset = MaxValue<uint16_t>(max_voffset, id);
	}

	void Reserve(idx_t extra) {
		if (storage.size() - used >= extra) {
			return;
		}
		auto new_size = MaxValue<idx_t>(storage.size() * 2, used + extra + 64);
		vector<uint8_t> grown(new_size, 0);
		if (used > 0) {
			memcpy(grown.data() + new_size - used, storage.data() + storage.size() - used, used);
		}
		storage = std::move(grown);
	}

	void Fill(idx_t count) {
		Reserve(count);
		used += count;
		memset(storage.data() + storage.size() - used, 0, count);
	}

	void PushBytes(const void *data, idx_t count) {
		Reserve(count);
		used += count;
		if (count > 0) {
			memcpy(storage.data() + storage.size() - used, data, count);
		}
	}

	template <class T>
	void Push(T value) {
		PushBytes(&value, sizeof(T));
	}

	template <class T>
	static void WriteAt(uint8_t *position, T value) {
		memcpy(position, &value, sizeof(T));
	}

	static idx_t Padding(idx_t size, idx_t alignment) {
		return ((~size) + 1) & (alignment - 1);
	}

	void Align(idx_t alignment) {
		min_align = MaxValue<idx_t>(min_align, alignment);
		Fill(Padding(used, alignment));
	}

	void PreAlign(idx_t length, idx_t alignment) {
		if (length == 0) {
			return;
		}
		min_align = MaxValue<idx_t>(min_align, alignment);
		Fill(Padding(used + length, alignment));
	}

private:
	vector<uint8_t> storage = vector<uint8_t>(1024, 0);
	idx_t used = 0;
	idx_t min_align = 1;
	bool in_table = false;
	idx_t table_start = 0;
	uint16_t max_voffset = 0;
	//! The location of the fields of the table that is built, and their position in the vtable
	vector<pair<Offset, uint16_t>> fields;
};

//! Reads a flatbuffer, with checks for malformed buffers
class FlatBufferReader {
public:
	FlatBufferReader(const uint8_t *data, idx_t size) : data(data), size(size) {
	}

	template <class T>
	T Read(idx_t position) const {
		Check(position, sizeof(T));
		T result;
		memcpy(&result, data + position, sizeof(T));
		return result;
	}

	void Check(idx_t position, idx_t length) const {
		if (position > size || length > size - position) {
			throw InvalidInputException("Failed to read the Arrow IPC stream: the metadata is corrupt");
		}
	}

	//! The table that the root offset refers to
	idx_t Root() const {
		return Read<uint32_t>(0);
	}

	const uint8_t *data;
	idx_t size;
};

//! A table of a flatbuffer
class FlatTable {
public:
	FlatTable(const FlatBufferReader &reader, idx_t position) : reader(reader), position(position) {
		auto offset = reader.Read<int32_t>(position);
		vtable = NumericCast<idx_t>(NumericCast<int64_t>(position) - offset);
		vtable_size = reader.Read<uint16_t>(vtable);
	}

	template <class T>
	T Get(idx_t slot, T default_value) const {
		auto field = FieldPosition(slot);
		return field ? reader.Read<T>(field) : default_value;
	}

	bool Has(idx_t slot) const {
		return FieldPosition(slot) != 0;
	}

	FlatTable GetTable(idx_t slot) const {
		auto field = FieldPosition(slot);
		if (!field) {
			throw InvalidInputException("Failed to read the Arrow IPC stream: a field of the metadata is missing");
		}
		return FlatTable(reader, field + reader.Read<uint32_t>(field));
	}

	string GetString(idx_t slot) const {
		auto field = FieldPosition(slot);
		if (!field) {
			return string();
		}
		auto string_position = field + reader.Read<uint32_t>(field);
		auto length = reader.Read<uint32_t>(string_position);
		reader.Check(string_position + 4, length);
		return string(reinterpret_cast<const char *>(reader.data + string_position + 4), length);
	}

	//! The number of elements of a vector field and the position of its first element
	idx_t GetVector(idx_t slot, idx_t &first) const {
		auto field = FieldPosition(slot);
		if (!field) {
			first = 0;
			return 0;
		}
		auto vector_position = field + reader.Read<uint32_t>(field);
		first = vector_position + 4;
		return reader.Read<uint32_t>(vector_position);
	}

	FlatTable GetTableAt(idx_t element_position) const {
		return FlatTable(reader, element_position + reader.Read<uint32_t>(element_position));
	}

private:
	idx_t FieldPosition(idx_t slot) const {
		auto entry = 4 + slot * 2;
		if (entry + 2 > vtable_size) {
			return 0;
		}
		auto offset = reader.Read<uint16_t>(vtable + entry);
		return offset ? position + offset : 0;
	}

private:
	const FlatBufferReader &reader;
	idx_t position;
	idx_t vtable;
	idx_t vtable_size;
};

//===--------------------------------------------------------------------===//
// Arrow IPC message structure
//===--------------------------------------------------------------------===//
//! The union of the types of a field
enum class ArrowIPCType : uint8_t {
	NONE = 0,
	NULL_TYPE = 1,
	INT = 2,
	FLOATING_POINT = 3,
	BINARY = 4,
	UTF8 = 5,
	BOOL = 6,
	DECIMAL = 7,
	DATE = 8,
	TIME = 9,
	TIMESTAMP = 10,
	INTERVAL = 11,
	LIST = 12,
	STRUCT = 13,
	FIXED_SIZE_BINARY = 15,
	FIXED_SIZE_LIST = 16,
	MAP = 17,
	LARGE_BINARY = 19,
	LARGE_UTF8 = 20,
	LARGE_LIST = 21
};

enum class ArrowIPCHeader : uint8_t { NONE = 0, SCHEMA = 1, DICTIONARY_BATCH = 2, RECORD_BATCH = 3 };

constexpr int16_t ARROW_METADATA_VERSION_V5 = 4;
constexpr uint32_t ARROW_CONTINUATION = 0xFFFFFFFF;

//! The buffers of the array with the format
idx_t ArrowBufferCount(const string &format) {
	if (format == "n") {
		return 0;
	}
	if (format == "+s" || StringUtil::StartsWith(format, "+w:")) {
		return 1;
	}
	if (format == "+l" || format == "+L" || format == "+m") {
		return 2;
	}
	if (format == "u" || format == "U" || format == "z" || format == "Z") {
		return 3;
	}
	return 2;
}

//! The width of the values of a fixed width format, in bytes, 0 for bits
idx_t ArrowValueWidth(const string &format) {
	if (format.empty()) {
		throw InvalidInputException("Arrow IPC: empty format");
	}
	if (format == "b") {
		return 0;
	}
	switch (format[0]) {
	case 'c':
	case 'C':
		return 1;
	case 's':
	case 'S':
		return 2;
	case 'i':
	case 'I':
	case 'f':
		return 4;
	case 'l':
	case 'L':
	case 'g':
		return 8;
	default:
		break;
	}
	if (format == "tdD" || format == "tts" || format == "ttm" || format == "tiM") {
		return 4;
	}
	if (format == "tdm" || format == "ttu" || format == "ttn" || format == "tiD") {
		return 8;
	}
	if (StringUtil::StartsWith(format, "ts")) {
		return 8;
	}
	if (format == "tin") {
		return 16;
	}
	if (StringUtil::StartsWith(format, "d:")) {
		auto parts = StringUtil::Split(format.substr(2), ',');
		return parts.size() > 2 ? std::stoull(parts[2]) / 8 : 16;
	}
	if (StringUtil::StartsWith(format, "w:")) {
		return std::stoull(format.substr(2));
	}
	throw NotImplementedException("Arrow IPC: the format \"%s\" is not supported", format);
}

//===--------------------------------------------------------------------===//
// Writer
//===--------------------------------------------------------------------===//
struct ArrowIPCWriter {
	typedef FlatBufferBuilder::Offset Offset;

	static vector<pair<string, string>> ReadMetadata(const char *metadata) {
		vector<pair<string, string>> result;
		if (!metadata) {
			return result;
		}
		int32_t count;
		memcpy(&count, metadata, 4);
		metadata += 4;
		for (int32_t i = 0; i < count; i++) {
			int32_t length;
			memcpy(&length, metadata, 4);
			metadata += 4;
			string key(metadata, NumericCast<idx_t>(length));
			metadata += length;
			memcpy(&length, metadata, 4);
			metadata += 4;
			string value(metadata, NumericCast<idx_t>(length));
			metadata += length;
			result.emplace_back(std::move(key), std::move(value));
		}
		return result;
	}

	//! Builds the type table of a field, returns the tag of the union and the offset of the table
	static pair<ArrowIPCType, Offset> CreateType(FlatBufferBuilder &builder, const string &format) {
		auto empty_table = [&]() {
			builder.StartTable();
			return builder.EndTable();
		};
		if (format == "n") {
			return {ArrowIPCType::NULL_TYPE, empty_table()};
		}
		if (format == "b") {
			return {ArrowIPCType::BOOL, empty_table()};
		}
		if (format == "u") {
			return {ArrowIPCType::UTF8, empty_table()};
		}
		if (format == "U") {
			return {ArrowIPCType::LARGE_UTF8, empty_table()};
		}
		if (format == "z") {
			return {ArrowIPCType::BINARY, empty_table()};
		}
		if (format == "Z") {
			return {ArrowIPCType::LARGE_BINARY, empty_table()};
		}
		if (format == "+s") {
			return {ArrowIPCType::STRUCT, empty_table()};
		}
		if (format == "+l") {
			return {ArrowIPCType::LIST, empty_table()};
		}
		if (format == "+L") {
			return {ArrowIPCType::LARGE_LIST, empty_table()};
		}
		if (format == "+m") {
			builder.StartTable();
			builder.AddScalar<uint8_t>(0, 0, 0);
			return {ArrowIPCType::MAP, builder.EndTable()};
		}
		if (StringUtil::StartsWith(format, "+w:")) {
			builder.StartTable();
			builder.AddScalar<int32_t>(0, NumericCast<int32_t>(std::stoull(format.substr(3))), 0);
			return {ArrowIPCType::FIXED_SIZE_LIST, builder.EndTable()};
		}
		if (StringUtil::StartsWith(format, "w:")) {
			builder.StartTable();
			builder.AddScalar<int32_t>(0, NumericCast<int32_t>(std::stoull(format.substr(2))), 0);
			return {ArrowIPCType::FIXED_SIZE_BINARY, builder.EndTable()};
		}
		auto integer = [&](int32_t bits, bool is_signed) {
			builder.StartTable();
			builder.AddScalar<int32_t>(0, bits, 0);
			builder.AddScalar<uint8_t>(1, is_signed ? 1 : 0, 0);
			return pair<ArrowIPCType, Offset> {ArrowIPCType::INT, builder.EndTable()};
		};
		auto floating = [&](int16_t precision) {
			builder.StartTable();
			builder.AddScalar<int16_t>(0, precision, 0);
			return pair<ArrowIPCType, Offset> {ArrowIPCType::FLOATING_POINT, builder.EndTable()};
		};
		if (format == "c") {
			return integer(8, true);
		}
		if (format == "s") {
			return integer(16, true);
		}
		if (format == "i") {
			return integer(32, true);
		}
		if (format == "l") {
			return integer(64, true);
		}
		if (format == "C") {
			return integer(8, false);
		}
		if (format == "S") {
			return integer(16, false);
		}
		if (format == "I") {
			return integer(32, false);
		}
		if (format == "L") {
			return integer(64, false);
		}
		if (format == "e") {
			return floating(0);
		}
		if (format == "f") {
			return floating(1);
		}
		if (format == "g") {
			return floating(2);
		}
		auto unit_of = [&](char unit) -> int16_t {
			switch (unit) {
			case 's':
				return 0;
			case 'm':
				return 1;
			case 'u':
				return 2;
			case 'n':
				return 3;
			default:
				throw NotImplementedException("Arrow IPC: the format \"%s\" is not supported", format);
			}
		};
		if (format == "tdD" || format == "tdm") {
			builder.StartTable();
			builder.AddScalar<int16_t>(0, format == "tdD" ? 0 : 1, 1);
			return {ArrowIPCType::DATE, builder.EndTable()};
		}
		if (format.size() == 3 && format[0] == 't' && format[1] == 't') {
			auto unit = unit_of(format[2]);
			builder.StartTable();
			builder.AddScalar<int16_t>(0, unit, 1);
			builder.AddScalar<int32_t>(1, unit <= 1 ? 32 : 64, 32);
			return {ArrowIPCType::TIME, builder.EndTable()};
		}
		if (format.size() >= 4 && format[0] == 't' && format[1] == 's' && format[3] == ':') {
			auto unit = unit_of(format[2]);
			auto timezone = format.substr(4);
			Offset timezone_offset = timezone.empty() ? 0 : builder.CreateString(timezone);
			builder.StartTable();
			builder.AddScalar<int16_t>(0, unit, 0);
			builder.AddOffset(1, timezone_offset);
			return {ArrowIPCType::TIMESTAMP, builder.EndTable()};
		}
		if (format == "tiM" || format == "tiD" || format == "tin") {
			builder.StartTable();
			builder.AddScalar<int16_t>(0, format == "tiM" ? 0 : (format == "tiD" ? 1 : 2), 0);
			return {ArrowIPCType::INTERVAL, builder.EndTable()};
		}
		if (StringUtil::StartsWith(format, "d:")) {
			auto parts = StringUtil::Split(format.substr(2), ',');
			if (parts.size() < 2) {
				throw NotImplementedException("Arrow IPC: the format \"%s\" is not supported", format);
			}
			builder.StartTable();
			builder.AddScalar<int32_t>(0, NumericCast<int32_t>(std::stoll(parts[0])), 0);
			builder.AddScalar<int32_t>(1, NumericCast<int32_t>(std::stoll(parts[1])), 0);
			builder.AddScalar<int32_t>(2, parts.size() > 2 ? NumericCast<int32_t>(std::stoll(parts[2])) : 128, 128);
			return {ArrowIPCType::DECIMAL, builder.EndTable()};
		}
		throw NotImplementedException("Arrow IPC: the format \"%s\" is not supported", format);
	}

	static Offset CreateField(FlatBufferBuilder &builder, const ArrowSchema &schema) {
		if (schema.dictionary) {
			throw NotImplementedException("Arrow IPC: dictionary encoded columns are not supported");
		}
		string format = schema.format;
		auto name = builder.CreateString(schema.name ? schema.name : "");
		vector<Offset> children;
		for (int64_t i = 0; i < schema.n_children; i++) {
			children.push_back(CreateField(builder, *schema.children[i]));
		}
		auto metadata = ReadMetadata(schema.metadata);
		vector<Offset> key_values;
		for (auto &entry : metadata) {
			auto key = builder.CreateString(entry.first);
			auto value = builder.CreateString(entry.second);
			builder.StartTable();
			builder.AddOffset(0, key);
			builder.AddOffset(1, value);
			key_values.push_back(builder.EndTable());
		}
		auto type = CreateType(builder, format);
		Offset children_offset = children.empty() ? 0 : builder.CreateOffsetVector(children);
		Offset metadata_offset = key_values.empty() ? 0 : builder.CreateOffsetVector(key_values);
		builder.StartTable();
		builder.AddOffset(0, name);
		builder.AddScalar<uint8_t>(1, (schema.flags & ARROW_FLAG_NULLABLE) ? 1 : 0, 0);
		builder.AddScalar<uint8_t>(2, static_cast<uint8_t>(type.first), 0);
		builder.AddOffset(3, type.second);
		builder.AddOffset(5, children_offset);
		builder.AddOffset(6, metadata_offset);
		return builder.EndTable();
	}

	//! A message of the stream: the header table is created by the callback
	template <class HEADER>
	static void AppendMessage(string &out, ArrowIPCHeader header_type, int64_t body_length, HEADER &&create_header,
	                          FlatBufferBuilder &builder) {
		auto header = create_header(builder);
		builder.StartTable();
		builder.AddScalar<int16_t>(0, ARROW_METADATA_VERSION_V5, 0);
		builder.AddScalar<uint8_t>(1, static_cast<uint8_t>(header_type), 0);
		builder.AddOffset(2, header);
		builder.AddScalar<int64_t>(3, body_length, 0);
		auto message = builder.EndTable();
		auto metadata = builder.Finish(message);
		auto padded = (metadata.size() + 7) / 8 * 8;
		uint32_t marker = ARROW_CONTINUATION;
		int32_t length = NumericCast<int32_t>(padded);
		out.append(reinterpret_cast<const char *>(&marker), 4);
		out.append(reinterpret_cast<const char *>(&length), 4);
		out.append(metadata);
		out.append(padded - metadata.size(), '\0');
	}

	//! The buffers and nodes of an array, in the order of the stream. The buffers refer to the memory of the array
	struct BatchLayout {
		vector<pair<int64_t, int64_t>> nodes;
		//! The pointer and the length of each buffer
		vector<pair<const uint8_t *, idx_t>> buffers;
	};

	static idx_t Bytes(idx_t count, idx_t width) {
		return count * width;
	}

	static void AddArray(const ArrowSchema &schema, const ArrowArray &array, BatchLayout &layout) {
		string format = schema.format;
		if (array.offset != 0) {
			throw NotImplementedException("Arrow IPC: arrays with an offset are not supported");
		}
		auto length = NumericCast<idx_t>(array.length);
		auto buffer_count = ArrowBufferCount(format);
		if (buffer_count == 0) {
			layout.nodes.emplace_back(array.length, array.length);
			return;
		}
		if (NumericCast<idx_t>(array.n_buffers) != buffer_count) {
			throw InternalException("Arrow IPC: the array of the format \"%s\" has %lld buffers", format,
			                        array.n_buffers);
		}
		int64_t null_count = array.null_count;
		auto validity = buffer_count > 0 ? reinterpret_cast<const uint8_t *>(array.buffers[0]) : nullptr;
		if (validity && null_count < 0) {
			null_count = 0;
			for (idx_t i = 0; i < length; i++) {
				if (!(validity[i / 8] & (1 << (i % 8)))) {
					null_count++;
				}
			}
		}
		if (!validity) {
			null_count = 0;
		}
		layout.nodes.emplace_back(array.length, null_count);
		if (buffer_count == 0) {
			return;
		}
		// the validity is left out when there are no NULL values
		layout.buffers.emplace_back(null_count > 0 ? validity : nullptr, null_count > 0 ? (length + 7) / 8 : 0);
		if (format == "+s" || StringUtil::StartsWith(format, "+w:")) {
			// no more buffers
		} else if (format == "u" || format == "z" || format == "+l" || format == "+m") {
			auto offsets = reinterpret_cast<const uint8_t *>(array.buffers[1]);
			layout.buffers.emplace_back(offsets, Bytes(length + 1, 4));
			if (buffer_count == 3) {
				int32_t data_length = 0;
				if (offsets) {
					memcpy(&data_length, offsets + length * 4, 4);
				}
				layout.buffers.emplace_back(reinterpret_cast<const uint8_t *>(array.buffers[2]),
				                            NumericCast<idx_t>(data_length));
			}
		} else if (format == "U" || format == "Z" || format == "+L") {
			auto offsets = reinterpret_cast<const uint8_t *>(array.buffers[1]);
			layout.buffers.emplace_back(offsets, Bytes(length + 1, 8));
			if (buffer_count == 3) {
				int64_t data_length = 0;
				if (offsets) {
					memcpy(&data_length, offsets + length * 8, 8);
				}
				layout.buffers.emplace_back(reinterpret_cast<const uint8_t *>(array.buffers[2]),
				                            NumericCast<idx_t>(data_length));
			}
		} else if (format == "b") {
			layout.buffers.emplace_back(reinterpret_cast<const uint8_t *>(array.buffers[1]), (length + 7) / 8);
		} else {
			layout.buffers.emplace_back(reinterpret_cast<const uint8_t *>(array.buffers[1]),
			                            Bytes(length, ArrowValueWidth(format)));
		}
		if (schema.n_children != array.n_children) {
			throw InternalException("Arrow IPC: the array does not have the children of its schema");
		}
		for (int64_t i = 0; i < schema.n_children; i++) {
			AddArray(*schema.children[i], *array.children[i], layout);
		}
	}
};

//===--------------------------------------------------------------------===//
// Reader
//===--------------------------------------------------------------------===//
//! The memory of a schema that the reader builds
struct ArrowSchemaHolder {
	vector<unique_ptr<string>> strings;
	vector<unique_ptr<ArrowSchema>> nodes;
	vector<unique_ptr<vector<ArrowSchema *>>> child_lists;

	const char *AddString(string value) {
		strings.push_back(make_uniq<string>(std::move(value)));
		return strings.back()->c_str();
	}
};

void ReleaseArrowSchemaChild(ArrowSchema *schema) {
	schema->release = nullptr;
}

void ReleaseArrowSchemaRoot(ArrowSchema *schema) {
	delete static_cast<ArrowSchemaHolder *>(schema->private_data);
	schema->private_data = nullptr;
	schema->release = nullptr;
}

//! The memory of the arrays of a batch
struct ArrowArrayHolder {
	shared_ptr<const string> data;
	vector<unique_ptr<ArrowArray>> nodes;
	vector<unique_ptr<vector<const void *>>> buffer_lists;
	vector<unique_ptr<vector<ArrowArray *>>> child_lists;
};

void ReleaseArrowArrayChild(ArrowArray *array) {
	array->release = nullptr;
}

void ReleaseArrowArrayRoot(ArrowArray *array) {
	delete static_cast<ArrowArrayHolder *>(array->private_data);
	array->private_data = nullptr;
	array->release = nullptr;
}

struct ArrowIPCReader {
	static string EncodeMetadata(const vector<pair<string, string>> &entries) {
		string result;
		int32_t count = NumericCast<int32_t>(entries.size());
		result.append(reinterpret_cast<const char *>(&count), 4);
		for (auto &entry : entries) {
			for (auto *part : {&entry.first, &entry.second}) {
				int32_t length = NumericCast<int32_t>(part->size());
				result.append(reinterpret_cast<const char *>(&length), 4);
				result.append(*part);
			}
		}
		return result;
	}

	static ArrowSchema *ReadField(const FlatBufferReader &reader, const FlatTable &field, ArrowSchemaHolder &holder) {
		holder.nodes.push_back(make_uniq<ArrowSchema>());
		auto &node = *holder.nodes.back();
		node.Init();
		node.name = holder.AddString(field.GetString(0));
		node.flags = field.Get<uint8_t>(1, 0) ? ARROW_FLAG_NULLABLE : 0;
		node.metadata = nullptr;
		node.dictionary = nullptr;
		node.children = nullptr;
		node.release = ReleaseArrowSchemaChild;
		if (field.Has(4)) {
			throw NotImplementedException("Arrow IPC: dictionary encoded columns are not supported");
		}
		auto type = static_cast<ArrowIPCType>(field.Get<uint8_t>(2, 0));
		string format;
		if (type == ArrowIPCType::NONE) {
			throw InvalidInputException("Failed to read the Arrow IPC stream: a field has no type");
		}
		auto type_table = field.GetTable(3);
		switch (type) {
		case ArrowIPCType::NULL_TYPE:
			format = "n";
			break;
		case ArrowIPCType::BOOL:
			format = "b";
			break;
		case ArrowIPCType::UTF8:
			format = "u";
			break;
		case ArrowIPCType::LARGE_UTF8:
			format = "U";
			break;
		case ArrowIPCType::BINARY:
			format = "z";
			break;
		case ArrowIPCType::LARGE_BINARY:
			format = "Z";
			break;
		case ArrowIPCType::STRUCT:
			format = "+s";
			break;
		case ArrowIPCType::LIST:
			format = "+l";
			break;
		case ArrowIPCType::LARGE_LIST:
			format = "+L";
			break;
		case ArrowIPCType::MAP:
			format = "+m";
			if (type_table.Get<uint8_t>(0, 0)) {
				node.flags |= ARROW_FLAG_MAP_KEYS_SORTED;
			}
			break;
		case ArrowIPCType::FIXED_SIZE_LIST:
			format = "+w:" + to_string(type_table.Get<int32_t>(0, 0));
			break;
		case ArrowIPCType::FIXED_SIZE_BINARY:
			format = "w:" + to_string(type_table.Get<int32_t>(0, 0));
			break;
		case ArrowIPCType::INT: {
			auto bits = type_table.Get<int32_t>(0, 0);
			auto is_signed = type_table.Get<uint8_t>(1, 0) != 0;
			const char *signed_formats[] = {"c", "s", "i", "l"};
			const char *unsigned_formats[] = {"C", "S", "I", "L"};
			if (bits != 8 && bits != 16 && bits != 32 && bits != 64) {
				throw NotImplementedException("Arrow IPC: integers of %d bits are not supported", bits);
			}
			auto index = bits == 8 ? 0 : (bits == 16 ? 1 : (bits == 32 ? 2 : 3));
			format = is_signed ? signed_formats[index] : unsigned_formats[index];
			break;
		}
		case ArrowIPCType::FLOATING_POINT: {
			auto precision = type_table.Get<int16_t>(0, 0);
			format = precision == 0 ? "e" : (precision == 1 ? "f" : "g");
			break;
		}
		case ArrowIPCType::DECIMAL: {
			format = "d:" + to_string(type_table.Get<int32_t>(0, 0)) + "," + to_string(type_table.Get<int32_t>(1, 0));
			auto bits = type_table.Get<int32_t>(2, 128);
			if (bits != 128) {
				format += "," + to_string(bits);
			}
			break;
		}
		case ArrowIPCType::DATE:
			format = type_table.Get<int16_t>(0, 1) == 0 ? "tdD" : "tdm";
			break;
		case ArrowIPCType::TIME: {
			const char *units[] = {"tts", "ttm", "ttu", "ttn"};
			auto unit = type_table.Get<int16_t>(0, 1);
			if (unit < 0 || unit > 3) {
				throw InvalidInputException("Failed to read the Arrow IPC stream: invalid time unit");
			}
			format = units[unit];
			break;
		}
		case ArrowIPCType::TIMESTAMP: {
			const char *units[] = {"tss:", "tsm:", "tsu:", "tsn:"};
			auto unit = type_table.Get<int16_t>(0, 0);
			if (unit < 0 || unit > 3) {
				throw InvalidInputException("Failed to read the Arrow IPC stream: invalid timestamp unit");
			}
			format = units[unit] + type_table.GetString(1);
			break;
		}
		case ArrowIPCType::INTERVAL: {
			const char *units[] = {"tiM", "tiD", "tin"};
			auto unit = type_table.Get<int16_t>(0, 0);
			if (unit < 0 || unit > 2) {
				throw InvalidInputException("Failed to read the Arrow IPC stream: invalid interval unit");
			}
			format = units[unit];
			break;
		}
		default:
			throw NotImplementedException("Arrow IPC: the type %d is not supported", static_cast<int>(type));
		}
		node.format = holder.AddString(std::move(format));
		// children
		idx_t first;
		auto count = field.GetVector(5, first);
		if (count > 0) {
			auto list = make_uniq<vector<ArrowSchema *>>();
			for (idx_t i = 0; i < count; i++) {
				auto child = field.GetTableAt(first + i * 4);
				list->push_back(ReadField(reader, child, holder));
			}
			node.n_children = NumericCast<int64_t>(count);
			node.children = list->data();
			holder.child_lists.push_back(std::move(list));
		}
		// metadata
		count = field.GetVector(6, first);
		if (count > 0) {
			vector<pair<string, string>> entries;
			for (idx_t i = 0; i < count; i++) {
				auto entry = field.GetTableAt(first + i * 4);
				entries.emplace_back(entry.GetString(0), entry.GetString(1));
			}
			node.metadata = holder.AddString(EncodeMetadata(entries));
		}
		return &node;
	}

	static void ReadSchema(const FlatBufferReader &reader, const FlatTable &schema, RESTArrowStream &stream) {
		auto holder = make_uniq<ArrowSchemaHolder>();
		idx_t first;
		auto count = schema.GetVector(1, first);
		auto list = make_uniq<vector<ArrowSchema *>>();
		for (idx_t i = 0; i < count; i++) {
			auto field = schema.GetTableAt(first + i * 4);
			list->push_back(ReadField(reader, field, *holder));
		}
		auto &root = stream.schema;
		root.Init();
		root.format = "+s";
		root.name = "";
		root.metadata = nullptr;
		root.flags = 0;
		root.dictionary = nullptr;
		root.n_children = NumericCast<int64_t>(count);
		root.children = list->data();
		holder->child_lists.push_back(std::move(list));
		root.private_data = holder.release();
		root.release = ReleaseArrowSchemaRoot;
	}

	struct BatchState {
		const FlatBufferReader &reader;
		idx_t nodes_first;
		idx_t node_count;
		idx_t node_index = 0;
		idx_t buffers_first;
		idx_t buffer_count;
		idx_t buffer_index = 0;
		const uint8_t *body;
		idx_t body_size;
		ArrowArrayHolder &holder;
	};

	static const void *NextBuffer(BatchState &state, bool validity) {
		if (state.buffer_index >= state.buffer_count) {
			throw InvalidInputException("Failed to read the Arrow IPC stream: the batch has too few buffers");
		}
		auto position = state.buffers_first + state.buffer_index * 16;
		state.buffer_index++;
		auto offset = state.reader.Read<int64_t>(position);
		auto length = state.reader.Read<int64_t>(position + 8);
		if (offset < 0 || length < 0 || NumericCast<idx_t>(offset) > state.body_size ||
		    NumericCast<idx_t>(length) > state.body_size - NumericCast<idx_t>(offset)) {
			throw InvalidInputException("Failed to read the Arrow IPC stream: a buffer is out of the body");
		}
		if (length == 0) {
			// buffers of arrays without values are not null, except for the validity
			static const uint64_t EMPTY[2] = {0, 0};
			return validity ? nullptr : EMPTY;
		}
		return state.body + offset;
	}

	static ArrowArray *ReadArray(const ArrowSchema &schema, BatchState &state) {
		if (state.node_index >= state.node_count) {
			throw InvalidInputException("Failed to read the Arrow IPC stream: the batch has too few nodes");
		}
		auto node_position = state.nodes_first + state.node_index * 16;
		state.node_index++;
		state.holder.nodes.push_back(make_uniq<ArrowArray>());
		auto &array = *state.holder.nodes.back();
		array.Init();
		array.length = state.reader.Read<int64_t>(node_position);
		array.null_count = state.reader.Read<int64_t>(node_position + 8);
		array.offset = 0;
		array.dictionary = nullptr;
		array.children = nullptr;
		array.release = ReleaseArrowArrayChild;
		if (array.length < 0) {
			throw InvalidInputException("Failed to read the Arrow IPC stream: negative length");
		}
		string format = schema.format;
		auto buffer_count = ArrowBufferCount(format);
		auto buffers = make_uniq<vector<const void *>>();
		for (idx_t i = 0; i < buffer_count; i++) {
			buffers->push_back(NextBuffer(state, i == 0));
		}
		array.n_buffers = NumericCast<int64_t>(buffer_count);
		array.buffers = buffers->empty() ? nullptr : buffers->data();
		state.holder.buffer_lists.push_back(std::move(buffers));
		if (schema.n_children > 0) {
			auto children = make_uniq<vector<ArrowArray *>>();
			for (int64_t i = 0; i < schema.n_children; i++) {
				children->push_back(ReadArray(*schema.children[i], state));
			}
			array.n_children = schema.n_children;
			array.children = children->data();
			state.holder.child_lists.push_back(std::move(children));
		}
		return &array;
	}

	static void ReadBatch(const FlatBufferReader &reader, const FlatTable &batch, const uint8_t *body, idx_t body_size,
	                      const shared_ptr<const string> &data, RESTArrowStream &stream) {
		if (batch.Has(3)) {
			throw NotImplementedException("Arrow IPC: compressed batches are not supported");
		}
		auto holder = make_uniq<ArrowArrayHolder>();
		holder->data = data;
		BatchState state {reader, 0, 0, 0, 0, 0, 0, body, body_size, *holder};
		state.node_count = batch.GetVector(1, state.nodes_first);
		state.buffer_count = batch.GetVector(2, state.buffers_first);
		reader.Check(state.nodes_first, state.node_count * 16);
		reader.Check(state.buffers_first, state.buffer_count * 16);
		holder->nodes.push_back(make_uniq<ArrowArray>());
		auto &root = *holder->nodes.back();
		root.Init();
		root.length = batch.Get<int64_t>(0, 0);
		root.null_count = 0;
		root.offset = 0;
		root.dictionary = nullptr;
		auto root_buffers = make_uniq<vector<const void *>>();
		root_buffers->push_back(nullptr);
		root.n_buffers = 1;
		root.buffers = root_buffers->data();
		holder->buffer_lists.push_back(std::move(root_buffers));
		auto children = make_uniq<vector<ArrowArray *>>();
		for (int64_t i = 0; i < stream.schema.n_children; i++) {
			children->push_back(ReadArray(*stream.schema.children[i], state));
		}
		root.n_children = stream.schema.n_children;
		root.children = children->data();
		holder->child_lists.push_back(std::move(children));
		ArrowArray result = root;
		result.private_data = holder.get();
		result.release = ReleaseArrowArrayRoot;
		holder.release();
		stream.batches.push_back(result);
	}
};

} // namespace

//===--------------------------------------------------------------------===//
// Public interface
//===--------------------------------------------------------------------===//
RESTArrowStream::RESTArrowStream() {
	schema.Init();
}

RESTArrowStream::~RESTArrowStream() {
	for (auto &batch : batches) {
		if (batch.release) {
			batch.release(&batch);
		}
	}
	if (schema.release) {
		schema.release(&schema);
	}
}

string RESTArrowIPC::Write(const ArrowSchema &schema, const vector<ArrowArray> &batches) {
	string out;
	{
		FlatBufferBuilder builder;
		ArrowIPCWriter::AppendMessage(
		    out, ArrowIPCHeader::SCHEMA, 0,
		    [&](FlatBufferBuilder &builder) {
			    vector<FlatBufferBuilder::Offset> fields;
			    for (int64_t i = 0; i < schema.n_children; i++) {
				    fields.push_back(ArrowIPCWriter::CreateField(builder, *schema.children[i]));
			    }
			    auto fields_offset = fields.empty() ? 0 : builder.CreateOffsetVector(fields);
			    builder.StartTable();
			    builder.AddScalar<int16_t>(0, 0, 0);
			    builder.AddOffset(1, fields_offset);
			    return builder.EndTable();
		    },
		    builder);
	}
	for (auto &batch : batches) {
		ArrowIPCWriter::BatchLayout layout;
		for (int64_t i = 0; i < schema.n_children; i++) {
			ArrowIPCWriter::AddArray(*schema.children[i], *batch.children[i], layout);
		}
		// the body, with its buffers padded to 8 bytes
		vector<pair<int64_t, int64_t>> buffers;
		idx_t body_length = 0;
		for (auto &buffer : layout.buffers) {
			buffers.emplace_back(NumericCast<int64_t>(body_length), NumericCast<int64_t>(buffer.second));
			body_length += (buffer.second + 7) / 8 * 8;
		}
		FlatBufferBuilder builder;
		ArrowIPCWriter::AppendMessage(
		    out, ArrowIPCHeader::RECORD_BATCH, NumericCast<int64_t>(body_length),
		    [&](FlatBufferBuilder &builder) {
			    auto nodes_offset = builder.CreatePairVector(layout.nodes);
			    auto buffers_offset = builder.CreatePairVector(buffers);
			    builder.StartTable();
			    builder.AddScalar<int64_t>(0, batch.length, 0);
			    builder.AddOffset(1, nodes_offset);
			    builder.AddOffset(2, buffers_offset);
			    return builder.EndTable();
		    },
		    builder);
		auto body_start = out.size();
		out.resize(body_start + body_length, '\0');
		for (idx_t i = 0; i < layout.buffers.size(); i++) {
			auto &buffer = layout.buffers[i];
			if (buffer.second > 0) {
				memcpy(&out[body_start + NumericCast<idx_t>(buffers[i].first)], buffer.first, buffer.second);
			}
		}
	}
	// end of the stream
	uint32_t end[2] = {ARROW_CONTINUATION, 0};
	out.append(reinterpret_cast<const char *>(end), 8);
	return out;
}

unique_ptr<RESTArrowStream> RESTArrowIPC::Read(shared_ptr<const string> data, idx_t offset, idx_t size) {
	if (offset + size > data->size()) {
		throw InternalException("Arrow IPC: the stream is out of the data");
	}
	// the arrays of the stream are read in place, so the data has to be aligned
	if (reinterpret_cast<uintptr_t>(data->data() + offset) % 8 != 0) {
		data = make_shared_ptr<const string>(data->substr(offset, size));
		offset = 0;
	}
	auto base = reinterpret_cast<const uint8_t *>(data->data()) + offset;
	auto result = make_uniq<RESTArrowStream>();
	bool has_schema = false;
	idx_t position = 0;
	while (true) {
		if (position + 8 > size) {
			throw InvalidInputException("Failed to read the Arrow IPC stream: the stream ends without a marker");
		}
		uint32_t marker;
		int32_t metadata_length;
		memcpy(&marker, base + position, 4);
		memcpy(&metadata_length, base + position + 4, 4);
		position += 8;
		if (marker != ARROW_CONTINUATION) {
			throw InvalidInputException("Failed to read the Arrow IPC stream: unexpected message marker");
		}
		if (metadata_length == 0) {
			break;
		}
		if (metadata_length < 0 || NumericCast<idx_t>(metadata_length) > size - position) {
			throw InvalidInputException("Failed to read the Arrow IPC stream: invalid metadata length");
		}
		FlatBufferReader reader(base + position, NumericCast<idx_t>(metadata_length));
		FlatTable message(reader, reader.Root());
		auto header_type = static_cast<ArrowIPCHeader>(message.Get<uint8_t>(1, 0));
		auto body_length = message.Get<int64_t>(3, 0);
		auto body_position = position + NumericCast<idx_t>(metadata_length);
		if (body_length < 0 || NumericCast<idx_t>(body_length) > size - body_position) {
			throw InvalidInputException("Failed to read the Arrow IPC stream: invalid body length");
		}
		auto header = message.GetTable(2);
		switch (header_type) {
		case ArrowIPCHeader::SCHEMA:
			if (has_schema) {
				throw InvalidInputException("Failed to read the Arrow IPC stream: more than one schema");
			}
			ArrowIPCReader::ReadSchema(reader, header, *result);
			has_schema = true;
			break;
		case ArrowIPCHeader::RECORD_BATCH:
			if (!has_schema) {
				throw InvalidInputException("Failed to read the Arrow IPC stream: a batch before the schema");
			}
			ArrowIPCReader::ReadBatch(reader, header, base + body_position, NumericCast<idx_t>(body_length), data,
			                          *result);
			break;
		default:
			throw NotImplementedException("Arrow IPC: messages of type %d are not supported",
			                              static_cast<int>(header_type));
		}
		position = body_position + NumericCast<idx_t>(body_length);
	}
	if (!has_schema) {
		throw InvalidInputException("Failed to read the Arrow IPC stream: there is no schema");
	}
	return result;
}

} // namespace duckdb
