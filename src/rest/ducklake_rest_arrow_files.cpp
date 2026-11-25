#include "rest/ducklake_rest_arrow_files.hpp"

#include "rest/ducklake_rest_api.hpp"
#include "rest/ducklake_rest_arrow.hpp"
#include "rest/ducklake_rest_codec.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"

namespace duckdb {

namespace {

using FileRow = VectorStructType<string_t, string_t, uint64_t, uint64_t, string_t, uint64_t, uint64_t>;
using MinMaxRow = VectorStructType<uint64_t, string_t, string_t, bool>;
using InlinedDeletionRow = VectorStructType<uint64_t, uint64_t>;
using PartitionValueRow = VectorStructType<uint64_t, string_t>;
//! The value of a row of a vector of the type T
template <class T>
using RowValue = typename VectorIterator<T>::ValueEntry;

LogicalType FileType() {
	child_list_t<LogicalType> children;
	children.emplace_back("path", LogicalType::VARCHAR);
	children.emplace_back("encryption_key", LogicalType::BLOB);
	children.emplace_back("file_size_bytes", LogicalType::UBIGINT);
	children.emplace_back("footer_size", LogicalType::UBIGINT);
	children.emplace_back("format", LogicalType::VARCHAR);
	children.emplace_back("row_count", LogicalType::UBIGINT);
	children.emplace_back("row_group_count", LogicalType::UBIGINT);
	return LogicalType::STRUCT(std::move(children));
}

LogicalType MinMaxType() {
	child_list_t<LogicalType> children;
	children.emplace_back("column_id", LogicalType::UBIGINT);
	children.emplace_back("min", LogicalType::VARCHAR);
	children.emplace_back("max", LogicalType::VARCHAR);
	children.emplace_back("contains_nan", LogicalType::BOOLEAN);
	return LogicalType::STRUCT(std::move(children));
}

LogicalType PartitionValueType() {
	child_list_t<LogicalType> children;
	children.emplace_back("partition_key_index", LogicalType::UBIGINT);
	children.emplace_back("partition_value", LogicalType::VARCHAR);
	return LogicalType::STRUCT(std::move(children));
}

LogicalType InlinedDeletionType() {
	child_list_t<LogicalType> children;
	children.emplace_back("row_id", LogicalType::UBIGINT);
	children.emplace_back("snapshot_id", LogicalType::UBIGINT);
	return LogicalType::STRUCT(std::move(children));
}

//===--------------------------------------------------------------------===//
// Columns
//===--------------------------------------------------------------------===//
//! A column of the Arrow table of a list of entries: writes the column of the entries, and reads it into them
template <class ENTRY>
struct ArrowColumn {
	const char *name;
	LogicalType type;
	std::function<void(Vector &vector, const ENTRY *entries, idx_t count)> write;
	std::function<void(Vector &vector, ENTRY *entries, idx_t count)> read;
};

//! A column of values of the type T: writes the value of an entry, and sets it from the value of a row
template <class T, class ENTRY, class WRITE, class READ>
ArrowColumn<ENTRY> Column(const char *name, LogicalType type, WRITE write, READ read) {
	return {name, std::move(type),
	        [write](Vector &vector, const ENTRY *entries, idx_t count) {
		        auto writer = FlatVector::Writer<T>(vector, count);
		        for (idx_t i = 0; i < count; i++) {
			        write(writer, entries[i]);
		        }
	        },
	        [read](Vector &vector, ENTRY *entries, idx_t count) {
		        auto values = vector.Values<T>();
		        for (idx_t i = 0; i < count; i++) {
			        read(values[i], entries[i]);
		        }
	        }};
}

template <class ENTRY>
string WriteEntries(ClientContext &context, const vector<ArrowColumn<ENTRY>> &columns, const vector<ENTRY> &entries) {
	vector<string> names;
	vector<LogicalType> types;
	for (auto &column : columns) {
		names.push_back(column.name);
		types.push_back(column.type);
	}
	ColumnDataCollection rows(Allocator::DefaultAllocator(), types);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	for (idx_t start = 0; start < entries.size(); start += STANDARD_VECTOR_SIZE) {
		auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, entries.size() - start);
		chunk.Reset();
		for (idx_t i = 0; i < columns.size(); i++) {
			columns[i].write(chunk.data[i], entries.data() + start, count);
		}
		chunk.CheckCardinality(count);
		rows.Append(chunk);
	}
	return RESTArrow::Write(context, names, rows);
}

template <class ENTRY>
void ReadEntries(ClientContext &context, shared_ptr<const string> data, idx_t offset, idx_t size,
                 const vector<ArrowColumn<ENTRY>> &columns, vector<ENTRY> &result) {
	vector<LogicalType> types;
	for (auto &column : columns) {
		types.push_back(column.type);
	}
	vector<string> names;
	auto rows = RESTArrow::Read(context, std::move(data), offset, size, types, names);
	for (auto &chunk : rows->Chunks()) {
		auto start = result.size();
		result.resize(start + chunk.size());
		for (idx_t i = 0; i < columns.size(); i++) {
			columns[i].read(chunk.data[i], result.data() + start, chunk.size());
		}
	}
}

//===--------------------------------------------------------------------===//
// Numbers
//===--------------------------------------------------------------------===//
//! Ids are NULL if they are invalid
void WriteNumber(VectorWriter<uint64_t> &writer, const optional_idx &value) {
	if (value.IsValid()) {
		writer.WriteValue(value.GetIndex());
	} else {
		writer.WriteNull();
	}
}

template <class INDEX>
void WriteNumber(VectorWriter<uint64_t> &writer, const TypedIndex<INDEX> &value) {
	if (value.IsValid()) {
		writer.WriteValue(value.index);
	} else {
		writer.WriteNull();
	}
}

void WriteNumber(VectorWriter<uint64_t> &writer, idx_t value) {
	writer.WriteValue(value);
}

void ReadNumber(const RowValue<uint64_t> &value, optional_idx &result) {
	result = value.IsValid() ? optional_idx(value.GetValue()) : optional_idx();
}

template <class INDEX>
void ReadNumber(const RowValue<uint64_t> &value, TypedIndex<INDEX> &result) {
	result.index = value.IsValid() ? value.GetValue() : DConstants::INVALID_INDEX;
}

void ReadNumber(const RowValue<uint64_t> &value, idx_t &result) {
	result = value.GetValue();
}

template <class ENTRY, class MEMBER>
ArrowColumn<ENTRY> NumberColumn(const char *name, MEMBER ENTRY::*member) {
	return Column<uint64_t, ENTRY>(
	    name, LogicalType::UBIGINT,
	    [member](VectorWriter<uint64_t> &writer, const ENTRY &entry) { WriteNumber(writer, entry.*member); },
	    [member](const RowValue<uint64_t> &value, ENTRY &entry) { ReadNumber(value, entry.*member); });
}

//===--------------------------------------------------------------------===//
// Lists
//===--------------------------------------------------------------------===//
//! Writes the values of a container as a list, which is NULL for an empty container if `null_if_empty`
template <class T, class CONTAINER, class WRITE>
void WriteList(VectorWriter<VectorListType<T>> &writer, const CONTAINER &values, bool null_if_empty,
               WRITE write_value) {
	if (null_if_empty && values.empty()) {
		writer.WriteNull();
		return;
	}
	auto value = values.begin();
	for (auto &element : writer.WriteList(values.size())) {
		write_value(element, *value);
		++value;
	}
}

template <class ENTRY, class CONTAINER>
ArrowColumn<ENTRY> NumberListColumn(const char *name, CONTAINER ENTRY::*member, bool null_if_empty) {
	return Column<VectorListType<uint64_t>, ENTRY>(
	    name, LogicalType::LIST(LogicalType::UBIGINT),
	    [member, null_if_empty](VectorWriter<VectorListType<uint64_t>> &writer, const ENTRY &entry) {
		    WriteList(writer, entry.*member, null_if_empty,
		              [](VectorWriter<uint64_t> &element, idx_t value) { element.WriteValue(value); });
	    },
	    [member](const RowValue<VectorListType<uint64_t>> &value, ENTRY &entry) {
		    if (!value.IsValid()) {
			    return;
		    }
		    auto &numbers = entry.*member;
		    for (auto child : value.GetChildValues()) {
			    numbers.insert(numbers.end(), child.GetValue());
		    }
	    });
}

//===--------------------------------------------------------------------===//
// Files
//===--------------------------------------------------------------------===//
string_t Str(const string &value) {
	return string_t(value.c_str(), NumericCast<uint32_t>(value.size()));
}

void WriteFile(VectorWriter<FileRow> &writer, const DuckLakeFileData &file) {
	writer.WriteValue(
	    [&](auto &path, auto &key, auto &size, auto &footer, auto &format, auto &row_count, auto &row_group_count) {
		    path.WriteValue(Str(file.path));
		    if (file.encryption_key.empty()) {
			    key.WriteNull();
		    } else {
			    key.WriteValue(Str(file.encryption_key));
		    }
		    size.WriteValue(file.file_size_bytes);
		    WriteNumber(footer, file.footer_size);
		    if (file.format == DeleteFileFormat::PARQUET) {
			    format.WriteNull();
		    } else {
			    format.WriteValue(Str(RESTFileFormatCodec::ToString(file.format)));
		    }
		    WriteNumber(row_count, file.row_count);
		    WriteNumber(row_group_count, file.row_group_count);
	    });
}

//! A file, which is empty if the value is NULL
DuckLakeFileData ReadFile(const RowValue<FileRow> &entry) {
	DuckLakeFileData result;
	if (!entry.IsValid()) {
		return result;
	}
	auto mapper = RESTPathScope::Current();
	auto path = entry.GetChildValue<0>().GetValue().GetString();
	result.path = mapper ? mapper->ToClient(path) : path;
	auto key = entry.GetChildValue<1>();
	if (key.IsValid()) {
		result.encryption_key = key.GetValue().GetString();
	}
	result.file_size_bytes = entry.GetChildValue<2>().GetValue();
	ReadNumber(entry.GetChildValue<3>(), result.footer_size);
	auto format = entry.GetChildValue<4>();
	if (format.IsValid()) {
		result.format = RESTFileFormatCodec::FromString(format.GetValue().GetString());
	}
	ReadNumber(entry.GetChildValue<5>(), result.row_count);
	ReadNumber(entry.GetChildValue<6>(), result.row_group_count);
	return result;
}

//! A column of a file of the entries. A file that an entry can lack is NULL if it does
template <class ENTRY>
ArrowColumn<ENTRY> FileColumn(const char *name, DuckLakeFileData ENTRY::*member, bool optional) {
	return Column<FileRow, ENTRY>(
	    name, FileType(),
	    [member, optional](VectorWriter<FileRow> &writer, const ENTRY &entry) {
		    auto &file = entry.*member;
		    if (optional && file.path.empty()) {
			    writer.WriteNull();
		    } else {
			    WriteFile(writer, file);
		    }
	    },
	    [member](const RowValue<FileRow> &value, ENTRY &entry) { entry.*member = ReadFile(value); });
}

//===--------------------------------------------------------------------===//
// FileListEntry
//===--------------------------------------------------------------------===//
ArrowColumn<DuckLakeFileListEntry> MinMaxColumn() {
	return Column<VectorListType<MinMaxRow>, DuckLakeFileListEntry>(
	    "column_min_max", LogicalType::LIST(MinMaxType()),
	    [](VectorWriter<VectorListType<MinMaxRow>> &writer, const DuckLakeFileListEntry &entry) {
		    WriteList(writer, entry.column_min_max, true, [](VectorWriter<MinMaxRow> &element, const auto &column) {
			    auto &stats = column.second;
			    element.WriteValue([&](auto &column_id, auto &min, auto &max, auto &contains_nan) {
				    column_id.WriteValue(column.first);
				    if (stats.has_min) {
					    min.WriteValue(Str(stats.min));
				    } else {
					    min.WriteNull();
				    }
				    if (stats.has_max) {
					    max.WriteValue(Str(stats.max));
				    } else {
					    max.WriteNull();
				    }
				    if (stats.has_contains_nan) {
					    contains_nan.WriteValue(stats.contains_nan);
				    } else {
					    contains_nan.WriteNull();
				    }
			    });
		    });
	    },
	    [](const RowValue<VectorListType<MinMaxRow>> &value, DuckLakeFileListEntry &entry) {
		    if (!value.IsValid()) {
			    return;
		    }
		    for (auto stats : value.GetChildValues()) {
			    DuckLakeFileColumnStats column_stats;
			    auto min = stats.GetChildValue<1>();
			    auto max = stats.GetChildValue<2>();
			    auto contains_nan = stats.GetChildValue<3>();
			    if (min.IsValid()) {
				    column_stats.has_min = true;
				    column_stats.min = min.GetValue().GetString();
			    }
			    if (max.IsValid()) {
				    column_stats.has_max = true;
				    column_stats.max = max.GetValue().GetString();
			    }
			    if (contains_nan.IsValid()) {
				    column_stats.has_contains_nan = true;
				    column_stats.contains_nan = contains_nan.GetValue();
			    }
			    entry.column_min_max.emplace(stats.GetChildValue<0>().GetValue(), std::move(column_stats));
		    }
	    });
}

ArrowColumn<DuckLakeFileListEntry> PartitionValuesColumn() {
	return Column<VectorListType<PartitionValueRow>, DuckLakeFileListEntry>(
	    "partition_values", LogicalType::LIST(PartitionValueType()),
	    [](VectorWriter<VectorListType<PartitionValueRow>> &writer, const DuckLakeFileListEntry &entry) {
		    WriteList(writer, entry.partition_values, true,
		              [](VectorWriter<PartitionValueRow> &element, const DuckLakeFilePartition &partition) {
			              element.WriteValue([&](auto &key_index, auto &value) {
				              key_index.WriteValue(partition.partition_column_idx);
				              if (partition.partition_value.IsNull()) {
					              value.WriteNull();
				              } else {
					              value.WriteValue(Str(partition.partition_value.ToString()));
				              }
			              });
		              });
	    },
	    [](const RowValue<VectorListType<PartitionValueRow>> &value, DuckLakeFileListEntry &entry) {
		    if (!value.IsValid()) {
			    return;
		    }
		    for (auto child : value.GetChildValues()) {
			    DuckLakeFilePartition partition;
			    partition.partition_column_idx = child.GetChildValue<0>().GetValue();
			    auto text = child.GetChildValue<1>();
			    partition.partition_value =
			        text.IsValid() ? Value(text.GetValue().GetString()) : Value(LogicalType::VARCHAR);
			    entry.partition_values.push_back(std::move(partition));
		    }
	    });
}

vector<ArrowColumn<DuckLakeFileListEntry>> FileListColumns() {
	using ENTRY = DuckLakeFileListEntry;
	return {NumberColumn("data_file_id", &ENTRY::data_file_id),
	        NumberColumn("file_id", &ENTRY::file_id),
	        FileColumn("file", &ENTRY::file, false),
	        FileColumn("delete_file", &ENTRY::delete_file, true),
	        NumberColumn("row_id_start", &ENTRY::row_id_start),
	        NumberColumn("snapshot_id", &ENTRY::snapshot_id),
	        NumberColumn("snapshot_filter_max", &ENTRY::snapshot_filter_max),
	        NumberColumn("snapshot_filter_min", &ENTRY::snapshot_filter_min),
	        NumberColumn("mapping_id", &ENTRY::mapping_id),
	        NumberListColumn("inlined_file_deletions", &ENTRY::inlined_file_deletions, true),
	        MinMaxColumn(),
	        NumberColumn("partition_id", &ENTRY::partition_id),
	        PartitionValuesColumn(),
	        NumberListColumn("snapshot_filter_excluded", &ENTRY::snapshot_filter_excluded, false)};
}

//===--------------------------------------------------------------------===//
// FileListExtendedEntry
//===--------------------------------------------------------------------===//
vector<ArrowColumn<DuckLakeFileListExtendedEntry>> ExtendedColumns() {
	using ENTRY = DuckLakeFileListExtendedEntry;
	return {NumberColumn("file_id", &ENTRY::file_id),
	        NumberColumn("delete_file_id", &ENTRY::delete_file_id),
	        FileColumn("file", &ENTRY::file, false),
	        FileColumn("delete_file", &ENTRY::delete_file, true),
	        NumberColumn("row_id_start", &ENTRY::row_id_start),
	        NumberColumn("snapshot_id", &ENTRY::snapshot_id),
	        NumberColumn("delete_file_begin_snapshot", &ENTRY::delete_file_begin_snapshot),
	        NumberColumn("row_count", &ENTRY::row_count),
	        NumberColumn("mapping_id", &ENTRY::mapping_id)};
}

//===--------------------------------------------------------------------===//
// DeleteScanEntry
//===--------------------------------------------------------------------===//
ArrowColumn<DuckLakeDeleteScanEntry> InlinedDeletionsColumn() {
	return Column<VectorListType<InlinedDeletionRow>, DuckLakeDeleteScanEntry>(
	    "inlined_file_deletions", LogicalType::LIST(InlinedDeletionType()),
	    [](VectorWriter<VectorListType<InlinedDeletionRow>> &writer, const DuckLakeDeleteScanEntry &entry) {
		    WriteList(writer, entry.inlined_file_deletions, true,
		              [](VectorWriter<InlinedDeletionRow> &element, const auto &deletion) {
			              element.WriteValue([&](auto &row_id, auto &deleted_in) {
				              row_id.WriteValue(deletion.first);
				              deleted_in.WriteValue(deletion.second);
			              });
		              });
	    },
	    [](const RowValue<VectorListType<InlinedDeletionRow>> &value, DuckLakeDeleteScanEntry &entry) {
		    if (!value.IsValid()) {
			    return;
		    }
		    for (auto child : value.GetChildValues()) {
			    entry.inlined_file_deletions.emplace(child.GetChildValue<0>().GetValue(),
			                                         child.GetChildValue<1>().GetValue());
		    }
	    });
}

vector<ArrowColumn<DuckLakeDeleteScanEntry>> DeleteScanColumns() {
	using ENTRY = DuckLakeDeleteScanEntry;
	return {FileColumn("file", &ENTRY::file, false),
	        FileColumn("delete_file", &ENTRY::delete_file, true),
	        FileColumn("previous_delete_file", &ENTRY::previous_delete_file, true),
	        NumberColumn("row_count", &ENTRY::row_count),
	        NumberColumn("row_id_start", &ENTRY::row_id_start),
	        NumberColumn("mapping_id", &ENTRY::mapping_id),
	        NumberColumn("snapshot_id", &ENTRY::snapshot_id),
	        NumberColumn("insert_snapshot", &ENTRY::insert_snapshot),
	        Column<bool, ENTRY>(
	            "file_deleted", LogicalType::BOOLEAN,
	            [](VectorWriter<bool> &writer, const ENTRY &entry) { writer.WriteValue(entry.file_deleted); },
	            [](const RowValue<bool> &value, ENTRY &entry) { entry.file_deleted = value.GetValue(); }),
	        NumberColumn("start_snapshot", &ENTRY::start_snapshot),
	        NumberColumn("end_snapshot", &ENTRY::end_snapshot),
	        NumberColumn("file_id", &ENTRY::file_id),
	        InlinedDeletionsColumn()};
}

} // namespace

string RESTArrowFiles::Write(ClientContext &context, const vector<DuckLakeFileListEntry> &files) {
	return WriteEntries(context, FileListColumns(), files);
}

void RESTArrowFiles::Read(ClientContext &context, shared_ptr<const string> data, idx_t offset, idx_t size,
                          vector<DuckLakeFileListEntry> &result) {
	ReadEntries(context, std::move(data), offset, size, FileListColumns(), result);
}

string RESTArrowFiles::Write(ClientContext &context, const vector<DuckLakeFileListExtendedEntry> &files) {
	return WriteEntries(context, ExtendedColumns(), files);
}

void RESTArrowFiles::Read(ClientContext &context, shared_ptr<const string> data, idx_t offset, idx_t size,
                          vector<DuckLakeFileListExtendedEntry> &result) {
	ReadEntries(context, std::move(data), offset, size, ExtendedColumns(), result);
}

string RESTArrowFiles::Write(ClientContext &context, const vector<DuckLakeDeleteScanEntry> &files) {
	return WriteEntries(context, DeleteScanColumns(), files);
}

void RESTArrowFiles::Read(ClientContext &context, shared_ptr<const string> data, idx_t offset, idx_t size,
                          vector<DuckLakeDeleteScanEntry> &result) {
	ReadEntries(context, std::move(data), offset, size, DeleteScanColumns(), result);
}

} // namespace duckdb
