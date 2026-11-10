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
using FileWriter = VectorWriter<FileRow>;
using MinMaxRow = VectorStructType<uint64_t, string_t, string_t, bool>;
using InlinedDeletionRow = VectorStructType<uint64_t, uint64_t>;
using PartitionValueRow = VectorStructType<uint64_t, string_t>;

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
// Writing
//===--------------------------------------------------------------------===//
string_t Str(const string &value) {
	return string_t(value.c_str(), NumericCast<uint32_t>(value.size()));
}

void WriteOptional(VectorWriter<uint64_t> &writer, const optional_idx &value) {
	if (value.IsValid()) {
		writer.WriteValue(value.GetIndex());
	} else {
		writer.WriteNull();
	}
}

template <class INDEX>
void WriteIndex(VectorWriter<uint64_t> &writer, const INDEX &value) {
	if (value.IsValid()) {
		writer.WriteValue(value.index);
	} else {
		writer.WriteNull();
	}
}

void WriteFile(FileWriter &writer, const DuckLakeFileData &file) {
	writer.WriteValue(
	    [&](auto &path, auto &key, auto &size, auto &footer, auto &format, auto &row_count, auto &row_group_count) {
		    path.WriteValue(Str(file.path));
		    if (file.encryption_key.empty()) {
			    key.WriteNull();
		    } else {
			    key.WriteValue(Str(file.encryption_key));
		    }
		    size.WriteValue(file.file_size_bytes);
		    WriteOptional(footer, file.footer_size);
		    if (file.format == DeleteFileFormat::PARQUET) {
			    format.WriteNull();
		    } else {
			    format.WriteValue(Str(RESTFileFormatCodec::ToString(file.format)));
		    }
		    WriteOptional(row_count, file.row_count);
		    WriteOptional(row_group_count, file.row_group_count);
	    });
}

//! Delete files are NULL if there is none
void WriteOptionalFile(FileWriter &writer, const DuckLakeFileData &file) {
	if (file.path.empty()) {
		writer.WriteNull();
	} else {
		WriteFile(writer, file);
	}
}

template <class ENTRY, class FILL>
string WriteEntries(ClientContext &context, const vector<string> &names, const vector<LogicalType> &types,
                    const vector<ENTRY> &entries, FILL &&fill) {
	ColumnDataCollection rows(Allocator::DefaultAllocator(), types);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	for (idx_t start = 0; start < entries.size(); start += STANDARD_VECTOR_SIZE) {
		auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, entries.size() - start);
		chunk.Reset();
		fill(chunk, entries, start, count);
		chunk.CheckCardinality(count);
		rows.Append(chunk);
	}
	return RESTArrow::Write(context, names, rows);
}

//===--------------------------------------------------------------------===//
// Reading
//===--------------------------------------------------------------------===//
template <class ENTRY>
optional_idx ReadOptional(const ENTRY &entry) {
	return entry.IsValid() ? optional_idx(entry.GetValue()) : optional_idx();
}

template <class INDEX, class ENTRY>
INDEX ReadIndex(const ENTRY &entry) {
	return entry.IsValid() ? INDEX(entry.GetValue()) : INDEX();
}

string ReadPath(const string_t &path) {
	auto mapper = RESTPathScope::Current();
	return mapper ? mapper->ToClient(path.GetString()) : path.GetString();
}

//! A file, which is empty if the value is NULL
template <class ENTRY>
DuckLakeFileData ReadFile(const ENTRY &entry) {
	DuckLakeFileData result;
	if (!entry.IsValid()) {
		return result;
	}
	result.path = ReadPath(entry.template GetChildValue<0>().GetValue());
	auto key = entry.template GetChildValue<1>();
	if (key.IsValid()) {
		result.encryption_key = key.GetValue().GetString();
	}
	result.file_size_bytes = entry.template GetChildValue<2>().GetValue();
	result.footer_size = ReadOptional(entry.template GetChildValue<3>());
	auto format = entry.template GetChildValue<4>();
	if (format.IsValid()) {
		result.format = RESTFileFormatCodec::FromString(format.GetValue().GetString());
	}
	result.row_count = ReadOptional(entry.template GetChildValue<5>());
	result.row_group_count = ReadOptional(entry.template GetChildValue<6>());
	return result;
}

//! Reads the rows of the stream, calls the callback with each chunk
template <class CALLBACK>
void ReadChunks(ClientContext &context, shared_ptr<const string> data, idx_t offset, idx_t size,
                const vector<LogicalType> &types, CALLBACK &&callback) {
	vector<string> names;
	auto rows = RESTArrow::Read(context, std::move(data), offset, size, types, names);
	for (auto &chunk : rows->Chunks()) {
		callback(chunk);
	}
}

} // namespace

//===--------------------------------------------------------------------===//
// FileListEntry
//===--------------------------------------------------------------------===//
static vector<string> FileListNames() {
	return {"data_file_id",
	        "file_id",
	        "file",
	        "delete_file",
	        "row_id_start",
	        "snapshot_id",
	        "snapshot_filter_max",
	        "snapshot_filter_min",
	        "mapping_id",
	        "inlined_file_deletions",
	        "column_min_max",
	        "partition_id",
	        "partition_values",
	        "snapshot_filter_excluded"};
}

static vector<LogicalType> FileListTypes() {
	return {LogicalType::UBIGINT,
	        LogicalType::UBIGINT,
	        FileType(),
	        FileType(),
	        LogicalType::UBIGINT,
	        LogicalType::UBIGINT,
	        LogicalType::UBIGINT,
	        LogicalType::UBIGINT,
	        LogicalType::UBIGINT,
	        LogicalType::LIST(LogicalType::UBIGINT),
	        LogicalType::LIST(MinMaxType()),
	        LogicalType::UBIGINT,
	        LogicalType::LIST(PartitionValueType()),
	        LogicalType::LIST(LogicalType::UBIGINT)};
}

string RESTArrowFiles::Write(ClientContext &context, const vector<DuckLakeFileListEntry> &files) {
	return WriteEntries(
	    context, FileListNames(), FileListTypes(), files,
	    [](DataChunk &chunk, const vector<DuckLakeFileListEntry> &entries, idx_t start, idx_t count) {
		    auto data_file_id = FlatVector::Writer<uint64_t>(chunk.data[0], count);
		    auto file_id = FlatVector::Writer<uint64_t>(chunk.data[1], count);
		    auto file = FlatVector::Writer<FileRow>(chunk.data[2], count);
		    auto delete_file = FlatVector::Writer<FileRow>(chunk.data[3], count);
		    auto row_id_start = FlatVector::Writer<uint64_t>(chunk.data[4], count);
		    auto snapshot_id = FlatVector::Writer<uint64_t>(chunk.data[5], count);
		    auto snapshot_filter_max = FlatVector::Writer<uint64_t>(chunk.data[6], count);
		    auto snapshot_filter_min = FlatVector::Writer<uint64_t>(chunk.data[7], count);
		    auto mapping_id = FlatVector::Writer<uint64_t>(chunk.data[8], count);
		    auto inlined_deletions = FlatVector::Writer<VectorListType<uint64_t>>(chunk.data[9], count);
		    auto min_max = FlatVector::Writer<VectorListType<MinMaxRow>>(chunk.data[10], count);
		    auto partition_id = FlatVector::Writer<uint64_t>(chunk.data[11], count);
		    auto partition_values = FlatVector::Writer<VectorListType<PartitionValueRow>>(chunk.data[12], count);
		    auto excluded = FlatVector::Writer<VectorListType<uint64_t>>(chunk.data[13], count);
		    for (idx_t i = 0; i < count; i++) {
			    auto &entry = entries[start + i];
			    WriteOptional(data_file_id, entry.data_file_id);
			    WriteIndex(file_id, entry.file_id);
			    WriteFile(file, entry.file);
			    WriteOptionalFile(delete_file, entry.delete_file);
			    WriteOptional(row_id_start, entry.row_id_start);
			    WriteOptional(snapshot_id, entry.snapshot_id);
			    WriteOptional(snapshot_filter_max, entry.snapshot_filter_max);
			    WriteOptional(snapshot_filter_min, entry.snapshot_filter_min);
			    WriteIndex(mapping_id, entry.mapping_id);
			    if (entry.inlined_file_deletions.empty()) {
				    inlined_deletions.WriteNull();
			    } else {
				    auto range = inlined_deletions.WriteList(entry.inlined_file_deletions.size());
				    auto it = entry.inlined_file_deletions.begin();
				    for (auto &writer : range) {
					    writer.WriteValue(*it);
					    ++it;
				    }
			    }
			    WriteOptional(partition_id, entry.partition_id);
			    auto excluded_range = excluded.WriteList(entry.snapshot_filter_excluded.size());
			    auto excluded_it = entry.snapshot_filter_excluded.begin();
			    for (auto &writer : excluded_range) {
				    writer.WriteValue(*excluded_it);
				    ++excluded_it;
			    }
			    if (entry.partition_values.empty()) {
				    partition_values.WriteNull();
			    } else {
				    auto range = partition_values.WriteList(entry.partition_values.size());
				    auto it = entry.partition_values.begin();
				    for (auto &writer : range) {
					    writer.WriteValue([&](auto &key_index, auto &value) {
						    key_index.WriteValue(it->partition_column_idx);
						    if (it->partition_value.IsNull()) {
							    value.WriteNull();
						    } else {
							    value.WriteValue(Str(it->partition_value.ToString()));
						    }
					    });
					    ++it;
				    }
			    }
			    if (entry.column_min_max.empty()) {
				    min_max.WriteNull();
			    } else {
				    auto range = min_max.WriteList(entry.column_min_max.size());
				    auto it = entry.column_min_max.begin();
				    for (auto &writer : range) {
					    auto &stats = it->second;
					    writer.WriteValue([&](auto &column_id, auto &min, auto &max, auto &contains_nan) {
						    column_id.WriteValue(it->first);
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
					    ++it;
				    }
			    }
		    }
	    });
}

void RESTArrowFiles::Read(ClientContext &context, shared_ptr<const string> data, idx_t offset, idx_t size,
                          vector<DuckLakeFileListEntry> &result) {
	ReadChunks(context, std::move(data), offset, size, FileListTypes(), [&](DataChunk &chunk) {
		auto data_file_id = chunk.data[0].Values<uint64_t>();
		auto file_id = chunk.data[1].Values<uint64_t>();
		auto file = chunk.data[2].Values<FileRow>();
		auto delete_file = chunk.data[3].Values<FileRow>();
		auto row_id_start = chunk.data[4].Values<uint64_t>();
		auto snapshot_id = chunk.data[5].Values<uint64_t>();
		auto snapshot_filter_max = chunk.data[6].Values<uint64_t>();
		auto snapshot_filter_min = chunk.data[7].Values<uint64_t>();
		auto mapping_id = chunk.data[8].Values<uint64_t>();
		auto inlined_deletions = chunk.data[9].Values<VectorListType<uint64_t>>();
		auto min_max = chunk.data[10].Values<VectorListType<MinMaxRow>>();
		auto partition_id = chunk.data[11].Values<uint64_t>();
		auto partition_values = chunk.data[12].Values<VectorListType<PartitionValueRow>>();
		auto excluded = chunk.data[13].Values<VectorListType<uint64_t>>();
		for (idx_t i = 0; i < chunk.size(); i++) {
			DuckLakeFileListEntry entry;
			entry.data_file_id = ReadOptional(data_file_id[i]);
			entry.file_id = ReadIndex<DataFileIndex>(file_id[i]);
			entry.file = ReadFile(file[i]);
			entry.delete_file = ReadFile(delete_file[i]);
			entry.row_id_start = ReadOptional(row_id_start[i]);
			entry.snapshot_id = ReadOptional(snapshot_id[i]);
			entry.snapshot_filter_max = ReadOptional(snapshot_filter_max[i]);
			entry.snapshot_filter_min = ReadOptional(snapshot_filter_min[i]);
			entry.mapping_id = ReadIndex<MappingIndex>(mapping_id[i]);
			auto deletions = inlined_deletions[i];
			if (deletions.IsValid()) {
				for (auto child : deletions.GetChildValues()) {
					entry.inlined_file_deletions.insert(child.GetValue());
				}
			}
			entry.partition_id = ReadOptional(partition_id[i]);
			auto excluded_snapshots = excluded[i];
			if (excluded_snapshots.IsValid()) {
				for (auto child : excluded_snapshots.GetChildValues()) {
					entry.snapshot_filter_excluded.push_back(child.GetValue());
				}
			}
			auto values = partition_values[i];
			if (values.IsValid()) {
				for (auto child : values.GetChildValues()) {
					DuckLakeFilePartition partition_value;
					partition_value.partition_column_idx = child.GetChildValue<0>().GetValue();
					auto text = child.GetChildValue<1>();
					partition_value.partition_value =
					    text.IsValid() ? Value(text.GetValue().GetString()) : Value(LogicalType::VARCHAR);
					entry.partition_values.push_back(std::move(partition_value));
				}
			}
			auto stats_list = min_max[i];
			if (stats_list.IsValid()) {
				for (auto stats : stats_list.GetChildValues()) {
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
			}
			result.push_back(std::move(entry));
		}
	});
}

//===--------------------------------------------------------------------===//
// FileListExtendedEntry
//===--------------------------------------------------------------------===//
static vector<string> ExtendedNames() {
	return {"file_id",
	        "delete_file_id",
	        "file",
	        "delete_file",
	        "row_id_start",
	        "snapshot_id",
	        "delete_file_begin_snapshot",
	        "row_count",
	        "mapping_id"};
}

static vector<LogicalType> ExtendedTypes() {
	return {LogicalType::UBIGINT, LogicalType::UBIGINT, FileType(),           FileType(),          LogicalType::UBIGINT,
	        LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT};
}

string RESTArrowFiles::Write(ClientContext &context, const vector<DuckLakeFileListExtendedEntry> &files) {
	return WriteEntries(
	    context, ExtendedNames(), ExtendedTypes(), files,
	    [](DataChunk &chunk, const vector<DuckLakeFileListExtendedEntry> &entries, idx_t start, idx_t count) {
		    auto file_id = FlatVector::Writer<uint64_t>(chunk.data[0], count);
		    auto delete_file_id = FlatVector::Writer<uint64_t>(chunk.data[1], count);
		    auto file = FlatVector::Writer<FileRow>(chunk.data[2], count);
		    auto delete_file = FlatVector::Writer<FileRow>(chunk.data[3], count);
		    auto row_id_start = FlatVector::Writer<uint64_t>(chunk.data[4], count);
		    auto snapshot_id = FlatVector::Writer<uint64_t>(chunk.data[5], count);
		    auto delete_begin = FlatVector::Writer<uint64_t>(chunk.data[6], count);
		    auto row_count = FlatVector::Writer<uint64_t>(chunk.data[7], count);
		    auto mapping_id = FlatVector::Writer<uint64_t>(chunk.data[8], count);
		    for (idx_t i = 0; i < count; i++) {
			    auto &entry = entries[start + i];
			    WriteIndex(file_id, entry.file_id);
			    WriteIndex(delete_file_id, entry.delete_file_id);
			    WriteFile(file, entry.file);
			    WriteOptionalFile(delete_file, entry.delete_file);
			    WriteOptional(row_id_start, entry.row_id_start);
			    WriteOptional(snapshot_id, entry.snapshot_id);
			    WriteOptional(delete_begin, entry.delete_file_begin_snapshot);
			    row_count.WriteValue(entry.row_count);
			    WriteIndex(mapping_id, entry.mapping_id);
		    }
	    });
}

void RESTArrowFiles::Read(ClientContext &context, shared_ptr<const string> data, idx_t offset, idx_t size,
                          vector<DuckLakeFileListExtendedEntry> &result) {
	ReadChunks(context, std::move(data), offset, size, ExtendedTypes(), [&](DataChunk &chunk) {
		auto file_id = chunk.data[0].Values<uint64_t>();
		auto delete_file_id = chunk.data[1].Values<uint64_t>();
		auto file = chunk.data[2].Values<FileRow>();
		auto delete_file = chunk.data[3].Values<FileRow>();
		auto row_id_start = chunk.data[4].Values<uint64_t>();
		auto snapshot_id = chunk.data[5].Values<uint64_t>();
		auto delete_begin = chunk.data[6].Values<uint64_t>();
		auto row_count = chunk.data[7].Values<uint64_t>();
		auto mapping_id = chunk.data[8].Values<uint64_t>();
		for (idx_t i = 0; i < chunk.size(); i++) {
			DuckLakeFileListExtendedEntry entry;
			entry.file_id = ReadIndex<DataFileIndex>(file_id[i]);
			entry.delete_file_id = ReadIndex<DataFileIndex>(delete_file_id[i]);
			entry.file = ReadFile(file[i]);
			entry.delete_file = ReadFile(delete_file[i]);
			entry.row_id_start = ReadOptional(row_id_start[i]);
			entry.snapshot_id = ReadOptional(snapshot_id[i]);
			entry.delete_file_begin_snapshot = ReadOptional(delete_begin[i]);
			entry.row_count = row_count[i].GetValue();
			entry.mapping_id = ReadIndex<MappingIndex>(mapping_id[i]);
			result.push_back(std::move(entry));
		}
	});
}

//===--------------------------------------------------------------------===//
// DeleteScanEntry
//===--------------------------------------------------------------------===//
static vector<string> DeleteScanNames() {
	return {"file",         "delete_file", "previous_delete_file",  "row_count",    "row_id_start",
	        "mapping_id",   "snapshot_id", "insert_snapshot",       "file_deleted", "start_snapshot",
	        "end_snapshot", "file_id",     "inlined_file_deletions"};
}

static vector<LogicalType> DeleteScanTypes() {
	return {FileType(),
	        FileType(),
	        FileType(),
	        LogicalType::UBIGINT,
	        LogicalType::UBIGINT,
	        LogicalType::UBIGINT,
	        LogicalType::UBIGINT,
	        LogicalType::UBIGINT,
	        LogicalType::BOOLEAN,
	        LogicalType::UBIGINT,
	        LogicalType::UBIGINT,
	        LogicalType::UBIGINT,
	        LogicalType::LIST(InlinedDeletionType())};
}

string RESTArrowFiles::Write(ClientContext &context, const vector<DuckLakeDeleteScanEntry> &files) {
	return WriteEntries(context, DeleteScanNames(), DeleteScanTypes(), files,
	                    [](DataChunk &chunk, const vector<DuckLakeDeleteScanEntry> &entries, idx_t start, idx_t count) {
		                    auto file = FlatVector::Writer<FileRow>(chunk.data[0], count);
		                    auto delete_file = FlatVector::Writer<FileRow>(chunk.data[1], count);
		                    auto previous_delete_file = FlatVector::Writer<FileRow>(chunk.data[2], count);
		                    auto row_count = FlatVector::Writer<uint64_t>(chunk.data[3], count);
		                    auto row_id_start = FlatVector::Writer<uint64_t>(chunk.data[4], count);
		                    auto mapping_id = FlatVector::Writer<uint64_t>(chunk.data[5], count);
		                    auto snapshot_id = FlatVector::Writer<uint64_t>(chunk.data[6], count);
		                    auto insert_snapshot = FlatVector::Writer<uint64_t>(chunk.data[7], count);
		                    auto file_deleted = FlatVector::Writer<bool>(chunk.data[8], count);
		                    auto start_snapshot = FlatVector::Writer<uint64_t>(chunk.data[9], count);
		                    auto end_snapshot = FlatVector::Writer<uint64_t>(chunk.data[10], count);
		                    auto file_id = FlatVector::Writer<uint64_t>(chunk.data[11], count);
		                    auto inlined_deletions =
		                        FlatVector::Writer<VectorListType<InlinedDeletionRow>>(chunk.data[12], count);
		                    for (idx_t i = 0; i < count; i++) {
			                    auto &entry = entries[start + i];
			                    WriteFile(file, entry.file);
			                    WriteOptionalFile(delete_file, entry.delete_file);
			                    WriteOptionalFile(previous_delete_file, entry.previous_delete_file);
			                    row_count.WriteValue(entry.row_count);
			                    WriteOptional(row_id_start, entry.row_id_start);
			                    WriteIndex(mapping_id, entry.mapping_id);
			                    WriteOptional(snapshot_id, entry.snapshot_id);
			                    WriteOptional(insert_snapshot, entry.insert_snapshot);
			                    file_deleted.WriteValue(entry.file_deleted);
			                    WriteOptional(start_snapshot, entry.start_snapshot);
			                    WriteOptional(end_snapshot, entry.end_snapshot);
			                    WriteIndex(file_id, entry.file_id);
			                    if (entry.inlined_file_deletions.empty()) {
				                    inlined_deletions.WriteNull();
			                    } else {
				                    auto range = inlined_deletions.WriteList(entry.inlined_file_deletions.size());
				                    auto it = entry.inlined_file_deletions.begin();
				                    for (auto &writer : range) {
					                    writer.WriteValue([&](auto &row_id, auto &deleted_in) {
						                    row_id.WriteValue(it->first);
						                    deleted_in.WriteValue(it->second);
					                    });
					                    ++it;
				                    }
			                    }
		                    }
	                    });
}

void RESTArrowFiles::Read(ClientContext &context, shared_ptr<const string> data, idx_t offset, idx_t size,
                          vector<DuckLakeDeleteScanEntry> &result) {
	ReadChunks(context, std::move(data), offset, size, DeleteScanTypes(), [&](DataChunk &chunk) {
		auto file = chunk.data[0].Values<FileRow>();
		auto delete_file = chunk.data[1].Values<FileRow>();
		auto previous_delete_file = chunk.data[2].Values<FileRow>();
		auto row_count = chunk.data[3].Values<uint64_t>();
		auto row_id_start = chunk.data[4].Values<uint64_t>();
		auto mapping_id = chunk.data[5].Values<uint64_t>();
		auto snapshot_id = chunk.data[6].Values<uint64_t>();
		auto insert_snapshot = chunk.data[7].Values<uint64_t>();
		auto file_deleted = chunk.data[8].Values<bool>();
		auto start_snapshot = chunk.data[9].Values<uint64_t>();
		auto end_snapshot = chunk.data[10].Values<uint64_t>();
		auto file_id = chunk.data[11].Values<uint64_t>();
		auto inlined_deletions = chunk.data[12].Values<VectorListType<InlinedDeletionRow>>();
		for (idx_t i = 0; i < chunk.size(); i++) {
			DuckLakeDeleteScanEntry entry;
			entry.file = ReadFile(file[i]);
			entry.delete_file = ReadFile(delete_file[i]);
			entry.previous_delete_file = ReadFile(previous_delete_file[i]);
			entry.row_count = row_count[i].GetValue();
			entry.row_id_start = ReadOptional(row_id_start[i]);
			entry.mapping_id = ReadIndex<MappingIndex>(mapping_id[i]);
			entry.snapshot_id = ReadOptional(snapshot_id[i]);
			entry.insert_snapshot = ReadOptional(insert_snapshot[i]);
			entry.file_deleted = file_deleted[i].GetValue();
			entry.start_snapshot = ReadOptional(start_snapshot[i]);
			entry.end_snapshot = ReadOptional(end_snapshot[i]);
			entry.file_id = ReadIndex<DataFileIndex>(file_id[i]);
			auto deletions = inlined_deletions[i];
			if (deletions.IsValid()) {
				for (auto child : deletions.GetChildValues()) {
					entry.inlined_file_deletions.emplace(child.GetChildValue<0>().GetValue(),
					                                     child.GetChildValue<1>().GetValue());
				}
			}
			result.push_back(std::move(entry));
		}
	});
}

} // namespace duckdb
