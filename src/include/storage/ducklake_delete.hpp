//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_delete_filter.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "common/ducklake_data_file.hpp"

namespace duckdb {
class DuckLakeTableEntry;
class DuckLakeDeleteGlobalState;
class DuckLakeTransaction;

//! Input parameters for writing a delete file
struct WriteDeleteFileInput {
	ClientContext &context;
	DuckLakeTransaction &transaction;
	FileSystem &fs;
	string data_path;
	string encryption_key;
	string data_file_path;
	set<idx_t> positions;
	optional_idx begin_snapshot;
	optional_idx end_snapshot;
};

//! Utility class for writing delete files
struct DuckLakeDeleteFileWriter {
	//! Write a delete file and return the resulting DuckLakeDeleteFile
	static DuckLakeDeleteFile WriteDeleteFile(WriteDeleteFileInput &input);
};

struct DuckLakeDeleteMap {
	void AddExtendedFileInfo(DuckLakeFileListExtendedEntry file_entry) {
		auto filename = file_entry.file.path;
		file_map.emplace(std::move(filename), std::move(file_entry));
	}

	DuckLakeFileListExtendedEntry GetExtendedFileInfo(const string &filename) {
		auto delete_entry = file_map.find(filename);
		if (delete_entry == file_map.end()) {
			throw InternalException("Could not find matching file for written delete file");
		}
		return delete_entry->second;
	}

	optional_ptr<DuckLakeDeleteData> GetDeleteData(const string &filename) {
		lock_guard<mutex> guard(lock);
		auto entry = delete_data_map.find(filename);
		if (entry == delete_data_map.end()) {
			return nullptr;
		}
		return entry->second.get();
	}

	void ClearDeletes(const string &filename) {
		lock_guard<mutex> guard(lock);
		delete_data_map.erase(filename);
	}

	void AddDeleteData(const string &filename, shared_ptr<DuckLakeDeleteData> delete_data) {
		lock_guard<mutex> guard(lock);
		delete_data_map.emplace(filename, std::move(delete_data));
	}

private:
	mutex lock;
	unordered_map<string, DuckLakeFileListExtendedEntry> file_map;
	unordered_map<string, shared_ptr<DuckLakeDeleteData>> delete_data_map;
};

class DuckLakeDelete : public PhysicalOperator {
public:
	DuckLakeDelete(PhysicalPlan &physical_plan, DuckLakeTableEntry &table, PhysicalOperator &child,
	               shared_ptr<DuckLakeDeleteMap> delete_map, vector<idx_t> row_id_indexes, string encryption_key,
	               bool allow_duplicates);

	//! The table to delete from
	DuckLakeTableEntry &table;
	//! A map of filename -> data file index and filename -> delete data
	shared_ptr<DuckLakeDeleteMap> delete_map;
	//! The column indexes for the relevant row-id columns
	vector<idx_t> row_id_indexes;
	//! The encryption key used to encrypt the written files
	string encryption_key;
	//! Whether or not we allow duplicate deletes
	bool allow_duplicates;

public:
	// // Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	static PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
	                                    DuckLakeTableEntry &table, PhysicalOperator &child_plan,
	                                    vector<idx_t> row_id_indexes, string encryption_key,
	                                    bool allow_duplicates = true);

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
	void FlushDelete(DuckLakeTransaction &transaction, ClientContext &context, DuckLakeDeleteGlobalState &global_state,
	                 const string &filename, ColumnDataCollection &deleted_rows) const;
};

} // namespace duckdb
