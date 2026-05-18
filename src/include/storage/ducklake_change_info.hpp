//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_change_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/set.hpp"
#include "common/index.hpp"

namespace duckdb {

enum class ChangeKind : uint8_t {
	NONE,
	INSERTED_INTO,
};

template <class T, ChangeKind Kind = ChangeKind::NONE>
class ChangeInfo {
public:
	ChangeInfo() = default;
	explicit ChangeInfo(T data_p) : data(std::move(data_p)) {
	}

	string Serialize(const string &uuid_suffix) const {
		return "";
	}
	string Drop(const string &uuid_suffix) const {
		return "";
	}

	T &Items() {
		return data;
	}
	const T &Items() const {
		return data;
	}

private:
	T data;
};

} // namespace duckdb
