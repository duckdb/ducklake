//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

struct DuckLakeConstants {
	static constexpr const idx_t TRANSACTION_LOCAL_ID_START = 9223372036854775808ULL;
};

struct SchemaIndex {
	SchemaIndex() : index(DConstants::INVALID_INDEX) {
	}
	explicit SchemaIndex(idx_t index) : index(index) {
	}

	idx_t index;

	inline bool operator==(const SchemaIndex &rhs) const {
		return index == rhs.index;
	};
	inline bool operator!=(const SchemaIndex &rhs) const {
		return index != rhs.index;
	};
	inline bool operator<(const SchemaIndex &rhs) const {
		return index < rhs.index;
	};
	bool IsValid() const {
		return index != DConstants::INVALID_INDEX;
	}
	bool IsTransactionLocal() const {
		D_ASSERT(IsValid());
		return index >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START;
	}
};

struct TableIndex {
	TableIndex() : index(DConstants::INVALID_INDEX) {
	}
	explicit TableIndex(idx_t index) : index(index) {
	}

	idx_t index;

	inline bool operator==(const TableIndex &rhs) const {
		return index == rhs.index;
	};
	inline bool operator!=(const TableIndex &rhs) const {
		return index != rhs.index;
	};
	inline bool operator<(const TableIndex &rhs) const {
		return index < rhs.index;
	};
	bool IsValid() const {
		return index != DConstants::INVALID_INDEX;
	}
	bool IsTransactionLocal() const {
		D_ASSERT(IsValid());
		return index >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START;
	}
};

struct MacroIndex {
	MacroIndex() : index(DConstants::INVALID_INDEX) {
	}
	explicit MacroIndex(idx_t index) : index(index) {
	}

	idx_t index;

	inline bool operator==(const MacroIndex &rhs) const {
		return index == rhs.index;
	};
	inline bool operator!=(const MacroIndex &rhs) const {
		return index != rhs.index;
	};
	inline bool operator<(const MacroIndex &rhs) const {
		return index < rhs.index;
	};
	bool IsValid() const {
		return index != DConstants::INVALID_INDEX;
	}
	bool IsTransactionLocal() const {
		D_ASSERT(IsValid());
		return index >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START;
	}
};

struct FieldIndex {
	FieldIndex() : index(DConstants::INVALID_INDEX) {
	}
	explicit FieldIndex(idx_t index) : index(index) {
	}

	idx_t index;

	inline bool operator==(const FieldIndex &rhs) const {
		return index == rhs.index;
	};
	inline bool operator!=(const FieldIndex &rhs) const {
		return index != rhs.index;
	};
	inline bool operator<(const FieldIndex &rhs) const {
		return index < rhs.index;
	};
	bool IsValid() const {
		return index != DConstants::INVALID_INDEX;
	}
};

struct DataFileIndex {
	DataFileIndex() : index(DConstants::INVALID_INDEX) {
	}
	explicit DataFileIndex(idx_t index) : index(index) {
	}

	idx_t index;

	inline bool operator==(const DataFileIndex &rhs) const {
		return index == rhs.index;
	};
	inline bool operator!=(const DataFileIndex &rhs) const {
		return index != rhs.index;
	};
	inline bool operator<(const DataFileIndex &rhs) const {
		return index < rhs.index;
	};
	bool IsValid() const {
		return index != DConstants::INVALID_INDEX;
	}
};

struct MappingIndex {
	MappingIndex() : index(DConstants::INVALID_INDEX) {
	}
	explicit MappingIndex(idx_t index) : index(index) {
	}

	idx_t index;

	inline bool operator==(const MappingIndex &rhs) const {
		return index == rhs.index;
	};
	inline bool operator!=(const MappingIndex &rhs) const {
		return index != rhs.index;
	};
	inline bool operator<(const MappingIndex &rhs) const {
		return index < rhs.index;
	};
	bool IsValid() const {
		return index != DConstants::INVALID_INDEX;
	}
};

} // namespace duckdb
