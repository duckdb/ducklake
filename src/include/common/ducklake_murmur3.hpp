//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_murmur3.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

//! Murmur3 x86 32-bit hash, compatible with the Apache Iceberg specification.
//! Reference: https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
struct DuckLakeMurmur3 {
	static int32_t Hash(const uint8_t *data, idx_t len, uint32_t seed = 0) {
		const idx_t nblocks = len / 4;
		uint32_t h1 = seed;

		const uint32_t c1 = 0xcc9e2d51;
		const uint32_t c2 = 0x1b873593;

		// body
		auto blocks = reinterpret_cast<const uint32_t *>(data);
		for (idx_t i = 0; i < nblocks; i++) {
			uint32_t k1;
			memcpy(&k1, &blocks[i], sizeof(uint32_t));

			k1 *= c1;
			k1 = (k1 << 15) | (k1 >> 17);
			k1 *= c2;

			h1 ^= k1;
			h1 = (h1 << 13) | (h1 >> 19);
			h1 = h1 * 5 + 0xe6546b64;
		}

		// tail
		auto tail = data + nblocks * 4;
		uint32_t k1 = 0;
		switch (len & 3) {
		case 3:
			k1 ^= static_cast<uint32_t>(tail[2]) << 16;
			DUCKDB_EXPLICIT_FALLTHROUGH;
		case 2:
			k1 ^= static_cast<uint32_t>(tail[1]) << 8;
			DUCKDB_EXPLICIT_FALLTHROUGH;
		case 1:
			k1 ^= static_cast<uint32_t>(tail[0]);
			k1 *= c1;
			k1 = (k1 << 15) | (k1 >> 17);
			k1 *= c2;
			h1 ^= k1;
		}

		// finalization
		h1 ^= static_cast<uint32_t>(len);
		// fmix32
		h1 ^= h1 >> 16;
		h1 *= 0x85ebca6b;
		h1 ^= h1 >> 13;
		h1 *= 0xc2b2ae35;
		h1 ^= h1 >> 16;

		return static_cast<int32_t>(h1);
	}

	//! Hash a value by its byte representation
	template <class T>
	static int32_t HashValue(T value) {
		return Hash(reinterpret_cast<const uint8_t *>(&value), sizeof(T));
	}
};

} // namespace duckdb
