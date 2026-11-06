#pragma once

#include <cstdint>
#include <string>

#include "engine/store.h"

namespace persist {

// File layout: "KVSNAP01" + u64 next_wal_seq, then entries
//   u32 klen | key | u32 vlen | value | i64 expire_at_ms (-1 = none)
// then a footer: u32 0xFFFFFFFF | u64 entry_count | u32 crc32(all bytes before the crc).
// next_wal_seq is the first WAL file the snapshot does not already include.
inline constexpr char kSnapshotMagic[8] = {'K', 'V', 'S', 'N', 'A', 'P', '0', '1'};
inline constexpr uint32_t kSnapshotFooterMarker = 0xFFFFFFFFu;
inline constexpr uint32_t kMaxSnapshotString = 512u * 1024u * 1024u;

// Writes `<dir>/snapshot.tmp`, syncs it and renames it over `<dir>/snapshot.bin`.
// Returns false with `error` set on I/O failure, leaving any old snapshot intact.
bool write_snapshot(engine::Store& store, const std::string& dir, uint64_t next_wal_seq,
                    uint64_t* bytes, std::string* error);

// Loads into `store`. Throws std::runtime_error if the file is damaged.
void load_snapshot(const std::string& path, engine::Store& store, uint64_t* next_wal_seq,
                   uint64_t* entries);

}  // namespace persist
