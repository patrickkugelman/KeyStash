#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "engine/store.h"

namespace persist {

// "KVSNAP01" u64 next_wal_seq, entries, footer
//   entry:  u32 klen | key | u32 vlen | value | i64 expire_ms (-1 = none)
//   footer: u32 0xFFFFFFFF | u64 count | u32 crc32 of everything before the crc
// next_wal_seq = first wal file the snapshot does not cover
inline constexpr char kSnapshotMagic[8] = {'K', 'V', 'S', 'N', 'A', 'P', '0', '1'};
inline constexpr uint32_t kSnapshotFooterMarker = 0xFFFFFFFFu;
inline constexpr uint32_t kMaxSnapshotString = 512u * 1024u * 1024u;

// serialized in-memory copy of the keyspace. capturing it runs on the loop
// (that is the pause), writing it can run on any thread
struct SnapshotBlob {
  uint64_t next_wal_seq = 0;
  uint64_t count = 0;
  uint64_t bytes = 0;
  std::vector<std::string> chunks;  // header + entries in file order
};

SnapshotBlob capture_snapshot(engine::Store& store, uint64_t next_wal_seq);

// writes snapshot.tmp, fsyncs, renames over snapshot.bin. never touches the
// store so any thread can run it. on failure the old snapshot stays
bool write_snapshot(const SnapshotBlob& blob, const std::string& dir, uint64_t* file_bytes,
                    std::string* error);

// throws if the file is damaged
void load_snapshot(const std::string& path, engine::Store& store, uint64_t* next_wal_seq,
                   uint64_t* entries);

}  // namespace persist
