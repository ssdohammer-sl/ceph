// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Myoungwon Oh <ohmyoungwon@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common.h"

struct EstimateResult {
  std::unique_ptr<CDC> cdc;

  uint64_t chunk_size;

  ceph::mutex lock = ceph::make_mutex("EstimateResult::lock");

  // < key, <count, chunk_size> >
  map< string, pair <uint64_t, uint64_t> > chunk_statistics;
  uint64_t total_bytes = 0;
  std::atomic<uint64_t> total_objects = {0};

  EstimateResult(std::string alg, int chunk_size)
    : cdc(CDC::create(alg, chunk_size)),
      chunk_size(1ull << chunk_size) {}

  void add_chunk(bufferlist& chunk, const std::string& fp_algo) {
    string fp = generate_fp(chunk, fp_algo);

    std::lock_guard l(lock);
    auto p = chunk_statistics.find(fp);
    if (p != chunk_statistics.end()) {
      p->second.first++;
      if (p->second.second != chunk.length()) {
	cerr << "warning: hash collision on " << fp
	     << ": was " << p->second.second
	     << " now " << chunk.length() << std::endl;
      }
    } else {
      chunk_statistics[fp] = make_pair(1, chunk.length());
    }
    total_bytes += chunk.length();
  }

  string generate_fp(bufferlist& chunk, const string& fp_algo) {
    string fp;
    if (fp_algo == "sha1") {
      sha1_digest_t sha1_val = crypto::digest<crypto::SHA1>(chunk);
      fp = sha1_val.to_str();
    } else if (fp_algo == "sha256") {
      sha256_digest_t sha256_val = crypto::digest<crypto::SHA256>(chunk);
      fp = sha256_val.to_str();
    } else if (fp_algo == "sha512") {
      sha512_digest_t sha512_val = crypto::digest<crypto::SHA512>(chunk);
      fp = sha512_val.to_str();
    } else {
      ceph_assert(0 == "no support fingerperint algorithm");
    }
    return fp;
  }

  vector<string> get_fps(bufferlist& obj,
      const vector<pair<uint64_t, uint64_t>>& chunk_ranges,
      const string fp_algo) {
    vector<string> fps;
    for (auto& r : chunk_ranges) {
      bufferlist chunk;
      chunk.substr_of(obj, r.first, r.second);
      fps.emplace_back(generate_fp(chunk, fp_algo));
      add_chunk(chunk, fp_algo);
    }
    return fps;
  }

  void dump(Formatter *f) const {
    f->dump_unsigned("target_chunk_size", chunk_size);

    uint64_t dedup_bytes = 0;
    uint64_t dedup_objects = chunk_statistics.size();
    for (auto& j : chunk_statistics) {
      dedup_bytes += j.second.second;
    }
    //f->dump_unsigned("dedup_bytes", dedup_bytes);
    //f->dump_unsigned("original_bytes", total_bytes);
    f->dump_float("dedup_bytes_ratio",
		  (double)dedup_bytes / (double)total_bytes);
    f->dump_float("dedup_objects_ratio",
		  (double)dedup_objects / (double)total_objects);

    uint64_t avg = total_bytes / dedup_objects;
    uint64_t sqsum = 0;
    for (auto& j : chunk_statistics) {
      sqsum += (avg - j.second.second) * (avg - j.second.second);
    }
    uint64_t stddev = sqrt(sqsum / dedup_objects);
    f->dump_unsigned("chunk_size_average", avg);
    f->dump_unsigned("chunk_size_stddev", stddev);
  }
};

map<uint64_t, EstimateResult> dedup_estimates;  // chunk size -> result
ceph::mutex glock = ceph::make_mutex("glock");

po::options_description make_usage() {
  po::options_description desc("Usage");
  desc.add_options()
    ("help,h", ": produce help message")
    ("op estimate --pool <POOL> --chunk-size <CHUNK_SIZE> --chunk-algorithm <ALGO> --fingerprint-algorithm <FP_ALGO>", 
     ": estimate how many chunks are redundant")
    ("op chunk-scrub --chunk-pool <POOL>",
     ": perform chunk scrub")
    ("op chunk-get-ref --chunk-pool <POOL> --object <OID> --target-ref <OID> --target-ref-pool-id <POOL_ID>",
     ": get chunk object's reference")
    ("op chunk-put-ref --chunk-pool <POOL> --object <OID> --target-ref <OID> --target-ref-pool-id <POOL_ID>",
     ": put chunk object's reference")
    ("op chunk-repair --chunk-pool <POOL> --object <OID> --target-ref <OID> --target-ref-pool-id <POOL_ID>",
     ": fix mismatched references")
    ("op dump-chunk-refs --chunk-pool <POOL> --object <OID>",
     ": dump chunk object's references")
    ("op chunk-dedup --pool <POOL> --object <OID> --chunk-pool <POOL> --fingerprint-algorithm <FP> --source-off <OFFSET> --source-length <LENGTH>",
     ": perform a chunk dedup---deduplicate only a chunk, which is a part of object.")
    ("op object-dedup --pool <POOL> --object <OID> --chunk-pool <POOL> --fingerprint-algorithm <FP> --dedup-cdc-chunk-size <CHUNK_SIZE> [--snap]",
     ": perform a object dedup---deduplicate the entire object, not a chunk. Related snapshots are also deduplicated if --snap is given")
    ("op enable --pool <POOL> --chunk-pool <POOL>",
     ": enable deduplication")
    ("op set-dedup-conf --pool <POOL> --option-name <OPTION NAME> --value <VALUE>",
     ": set deduplication configuration. Users must specify the target POOL")
    ;
  po::options_description op_desc("Opational arguments");
  op_desc.add_options()
    ("op", po::value<std::string>(), ": estimate|chunk-scrub|chunk-get-ref|chunk-put-ref|chunk-repair|dump-chunk-refs|chunk-dedup|object-dedup|enable|set-dedup-conf")
    ("target-ref", po::value<std::string>(), ": set target object")
    ("target-ref-pool-id", po::value<uint64_t>(), ": set target pool id")
    ("object", po::value<std::string>(), ": set object name")
    ("chunk-size", po::value<int>(), ": chunk size (byte)")
    ("chunk-algorithm", po::value<std::string>(), ": <fixed|fastcdc>, set chunk-algorithm")
    ("fingerprint-algorithm", po::value<std::string>(), ": <sha1|sha256|sha512>, set fingerprint-algorithm")
    ("chunk-pool", po::value<std::string>(), ": set chunk pool name")
    ("index-pool", po::value<std::string>(), ": set index pool name")
    ("max-thread", po::value<int>(), ": set max thread")
    ("report-period", po::value<int>(), ": set report-period")
    ("max-seconds", po::value<int>(), ": set max runtime")
    ("max-read-size", po::value<int>(), ": set max read size")
    ("pool", po::value<std::string>(), ": set pool name")
    ("min-chunk-size", po::value<int>(), ": min chunk size (byte)")
    ("max-chunk-size", po::value<int>(), ": max chunk size (byte)")
    ("source-off", po::value<uint64_t>(), ": set source offset")
    ("source-length", po::value<uint64_t>(), ": set source length")
    ("dedup-cdc-chunk-size", po::value<unsigned int>(), ": set dedup chunk size for cdc")
    ("snap", ": deduplciate snapshotted object")
    ("debug", ": enable debug")
    ("pgid", ": set pgid")
    ("daemon", ": execute sample dedup in daemon mode")
    ("option-name", po::value<std::string>(),": dedup option name, max_thread|chunk_pool|chunk_size|chunk_algorithm|fingerprint_algorithm|chunk_dedup_threshold|wakeup_period|report_period|fpstore_threshold|sampling_ratio")
    ("value", po::value<std::string>(),": set corresponding dedup option value")
    ("pack-obj-size", po::value<uint64_t>(), ": pack object size (byte)")
    ("chunk-dedup-threshold", po::value<uint64_t>(), ": chunk dedup threshold")
    ("packer-type", po::value<std::string>(), ": type of object packer")
    ("max-apo-entries", po::value<uint32_t>(), ": maximum number of ActivePackObject entries")
    ("bit-vector-len", po::value<uint32_t>(), ": bit vector length of the bloom filter")
    ("similarity-threshold", po::value<uint32_t>(), ": low threshold value of hamming similarity")
    ("do-dedup", ": store chunk data and chunk metadata object")
  ;
  desc.add(op_desc);
  return desc;
}

template <typename I, typename T>
static int rados_sistrtoll(I &i, T *val) {
  std::string err;
  *val = strict_iecstrtoll(i->second, &err);
  if (err != "") {
    cerr << "Invalid value for " << i->first << ": " << err << std::endl;
    return -EINVAL;
  } else {
    return 0;
  }
}

class EstimateDedupRatio;
class ChunkScrub;
class EstimateFragmentationRatio;
class CrawlerThread : public Thread
{
  IoCtx io_ctx;
  int n;
  int m;
  ObjectCursor begin;
  ObjectCursor end;
  ceph::mutex m_lock = ceph::make_mutex("CrawlerThread::Locker");
  ceph::condition_variable m_cond;
  int32_t report_period;
  bool m_stop = false;
  uint64_t total_bytes = 0;
  uint64_t total_objects = 0;
  uint64_t examined_objects = 0;
  uint64_t examined_bytes = 0;
  uint64_t max_read_size = 0;
  bool debug = false;
#define COND_WAIT_INTERVAL 10

public:
  CrawlerThread(IoCtx& io_ctx, int n, int m,
		ObjectCursor begin, ObjectCursor end, int32_t report_period,
		uint64_t num_objects, uint64_t max_read_size = default_op_size):
    io_ctx(io_ctx), n(n), m(m), begin(begin), end(end), 
    report_period(report_period), total_objects(num_objects), max_read_size(max_read_size)
  {}

  void signal(int signum) {
    std::lock_guard l{m_lock};
    m_stop = true;
    m_cond.notify_all();
  }
  virtual void print_status(Formatter *f, ostream &out) {}
  uint64_t get_examined_objects() { return examined_objects; }
  uint64_t get_examined_bytes() { return examined_bytes; }
  uint64_t get_total_bytes() { return total_bytes; }
  uint64_t get_total_objects() { return total_objects; }
  void set_debug(const bool debug_) { debug = debug_; }
  friend class EstimateDedupRatio;
  friend class ChunkScrub;
  friend class EstimateFragmentationRatio;
};

class Packer
{
protected:
  enum class PackerType : unsigned int {
    Simple = 1,
    Heuristic,
    None 
  } type;

  IoCtx base_ioctx;
  IoCtx chunk_ioctx;
  IoCtx index_ioctx;

  // Accessed in the estimate threads under fpmap_lock
  // <fp_value, <count, pack object id>>
  unordered_map<string, pair<uint32_t, int>> fp_store;
  ceph::shared_mutex fpmap_lock = ceph::make_shared_mutex("fpmap lock");

  uint64_t max_packobj_size;
  uint64_t dedup_threshold;
  bool debug = false;
  bool do_dedup = false;

public:
  Packer(IoCtx& base_ioctx, IoCtx& chunk_ioctx, IoCtx& index_ioctx,
      uint64_t max_packobj_size, uint64_t dedup_threshold, bool debug, bool do_dedup)
    : base_ioctx(base_ioctx), chunk_ioctx(chunk_ioctx), index_ioctx(index_ioctx),
    max_packobj_size(max_packobj_size), dedup_threshold(dedup_threshold),
      debug(debug), do_dedup(do_dedup) {}
  virtual ~Packer() {}

  virtual string pack(string fp, int chunk_size, string src_oid, uint64_t src_offset,
      uint64_t src_len, bufferlist& bl, int* poid = nullptr) = 0;

  void add(string fp) {
    unique_lock l{fpmap_lock};
    auto i = fp_store.find(fp);
    // update fp count
    if (i != fp_store.end()) {
      i->second.first++;
    } else {
      fp_store.emplace(fp, make_pair(1, -1));
    }
    l.unlock();
  }

  bool check_duplicated(string fp) {
    shared_lock l{fpmap_lock};
    auto i = fp_store.find(fp);
    l.unlock();
    if (i->second.first >= dedup_threshold) {
      return true;
    }
    return false;
  }

  pair<string, int> check_packed(string fp) {
    bufferlist bl;
    int ret = index_ioctx.getxattr(fp, "chunk_location", bl);
    if (ret < 0) {
      cout << "fp: " << fp << " is not deduped. errno: " << ret << std::endl;
      return make_pair(string(), 0);
    }

    pair<string, uint32_t> chunk_loc;
    try {
      auto iter = bl.cbegin();
      decode(chunk_loc, iter);
    } catch (buffer::error& err) {
      cerr << "get deduped_loc of " << fp << " failed" << std::endl;
      return make_pair(string(), 0);
    }
    return chunk_loc;
  }

  bool check_packed_simulate(string fp, int& oid) {
    shared_lock l{fpmap_lock};
    auto i = fp_store.find(fp);
    l.unlock();
    if (i->second.second < 0) {
      return false;
    }
    oid = i->second.second;
    return true;
  }

  string get_type() {
    switch (type) {
    case PackerType::Simple:
      return "simple";
    case PackerType::Heuristic:
      return "heuristic";
    default:
      return "none";
    }
  }
};

class SimplePacker : public Packer
{
  std::atomic<int> target_poid = 0; // pack object id
  uint64_t pack_obj_size = 0; // Accessed in the estimate threads under packing_lock
  ceph::shared_mutex packing_lock = ceph::make_shared_mutex("packing lock");

public:
  SimplePacker(IoCtx& base_ioctx, IoCtx& chunk_ioctx, IoCtx& index_ioctx,
      uint64_t max_packobj_size, uint32_t dedup_threshold, bool debug, bool do_dedup)
    : Packer(base_ioctx, chunk_ioctx, index_ioctx, max_packobj_size,
        dedup_threshold, debug, do_dedup) {
      type = PackerType::Simple;
    }
  virtual ~SimplePacker() {}

  virtual string pack(string fp, int chunk_size, string src_oid, uint64_t src_offset,
      uint64_t src_len, bufferlist& chunk_data, int* poid = nullptr) override {
    assert(*poid < 0);
    int ret;

    // check if fp is packed
    pair<string, int> chunk_loc = check_packed(fp);
    if (!chunk_loc.first.empty()) {
      if (do_dedup) {
        // do set-packed-chunk
        ObjectReadOperation rop;
        rop.set_packed_chunk(src_offset, src_len, chunk_ioctx, chunk_loc.first,
            chunk_loc.second, index_ioctx, fp, CEPH_OSD_OP_FLAG_WITH_REFERENCE);
        ret = base_ioctx.operate(src_oid, &rop, nullptr);
        if (ret < 0) {
          cerr << fp << " already set-packed-chunked. srd oid: " << src_oid << ", fp: " << fp
            << ", poid: " << chunk_loc.first << ", offset: " << chunk_loc.second << std::endl;
          return string();
        }

        if (debug) {
          cout << fp << " set-packed-chunk success on " << chunk_loc.first <<
            ", offset: " << chunk_loc.second << std::endl;
        }
      }
      return chunk_loc.first;
    }

    string po_name = "po_" + to_string(target_poid);
    if (pack_obj_size >= max_packobj_size || pack_obj_size == 0) {
      if (pack_obj_size >= max_packobj_size) {
        ++target_poid;
        pack_obj_size = 0;
        po_name = "po_" + to_string(target_poid);
      }

      if (do_dedup) {
        // create a new pack object in chunk pool
        bufferlist bl;
        ret = chunk_ioctx.write_full(po_name, bl);
        if (ret < 0) {
          cerr << "create pack object failed. " << cpp_strerror(ret) << std::endl;
          return string();
        }
      }
    }

    if (do_dedup) {
      // append chunk data in the pack object
      //ret = chunk_ioctx.append(po_name, chunk_data, chunk_data.length());
      ret = chunk_ioctx.write(po_name, chunk_data, chunk_data.length(), pack_obj_size);
      if (ret < 0) {
        cerr << "append chunk data into " << po_name << " failed. "
          << cpp_strerror(ret) << std::endl;
        return string();
      }

      // create a new chunk metadata object in index pool
      bufferlist bl;
      ret = index_ioctx.write_full(fp, bl);
      if (ret < 0) {
        cerr << "create chunk metadata object failed. " << cpp_strerror(ret) << std::endl;
        return string();
      }

      // write packobj location in chunk metadata object's xattr
      bufferlist loc;
      encode(make_pair(po_name, pack_obj_size), loc);
      ret = index_ioctx.setxattr(fp, "chunk_location", loc);

      // do set-packed-chunk
      ObjectReadOperation rop;
      rop.set_packed_chunk(src_offset, src_len, chunk_ioctx, po_name,
          pack_obj_size, index_ioctx, fp, CEPH_OSD_OP_FLAG_WITH_REFERENCE);
      ret = base_ioctx.operate(src_oid, &rop, nullptr);
      if (ret < 0) {
        cerr << "set-chunk failed. srd oid: " << src_oid << ", fp: " << fp
          << ", poid: " << po_name << std::endl;
        return string();
      }
    }

    /*
    unique_lock fl(fpmap_lock);
    auto entry = fp_store.find(fp);
    entry->second.second = target_poid;
    fl.unlock();
    */
    pack_obj_size += chunk_size;

    return po_name;
  }
};

class HeuristicPacker : public Packer
{
public:
  std::atomic<int> poid_idx = 0;
  uint32_t max_apo_entries = 0;
  map<int, pair<bloom_filter, uint64_t>> active_pack_objs;
  ceph::shared_mutex apo_lock = ceph::make_shared_mutex("apo lock");

  uint32_t bv_len;
  uint32_t similarity_threshold;

public:
  HeuristicPacker(IoCtx& base_ioctx, IoCtx& chunk_ioctx, IoCtx& index_ioctx,
      uint64_t max_packobj_size, uint32_t dedup_threshold,
      bool debug, uint32_t max_apo_entries, uint32_t bv_len,
      uint32_t similarity_threshold, bool do_dedup)
    : Packer(base_ioctx, chunk_ioctx, index_ioctx,
        max_packobj_size, dedup_threshold, debug, do_dedup),
      max_apo_entries(max_apo_entries), bv_len(bv_len),
      similarity_threshold(similarity_threshold) {
      type = PackerType::Heuristic;
    }
  virtual ~HeuristicPacker() {}

  virtual string pack(string fp, int chunk_size, string src_oid, uint64_t src_offset,
      uint64_t src_len, bufferlist& chunk_data, int* poid = nullptr) override {
    int ret = 0;
    // check if fp is packed
    pair<string, int> chunk_loc = check_packed(fp);
    if (!chunk_loc.first.empty()) {
      if (do_dedup) {
        // do set-packed-chunk
        ObjectReadOperation rop;
        rop.set_packed_chunk(src_offset, src_len, chunk_ioctx, chunk_loc.first,
            chunk_loc.second, index_ioctx, fp, CEPH_OSD_OP_FLAG_WITH_REFERENCE);
        ret = base_ioctx.operate(src_oid, &rop, nullptr);
        if (ret < 0) {
          cerr << "already set-packed-chunked. srd oid: " << src_oid << ", fp: " << fp
            << ", poid: " << chunk_loc.first << ", offset: " << chunk_loc.second << std::endl;
          return string();
        }

        if (debug) {
          cout << fp << " set-packed-chunk success on " << chunk_loc.first <<
            ", offset: " << chunk_loc.second << std::endl;
        }
      }
      return chunk_loc.first;
    }

    int target_poid = *poid;
    unique_lock al(apo_lock);
    string po_name = "po_" + to_string(target_poid);
    if (active_pack_objs.find(target_poid) == active_pack_objs.end()) {
      // create pack object
      bufferlist bl;
      ret = chunk_ioctx.write_full(po_name, bl);
      if (ret < 0) {
        cerr << "create pack object failed. " << cpp_strerror(ret) << std::endl;
        al.unlock();
        return string();
      }

      active_pack_objs.emplace(target_poid,
          make_pair(generate_bf(vector<string>(), chunk_size), 0));
      ++poid_idx;
    }

    // target pack object is full
    pair<bloom_filter, uint64_t>& apo_entry = active_pack_objs[target_poid];
    if (apo_entry.second >= max_packobj_size) {
      active_pack_objs.erase(target_poid);
      al.unlock();
      return "FULL";
    }

    if (do_dedup) {
      // write chunk data in the pack object
      ret = chunk_ioctx.write(po_name, chunk_data, chunk_data.length(), apo_entry.second);
      if (ret < 0) {
        cerr << "append chunk data into " << po_name << " failed. "
          << cpp_strerror(ret) << std::endl;
        al.unlock();
        return string();
      }

      // create chunk metadata object
      bufferlist bl;
      ret = index_ioctx.write_full(fp, bl);
      if (ret < 0) {
        cerr << "create chunk metadata object failed. " << cpp_strerror(ret)
          << std::endl;
        al.unlock();
        return string();
      }

      // write packobj location in chunk metadata object's xattr
      bufferlist loc;
      encode(make_pair(po_name, apo_entry.second), loc);
      ret = index_ioctx.setxattr(fp, "chunk_location", loc);

      // do set-packed-chunk
      ObjectReadOperation rop;
      rop.set_packed_chunk(src_offset, src_len, chunk_ioctx, po_name,
          apo_entry.second, index_ioctx, fp, CEPH_OSD_OP_FLAG_WITH_REFERENCE);
      ret = base_ioctx.operate(src_oid, &rop, nullptr);
      if (ret < 0) {
        cerr << "set-chunk failed. srd oid: " << src_oid << ", fp: " << fp
          << ", poid: " << po_name << std::endl;
        al.unlock();
        return string();
      }
      cout << fp << " set-packed-chunk on poid: " << po_name << ", offset: " << apo_entry.second << std::endl;
    }

    // update apo entry
    apo_entry.first.insert(fp);
    apo_entry.second += chunk_size;
    al.unlock();

    /*
    unique_lock fl(fpmap_lock);
    auto fp_entry = fp_store.find(fp);
    ceph_assert(fp_entry != fp_store.end());
    fp_entry->second.second = target_poid;
    fl.unlock();
    */
    return po_name;
  }

  bloom_filter generate_bf(const vector<string>& fps, uint64_t chunk_size) {
    uint64_t expected_cnt = max_packobj_size / chunk_size;
    bloom_filter bf(1, bv_len, 0, expected_cnt * 10);
    for (auto& fp : fps) {
      bf.insert(fp);
    }
    return bf;
  }

  uint32_t get_hamming_similarity(bloom_filter& mo_bf, bloom_filter& entry_bf) {
    uint32_t score = 0;
    // bit-vector of the metadata object
    const unsigned char* mo_bv = mo_bf.table();
    // bit-vector of the APO entry
    const unsigned char* entry_bv = entry_bf.table();

    for (uint32_t i = 0; i < mo_bf.size() / CHAR_BIT; ++i) {
      unsigned char common_bits = mo_bv[i] &  entry_bv[i];
      // count set bits
      while (common_bits) {
        score += common_bits & 1;
        common_bits >>= 1;
      }
    }
    return score;
  }

  uint32_t get_similarity(vector<string>& mo_fps, bloom_filter& entry_bf) {
    uint32_t score = 0;
    for (auto& fp : mo_fps) {
      if (entry_bf.contains(fp)) {
        ++score;
      }
    }
    return score;
  }

  int get_similar_poid(bloom_filter& mo_bf, uint64_t chunk_size) {
    int target_poid = -1;
    uint32_t max_score = 0;
    int min_size_poid = -1;
    int min_po_size = INT_MAX;

    unique_lock al(apo_lock);
    if (active_pack_objs.empty()) {
      target_poid = poid_idx;
      al.unlock();
      return target_poid;
    }

    // find the most similar pack object
    for (auto& kv : active_pack_objs) {
      if (kv.second.second <= 0) {
        continue;
      }
      cout << kv.second.first.element_count() << std::endl;
      bloom_filter& po_bf = kv.second.first;
      uint32_t score = get_hamming_similarity(mo_bf, po_bf);
      cout << "score: " << score << std::endl;
      int poid = kv.first;
      if (score > max_score) {
        max_score = score;
        target_poid = poid;
      }

      // update minimum pack object size just in case of similarity not found
      int po_size = kv.second.second;
      if (po_size < min_po_size) {
        min_po_size = po_size;
        min_size_poid = poid;
      }
    }
    cout << "max score: " << max_score << std::endl;

    // similarity not found
    if (max_score < similarity_threshold) {
      cout << "similarity not found" << std::endl;

      // APO entries full. append to smallest pack object
      if (active_pack_objs.size() >= max_apo_entries) {
        cout << "apo map full. " << min_size_poid << " is selected" << std::endl;
        target_poid = min_size_poid;
      } else {
        target_poid = poid_idx;
      }
    }
    al.unlock();
    return target_poid;
  }
};

class EstimateDedupRatio : public CrawlerThread
{
  string chunk_algo;
  string fp_algo;
  uint64_t chunk_size;
  uint64_t max_seconds;

public:
  EstimateDedupRatio(
    IoCtx& io_ctx, int n, int m, ObjectCursor begin, ObjectCursor end,
    string chunk_algo, string fp_algo, uint64_t chunk_size, int32_t report_period,
    uint64_t num_objects, uint64_t max_read_size,
    uint64_t max_seconds):
    CrawlerThread(io_ctx, n, m, begin, end, report_period, num_objects,
		  max_read_size),
    chunk_algo(chunk_algo),
    fp_algo(fp_algo),
    chunk_size(chunk_size),
    max_seconds(max_seconds) {
  }

  void* entry() {
    estimate_dedup_ratio();
    return NULL;
  }
  void estimate_dedup_ratio();
};

class ChunkScrub: public CrawlerThread
{
  IoCtx chunk_io_ctx;
  int damaged_objects = 0;

public:
  ChunkScrub(IoCtx& io_ctx, int n, int m, ObjectCursor begin, ObjectCursor end, 
	     IoCtx& chunk_io_ctx, int32_t report_period, uint64_t num_objects):
    CrawlerThread(io_ctx, n, m, begin, end, report_period, num_objects), chunk_io_ctx(chunk_io_ctx)
    { }
  void* entry() {
    chunk_scrub_common();
    return NULL;
  }
  void chunk_scrub_common();
  int get_damaged_objects() { return damaged_objects; }
  void print_status(Formatter *f, ostream &out);
};

vector<std::unique_ptr<CrawlerThread>> estimate_threads;
vector<std::unique_ptr<EstimateFragmentationRatio>> estimate_frag_threads;

static void print_dedup_estimate(std::ostream& out, std::string chunk_algo)
{
  /*
  uint64_t total_bytes = 0;
  uint64_t total_objects = 0;
  */
  uint64_t examined_objects = 0;
  uint64_t examined_bytes = 0;

  for (auto &et : estimate_threads) {
    examined_objects += et->get_examined_objects();
    examined_bytes += et->get_examined_bytes();
  }

  auto f = Formatter::create("json-pretty");
  f->open_object_section("results");
  f->dump_string("chunk_algo", chunk_algo);
  f->open_array_section("chunk_sizes");
  for (auto& i : dedup_estimates) {
    f->dump_object("chunker", i.second);
  }
  f->close_section();

  f->open_object_section("summary");
  f->dump_unsigned("examined_objects", examined_objects);
  f->dump_unsigned("examined_bytes", examined_bytes);
  /*
  f->dump_unsigned("total_objects", total_objects);
  f->dump_unsigned("total_bytes", total_bytes);
  f->dump_float("examined_ratio", (float)examined_bytes / (float)total_bytes);
  */
  f->close_section();
  f->close_section();
  f->flush(out);
}

static void handle_signal(int signum) 
{
  std::lock_guard l{glock};
  for (auto &p : estimate_threads) {
    p->signal(signum);
  }
}

void EstimateDedupRatio::estimate_dedup_ratio()
{
  ObjectCursor shard_start;
  ObjectCursor shard_end;

  io_ctx.object_list_slice(
    begin,
    end,
    n,
    m,
    &shard_start,
    &shard_end);

  utime_t start = ceph_clock_now();
  utime_t end;
  if (max_seconds) {
    end = start;
    end += max_seconds;
  }

  utime_t next_report;
  if (report_period) {
    next_report = start;
    next_report += report_period;
  }

  ObjectCursor c(shard_start);
  while (c < shard_end)
  {
    std::vector<ObjectItem> result;
    int r = io_ctx.object_list(c, shard_end, 12, {}, &result, &c);
    if (r < 0 ){
      cerr << "error object_list : " << cpp_strerror(r) << std::endl;
      return;
    }

    unsigned op_size = max_read_size;

    for (const auto & i : result) {
      const auto &oid = i.oid;

      utime_t now = ceph_clock_now();
      if (max_seconds && now > end) {
	m_stop = true;
      }
      if (m_stop) {
	return;
      }

      if (n == 0 && // first thread only
	  next_report != utime_t() && now > next_report) {
	cerr << (int)(now - start) << "s : read "
	     << dedup_estimates.begin()->second.total_bytes << " bytes so far..."
	     << std::endl;
	print_dedup_estimate(cerr, chunk_algo);
	next_report = now;
	next_report += report_period;
      }

      // read entire object
      bufferlist bl;
      uint64_t offset = 0;
      while (true) {
	bufferlist t;
	int ret = io_ctx.read(oid, t, op_size, offset);
	if (ret <= 0) {
	  break;
	}
	offset += ret;
	bl.claim_append(t);
      }
      examined_objects++;
      examined_bytes += bl.length();

      // do the chunking
      for (auto& i : dedup_estimates) {
	vector<pair<uint64_t, uint64_t>> chunks;
	i.second.cdc->calc_chunks(bl, &chunks);
	for (auto& p : chunks) {
	  bufferlist chunk;
	  chunk.substr_of(bl, p.first, p.second);
	  i.second.add_chunk(chunk, fp_algo);
	  if (debug) {
	    cout << " " << oid <<  " " << p.first << "~" << p.second << std::endl;
	  }
	}
	++i.second.total_objects;
      }
    }
  }
}

static void print_chunk_scrub();
void ChunkScrub::chunk_scrub_common()
{
  ObjectCursor shard_start;
  ObjectCursor shard_end;
  int ret;
  Rados rados;

  ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     return;
  }
  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     return;
  }

  chunk_io_ctx.object_list_slice(
    begin,
    end,
    n,
    m,
    &shard_start,
    &shard_end);

  const utime_t start = ceph_clock_now();
  utime_t next_report;
  if (report_period) {
    next_report = start;
    next_report += report_period;
  }

  ObjectCursor c(shard_start);
  while(c < shard_end)
  {
    std::vector<ObjectItem> result;
    int r = chunk_io_ctx.object_list(c, shard_end, 12, {}, &result, &c);
    if (r < 0 ){
      cerr << "error object_list : " << cpp_strerror(r) << std::endl;
      return;
    }

    for (const auto & i : result) {
      unique_lock l{m_lock};
      if (m_stop) {
	Formatter *formatter = Formatter::create("json-pretty");
	print_status(formatter, cout);
	delete formatter;
	return;
      }

      utime_t now = ceph_clock_now();
      if (n == 0 && // first thread only
	  next_report != utime_t() && now > next_report) {
	cerr << (int)(now - start) << "s, interim findings is : "
	     << std::endl;
	print_chunk_scrub();
	next_report = now;
	next_report += report_period;
      }

      auto oid = i.oid;
      if (debug) {
	cout << oid << std::endl;
      }
      chunk_refs_t refs;
      {
	bufferlist t;
	ret = chunk_io_ctx.getxattr(oid, CHUNK_REFCOUNT_ATTR, t);
	if (ret < 0) {
	  continue;
	}
	auto p = t.cbegin();
	decode(refs, p);
      }

      examined_objects++;
      if (refs.get_type() != chunk_refs_t::TYPE_BY_OBJECT) {
	// we can't do anything here
	continue;
      }

      // check all objects
      chunk_refs_by_object_t *byo =
	static_cast<chunk_refs_by_object_t*>(refs.r.get());
      set<hobject_t> real_refs;

      uint64_t pool_missing = 0;
      uint64_t object_missing = 0;
      uint64_t does_not_ref = 0;
      for (auto& pp : byo->by_object) {
	IoCtx target_io_ctx;
	ret = rados.ioctx_create2(pp.pool, target_io_ctx);
	if (ret < 0) {
	  cerr << oid << " ref " << pp
	       << ": referencing pool does not exist" << std::endl;
	  ++pool_missing;
	  continue;
	}

	ret = cls_cas_references_chunk(target_io_ctx, pp.oid.name, oid);
	if (ret == -ENOENT) {
	  cerr << oid << " ref " << pp
	       << ": referencing object missing" << std::endl;
	  ++object_missing;
	} else if (ret == -ENOLINK) {
	  cerr << oid << " ref " << pp
	       << ": referencing object does not reference chunk"
	       << std::endl;
	  ++does_not_ref;
	}
      }
      if (pool_missing || object_missing || does_not_ref) {
	++damaged_objects;
      }
    }
  }
  cout << "--done--" << std::endl;
}

void ChunkScrub::print_status(Formatter *f, ostream &out)
{
  if (f) {
    f->open_array_section("chunk_scrub");
    f->dump_string("PID", stringify(get_pid()));
    f->open_object_section("Status");
    f->dump_string("Total object", stringify(total_objects));
    f->dump_string("Examined objects", stringify(examined_objects));
    f->dump_string("damaged objects", stringify(damaged_objects));
    f->close_section();
    f->flush(out);
    cout << std::endl;
  }
}

int estimate_dedup_ratio(const po::variables_map &opts)
{
  Rados rados;
  IoCtx io_ctx;
  string pool_name;
  uint64_t chunk_size = 8192;
  uint64_t min_chunk_size = 8192;
  uint64_t max_chunk_size = 4*1024*1024;
  uint64_t max_read_size = default_op_size;
  uint64_t max_seconds = 0;
  int ret;
  std::map<std::string, std::string>::const_iterator i;
  bool debug = false;
  ObjectCursor begin;
  ObjectCursor end;
  librados::pool_stat_t s; 
  list<string> pool_names;
  map<string, librados::pool_stat_t> stats;
  struct ceph_dedup_options d_opts;

  pool_name = get_opts_pool_name(opts);
  d_opts.load_dedup_conf_by_default(g_ceph_context);
  d_opts.load_dedup_conf_from_argument(opts);

  chunk_size = d_opts.get_chunk_size();
  if (opts.count("min-chunk-size")) {
    chunk_size = opts["min-chunk-size"].as<int>();
  } else {
    cout << "8192 is set as min chunk size by default" << std::endl;
  }
  if (opts.count("max-chunk-size")) {
    chunk_size = opts["max-chunk-size"].as<int>();
  } else {
    cout << "4MB is set as max chunk size by default" << std::endl;
  }
  if (opts.count("max-seconds")) {
    max_seconds = opts["max-seconds"].as<int>();
  } else {
    cout << "max seconds is not set" << std::endl;
  }
  if (opts.count("max-read-size")) {
    max_read_size = opts["max-read-size"].as<int>();
  } else {
    cout << default_op_size << " is set as max-read-size by default" << std::endl;
  }
  if (opts.count("debug")) {
    debug = true;
  }
  boost::optional<pg_t> pgid(opts.count("pgid"), pg_t());

  ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     goto out;
  }
  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     ret = -1;
     goto out;
  }
  if (pool_name.empty()) {
    cerr << "--create-pool requested but pool_name was not specified!" << std::endl;
    exit(1);
  }
  ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << pool_name << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
  }

  // set up chunkers
  if (chunk_size) {
    dedup_estimates.emplace(std::piecewise_construct,
			    std::forward_as_tuple(d_opts.get_chunk_size()),
			    std::forward_as_tuple(d_opts.get_chunk_algo(),
			    cbits(d_opts.get_chunk_size())-1));
  } else {
    for (size_t cs = min_chunk_size; cs <= max_chunk_size; cs *= 2) {
      dedup_estimates.emplace(std::piecewise_construct,
			      std::forward_as_tuple(cs),
			      std::forward_as_tuple(d_opts.get_chunk_algo(), cbits(cs)-1));
    }
  }

  glock.lock();
  begin = io_ctx.object_list_begin();
  end = io_ctx.object_list_end();
  pool_names.push_back(pool_name);
  ret = rados.get_pool_stats(pool_names, stats);
  if (ret < 0) {
    cerr << "error fetching pool stats: " << cpp_strerror(ret) << std::endl;
    glock.unlock();
    return ret;
  }
  if (stats.find(pool_name) == stats.end()) {
    cerr << "stats can not find pool name: " << pool_name << std::endl;
    glock.unlock();
    return ret;
  }
  s = stats[pool_name];

  for (int i = 0; i < d_opts.get_max_thread(); i++) {
    std::unique_ptr<CrawlerThread> ptr (
      new EstimateDedupRatio(io_ctx, i, d_opts.get_max_thread(), begin, end,
			     d_opts.get_chunk_algo(), d_opts.get_fp_algo(),
			     d_opts.get_chunk_size(), d_opts.get_report_period(),
			     s.num_objects, max_read_size,
			     max_seconds));
    ptr->create("estimate_thread");
    ptr->set_debug(debug);
    estimate_threads.push_back(std::move(ptr));
  }
  glock.unlock();

  for (auto &p : estimate_threads) {
    p->join();
  }

  print_dedup_estimate(cout, d_opts.get_chunk_algo());

 out:
  return (ret < 0) ? 1 : 0;
}

static void print_chunk_scrub()
{
  uint64_t total_objects = 0;
  uint64_t examined_objects = 0;
  int damaged_objects = 0;

  for (auto &et : estimate_threads) {
    if (!total_objects) {
      total_objects = et->get_total_objects();
    }
    examined_objects += et->get_examined_objects();
    ChunkScrub *ptr = static_cast<ChunkScrub*>(et.get());
    damaged_objects += ptr->get_damaged_objects();
  }

  cout << " Total object : " << total_objects << std::endl;
  cout << " Examined object : " << examined_objects << std::endl;
  cout << " Damaged object : " << damaged_objects << std::endl;
}

int chunk_scrub_common(const po::variables_map &opts)
{
  Rados rados;
  IoCtx io_ctx, chunk_io_ctx;
  std::string object_name, target_object_name;
  string op_name;
  int ret;
  std::map<std::string, std::string>::const_iterator i;
  ObjectCursor begin;
  ObjectCursor end;
  librados::pool_stat_t s; 
  list<string> pool_names;
  map<string, librados::pool_stat_t> stats;
  struct ceph_dedup_options d_opts;

  d_opts.load_dedup_conf_by_default(g_ceph_context);
  d_opts.load_dedup_conf_from_argument(opts);
  op_name = get_opts_op_name(opts);
  boost::optional<pg_t> pgid(opts.count("pgid"), pg_t());

  ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     goto out;
  }
  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     ret = -1;
     goto out;
  }
  ret = rados.ioctx_create(d_opts.get_chunk_pool_name().c_str(), chunk_io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << d_opts.get_chunk_pool_name() << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
  }

  if (op_name == "chunk-get-ref" ||
      op_name == "chunk-put-ref" ||
      op_name == "chunk-repair") {
    string target_object_name;
    uint64_t pool_id;
    object_name = get_opts_object_name(opts);
    if (opts.count("target-ref")) {
      target_object_name = opts["target-ref"].as<string>();
    } else {
      cerr << "must specify target ref" << std::endl;
      exit(1);
    }
    if (opts.count("target-ref-pool-id")) {
      pool_id = opts["target-ref-pool-id"].as<uint64_t>();
    } else {
      cerr << "must specify target-ref-pool-id" << std::endl;
      exit(1);
    }

    uint32_t hash;
    ret = chunk_io_ctx.get_object_hash_position2(object_name, &hash);
    if (ret < 0) {
      return ret;
    }
    hobject_t oid(sobject_t(target_object_name, CEPH_NOSNAP), "", hash, pool_id, "");

    auto run_op = [] (ObjectWriteOperation& op, hobject_t& oid,
      string& object_name, IoCtx& chunk_io_ctx) -> int {
      int ret = chunk_io_ctx.operate(object_name, &op);
      if (ret < 0) {
	cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
      }
      return ret;
    };

    ObjectWriteOperation op;
    if (op_name == "chunk-get-ref") {
      cls_cas_chunk_get_ref(op, oid);
      ret = run_op(op, oid, object_name, chunk_io_ctx);
    } else if (op_name == "chunk-put-ref") {
      cls_cas_chunk_put_ref(op, oid);
      ret = run_op(op, oid, object_name, chunk_io_ctx);
    } else if (op_name == "chunk-repair") {
      ret = rados.ioctx_create2(pool_id, io_ctx);
      if (ret < 0) {
	cerr << oid << " ref " << pool_id
	     << ": referencing pool does not exist" << std::endl;
	return ret;
      }
      int chunk_ref = -1, base_ref = -1;
      // read object on chunk pool to know how many reference the object has
      bufferlist t;
      ret = chunk_io_ctx.getxattr(object_name, CHUNK_REFCOUNT_ATTR, t);
      if (ret < 0) {
	return ret;
      }
      chunk_refs_t refs;
      auto p = t.cbegin();
      decode(refs, p);
      if (refs.get_type() != chunk_refs_t::TYPE_BY_OBJECT) {
	cerr << " does not supported chunk type " << std::endl;
	return -1;
      }
      chunk_ref =
	static_cast<chunk_refs_by_object_t*>(refs.r.get())->by_object.count(oid);
      if (chunk_ref < 0) {
	cerr << object_name << " has no reference of " << target_object_name
	     << std::endl;
	return chunk_ref;
      }
      cout << object_name << " has " << chunk_ref << " references for "
	   << target_object_name << std::endl;

      // read object on base pool to know the number of chunk object's references
      base_ref = cls_cas_references_chunk(io_ctx, target_object_name, object_name);
      if (base_ref < 0) {
	if (base_ref == -ENOENT || base_ref == -ENOLINK) {
	  base_ref = 0;
	} else {
	  return base_ref;
	}
      }
      cout << target_object_name << " has " << base_ref << " references for "
	   << object_name << std::endl;
      if (chunk_ref != base_ref) {
	if (base_ref > chunk_ref) {
	  cerr << "error : " << target_object_name << "'s ref. < " << object_name
	       << "' ref. " << std::endl;
	  return -EINVAL;
	}
	cout << " fix dangling reference from " << chunk_ref << " to " << base_ref
	     << std::endl;
	while (base_ref != chunk_ref) {
	  ObjectWriteOperation op;
	  cls_cas_chunk_put_ref(op, oid);
	  chunk_ref--;
	  ret = run_op(op, oid, object_name, chunk_io_ctx);
	  if (ret < 0) {
	    return ret;
	  }
	}
      }
    }
    return ret;

  } else if (op_name == "dump-chunk-refs") {
    object_name = get_opts_object_name(opts);
    bufferlist t;
    ret = chunk_io_ctx.getxattr(object_name, CHUNK_REFCOUNT_ATTR, t);
    if (ret < 0) {
      return ret;
    }
    chunk_refs_t refs;
    auto p = t.cbegin();
    decode(refs, p);
    auto f = Formatter::create("json-pretty");
    f->dump_object("refs", refs);
    f->flush(cout);
    return 0;
  }

  glock.lock();
  begin = chunk_io_ctx.object_list_begin();
  end = chunk_io_ctx.object_list_end();
  pool_names.push_back(d_opts.get_chunk_pool_name());
  ret = rados.get_pool_stats(pool_names, stats);
  if (ret < 0) {
    cerr << "error fetching pool stats: " << cpp_strerror(ret) << std::endl;
    glock.unlock();
    return ret;
  }
  if (stats.find(d_opts.get_chunk_pool_name()) == stats.end()) {
    cerr << "stats can not find pool name: " << d_opts.get_chunk_pool_name() << std::endl;
    glock.unlock();
    return ret;
  }
  s = stats[d_opts.get_chunk_pool_name()];

  for (int i = 0; i < d_opts.get_max_thread(); i++) {
    std::unique_ptr<CrawlerThread> ptr (
      new ChunkScrub(io_ctx, i, d_opts.get_max_thread(), begin, end, chunk_io_ctx,
		     d_opts.get_report_period(), s.num_objects));
    ptr->create("estimate_thread");
    estimate_threads.push_back(std::move(ptr));
  }
  glock.unlock();

  for (auto &p : estimate_threads) {
    cout << "join " << std::endl;
    p->join();
    cout << "joined " << std::endl;
  }

  print_chunk_scrub();

out:
  return (ret < 0) ? 1 : 0;
}

int make_dedup_object(const po::variables_map &opts)
{
  Rados rados;
  IoCtx io_ctx, chunk_io_ctx;
  std::string object_name, op_name, pool_name, fp_algo;
  int ret;
  std::map<std::string, std::string>::const_iterator i;
  struct ceph_dedup_options d_opts;

  op_name = get_opts_op_name(opts);
  pool_name = get_opts_pool_name(opts);
  object_name = get_opts_object_name(opts);
  boost::optional<pg_t> pgid(opts.count("pgid"), pg_t());

  d_opts.load_dedup_conf_by_default(g_ceph_context);
  d_opts.load_dedup_conf_from_argument(opts);

  ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     goto out;
  }
  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     ret = -1;
     goto out;
  }
  ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << d_opts.get_chunk_pool_name() << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
  }
  ret = rados.ioctx_create(d_opts.get_chunk_pool_name().c_str(), chunk_io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << d_opts.get_chunk_pool_name() << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
  }

  if (op_name == "chunk-dedup") {
    uint64_t offset, length;
    string chunk_object;
    if (opts.count("source-off")) {
      offset = opts["source-off"].as<uint64_t>();
    } else {
      cerr << "must specify --source-off" << std::endl;
      exit(1);
    }
    if (opts.count("source-length")) {
      length = opts["source-length"].as<uint64_t>();
    } else {
      cerr << "must specify --source-length" << std::endl;
      exit(1);
    }
    // 1. make a copy from manifest object to chunk object
    bufferlist bl;
    ret = io_ctx.read(object_name, bl, length, offset);
    if (ret < 0) {
      cerr << " reading object in base pool fails : " << cpp_strerror(ret) << std::endl;
      goto out;
    }
    chunk_object = [fp_algo=d_opts.get_fp_algo(), &bl]() -> string {
      if (fp_algo == "sha1") {
        return ceph::crypto::digest<ceph::crypto::SHA1>(bl).to_str();
      } else if (fp_algo == "sha256") {
        return ceph::crypto::digest<ceph::crypto::SHA256>(bl).to_str();
      } else if (fp_algo == "sha512") {
        return ceph::crypto::digest<ceph::crypto::SHA512>(bl).to_str();
      } else {
        assert(0 == "unrecognized fingerprint type");
        return {};
      }
    }();
    ret = chunk_io_ctx.write(chunk_object, bl, length, offset);
    if (ret < 0) {
      cerr << " writing object in chunk pool fails : " << cpp_strerror(ret) << std::endl;
      goto out;
    }
    // 2. call set_chunk
    ObjectReadOperation op;
    op.set_chunk(offset, length, chunk_io_ctx, chunk_object, 0,
	CEPH_OSD_OP_FLAG_WITH_REFERENCE);
    ret = io_ctx.operate(object_name, &op, NULL);
    if (ret < 0) {
      cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
      goto out;
    }
  } else if (op_name == "object-dedup") {
    bool snap = false;
    if (opts.count("snap")) {
      snap = true;
    }

    bufferlist inbl;
    ret = rados.mon_command(
	make_pool_str(pool_name, "fingerprint_algorithm", d_opts.get_fp_algo()),
	inbl, NULL, NULL);
    if (ret < 0) {
      cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    ret = rados.mon_command(
	make_pool_str(pool_name, "dedup_tier", d_opts.get_chunk_pool_name()),
	inbl, NULL, NULL);
    if (ret < 0) {
      cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    ret = rados.mon_command(
	make_pool_str(pool_name, "dedup_chunk_algorithm", d_opts.get_chunk_algo()),
	inbl, NULL, NULL);
    if (ret < 0) {
      cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    ret = rados.mon_command(
	make_pool_str(pool_name, "dedup_cdc_chunk_size", d_opts.get_chunk_size()),
	inbl, NULL, NULL);
    if (ret < 0) {
      cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
      return ret;
    }

    auto create_new_deduped_object =
      [&io_ctx](string object_name) -> int {

      // tier-flush to perform deduplication
      ObjectReadOperation flush_op;
      flush_op.tier_flush();
      int ret = io_ctx.operate(object_name, &flush_op, NULL);
      if (ret < 0) {
	cerr << " tier_flush fail : " << cpp_strerror(ret) << std::endl;
	return ret;
      }
      // tier-evict
      ObjectReadOperation evict_op;
      evict_op.tier_evict();
      ret = io_ctx.operate(object_name, &evict_op, NULL);
      if (ret < 0) {
	cerr << " tier_evict fail : " << cpp_strerror(ret) << std::endl;
	return ret;
      }
      return ret;
    };

    if (snap) {
      io_ctx.snap_set_read(librados::SNAP_DIR);
      snap_set_t snap_set;
      int snap_ret;
      ObjectReadOperation op;
      op.list_snaps(&snap_set, &snap_ret);
      io_ctx.operate(object_name, &op, NULL);

      for (vector<librados::clone_info_t>::const_iterator r = snap_set.clones.begin();
	r != snap_set.clones.end();
	++r) {
	io_ctx.snap_set_read(r->cloneid);
	ret = create_new_deduped_object(object_name);
	if (ret < 0) {
	  goto out;
	}
      }
    } else {
      ret = create_new_deduped_object(object_name);
    }
  }

out:
  return (ret < 0) ? 1 : 0;
}

int enable_dedup(const po::variables_map &opts)
{
  Rados rados;
  IoCtx io_ctx;
  std::string base_pool, chunk_pool;
  int ret = -1;
  struct ceph_dedup_options d_opts;
  bool conf_done;
  CephContext *_cct = g_ceph_context;

  if (opts.count("pool")) {
    base_pool = opts["pool"].as<string>();
  } else {
    cerr << "must specify pool" << std::endl;
    goto out;
  }
  if (opts.count("chunk-pool")) {
    chunk_pool = opts["chunk-pool"].as<string>();
  } else {
    cerr << "must specify chunk-pool" << std::endl;
    goto out;
  }

  ret = rados.init_with_context(_cct);
  if (ret < 0) {
    cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
    goto out;
  }
  ret = rados.connect();
  if (ret) {
    cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
    ret = -1;
    goto out;
  }
  ret = rados.ioctx_create(base_pool.c_str(), io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << base_pool << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
  }

  conf_done = d_opts.load_dedup_conf_from_pool(io_ctx);
  if (!conf_done) {
    d_opts.load_dedup_conf_by_default(_cct);
  }
  d_opts.set_dedup_conf("", CHUNK_POOL, chunk_pool);
  d_opts.set_dedup_conf("", POOL, base_pool);
  d_opts.store_dedup_conf(io_ctx);

out:
  return (ret < 0) ? 1 : 0;
}

int set_dedup_conf(const po::variables_map &opts)
{
  Rados rados;
  IoCtx io_ctx;
  std::string pool_name, fp_algo;
  int ret;
  std::map<std::string, std::string>::const_iterator i;
  struct ceph_dedup_options d_opts;
  bool conf_done;
  std::string key, value;
  CephContext *_cct = g_ceph_context;

  pool_name = get_opts_pool_name(opts);

  ret = rados.init_with_context(_cct);
  if (ret < 0) {
    cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
    goto out;
  }
  ret = rados.connect();
  if (ret) {
    cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
    ret = -1;
    goto out;
  }
  ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
      << pool_name << ": "
      << cpp_strerror(ret) << std::endl;
    goto out;
  }

  if (opts.count("option-name")) {
    key = opts["option-name"].as<string>();
  } else {
    cerr << "must specify option-name" << std::endl;
    goto out;
  } 
  if (opts.count("value")) {
    value = opts["value"].as<string>();
  } else {
    cerr << "must specify value" << std::endl;
    goto out;
  }

  conf_done = d_opts.load_dedup_conf_from_pool(io_ctx);
  if (!conf_done) {
    d_opts.load_dedup_conf_by_default(_cct);
    d_opts.set_conf(POOL, pool_name);
  }
  d_opts.set_dedup_conf("", key, value);
  d_opts.store_dedup_conf(io_ctx);
  conf_done = d_opts.load_dedup_conf_from_pool(io_ctx);
  if (!conf_done) {
    cerr << " failed to store dedudp conf " << std::endl;
    goto out;
  }
  cout << d_opts << std::endl;

out:
  return (ret < 0) ? 1 : 0;
}

class EstimateFragmentationRatio : public CrawlerThread
{
  shared_ptr<Packer> packer;

  string chunk_algo;
  string fp_algo;
  uint64_t chunk_size;
  uint64_t dedup_threshold;
  bool do_dedup = false;
  set<string> oid_for_evict;

  /*
   * the number of operations to get all scanned metadata objects
   * worst case of required ops per metadata object
   *  = metadata object size / chunk size
   */
  uint64_t required_read_ops = 0;
  uint64_t num_deduped_chunks = 0;
  uint64_t num_deduped_metaobjs = 0;

public:
  EstimateFragmentationRatio(IoCtx& ioctx, int tid, int num_threads,
      ObjectCursor begin, ObjectCursor end, string chunk_algo, string fp_algo,
      uint64_t chunk_size, int32_t report_period, uint64_t num_objects,
      uint64_t dedup_threshold, shared_ptr<Packer> packer, bool do_dedup)
    : CrawlerThread(ioctx, tid, num_threads, begin, end, report_period,
        num_objects, default_op_size), packer(packer), chunk_algo(chunk_algo),
        fp_algo(fp_algo), chunk_size(chunk_size),
        dedup_threshold(dedup_threshold), do_dedup(do_dedup) {}
  virtual ~EstimateFragmentationRatio() override {}

  uint64_t get_required_read_ops() {
    return required_read_ops;
  }
  uint64_t get_num_deduped_chunks() {
    return num_deduped_chunks;
  }
  uint64_t get_num_deduped_metaobjs() {
    return num_deduped_metaobjs;
  }

  void estimate_fragmentation() {
    ObjectCursor shard_start, shard_end;
    io_ctx.object_list_slice(
      begin, end, n, m, &shard_start, &shard_end);

    utime_t start = ceph_clock_now();
    utime_t end;
    utime_t next_report;
    if (report_period) {
      next_report = start;
      next_report += report_period;
    }

    auto est = dedup_estimates.find(chunk_size);
    if (est == dedup_estimates.end()) {
      cerr << "dedup estimate result set error" << std::endl;
    }

    ObjectCursor c(shard_start);
    while (c < shard_end) {
      std::vector<ObjectItem> result;
      int r = io_ctx.object_list(c, shard_end, 12, {}, &result, &c);
      if (r < 0) {
        cerr << "error object_list : " << cpp_strerror(r) << std::endl;
        return;
      }

      unsigned op_size = max_read_size;
      for (const auto & i : result) {
        const auto &oid = i.oid;
        if (m_stop) {
          return;
        }

        utime_t now = ceph_clock_now();
        if (n == 0 && next_report != utime_t() && now > next_report) {
  cerr << (int)(now - start) << "s : read "
    << dedup_estimates.begin()->second.total_bytes << " bytes so far"
    << std::endl;
  print_dedup_estimate(cerr, chunk_algo);

  // print fragmentation info
  uint64_t total_read_ops = 0;
  uint64_t num_deduped_chunks= 0;
  uint64_t num_deduped_metaobjs = 0;
  for (auto& et : estimate_threads) {
    total_read_ops
      += static_cast<EstimateFragmentationRatio*>(et.get())->get_required_read_ops();
    num_deduped_chunks
      += static_cast<EstimateFragmentationRatio*>(et.get())->get_num_deduped_chunks();
    num_deduped_metaobjs
      += static_cast<EstimateFragmentationRatio*>(et.get())->get_num_deduped_metaobjs();
  }
  cerr << "read ops to get scanned metadata objs: " << total_read_ops << std::endl;
  cerr << "the number of packed chunks: " << num_deduped_chunks << std::endl;
  cerr << "the number of deduped metadata objs: " << num_deduped_metaobjs << std::endl;
  cerr << "avg read ops: " << total_read_ops / (double) num_deduped_metaobjs << std::endl;

  next_report = now;
  next_report += report_period;
        }

        // read entire object
        bufferlist bl;
        uint64_t offset = 0;
        while (true) {
  bufferlist t;
  int ret = io_ctx.read(oid, t, op_size, offset);
  if (ret <= 0) {
    break;
  }
  offset += ret;
  bl.claim_append(t);
        }
        if (!bl.length()) {
          continue;
        }

        examined_objects++;
        examined_bytes += bl.length();

        vector<pair<uint64_t, uint64_t>> chunks;
        set<string> pack_objs;  // for estimating fragmetation ratio
        // do chunking
        est->second.cdc->calc_chunks(bl, &chunks);
        bloom_filter mo_bf;

        vector<string> fps = est->second.get_fps(bl, chunks, fp_algo);
        int poid = -1;  // only for heuristic packing
        if (packer->get_type() == "heuristic") {
  mo_bf
    = static_cast<HeuristicPacker*>(packer.get())->generate_bf(fps, chunk_size);

  poid
    = static_cast<HeuristicPacker*>(packer.get())->get_similar_poid(mo_bf, chunk_size);
        }
        //cout << "moid: " << oid << ", poid: " << poid << ", apo size: " << static_cast<HeuristicPacker*>(packer.get())->active_pack_objs.size() << std::endl;

        uint32_t deduped = 0;
        uint32_t not_deduped = 0;   // just for debug
        for (uint32_t i = 0; i < fps.size(); ++i) {
  string fp = fps[i];
  uint64_t chunk_offset = chunks[i].first;
  uint64_t chunk_len = chunks[i].second;
  packer->add(fp);

  bufferlist chunk_data;
  chunk_data.substr_of(bl, chunk_offset, chunk_len);

  // if chunk is duplicated enough, do packing (dedup)
  if (packer->check_duplicated(fp)) {
    string po_name = packer->pack(fp, chunk_len, oid, chunk_offset, chunk_len,
        chunk_data, &poid);
    cout << fp << " packed in " << po_name << std::endl;
    while (po_name == "FULL" && packer->get_type() == "heuristic") {
      //cout << "oid: " << oid << " calculate again" << std::endl;
      // recalculate target pack object and retry packing if po pull
      poid
        = static_cast<HeuristicPacker*>(packer.get())->get_similar_poid(mo_bf, chunk_size);
      po_name= packer->pack(fp,chunk_len, oid, chunk_offset, chunk_len,
          chunk_data, &poid);
    }

    // accumulate fragmentation info
    if (!po_name.empty()) {
      pack_objs.emplace(po_name);
      deduped++;
    }
  } else {
    cout << fp << " not duped" << std::endl;
    // add metadata object oid for not duplicated chunk
    not_deduped++;
  }
        }
        ++(est->second.total_objects);

        if (debug) {
  cerr << est->second.total_objects << " " << oid << " needs " << pack_objs.size()
    << " read ops to restore data" << std::endl;
  cerr << deduped << " deduped, " << not_deduped << " not deduped" << std::endl;
        }

        // update packing metrics
        if (pack_objs.size() > 0) {
  required_read_ops += pack_objs.size();
  num_deduped_metaobjs++;
  num_deduped_chunks += deduped;
  oid_for_evict.insert(oid);
        }
      }

      if (do_dedup) {
        int i = 0;
        uint32_t num_evict_oids = oid_for_evict.size();
        vector<unique_ptr<AioCompletion>> evict_completions(num_evict_oids);
        for (auto& oid : oid_for_evict) {
          evict_completions[i++] = do_async_evict(oid);
        }
        for (auto& completion : evict_completions) {
          completion->wait_for_complete();
        }
        cout << num_evict_oids << " evicted" << std::endl;
      }
    }
  }

  void* entry() {
    estimate_fragmentation();
    return NULL;
  }

  unique_ptr<AioCompletion> do_async_evict(string oid) {
    Rados rados;
    ObjectReadOperation rop;
    unique_ptr<AioCompletion> completion(rados.aio_create_completion());
    rop.tier_evict();
    io_ctx.aio_operate(oid, completion.get(), &rop, NULL);
    return completion;
  }
};

int estimate_fragmentation(const po::variables_map &opts)
{
  Rados rados;
  IoCtx io_ctx, chunk_ioctx, index_ioctx;
  int ret;

  uint64_t pack_obj_size = 64 * 1024 * 1024;   // default 64MB
  if (opts.count("pack-obj-size")) {
    pack_obj_size = opts["pack-obj-size"].as<uint64_t>();
  }

  int chunk_size = 16384;   // default 16KB
  if (opts.count("chunk-size")) {
    chunk_size = opts["chunk-size"].as<int>();
  }

  string chunk_algo = "fastcdc"; // default fastcdc
  if (opts.count("chunk-algorithm")) {
    chunk_algo = opts["chunk-algorithm"].as<string>();
  }

  string fp_algo = "sha1";  // default sha1
  if (opts.count("fingerprint-algorithm")) {
    fp_algo = opts["fingerprint-algorithm"].as<string>();
  }

  uint64_t dedup_threshold = 2; // default threshold 2
  if (opts.count("chunk-dedup-threshold")) {
    dedup_threshold = opts["chunk-dedup-threshold"].as<uint64_t>();
  }

  int num_threads = 1;  // default 1 thread
  if (opts.count("max-thread")) {
    num_threads = opts["max-thread"].as<int>();
  }

  int report_period = 10; // default 10 secs
  if (opts.count("report-period")) {
    report_period = opts["report-period"].as<int>();
  }

  bool debug = false; // default debug false
  if (opts.count("debug")) {
    debug = true;
  }

  string packer_type = "simple"; // default simple packing
  if (opts.count("packer-type")) {
    packer_type = opts["packer-type"].as<string>();
  }

  uint32_t max_apo_entries = 0; // default max apo entries
  if (opts.count("max-apo-entries")) {
    max_apo_entries = opts["max-apo-entries"].as<uint32_t>();
  }

  uint32_t bv_len = 32768;  // default bit vector length
  if (opts.count("bit-vector-len")) {
    bv_len = opts["bit-vector-len"].as<uint32_t>();
  }

  uint32_t similarity_threshold = 10; // default similarity threshold
  if (opts.count("similarity-threshold")) {
    similarity_threshold = opts["similarity-threshold"].as<uint32_t>();
  }

  bool do_dedup = false;
  if (opts.count("do-dedup")) {
    do_dedup = true;
  }

  ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     return ret;
  }
  ret = rados.connect();
  if (ret < 0) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     ret = ret;
  }

  // init base pool
  string pool_name = get_opts_pool_name(opts);
  if (pool_name.empty()) {
    cerr << "pool name missed" << std::endl;
    return ret;
  }
  ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (ret < 0) {
    cerr << "error opening pool " << pool_name << ": "
      << cpp_strerror(ret) << std::endl;
    return ret;
  }
  cout << "base pool (" << pool_name << ") initialized" << std::endl;

  if (do_dedup) {
    // init chunk pool
    string chunk_pool_name;
    if (opts.count("chunk-pool")) {
      chunk_pool_name = opts["chunk-pool"].as<string>();
    } else {
      cerr << "chunk pool name missed" << std::endl;
      return ret;
    }
    ret = rados.ioctx_create(chunk_pool_name.c_str(), chunk_ioctx);
    if (ret < 0) {
      cerr << "error opening chunk pool " << chunk_pool_name << ": "
        << cpp_strerror(ret) << std::endl;
      return ret;
    }
    cout << "chunke pool (" << chunk_pool_name << ") initialized" << std::endl;

    // init index pool
    string index_pool_name;
    if (opts.count("index-pool")) {
      index_pool_name = opts["index-pool"].as<string>();
    } else {
      cerr << "index pool name missed" << std::endl;
      return ret;
    }
    ret = rados.ioctx_create(index_pool_name.c_str(), index_ioctx);
    if (ret < 0) {
      cerr << "error opening index pool " << index_pool_name << ": "
        << cpp_strerror(ret) << std::endl;
      return ret;
    }
    cout << "index pool (" << index_pool_name << ") initialized" << std::endl;
  }

  /*
  // for test write 0-filled obj
  for (int i = 0; i < 100; ++i) {
    bufferlist bl;
    for (int i = 0; i < 1024 * 256; ++i) {
      bl.append("0000000000000000");
    }

    ret = io_ctx.write_full("testobj_" + to_string(i), bl);
    if (ret < 0) {
      cout << cpp_strerror(ret) << std::endl;
    }
  }
  cout << "write obj done" << std::endl;
  sleep(10);
  */

  // set up chunker
  dedup_estimates.emplace(std::piecewise_construct,
                          std::forward_as_tuple(chunk_size),
                          std::forward_as_tuple(chunk_algo, cbits(chunk_size-1)));

  glock.lock();
  ObjectCursor begin, end;
  begin = io_ctx.object_list_begin();
  end = io_ctx.object_list_end();

  list<string> pool_names;
  pool_names.emplace_back(pool_name);
  map<string, librados::pool_stat_t> stats;
  ret = rados.get_pool_stats(pool_names, stats);
  if (ret < 0) {
    cerr << "error fetching pool stats: " << cpp_strerror(ret) << std::endl;
    glock.unlock();
    return ret;
  }
  if (stats.find(pool_name) == stats.end()) {
    cerr << "stats can not find pool name: " << pool_name << std::endl;
    glock.unlock();
    return ret;
  }
  librados::pool_stat_t s = stats[pool_name];

  shared_ptr<Packer> packer;
  if (packer_type == "simple") {
    packer = make_shared<SimplePacker>(io_ctx, chunk_ioctx, index_ioctx,
        pack_obj_size, dedup_threshold, debug, do_dedup);
  } else {
    packer = make_shared<HeuristicPacker>(io_ctx, chunk_ioctx, index_ioctx,
        pack_obj_size, dedup_threshold, debug,
        max_apo_entries, bv_len, similarity_threshold, do_dedup);
  }
  cout << packer_type << " packer created" << std::endl;

  for (int i = 0; i < num_threads; ++i) {
    std::unique_ptr<EstimateFragmentationRatio> ptr (
      new EstimateFragmentationRatio(io_ctx, i, num_threads, begin, end,
        chunk_algo, fp_algo, chunk_size, report_period, s.num_objects,
        dedup_threshold, packer, do_dedup));
    ptr->create("frag estimate");
    ptr->set_debug(debug);
    estimate_threads.push_back(std::move(ptr));
  }
  glock.unlock();

  for (auto& p : estimate_threads) {
    p->join();
  }
  cout << "all threads dead" << std::endl;

  print_dedup_estimate(cerr, chunk_algo);
  cout << "print dedup estimate done" << std::endl;
  // print fragmentation info
  uint64_t total_read_ops = 0;
  uint64_t num_deduped_chunks= 0;
  uint64_t num_deduped_metaobjs = 0;
  for (auto& et : estimate_threads) {
    total_read_ops
      += static_cast<EstimateFragmentationRatio*>(et.get())->get_required_read_ops();
    num_deduped_chunks
      += static_cast<EstimateFragmentationRatio*>(et.get())->get_num_deduped_chunks();
    num_deduped_metaobjs
      += static_cast<EstimateFragmentationRatio*>(et.get())->get_num_deduped_metaobjs();
  }
  cout << "num_deduped_metaobjs: " << num_deduped_metaobjs << std::endl;
  cerr << "read ops to get scanned metadata objs: " << total_read_ops << std::endl;
  cerr << "the number of packed chunks: " << num_deduped_chunks << std::endl;
  cerr << "the number of deduped metadata objs: " << num_deduped_metaobjs << std::endl;
  cerr << "avg read ops: " << total_read_ops / (double) num_deduped_metaobjs << std::endl;

  bloom_filter tmp_bf(1, bv_len, 0, pack_obj_size / chunk_size * 10);
  cerr << "bit vector size : " << tmp_bf.size() / CHAR_BIT << ", bloom_filter size: " << sizeof(tmp_bf) << std::endl;

  return 0;
}

string generate_fingerprint(bufferlist chunk_data)
{
  return crypto::digest<crypto::SHA1>(chunk_data).to_str();
}

vector<tuple<bufferlist,pair<uint64_t, uint64_t>>>
do_cdc(ObjectItem& object, bufferlist& data, size_t chunk_size)
{
  vector<tuple<bufferlist, pair<uint64_t, uint64_t>>> ret;
  unique_ptr<CDC> cdc = CDC::create("fastcdc", cbits(chunk_size) - 1);
  vector<pair<uint64_t, uint64_t>> chunks;
  cdc->calc_chunks(data, &chunks);
  for (auto& p : chunks) {
    bufferlist chunk;
    chunk.substr_of(data, p.first, p.second);
    ret.push_back(make_tuple(chunk, p));
  }
  return ret;
}

int test_many_refs(const po::variables_map &opts)
{
  /*
  string base_pool_name = get_opts_pool_name(opts);
  string chunk_pool_name;
  if (opts.count("chunk-pool")) {
    return opts["pool"].as<string>();
  } else {
    cout << "chunk pool name missed" << std::endl;
  }
  cout << "base pool name: " << base_pool_name << ", chunk pool name: "
       << chunk_pool_name << std::endl;

  size_t chunk_size = 1024;
  if (opts.count("chunk-size")) {
    chunk_size = opts["chunk-size"].as<int>();
  } else {
    cout << "1024 is set as chunk size by default" << std::endl;
  }

  Rados rados;
  int ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
    cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }
  ret = rados.connect();
  if (ret) {
    cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }

  IoCtx ioctx, chunk_ioctx;
  ret = rados.ioctx_create(base_pool_name.c_str(), ioctx);
  if (ret < 0) {
    cerr << "error opening base pool "
      << base_pool_name << ": "
      << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }
  ret = rados.ioctx_create(chunk_pool_name.c_str(), chunk_ioctx);
  if (ret < 0) {
    cerr << "error opening chunk pool "
      << chunk_pool_name << ": "
      << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }
  bufferlist inbl;
  string fp_algo = "sha1";
  ret = rados.mon_command(
      make_pool_str(base_pool_name, "fingerprint_algorithm", fp_algo),
      inbl, NULL, NULL);
  if (ret < 0) {
    cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  string chunk_algo = "fixed";
  ret = rados.mon_command(
      make_pool_str(base_pool_name, "dedup_chunk_algorithm", chunk_algo),
      inbl, NULL, NULL);
  if (ret < 0) {
    cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  ret = rados.mon_command(
      make_pool_str(base_pool_name, "dedup_cdc_chunk_size", chunk_size),
      inbl, NULL, NULL);
  if (ret < 0) {
    cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
    return ret;
  }

  bufferlist zerodata;
  for (int i = 0; i < 100; ++i) {
    zerodata.append("0000000000");  // 10 bytes
  }

  for (int i = 0; i < 25; ++i) {
    bufferlist bl;
    for (int j = 0; j < 4000; ++j) {
      bl.append(zerodata);
    }
    ObjectWriteOperation wop;
    wop.write_full(bl);
    chunk_io_ctx.operate("zero_" + to_string(j), &wop);
  }
  cout << "Write chunk object " << fp << "  done" << std::endl;

  // 1,000,000 refs
  // chunk size 100
  // 100MB object -> 25 * 4MB 

  bufferlist chunkdata;
  for(int i = 0; i < 10; ++i) {
    chunkdata.apppend("0000000000");  // 100 bytes
  }

  ObjectCursor cursor = ioctx.object_list_begin();
  ObjectCursor end = ioctx.object_list_end();
  time_t start_dedup, end_dedup;
  time_t set_chunk_start, set_chunk_end;
  double set_chunk_sum = 0.0;
  while (cursor < end) {
    vector<ObjectItem> objs;
    start_dedup = time(NULL);
    ret = ioctx.object_list(cursor, end, 12, {}, &objs, &cursor);
    if (ret < 0) {
      cerr << "error object_list: " << cpp_strerror(ret) << std::endl;
    }

    for (auto& obj : objs) {
      // read object
      bufferlist obj_data;

      size_t offset = 0;
      while (ret != 0) {
        bufferlist partial;
        ret = ioctx.read(obj.oid, partial, default_op_size, offset);
        if (ret < 0) {
          cerr << "read object errer " << obj.oid << " offset: " << offset
            << " error(" << cpp_strerror(ret) << std::endl;
          return -1;
        }
        offset += ret;
        obj_data.claim_append(partial);
      }

      // chunking
      auto chunks = do_cdc(obj, obj_data);

      // fingerprinting
      for (auto* chunk : chunks) {
        auto& chunk_data = get<0>(chunk);
        string fp = generate_fingerprint(chunk_data);
        pair<uint64_t, uint64_t> chunk_boundary = get<1>(chunk);
        chunk_t chunk_info = {
          .oid = object.oid,
          .start = chunk_boundary.first,
          .size = chunk_boundary.second,
          .fingerprint = fp,
          .data = chunk_data
        };

        // comparing
        sample_dedup_global.fp_store.contains(fp);
        sample_dedup_global.fp_store.add(chunk_info);

        // chunk deduped
        uint64_t size;
        time_t mtime;
        ret = chunk_ioctx.stat(fp, &size, &mtime);
        if (ret == -ENOENT) {
          bufferlist bl;
          bl.append(chunk_data);
          ObjectWriteOperation wop;
          wop.write_full(bl);
          chunk_ioctx.operate(fp, &wop);
        }

        set_chunk_start = time(NULL);
        // do set chunk
        ObjectReadOperation rop;
        rop.set_chunk(chunk_boundary.first, chunk_boundary.second, chunk_ioctx,
            fp, 0, CEPH_OSD_FLAG_WITH_REFERENCE);
        ret = ioctx.operate(obj.oid, &rop, nullptr);
        set_chunk_end = time(NULL);
        set_chunk_sum += (double)(set_chunk_end - set_chunk_start);
      }
    }
  }
  
  dedup_end = time(NULL);
  double dedup_latency = (double)(dedup_end - dedup_start);
  cout << idx << ",
  */
    return 0;
}

int test_dist_refs(const po::variables_map &opts)
{
  string base_pool_name = get_opts_pool_name(opts);
  string chunk_pool_name;
  if (opts.count("chunk-pool")) {
    chunk_pool_name = opts["chunk-pool"].as<string>();
  } else {
    cout << "chunk pool name missed" << std::endl;
  }
  cout << "base pool name: " << base_pool_name << ", chunk pool name: "
       << chunk_pool_name << std::endl;

  size_t chunk_size = 16384;
  if (opts.count("chunk-size")) {
    chunk_size = opts["chunk-size"].as<int>();
  } else {
    cout << "1024 is set as chunk size by default" << std::endl;
  }

  Rados rados;
  int ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
    cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }
  ret = rados.connect();
  if (ret) {
    cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }

  IoCtx ioctx, chunk_ioctx;
  ret = rados.ioctx_create(base_pool_name.c_str(), ioctx);
  if (ret < 0) {
    cerr << "error opening base pool "
      << base_pool_name << ": "
      << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }
  ret = rados.ioctx_create(chunk_pool_name.c_str(), chunk_ioctx);
  if (ret < 0) {
    cerr << "error opening chunk pool "
      << chunk_pool_name << ": "
      << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }

  // write 4MB zero-filled data
  bufferlist zerodata;
  for (int i = 0; i < 256 * 1024; ++i) {
    zerodata.append("0000000000000000");
  }

  for (int i = 0; i < 1024; ++i) {
    bufferlist bl;
    bl.append(zerodata);
    ObjectWriteOperation wop;
    wop.write_full(bl);
    chunk_ioctx.operate("zero_" + to_string(i), &wop);
  }
  cout << "Write chunk object done" << std::endl;
  sleep(10);

  unordered_map<string, uint64_t> fp_map;
  map<string, uint32_t> highly_dup_chunks;
  ObjectCursor cursor = ioctx.object_list_begin();
  ObjectCursor end = ioctx.object_list_end();
  uint32_t idx = 0;
  uint64_t set_chunk_latency_sum = 0;
  while (cursor < end) {
    vector<ObjectItem> objs;
    chrono::system_clock::time_point start_dedup = chrono::system_clock::now();
    ret = ioctx.object_list(cursor, end, 12, {}, &objs, &cursor);
    if (ret < 0) {
      cerr << "error object_list: " << cpp_strerror(ret) << std::endl;
    }

    for (auto& obj : objs) {
      // read object
      bufferlist obj_data;
      size_t offset = 0;
      ret = -1;
      while (ret != 0) {
        bufferlist partial;
        ret = ioctx.read(obj.oid, partial, default_op_size, offset);
        if (ret < 0) {
          cerr << "read object errer " << obj.oid << " offset: " << offset
            << " error(" << cpp_strerror(ret) << std::endl;
          return -1;
        }
        offset += ret;
        obj_data.claim_append(partial);
      }

      // chunking
      auto chunks = do_cdc(obj, obj_data, chunk_size);

      // fingerprinting
      for (auto& chunk : chunks) {
        auto& chunk_data = get<0>(chunk);
        string fp = generate_fingerprint(chunk_data);
        pair<uint64_t, uint64_t> chunk_boundary = get<1>(chunk);

        // comparing
        auto iter = fp_map.find(fp);
        if (iter != fp_map.end()) {
          ++iter->second;
        } else {
          fp_map.insert({fp, 1});
        }

        /*
        // check deduped
        uint64_t size;
        time_t mtime;
        ret = chunk_ioctx.stat(fp, &size, &mtime);
        if (ret == -ENOENT) {
          bufferlist bl;
          bl.append(chunk_data);
          ObjectWriteOperation wop;
          wop.write_full(bl);
          chunk_ioctx.operate(fp, &wop);
        }
        */
        string refcnt_key = CHUNK_REFCOUNT_ATTR;
        if (highly_dup_chunks.find(fp) != highly_dup_chunks.end()) {
          string postfix = "_" + to_string(highly_dup_chunks[fp]);
          refcnt_key += postfix;
        }

        bufferlist t;
        ret = chunk_ioctx.getxattr(fp, refcnt_key.c_str(), t);
        if (ret == -ENOENT) {
          bufferlist bl;
          bl.append(chunk_data);
          ObjectWriteOperation wop;
          wop.write_full(bl);
          chunk_ioctx.operate(fp, &wop);
        } else if (ret < 0) {
          return ret;
        }
        chunk_refs_t refs;
        auto p = t.cbegin();
        decode(refs, p);

        uint32_t ref_set_num = 0;
        if (refs.count() >= 10000) {
          if (highly_dup_chunks.find(fp) != highly_dup_chunks.end()) {
            ++highly_dup_chunks[fp];
          } else {
            highly_dup_chunks.insert({fp, 1});
          }
          ref_set_num = highly_dup_chunks[fp];
        }

        // do set chunk
        ObjectReadOperation rop;
        rop.set_chunk(chunk_boundary.first, chunk_boundary.second, chunk_ioctx,
            fp, 0, CEPH_OSD_OP_FLAG_WITH_REFERENCE, ref_set_num);
        chrono::system_clock::time_point start_set_chunk = chrono::system_clock::now();
        ret = ioctx.operate(obj.oid, &rop, nullptr);
        chrono::system_clock::time_point end_set_chunk = chrono::system_clock::now();
        chrono::duration<long long, std::milli> set_chunk_milli = chrono::duration_cast<chrono::milliseconds>(end_set_chunk - start_set_chunk);
        set_chunk_latency_sum += set_chunk_milli.count();
      }

      chrono::system_clock::time_point end_dedup = chrono::system_clock::now();
      chrono::duration<long long, std::milli> dedup_milli = chrono::duration_cast<chrono::milliseconds>(end_dedup - start_dedup);

      cout << ++idx << "," << dedup_milli.count() << "," << set_chunk_latency_sum << std::endl;
    }
  }

  return 0;
}

int main(int argc, const char **argv)
{
  auto args = argv_to_vec(argc, argv);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }

  po::variables_map opts;
  po::positional_options_description p;
  p.add("command", 1);
  po::options_description desc = make_usage();
  try {
    po::parsed_options parsed =
      po::command_line_parser(argc, argv).options(desc).positional(p).allow_unregistered().run();
    po::store(parsed, opts);
    po::notify(opts);
  } catch(po::error &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }
  if (opts.count("help") || opts.count("h")) {
    cout<< desc << std::endl;
    exit(0);
  }

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			CODE_ENVIRONMENT_DAEMON,
			CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  Preforker forker;
  if (global_init_prefork(g_ceph_context) >= 0) {
    std::string err;
    int r = forker.prefork(err);
    if (r < 0) {
      cerr << err << std::endl;
      return r;
    }
    if (forker.is_parent()) {
      g_ceph_context->_log->start();
      if (forker.parent_wait(err) != 0) {
        return -ENXIO;
      }
      return 0;
    }
    global_init_postfork_start(g_ceph_context);
  }
  common_init_finish(g_ceph_context);
  if (opts.count("daemon")) {
    global_init_postfork_finish(g_ceph_context);
    forker.daemonize();
  }
  init_async_signal_handler();
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);

  string op_name = get_opts_op_name(opts);
  int ret = 0;
  if (op_name == "estimate") {
    ret = estimate_dedup_ratio(opts);
  } else if (op_name == "chunk-scrub" ||
	     op_name == "chunk-get-ref" ||
	     op_name == "chunk-put-ref" ||
	     op_name == "chunk-repair" ||
	     op_name == "dump-chunk-refs") {
    ret = chunk_scrub_common(opts);
  } else if (op_name == "chunk-dedup" ||
	     op_name == "object-dedup") {
    /*
     * chunk-dedup:
     * using a chunk generated by given source,
     * create a new object in the chunk pool or increase the reference 
     * if the object exists
     * 
     * object-dedup:
     * perform deduplication on the entire object, not a chunk.
     *
     */
    ret = make_dedup_object(opts);
  } else if (op_name == "enable") {
    ret = enable_dedup(opts);
  } else if (op_name == "set-dedup-conf") {
    ret = set_dedup_conf(opts);
  } else if (op_name == "frag-info") {
    ret = estimate_fragmentation(opts);
  } else if (op_name == "many-refs-test") {
    ret = test_many_refs(opts);
  } else if (op_name == "ref-dist") {
    ret = test_dist_refs(opts);
  } else {
    cerr << "unrecognized op " << op_name << std::endl;
    exit(1);
  }

  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();
  
  return forker.signal_exit(ret);
}
