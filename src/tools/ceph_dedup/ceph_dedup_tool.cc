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

  // Accessed in the estimate threads under fpmap_lock
  // <fp_value, <count, pack object id>>
  unordered_map<string, pair<uint32_t, int>> fp_store;
  ceph::shared_mutex fpmap_lock = ceph::make_shared_mutex("fpmap lock");

  uint64_t max_packobj_size;
  uint64_t dedup_threshold;
  bool debug = false;

public:
  Packer(uint64_t max_packobj_size, uint64_t dedup_threshold, bool debug)
    : max_packobj_size(max_packobj_size), dedup_threshold(dedup_threshold),
      debug(debug) {}
  virtual ~Packer() {}

  virtual int pack(string fp, int chunk_size, string src_oid) = 0;

  void add(string fp) {
    unique_lock l{fpmap_lock};
    auto i = fp_store.find(fp);
    // update fp count
    if (i != fp_store.end()) {
      ++(i->second.first);
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

  bool check_packed(string fp, int& oid) {
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
    string ret;
    switch(type) {
    case PackerType::Simple:
      ret = "simple";
    case PackerType::Heuristic:
      ret = "heuristic";
    default:
      ret = "none";
    }
    return ret;
  }
};

class SimplePacker : public Packer
{
  std::atomic<int> target_poid = 0; // pack object id
  uint64_t pack_obj_size = 0; // Accessed in the estimate threads under packing_lock
  ceph::shared_mutex packing_lock = ceph::make_shared_mutex("packing lock");

  // for debug
  vector<pair<string, string>> active_packobj;

public:
  SimplePacker(uint64_t max_packobj_size, uint32_t dedup_threshold, bool debug)
    : Packer(max_packobj_size, dedup_threshold, debug) {
      type = PackerType::Simple;
    }
  virtual ~SimplePacker() {}

  virtual int pack(string fp, int chunk_size, string src_oid) override {
    unique_lock pl(packing_lock);
    // check if fp is packed
    int chunk_poid = -1;
    if (check_packed(fp, chunk_poid)) {
      pl.unlock();
      return chunk_poid;
    }

    if (pack_obj_size >= max_packobj_size) {
      if (debug) {
        cerr << "pack object info. size: " << pack_obj_size << std::endl;
        cerr << "new pack object created" << std::endl;
      }
      ++target_poid;
      pack_obj_size = 0;
      active_packobj.clear();
    }
    chunk_poid = target_poid;
    pack_obj_size += chunk_size;
    active_packobj.emplace_back(make_pair(fp, src_oid));
    pl.unlock();

    unique_lock fl(fpmap_lock);
    auto entry = fp_store.find(fp);
    entry->second.second = chunk_poid;
    fl.unlock();

    return chunk_poid;
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
      ++examined_objects;
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
      uint64_t dedup_threshold, shared_ptr<Packer> packer)
    : CrawlerThread(ioctx, tid, num_threads, begin, end, report_period,
        num_objects, default_op_size), packer(packer), chunk_algo(chunk_algo),
        fp_algo(fp_algo), chunk_size(chunk_size),
        dedup_threshold(dedup_threshold) {}
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
        cout << oid << std::endl;
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
        examined_objects++;
        examined_bytes += bl.length();

        // do not use chunk range estimation
        auto est = dedup_estimates.find(chunk_size);
        if (est == dedup_estimates.end()) {
  cerr << "dedup estimate result set error" << std::endl;
        }
        vector<pair<uint64_t, uint64_t>> chunks;  // chunk boundaries
        set<string> pack_objs;  // for estimating fragmentation ratio
        
        // do chunking
        est->second.cdc->calc_chunks(bl, &chunks);
        vector<string> fps = est->second.get_fps(bl, chunks, fp_algo);

        uint32_t deduped = 0;
        uint32_t not_deduped = 0;
        for (uint32_t i = 0; i < fps.size(); ++i) {
  string fp = fps[i];
  uint64_t chunk_len = chunks[i].second;
  packer->add(fp);

  // if chunk is duplicated enough, do packing (dedup)
  if (packer->check_duplicated(fp)) {
    int packed_oid = packer->pack(fp, chunk_len, oid);
    pack_objs.emplace(to_string(packed_oid));
    ++deduped;
  } else {
    // add metadata object oid for not duplicated chunk
    ++not_deduped;
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
        }
      }
    }
  }

  void* entry() {
    estimate_fragmentation();
    return NULL;
  }
};

int estimate_fragmentation(const po::variables_map &opts)
{
  Rados rados;
  IoCtx io_ctx;
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

  shared_ptr<SimplePacker> packer
    = make_shared<SimplePacker>(pack_obj_size, dedup_threshold, debug);

  for (int i = 0; i < num_threads; ++i) {
    std::unique_ptr<EstimateFragmentationRatio> ptr (
      new EstimateFragmentationRatio(io_ctx, i, num_threads, begin, end,
        chunk_algo, fp_algo, chunk_size, report_period, s.num_objects,
        dedup_threshold, packer));
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
  cerr << "num deduped metadata objs: " << num_deduped_metaobjs << std::endl;
  cerr << "read ops to get scanned metadata objs: " << total_read_ops << std::endl;
  cerr << "the number of packed chunks: " << num_deduped_chunks << std::endl;
  cerr << "the number of deduped metadata objs: " << num_deduped_metaobjs << std::endl;
  cerr << "avg read ops: " << total_read_ops / (double) num_deduped_metaobjs << std::endl;

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
  } else {
    cerr << "unrecognized op " << op_name << std::endl;
    exit(1);
  }

  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();
  
  return forker.signal_exit(ret);
}
