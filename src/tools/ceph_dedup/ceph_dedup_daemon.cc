#include "common.h"

#undef dout_prefix
#define dout_prefix *_dout << "ceph_dedup_daemon: " \
                           << __func__ << ": "

ceph::shared_mutex glock = ceph::make_shared_mutex("glock");
class SampleDedupWorkerThread;
bool all_stop = false; // Accessed in the main thread and in other worker threads under glock

po::options_description make_usage() {
  po::options_description desc("Usage");
  desc.add_options()
    ("help,h", ": produce help message")
    ("--pool <POOL> --chunk-pool <POOL>",
     ": perform deduplication on the target pool")
    ;
  po::options_description op_desc("Opational arguments");
  op_desc.add_options()
    ("chunk-size", po::value<int>(), ": chunk size (byte)")
    ("chunk-algorithm", po::value<std::string>(), ": <fixed|fastcdc>, set chunk-algorithm")
    ("fingerprint-algorithm", po::value<std::string>(), ": <sha1|sha256|sha512>, set fingerprint-algorithm")
    ("chunk-pool", po::value<std::string>(), ": set chunk pool name")
    ("index-pool", po::value<std::string>(), ": set index pool name")
    ("max-thread", po::value<int>(), ": set max thread")
    ("report-period", po::value<int>(), ": set report-period")
    ("pool", po::value<std::string>(), ": set pool name")
    ("snap", ": deduplciate snapshotted object")
    ("chunk-dedup-threshold", po::value<int>(), ": set the threshold for chunk dedup (number of duplication) ")
    ("sampling-ratio", po::value<int>(), ": set the sampling ratio (percentile)")
    ("wakeup-period", po::value<int>(), ": set the wakeup period of crawler thread (sec)")
    ("fpstore-threshold", po::value<size_t>()->default_value(100_M), ": set max size of in-memory fingerprint store (bytes)")
    ("pack-obj-size", po::value<size_t>()->default_value(4_M), ": set pack object size (bytes)")
    ("run-once", ": do a single iteration for debug")
  ;
  desc.add(op_desc);
  return desc;
}

using AioCompRef = unique_ptr<AioCompletion>;

class SampleDedupWorkerThread : public Thread
{
public:
  struct chunk_t {
    string oid = "";
    size_t start = 0;
    size_t size = 0;
    string fingerprint = "";
    bufferlist data;
  };

  using dup_count_t = size_t;

  template <typename K, typename V>
  class FpMap {
    using map_t = std::unordered_map<K, V>;
  public:
    /// Represents a nullable reference into logical container
    class entry_t {
      /// Entry may be into one of two maps or NONE, indicates which
      enum entry_into_t {
	UNDER, OVER, NONE
      } entry_into = NONE;

      /// Valid iterator into map for UNDER|OVER, default for NONE
      typename map_t::iterator iter;

      entry_t(entry_into_t entry_into, typename map_t::iterator iter) :
	entry_into(entry_into), iter(iter) {
	ceph_assert(entry_into != NONE);
      }

    public:
      entry_t() = default;

      auto &operator*() {
	ceph_assert(entry_into != NONE);
	return *iter;
      }
      auto operator->() {
	ceph_assert(entry_into != NONE);
	return iter.operator->();
      }
      bool is_valid() const {
	return entry_into != NONE;
      }
      bool is_above_threshold() const {
	return entry_into == entry_t::OVER;
      }
      friend class FpMap;
    };

    /// inserts str, count into container, must not already be present
    entry_t insert(const K &str, V count) {
      std::pair<typename map_t::iterator, bool> r;
      typename entry_t::entry_into_t s;
      if (count < dedup_threshold) {
       r = under_threshold_fp_map.insert({str, count});
       s = entry_t::UNDER;
      } else {
       r = over_threshold_fp_map.insert({str, count});
       s = entry_t::OVER;
      }
      ceph_assert(r.second);
      return entry_t{s, r.first};
    }

    /// increments refcount for entry, promotes as necessary, entry must be valid
    entry_t increment_reference(entry_t entry) {
      ceph_assert(entry.is_valid());
      entry.iter->second++;
      if (entry.entry_into == entry_t::OVER ||
	  entry.iter->second < dedup_threshold) {
	return entry;
      } else {
	auto [over_iter, inserted] = over_threshold_fp_map.insert(
	  *entry);
	ceph_assert(inserted);
	under_threshold_fp_map.erase(entry.iter);
	return entry_t{entry_t::OVER, over_iter};
      }
    }

    /// returns entry for fp, return will be !is_valid() if not present
    auto find(const K &fp) {
      if (auto iter = under_threshold_fp_map.find(fp);
	  iter != under_threshold_fp_map.end()) {
	return entry_t{entry_t::UNDER, iter};
      } else if (auto iter = over_threshold_fp_map.find(fp);
		 iter != over_threshold_fp_map.end()) {
	return entry_t{entry_t::OVER, iter};
      }  else {
	return entry_t{};
      }
    }

    /// true if container contains fp
    bool contains(const K &fp) {
      return find(fp).is_valid();
    }

    /// returns number of items
    size_t get_num_items() const {
      return under_threshold_fp_map.size() + over_threshold_fp_map.size();
    }

    /// returns estimate of total in-memory size (bytes)
    size_t estimate_total_size() const {
      size_t total = 0;
      if (!under_threshold_fp_map.empty()) {
	total += under_threshold_fp_map.size() *
	  (under_threshold_fp_map.begin()->first.size() + sizeof(V));
      }
      if (!over_threshold_fp_map.empty()) {
	total += over_threshold_fp_map.size() *
	  (over_threshold_fp_map.begin()->first.size() + sizeof(V));
      }
      return total;
    }

    /// true if empty
    bool empty() const {
      return under_threshold_fp_map.empty() && over_threshold_fp_map.empty();
    }

    /// instructs container to drop entries with refcounts below threshold
    void drop_entries_below_threshold() {
      under_threshold_fp_map.clear();
    }

    FpMap(size_t dedup_threshold) : dedup_threshold(dedup_threshold) {}
    FpMap() = delete;
  private:
    map_t under_threshold_fp_map;
    map_t over_threshold_fp_map;
    const size_t dedup_threshold;
  };

  class FpStore {
  public:
    void maybe_print_status() {
      utime_t now = ceph_clock_now();
      if (next_report != utime_t() && now > next_report) {
	dout(5) << (int)(now - start) << "s : read "
	     << total_bytes << " bytes so far..."
	     << dendl;
	next_report = now;
	next_report += report_period;
      }
    }

    bool contains(string& fp) {
      std::shared_lock lock(fingerprint_lock);
      return fp_map.contains(fp);
    }

    // return true if the chunk is duplicate
    bool add(chunk_t& chunk) {
      std::unique_lock lock(fingerprint_lock);
      auto entry = fp_map.find(chunk.fingerprint);
      total_bytes += chunk.size;
      if (!entry.is_valid()) {
	if (is_fpmap_full()) {
	  fp_map.drop_entries_below_threshold();
	  if (is_fpmap_full()) {
	    return false;
	  }
	}
	entry = fp_map.insert(chunk.fingerprint, 1);
      } else {
	entry = fp_map.increment_reference(entry);
      }
      return entry.is_above_threshold();
    }

    bool is_fpmap_full() const {
      return fp_map.estimate_total_size() >= memory_threshold;
    }

    FpStore(size_t chunk_threshold,
      uint32_t report_period,	
      size_t memory_threshold) :
      report_period(report_period),
      memory_threshold(memory_threshold),
      fp_map(chunk_threshold) { }
    FpStore() = delete;

  private:
    std::shared_mutex fingerprint_lock;
    const utime_t start = ceph_clock_now();
    utime_t next_report;
    const uint32_t report_period;
    size_t total_bytes = 0; // Accessed in the worker threads under fingerprint_lock
    const size_t memory_threshold;
    FpMap<std::string, dup_count_t> fp_map; // Accessed in the worker threads under fingerprint_lock
  };

  class Packer {
    size_t max_po_size;
    size_t po_size = 0; // Accessed in the worker threads under po_lock
    string poid = string(); // Accessed in the worker threads under po_lock
    std::shared_mutex po_lock;
    const string PO_PREFIX = "po_";
    //const string CHUNK_LOC_ATTR = "chunk_location";

  public:
    Packer() = delete;
    Packer(const Packer&) = delete;
    Packer& operator=(const Packer&) = delete;

    Packer(const size_t max_po_size) : 
      max_po_size(max_po_size) {}
    //Packer(Packer&& packer) :
      //max_po_size(packer.max_po_size) {}

    void pack_chunk(IoCtx& io_ctx, IoCtx& chunk_io_ctx, IoCtx& index_io_ctx,
        chunk_t chunk) {
      int ret;
      std::unique_lock pl(po_lock);
      // check pack object is available
      if (poid.empty() || po_size >= max_po_size) {
  poid = PO_PREFIX + chunk.fingerprint;
  if (create_pack_object(chunk_io_ctx) < 0) {
    return;
  }
  po_size = 0;
      }

      // check whether the chunk is deduped
      // if chunk is deduped, return its location (poid and offset)
      pair<string, int> chunk_loc = check_packed(index_io_ctx, chunk.fingerprint);
      int chunk_offset = chunk_loc.second;
      if (chunk_loc.first.empty()) {
  chunk_offset = po_size;

  // append chunk data to the pack object
  ret = chunk_io_ctx.write(poid, chunk.data, chunk.size, chunk_offset);
  if (ret < 0) {
    derr << "append chunk data to " << poid << " failed: " << cpp_strerror(ret) << dendl;
    return;
  }
  po_size += chunk.size;

  if (create_chunk_metadata_object(index_io_ctx, chunk.fingerprint, chunk_offset) < 0) {
    return;
  }
      }

      ObjectReadOperation rop;
      rop.set_packed_chunk(chunk.start, chunk.size, chunk_io_ctx,
          poid, chunk_offset, index_io_ctx, chunk.fingerprint,
          CEPH_OSD_OP_FLAG_WITH_REFERENCE);
      ret = io_ctx.operate(chunk.oid, &rop, NULL);
      if (ret < 0) {
  derr << chunk.fingerprint << " already set-packed-chunked. srd oid: "
    << chunk.oid << ", poid: " << poid << ", po offset: " << po_size 
    << dendl;
  return;
      }
    }

  private:
    pair<string, int> check_packed(IoCtx& index_io_ctx, string fp) {
      bufferlist bl;
      int ret = index_io_ctx.getxattr(fp, CHUNK_LOC_ATTR, bl);
      if (ret == -ENOENT) {
  dout(5) << "fp: " << fp << " is not deduped" << dendl;
  return make_pair(string(), 0);
      }

      pair<string, uint32_t> chunk_loc;
      try {
  auto iter = bl.cbegin();
  decode(chunk_loc, iter);
      } catch (buffer::error& err) {
  dout(5) << "get deduped location of " << fp << " failed " << dendl;
  return make_pair(string(), 0);
      }
      return chunk_loc;
    }

    int create_pack_object(IoCtx& chunk_io_ctx) {
      bufferlist bl;
      int ret = chunk_io_ctx.write_full(poid, bl);
      if (ret < 0) {
        derr << "create pack object (" << poid << ") failed: " << cpp_strerror(ret)
          << dendl;
      }
      return ret;
    }

    int create_chunk_metadata_object(IoCtx& index_io_ctx, string fp, int chunk_offset) {
      bufferlist bl;
      int ret = index_io_ctx.write_full(fp, bl);
      if (ret < 0) {
        derr << "create chunk metadata object (" << fp << ") failed: "
          << cpp_strerror(ret) << dendl;
        return ret;
      }

      // do setxattr to update chunk location
      bufferlist loc;
      encode(make_pair(poid, chunk_offset), loc);
      ret = index_io_ctx.setxattr(fp, CHUNK_LOC_ATTR, loc);
      if (ret < 0) {
        derr << "setxattr chunk metadata object (" << fp << ") failed: "
          << cpp_strerror(ret) << dendl;
      }
      return ret;
    }
  };

  struct SampleDedupGlobal {
    FpStore fp_store;
    Packer packer;
    const double sampling_ratio = -1;
    SampleDedupGlobal(
      size_t chunk_threshold,
      int sampling_ratio,
      uint32_t report_period,
      size_t fpstore_threshold,
      size_t pack_object_size) :
      fp_store(chunk_threshold, report_period, fpstore_threshold),
      packer(pack_object_size),
      sampling_ratio(static_cast<double>(sampling_ratio) / 100) { }
  };

  SampleDedupWorkerThread(
    IoCtx &io_ctx,
    IoCtx &chunk_io_ctx,
    IoCtx &index_io_ctx,
    ObjectCursor begin,
    ObjectCursor end,
    size_t chunk_size,
    std::string &fp_algo,
    std::string &chunk_algo,
    SampleDedupGlobal &sample_dedup_global,
    bool snap) :
    chunk_io_ctx(chunk_io_ctx),
    index_io_ctx(index_io_ctx),
    chunk_size(chunk_size),
    fp_type(pg_pool_t::get_fingerprint_from_str(fp_algo)),
    chunk_algo(chunk_algo),
    sample_dedup_global(sample_dedup_global),
    begin(begin),
    end(end),
    snap(snap) {
      this->io_ctx.dup(io_ctx);
    }

  ~SampleDedupWorkerThread() { };

  size_t get_total_duplicated_size() const {
    return total_duplicated_size;
  }

  size_t get_total_object_size() const {
    return total_object_size;
  }

protected:
  void* entry() override {
    crawl();
    return nullptr;
  }

private:
  void crawl();
  std::tuple<std::vector<ObjectItem>, ObjectCursor> get_objects(
    ObjectCursor current,
    ObjectCursor end,
    size_t max_object_count);
  std::vector<size_t> sample_object(size_t count);
  void try_dedup_and_accumulate_result(
      ObjectItem &object,
      bool deduped,
      bool archived,
      snap_t snap = 0);
  void do_chunk_dedup(chunk_t &chunk, snap_t snap);
  bufferlist read_object(ObjectItem &object);
  std::vector<std::tuple<bufferlist, pair<uint64_t, uint64_t>>> do_cdc(
    ObjectItem &object,
    bufferlist &data);
  std::string generate_fingerprint(bufferlist chunk_data);
  AioCompRef do_async_evict(string oid);
  bool is_hot(ObjectItem& object, bool& deduped, bool& archived);
  int make_object_archive(ObjectItem& object, bufferlist& data, snap_t snap);
  int clear_manifest(string oid);

  IoCtx io_ctx;
  IoCtx chunk_io_ctx;
  IoCtx index_io_ctx;
  size_t total_duplicated_size = 0;
  size_t total_object_size = 0;

  std::set<std::pair<std::string, snap_t>> oid_for_evict;
  const size_t chunk_size = 0;
  pg_pool_t::fingerprint_t fp_type = pg_pool_t::TYPE_FINGERPRINT_NONE;
  std::string chunk_algo;
  SampleDedupGlobal &sample_dedup_global;
  ObjectCursor begin;
  ObjectCursor end;
  bool snap;
};

void SampleDedupWorkerThread::crawl()
{
  ObjectCursor current_object = begin;
  std::shared_lock l{glock};
  while (!all_stop && current_object < end) {
    l.unlock();
    std::vector<ObjectItem> objects;
    // Get the list of object IDs to deduplicate
    std::tie(objects, current_object) = get_objects(current_object, end, 100);

    // Pick few objects to be processed. Sampling ratio decides how many
    // objects to pick. Lower sampling ratio makes crawler have lower crawling
    // overhead but find less duplication.
    auto sampled_indexes = sample_object(objects.size());
    for (size_t index : sampled_indexes) {
      ObjectItem target = objects[index];

      bool deduped = false;
      bool archived = false;
      if (!is_hot(target, deduped, archived))  {
  if (snap) {
    io_ctx.snap_set_read(librados::SNAP_DIR);
    snap_set_t snap_set;
    int snap_ret;
    ObjectReadOperation op;
    op.list_snaps(&snap_set, &snap_ret);
    io_ctx.operate(target.oid, &op, NULL);

    for (vector<librados::clone_info_t>::const_iterator r = snap_set.clones.begin();
      r != snap_set.clones.end();
      ++r) {
      io_ctx.snap_set_read(r->cloneid);
      try_dedup_and_accumulate_result(target, deduped, archived, r->cloneid);
    }
  } else {
    try_dedup_and_accumulate_result(target, deduped, archived);
  }
      }
      l.lock();
      if (all_stop) {
	oid_for_evict.clear();
	break;
      }
      l.unlock();
    }
    l.lock();
  }
  l.unlock();

  vector<AioCompRef> evict_completions(oid_for_evict.size());
  int i = 0;
  for (auto &oid : oid_for_evict) {
    if (snap) {
      io_ctx.snap_set_read(oid.second);
    }
    evict_completions[i] = do_async_evict(oid.first);
    i++;
  }
  for (auto &completion : evict_completions) {
    completion->wait_for_complete();
  }
}

AioCompRef SampleDedupWorkerThread::do_async_evict(string oid)
{
  Rados rados;
  ObjectReadOperation op_tier;
  AioCompRef completion(rados.aio_create_completion());
  op_tier.tier_evict();
  io_ctx.aio_operate(
      oid,
      completion.get(),
      &op_tier,
      NULL);
  return completion;
}

std::tuple<std::vector<ObjectItem>, ObjectCursor> SampleDedupWorkerThread::get_objects(
  ObjectCursor current, ObjectCursor end, size_t max_object_count)
{
  std::vector<ObjectItem> objects;
  ObjectCursor next;
  int ret = io_ctx.object_list(
    current,
    end,
    max_object_count,
    {},
    &objects,
    &next);
  if (ret < 0 ) {
    derr << "error object_list" << dendl;
    objects.clear();
  }

  return std::make_tuple(objects, next);
}

std::vector<size_t> SampleDedupWorkerThread::sample_object(size_t count)
{
  std::vector<size_t> indexes(count);
  for (size_t i = 0 ; i < count ; i++) {
    indexes[i] = i;
  }
  default_random_engine generator;
  shuffle(indexes.begin(), indexes.end(), generator);
  size_t sampling_count = static_cast<double>(count) *
    sample_dedup_global.sampling_ratio;
  indexes.resize(sampling_count);

  return indexes;
}

int SampleDedupWorkerThread::clear_manifest(string oid)
{
  ObjectWriteOperation promote_op;
  promote_op.tier_promote();
  int ret = 0;

  ret = io_ctx.operate(oid, &promote_op);
  if (ret < 0) {
    derr << "tier_promote failed " << oid << dendl;
    return ret;
  }

  ObjectWriteOperation unset_op;
  unset_op.unset_manifest();
  ret = io_ctx.operate(oid, &unset_op);
  if (ret < 0) {
    derr << "unset_manifest failed " << oid << dendl;
  }
  return ret;
}

void SampleDedupWorkerThread::try_dedup_and_accumulate_result(
  ObjectItem &object, bool deduped, bool archived, snap_t snap)
{
  bufferlist data = read_object(object);
  if (data.length() == 0) {
    derr << __func__ << " skip object " << object.oid
	 << " read returned size 0" << dendl;
    return;
  }
  auto chunks = do_cdc(object, data);
  size_t chunk_total_amount = 0;

  // First, check total size of created chunks
  for (auto &chunk : chunks) {
    auto &chunk_data = std::get<0>(chunk);
    chunk_total_amount += chunk_data.length();
  }
  if (chunk_total_amount != data.length()) {
    derr << __func__ << " sum of chunked length(" << chunk_total_amount
	 << ") is different from object data length(" << data.length() << ")"
	 << dendl;
    return;
  }

  size_t duplicated_size = 0;
  list<chunk_t> redundant_chunks;
  for (auto &chunk : chunks) {
    auto &chunk_data = std::get<0>(chunk);
    std::string fingerprint = generate_fingerprint(chunk_data);
    std::pair<uint64_t, uint64_t> chunk_boundary = std::get<1>(chunk);
    chunk_t chunk_info = {
      .oid = object.oid,
      .start = chunk_boundary.first,
      .size = chunk_boundary.second,
      .fingerprint = fingerprint,
      .data = chunk_data
      };

    if (sample_dedup_global.fp_store.contains(fingerprint)) {
      duplicated_size += chunk_data.length();
    }

    dout(20) << "generate a chunk (chunk oid: " << chunk_info.oid << ", offset: "
      << chunk_info.start << ", length: " << chunk_info.size << ", fingerprint: "
      << chunk_info.fingerprint << ")" << dendl;

    if (sample_dedup_global.fp_store.add(chunk_info)) {
      redundant_chunks.push_back(chunk_info);
    }
  }

  size_t object_size = data.length();
  if (!redundant_chunks.empty()) {
    // perform chunk-dedup
    if (archived) {
      dout(20) << object.oid << " is changed from archived to dedup" << dendl;
      if (clear_manifest(object.oid) < 0) {
        return;
      }
    }
    for (auto &p : redundant_chunks) {
      do_chunk_dedup(p, snap);
      dout(20) << p.fingerprint << " deduped" << dendl;
    }
    total_duplicated_size += duplicated_size;
    total_object_size += object_size;
  } else if (!archived && !deduped) {
    // perform object migration
    make_object_archive(object, data, snap);
    dout(20) << object.oid << " is cold and unique. Moved to cold pool" << dendl;
  }
}

bufferlist SampleDedupWorkerThread::read_object(ObjectItem &object)
{
  bufferlist whole_data;
  size_t offset = 0;
  int ret = -1;
  while (ret != 0) {
    bufferlist partial_data;
    ret = io_ctx.read(object.oid, partial_data, default_op_size, offset);
    if (ret < 0) {
      derr << "read object error " << object.oid << " offset " << offset
        << " size " << default_op_size << " error(" << cpp_strerror(ret)
        << dendl;
      bufferlist empty_buf;
      return empty_buf;
    }
    offset += ret;
    whole_data.claim_append(partial_data);
  }
  dout(20) << " got object: " << object.oid << " size: " << whole_data.length() << dendl;
  return whole_data;
}

std::vector<std::tuple<bufferlist, pair<uint64_t, uint64_t>>> SampleDedupWorkerThread::do_cdc(
  ObjectItem &object,
  bufferlist &data)
{
  std::vector<std::tuple<bufferlist, pair<uint64_t, uint64_t>>> ret;

  unique_ptr<CDC> cdc = CDC::create(chunk_algo, cbits(chunk_size) - 1);
  vector<pair<uint64_t, uint64_t>> chunks;
  cdc->calc_chunks(data, &chunks);
  for (auto &p : chunks) {
    bufferlist chunk;
    chunk.substr_of(data, p.first, p.second);
    ret.push_back(make_tuple(chunk, p));
  }

  return ret;
}

std::string SampleDedupWorkerThread::generate_fingerprint(bufferlist chunk_data)
{
  string ret;

  switch (fp_type) {
    case pg_pool_t::TYPE_FINGERPRINT_SHA1:
      ret = crypto::digest<crypto::SHA1>(chunk_data).to_str();
      break;

    case pg_pool_t::TYPE_FINGERPRINT_SHA256:
      ret = crypto::digest<crypto::SHA256>(chunk_data).to_str();
      break;

    case pg_pool_t::TYPE_FINGERPRINT_SHA512:
      ret = crypto::digest<crypto::SHA512>(chunk_data).to_str();
      break;
    default:
      ceph_assert(0 == "Invalid fp type");
      break;
  }
  return ret;
}

void SampleDedupWorkerThread::do_chunk_dedup(chunk_t &chunk, snap_t snap)
{
  sample_dedup_global.packer.pack_chunk(io_ctx, chunk_io_ctx,
      index_io_ctx, chunk);
  oid_for_evict.insert(make_pair(chunk.oid, snap));
}

bool SampleDedupWorkerThread::is_hot(ObjectItem& object,
    bool& deduped, bool& archived)
{
  ObjectReadOperation op;
  bool hot = false;
  int ret = -1;

  op.is_hot(&hot, &deduped, &archived, &ret);
  io_ctx.operate(object.oid, &op, NULL);
  if (ret < 0) {
    derr << "get " << object.oid << " temperature failed"
      << dendl;
    // return true to avoid dedup
    return true;
  }
  dout(20) << object.oid << " hot=" << hot << ", deduped=" << deduped
    << ", archived=" << deduped << dendl;

  return hot;
}

int SampleDedupWorkerThread::make_object_archive(ObjectItem& object,
	bufferlist& data, snap_t snap) {
  ObjectWriteOperation wop;
  wop.write_full(data);
  int ret = chunk_io_ctx.operate(object.oid, &wop);
  if (ret < 0) {
    derr << "write_full " << object.oid << " failed: "
      << cpp_strerror(ret) << dendl;
    return ret;
  }

  ObjectReadOperation rop;
  rop.set_chunk(0, data.length(), chunk_io_ctx, object.oid, 0,
      CEPH_OSD_OP_FLAG_WITH_REFERENCE);
  ret = io_ctx.operate(object.oid, &rop, NULL);
  if (ret < 0) {
    derr << object.oid << " set_chunk failed: " << cpp_strerror(ret) << dendl;
    return ret;
  }
  oid_for_evict.emplace(make_pair(object.oid, snap));

  return ret;
}

int make_crawling_daemon(const po::variables_map &opts)
{
  string base_pool_name = get_opts_pool_name(opts);
  string chunk_pool_name = get_opts_chunk_pool(opts);
  string index_pool_name = get_opts_index_pool(opts);
  unsigned max_thread = get_opts_max_thread(opts);
  uint32_t report_period = get_opts_report_period(opts);
  bool run_once = false; // for debug

  int sampling_ratio = -1;
  if (opts.count("sampling-ratio")) {
    sampling_ratio = opts["sampling-ratio"].as<int>();
  }
  size_t chunk_size = 8192;
  if (opts.count("chunk-size")) {
    chunk_size = opts["chunk-size"].as<int>();
  } else {
    cout << "8192 is set as chunk size by default" << std::endl;
  }
  bool snap = false;
  if (opts.count("snap")) {
    snap = true;
  }

  uint32_t chunk_dedup_threshold = -1;
  if (opts.count("chunk-dedup-threshold")) {
    chunk_dedup_threshold = opts["chunk-dedup-threshold"].as<int>();
  }

  size_t pack_object_size = 4 * 1024 * 1024;
  if (opts.count("pack-obj-size")) {
    pack_object_size = opts["pack-obj-size"].as<size_t>();
  }

  std::string chunk_algo = get_opts_chunk_algo(opts);

  Rados rados;
  int ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
    derr << "couldn't initialize rados: " << cpp_strerror(ret) << dendl;
    return -EINVAL;
  }
  ret = rados.connect();
  if (ret) {
    derr << "couldn't connect to cluster: " << cpp_strerror(ret) << dendl;
    return -EINVAL;
  }
  int wakeup_period = 5;
  if (opts.count("wakeup-period")) {
    wakeup_period = opts["wakeup-period"].as<int>();
  } else {
    cout << "100 second is set as wakeup period by default" << std::endl;
  }

  const size_t fp_threshold = opts["fpstore-threshold"].as<size_t>();

  std::string fp_algo = get_opts_fp_algo(opts);

  list<string> pool_names;
  IoCtx io_ctx, chunk_io_ctx, index_io_ctx;
  pool_names.push_back(base_pool_name);
  ret = rados.ioctx_create(base_pool_name.c_str(), io_ctx);
  if (ret < 0) {
    derr << "error opening base pool "
      << base_pool_name << ": "
      << cpp_strerror(ret) << dendl;
    return -EINVAL;
  }

  ret = rados.ioctx_create(chunk_pool_name.c_str(), chunk_io_ctx);
  if (ret < 0) {
    derr << "error opening chunk pool "
      << chunk_pool_name << ": "
      << cpp_strerror(ret) << dendl;
    return -EINVAL;
  }

  ret = rados.ioctx_create(index_pool_name.c_str(), index_io_ctx);
  if (ret < 0) {
    derr << "error opening index pool "
      << index_pool_name << ": "
      << cpp_strerror(ret) << dendl;
    return -EINVAL;
  }

  if (opts.count("run-once")) {
    run_once = true;
  }

  dout(0) << "ceph-dedup-daemon starts ( " 
    << "SampleRatio : " << sampling_ratio 
    << ", Chunk Dedup Threshold : " << chunk_dedup_threshold 
    << ", Chunk Size : " << chunk_size
    << ", Fingperint Argorithm : " << fp_algo
    << ", Chunk Argorithm : " << chunk_algo
    << ", Chunk Dedup Threshold : " << chunk_dedup_threshold 
    << ", Fingerprint Store Threshold : " << fp_threshold
    << ", Pack Object Size: " << pack_object_size 
    << ")" 
    << dendl;

  std::shared_lock l(glock);

  while (!all_stop) {
    l.unlock();
    ObjectCursor begin = io_ctx.object_list_begin();
    ObjectCursor end = io_ctx.object_list_end();

    SampleDedupWorkerThread::SampleDedupGlobal sample_dedup_global(
      chunk_dedup_threshold, sampling_ratio, report_period, fp_threshold, pack_object_size);

    std::list<SampleDedupWorkerThread> threads;
    size_t total_size = 0;
    size_t total_duplicate_size = 0;
    for (unsigned i = 0; i < max_thread; i++) {
      dout(15) << " spawn thread.. " << i << dendl;
      ObjectCursor shard_start;
      ObjectCursor shard_end;
      io_ctx.object_list_slice(
	begin,
	end,
	i,
	max_thread,
	&shard_start,
	&shard_end);

      threads.emplace_back(
	io_ctx,
	chunk_io_ctx,
  index_io_ctx,
	shard_start,
	shard_end,
	chunk_size,
	fp_algo,
	chunk_algo,
	sample_dedup_global,
	snap);
      threads.back().create("sample_dedup");
    }

    for (auto &p : threads) {
      p.join();
      total_size += p.get_total_object_size();
      total_duplicate_size += p.get_total_duplicated_size();
    }

    dout(5) << "Summary: read "
	 << total_size << " bytes so far and found saveable space ("
	 << total_duplicate_size << " bytes)."
	 << dendl;

    sleep(wakeup_period);

    map<string, librados::pool_stat_t> stats;
    ret = rados.get_pool_stats(pool_names, stats);
    if (ret < 0) {
      derr << "error fetching pool stats: " << cpp_strerror(ret) << dendl;
      return -EINVAL;
    }
    if (stats.find(base_pool_name) == stats.end()) {
      derr << "stats can not find pool name: " << base_pool_name << dendl;
      return -EINVAL;
    }

    l.lock();
    if (run_once) {
      all_stop = true;
      break;
    }
  }
  l.unlock();

  dout(0) << "done" << dendl;
  return 0;
}

static void handle_signal(int signum) 
{
  std::unique_lock l{glock};
  switch (signum) {
    case SIGINT:
    case SIGTERM:
      all_stop = true;
      dout(0) << "got a signal(" << signum << "), daemon wil be terminiated" << dendl;
      break;

    default:
      ceph_abort_msgf("unexpected signal %d", signum);
  }
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
  if (g_conf()->daemonize) {
    global_init_postfork_finish(g_ceph_context);
    forker.daemonize();
  }

  init_async_signal_handler();
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);

  int ret = make_crawling_daemon(opts);

  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();
  
  return forker.signal_exit(ret);
}
