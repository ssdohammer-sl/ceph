// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <tuple>
#include <utility>

#include "rgw_dedup.h"
#include "common/CDC.h"


using namespace std;

const int DEFAULT_NUM_WORKERS = 3;
const int DEFAULT_DEDUP_PERIOD = 3;
const double DEFAULT_SAMPLING_RATIO = 1.0;
const int MAX_OBJ_SCAN_SIZE = 100;
const int MAX_BUCKET_SCAN_SIZE = 100;

const string DEFAULT_CHUNK_ALGO = "fastcdc";
const string DEFAULT_FP_ALGO = "sha1";
const uint64_t DEFAULT_CHUNK_SIZE = 16384;
const int DEFAULT_CHUNK_DEDUP_THRESHOLD = 2;


void RGWDedup::initialize(CephContext* _cct, rgw::sal::Store* _store)
{
  cct = _cct;
  store = _store;
  proc = make_unique<DedupProcessor>(this, cct, this, store);
  proc->initialize();
}

void RGWDedup::finalize()
{
  // TODO: Destroy memory variables if needed
}

void RGWDedup::start_processor()
{
  proc->set_flag(false);
  proc->create("dedup_proc");
}

void RGWDedup::stop_processor()
{
  if (!proc->get_flag() && proc.get()) {
    proc->stop();
    proc->join();
    proc->finalize();
  }
  proc.reset();
}

RGWDedup::~RGWDedup()
{
  stop_processor();
  finalize();
}


void RGWDedup::DedupProcessor::initialize()
{
  // reserve DedupWorkers
  workers.reserve(num_workers);
  for (int i = 0; i < num_workers; ++i) {
    auto worker = std::make_unique<RGWDedup::DedupWorker>(dpp, cct, i, this);
    workers.emplace_back(std::move(worker));
  }
}

/* a main job of deduplication.
 * DedupWorkers call this function
 * to tell the objects which have benefits to dedup and dedup them.
 */
//int RGWDedup::DedupProcessor::process(const rgw_bucket_dir_entry obj)
int RGWDedup::DedupProcessor::process(rgw::sal::Object* obj)
{
  // read object
  unique_ptr<rgw::sal::Object::ReadOp> read_op(obj->get_read_op());
  int ret = read_op->prepare(null_yield, dpp);
  if (ret < 0) {
    cerr << __func__ << " failed to read " << obj->get_name() << std::endl;
    return ret;
  }
  uint64_t obj_size = obj->get_obj_size();
  bufferlist obj_data;
  ret = read_op->read(0, obj_size, obj_data, null_yield, dpp);

  // chunking
  size_t total_chunk_size = 0;
  vector<tuple<bufferlist, pair<uint64_t, uint64_t>>> chunk_info;
  unique_ptr<CDC> cdc = CDC::create(chunk_algo, cbits(chunk_size) - 1);
  vector<pair<uint64_t, uint64_t>> chunks;
  cdc->calc_chunks(obj_data, &chunks);
  for (auto& c : chunks) {
    bufferlist chunk_data;
    chunk_data.substr_of(obj_data, c.first, c.second);
    chunk_info.emplace_back(make_tuple(chunk_data, c));
    total_chunk_size += chunk_data.length();
  }
  if (total_chunk_size != obj_data.length()) {
    cerr << __func__ << " total size of all chunks (" << total_chunk_size
	 << ") is different from object data length (" << obj_data.length() << ")"
	 << std::endl;
    return -1;
  }

  // calculate dedup ratio
  //size_t duplicated_size = 0;
  //list<chunk_t> redundant_chunks;
  for (auto& chunk : chunk_info) {
    auto& chunk_data = std::get<0>(chunk);
    //string fingerprint = generate_fingerprint(chunk_data);
    // <offset, length>
    pair<uint64_t, uint64_t> chunk_boundary = std::get<1>(chunk);

    // TODO: check the chunk has how much duplication
    // update FpStore
    // if the chunk has duplications a lot, append redundant_chunks
  }

  return ret;
}

/*  Get all buckets in a zone
 */
int RGWDedup::DedupProcessor::get_buckets()
{
  void* handle = nullptr;
  bool truncated = true;
  int ret = 0;

  ret = store->meta_list_keys_init(dpp, "bucket", string(), &handle);
  if (ret < 0) {
    std::cout << "meta_list_keys_init() failed" << std::endl;
    cerr << __func__ << " ERROR: can't init key" << std::endl;
    return ret;
  }

  while (truncated) {
    list<string> bucket_list;
    ret = store->meta_list_keys_next(dpp, handle, MAX_BUCKET_SCAN_SIZE, 
                                     bucket_list, &truncated);
    if (ret < 0) {
      cerr << __func__ << " failed to get bucket info" << std::endl;
    }
    else {
      // do not allow duplicated bucket name
      for (auto bucket_name : bucket_list) {
        bool is_contain = false;
        for (auto& b : buckets) {
          if (b->get_name() == bucket_name) {
            is_contain = true;
            break;
          }
        }
        if (!is_contain) {
          unique_ptr<rgw::sal::Bucket> bkt;
          ret = store->get_bucket(dpp, nullptr, "", bucket_name, &bkt, null_yield);
          if (ret < 0) {
            ldout(cct, 0) << __func__ << " ERROR: can't get bucket" << dendl;
          }
          buckets.emplace_back(move(bkt));
        }
      }
    }
  }
  ldout(cct, 0) << buckets.size() << " buckets found" << dendl;
  store->meta_list_keys_complete(handle);
  return ret;
}

/*  Get dedup target objects of selected buckets
 */
int RGWDedup::DedupProcessor::get_objects()
{
  
  if (buckets.empty()) {
    ldout(cct, 0) << __func__ << " no selected buckets" << dendl;
    return -1;
  }

  int ret = 0;
  for (auto& bkt : buckets) {
    rgw::sal::Bucket::ListParams params;
    rgw::sal::Bucket::ListResults results;
    bool truncated = true;
    
    while (truncated) {
      ret = bkt->list(dpp, params, MAX_OBJ_SCAN_SIZE, results, null_yield);
      if (ret < 0) {
        ldout(cct, 0) << " ERROR: failed to get objects from a bucket" << dendl;
        return ret;
      }
      for (auto obj : results.objs) {
        bool is_contain = false;
        for (auto o : objects) {
          if (o.key.name == obj.key.name && o.tag == obj.tag) {
            is_contain = true;
            break;
          }
        }
        if (!is_contain) {
          objects.emplace_back(obj);

          objs.emplace_back(move(bkt->get_object(obj.key)));
        }
      }
      truncated = results.is_truncated;
    }
  }
  ldout(cct, 0) << __func__ << " " << objs.size() << " rgw.sal.Object found" << dendl;

  return ret;
}

void* RGWDedup::DedupProcessor::entry()
{
  while (!down_flag)
  {
    int ret = get_buckets();
    if (ret < 0) {
      break;
    }
    ret = get_objects();
    if (ret < 0) {
      break;
    }

    // sampling target objects from objects
    auto sampled_indexes = sample_objects();
    size_t num_objs_per_thread = sampled_indexes.size() / num_workers;
    int remain_objs = sampled_indexes.size() % num_workers;

    // clear allocated objects
    for (auto& worker : workers) {
      worker->clear_objs();
    }

    // allocate target objects to each DedupWorker
    vector<unique_ptr<RGWDedup::DedupWorker>>::iterator it = workers.begin();
    for (auto idx : sampled_indexes) {
      // fixit
      (*it)->append_obj(objects[idx]);
      (*it)->append_sal_obj(objs[idx].get());

      if ((*it)->get_num_objs() >= num_objs_per_thread) {
        // append remain object for even distribution if remain_objs exists
        if (remain_objs) {
          --remain_objs;
          continue;
        }
        ++it;
      }
    }

    // trigger DedupWorkers
    for (auto i = 0; i < num_workers; i++)
    {
      workers[i]->set_run(true);
      workers[i]->create("dedup_worker_" + i);
    }

    // all DedupWorkers synchronozed here
    for (auto& w: workers)
    {
      w->join();
    }

    if (down_flag) {
      break;
    }
    sleep(dedup_period);
  } // done while
  ldout(cct, 2) << __func__ << " DedupProcessor going down" << dendl;

  return nullptr;
}

void RGWDedup::DedupProcessor::stop()
{
  for (auto& worker : workers) {
    if (worker.get()) {
      worker->stop();
    }
  }
  down_flag = true;
  ldout(cct, 2) << "Stops all DedupWorkers done" << dendl;
}

void RGWDedup::DedupProcessor::finalize()
{
  for (auto& worker : workers) {
    worker.reset();
  }
  workers.clear();
}

bool RGWDedup::DedupProcessor::going_down()
{
  return down_flag;
}

vector<size_t> RGWDedup::DedupProcessor::sample_objects()
{
  size_t num_objs = objects.size();
  vector<size_t> indexes(num_objs);
  // fill out vector to get sampled indexes
  for (size_t i = 0; i < num_objs; i++) {
    indexes[i] = i;
  }

  // get seed of random dist from current time
  unsigned seed = chrono::system_clock::now().time_since_epoch().count();
  shuffle(indexes.begin(), indexes.end(), default_random_engine(seed));
  size_t sampling_count = static_cast<double>(num_objs) * sampling_ratio;
  indexes.resize(sampling_count);

  return indexes;
}


void* RGWDedup::DedupWorker::entry()
{
  ldpp_dout(dpp, 2) << " DedupWorker_" << id << " started with "
    << objects.size() << " objects" << dendl;
  
  // FIXME: This is temporary code. Need to be replaced with DedupWorker's task
  for (auto obj : objs) {
    if (!is_run) {
      break;
    }

    ldout(cct, 0) << "DedupWorker_" << id << " objname: " << obj->get_name() << dendl;
    proc->process(obj);

    sleep(2);
  }

  return nullptr;
}

void RGWDedup::DedupWorker::stop()
{
  is_run = false;
  ldout(cct, 2) << "DedupWorker " << id << " stop" << dendl;
}
