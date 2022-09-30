// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp


#include "rgw_dedup.h"
#include "common/CDC.h"


using namespace std;


int RGWDedup::initialize(CephContext* _cct, rgw::sal::Store* _store)
{
  cct = _cct;
  store = _store;
  proc = make_unique<DedupProcessor>(this, cct, this, store);
  return proc->initialize();
}

void RGWDedup::finalize()
{
  // Destroy memory variables.
}

void RGWDedup::start_processor()
{
  proc->create("dedup_proc");
}

void RGWDedup::stop_processor()
{
  if (proc.get()) {
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


int RGWDedup::DedupProcessor::initialize()
{
  // reserve DedupWorkers
  workers.reserve(num_workers);
  for (int i = 0; i < num_workers; ++i) {
    auto worker = std::make_unique<RGWDedup::DedupWorker>(dpp, cct, i, this);
    workers.emplace_back(std::move(worker));
  }
  int ret = get_buckets();
  if (ret < 0) {
    return ret;
  }
  ret = get_objects();
  if (ret < 0) {
    return ret;
  }
  ldout(cct, 5) << "  " << buckets.size() << " buckets, "
    << objects.size() << " objects found" << dendl;

  return 0;
}

/* Dedup main logic.
 * DedupWorkers call this function
 * to tell the objects which have benefits to dedup and dedup them.
 */
int RGWDedup::DedupProcessor::process()
{
  // TBD

  return 0;
}

/*  Get all buckets in a zone
 */
int RGWDedup::DedupProcessor::get_buckets()
{
  void* handle;
  int ret = store->meta_list_keys_init(dpp, "bucket", string(), &handle);
  if (ret < 0) {
    ldout(cct, 0) << __func__ << " ERROR: can't get key: " << dendl;
    return ret;
  }
  
  string bucket_name;
  bool truncated = true;
  list<string> bucket_list;
  while (truncated) {
    ret = store->meta_list_keys_next(dpp, handle, MAX_BUCKET_SCAN_SIZE, 
                                     bucket_list, &truncated);
    if (ret == -ENOENT && bucket_list.size() == 0) {
      ldout(cct, 0) << __func__ << " no bucket exists" << dendl;
      store->meta_list_keys_complete(handle);
      return ret; 
    }
    else if (ret < 0) {
      store->meta_list_keys_complete(handle);
      ldout(cct, 0) << __func__ << " failed to get bucket info" << dendl;
      return ret;
    }
    else {
      // do not allow duplicated bucket name
      for (auto bkt : bucket_list) {
        auto it = find(buckets.begin(), buckets.end(), bkt);
        if (it == buckets.end()) {
          buckets.emplace_back(bkt);
        }
      }
    }
  }
  store->meta_list_keys_complete(handle);

  return 0;
}

/*  Get dedup target objects of selected buckets
 */
int RGWDedup::DedupProcessor::get_objects()
{
  if (buckets.empty()) {
    ldout(cct, 0) << __func__ << " no selected buckets" << dendl;
    return -1;
  }

  for (auto bucket_name : buckets) {
    unique_ptr<rgw::sal::Bucket> bucket;
    rgw_bucket b{string(), bucket_name, string()};
    int ret = store->get_bucket(dpp, nullptr, b, &bucket, null_yield);
    if (ret < 0) {
      ldout(cct, 0) << __func__ << " ERROR: could not init bucket" << dendl;
      return ret;
    }

    bool truncated = true;
    rgw::sal::Bucket::ListParams params;
    rgw::sal::Bucket::ListResults results;
    while (truncated) {
      ret = bucket->list(dpp, params, MAX_OBJ_SCAN_SIZE, results, null_yield);
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
        }
      }
      truncated = results.is_truncated;
    }
  }
 
  return 0;
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
    ldout(cct, 5) << __func__ << " " << buckets.size() << " buckets, " 
      << objects.size() << " objects found" << dendl;

    // sampling target objects from objects
    auto sampled_indexes = sample_objects();

    // allocate target objects to DedupWorker


    // trigger DedupWorkers
    for (auto i = 0; i < num_workers; i++)
    {
      workers[i]->create("dedup_worker_" + i);
    }

    ldout(cct, 2) << __func__ << " " << workers.size() 
      << " workers started and will wait until they finish." << dendl;
    for (auto& w: workers)
    {
      w->join();
    }

    if (down_flag) {
      break;
    }
    sleep(dedup_period);
  } // done while
  ldout(cct, 0) << __func__ << " DedupProcessor going down" << dendl;

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
  ldout(cct, 0) << "Stops all DedupWorkers done" << dendl;
}

void RGWDedup::DedupProcessor::finalize()
{
  for (auto& worker : workers) {
    worker.reset();
  }
  workers.clear();
  ldout(cct, 0) << "DedupProcessor " << __func__ << " done" << dendl;
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

  default_random_engine generator;
  shuffle(indexes.begin(), indexes.end(), generator);
  size_t sampling_count = static_cast<double>(count) * sampling_ratio;
  indexes.resize(sampling_count);

  return indexes;
}


void* RGWDedup::DedupWorker::entry()
{
  ldpp_dout(dpp, 2) << " DedupWorker_" << id << " started" << dendl;
  
  // FIXME: This is temporary code. Need to be replaced with DedupWorker's task
  for (int i = 0; i < 10; i++) {
    if (!is_run) {
      break;
    }
    sleep(3);
  }

  return nullptr;
}

void RGWDedup::DedupWorker::stop()
{
  is_run = false;
}
