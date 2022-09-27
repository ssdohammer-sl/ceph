// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <sstream>

#include "rgw_tools.h"
#include "include/scope_guard.h"
#include "include/rados/librados.hpp"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/refcount/cls_refcount_client.h"
#include "cls/version/cls_version_client.h"
#include "rgw_perf_counters.h"
#include "cls/lock/cls_lock_client.h"

#include "rgw_dedup.h"

//#define dout_context g_ceph_context
//#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace librados;

static string dedup_oid_prefix = "dedup";
static string dedup_index_lock_name = "dedup_process";

// init member variables
void RGWDedup::initialize(CephContext* _cct, rgw::sal::Store* _store)
{
  cct = _cct;
  store = _store;
}

void RGWDedup::finalize()
{
  // wrapup member variables of RGWDedup
}

// DedupWorkers call it
int RGWDedup::process()
{
  // object operation
  // set chunk, tier flush, tier evict
  //

  int ret = 0;
  return ret;
}

bool RGWDedup::going_down()
{
  return true;  // TODO
}

// create DedupWorker threads
void RGWDedup::start_processor()
{
  // starts DedupProcessor
  if (run_dedup) {
    proc.reset(new DedupProcessor(this, cct, this, store));
    proc->initialize();
    proc->create("dedup_proc");
    ldout(cct, 0) << __func__ << " start DedupProcessor done" << dendl;
  }
}

void RGWDedup::stop_processor()
{
  // stop RGWDedup threads
  if (proc.get()) {
    proc->stop();
    proc->join();
    proc->finalize();
  }
  run_dedup = false;
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
  get_buckets();
  get_objects();
  ldout(cct, 0) << __func__ << " initialize DedupWorker done" << dendl;
  ldout(cct, 0) << "  " << buckets.size() << " buckets, " 
    << objects.size() << " objects found" << dendl;
}

int RGWDedup::DedupProcessor::get_users()
{
  // get user list
  void* handle;
  string marker;
  int ret = store->meta_list_keys_init(dpp, "user", marker, &handle);
  if (ret < 0) {
    cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  bool truncated;
  list<string> user_list;
  ret = store->meta_list_keys_next(dpp, handle, 10, user_list, &truncated);
  if (ret != -ENOENT) {
    if (user_list.size() == 0) {
       ldout(cct, 0) << __func__ << " no user exists" << dendl;
       return -1;
    }
    for (list<string>::iterator iter = user_list.begin(); 
	 iter != user_list.end(); 
	 ++iter) {
      users.emplace_back(*iter);
    }
  }
  store->meta_list_keys_complete(handle);

  return 0;
}

int RGWDedup::DedupProcessor::get_buckets()
{
  // get bucket list
  void* handle;
  int ret = store->meta_list_keys_init(dpp, "bucket", string(), &handle);
  if (ret < 0) {
    cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  
  string bucket_name;
  bool truncated = true;
  list<string> bucket_list;
  ret = store->meta_list_keys_next(dpp, handle, MAX_BUCKET_WINDOW_SIZE, 
                                   bucket_list, &truncated);
  if (ret != -ENOENT) {
    if (bucket_list.size() == 0) {
      ldout(cct, 0) << __func__ << " no bucket exists" << dendl;
      return -1;
    }
    for (list<string>::iterator iter = bucket_list.begin();
	 iter != bucket_list.end();
	 ++iter) {
      buckets.insert(*iter);
    }
  }
  store->meta_list_keys_complete(handle);

  return 0;
}

int RGWDedup::DedupProcessor::get_objects()
{
  // get all the objects from selected buckets
  if (buckets.empty()) {
    ldout(cct, 0) << __func__ << " no selected buckets" << dendl;
    return -1;
  }

  for (auto bucket_name : buckets) {
    // get bucket owner from stats
    unique_ptr<rgw::sal::Bucket> bucket;
    rgw_bucket b{string(), bucket_name, string()};
    int ret = store->get_bucket(dpp, nullptr, b, &bucket, null_yield);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket:" <<cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    bool is_truncated = true;
    rgw::sal::Bucket::ListParams params;
    rgw::sal::Bucket::ListResults results;
    while (is_truncated) {
      ret = bucket->list(dpp, params, MAX_OBJ_WINDOW_SIZE, results, null_yield);
      if (ret < 0) {
        cerr << "ERROR: store->list_objects(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      for (auto obj : results.objs) {
        bool is_contained = false;
        for (auto o : objects) {
          if (o.key.name == obj.key.name && o.tag == obj.tag) {
            is_contained = true;
            break;
          }
        }
        if (!is_contained) {
          objects.emplace_back(obj);
        }
        //ldout(cct, 0) << "  " << obj.key.name << "  index_ver: " << obj.index_ver 
        //    << "  versioned_epoch: " << obj.versioned_epoch << "  pool: " 
        //    << obj.ver.pool << "  epoch: " << obj.ver.epoch << dendl;
      }
      is_truncated = results.is_truncated;
    }
  }
 
  return 0;
}

void* RGWDedup::DedupProcessor::entry()
{
  while (!down_flag)
  {
    get_buckets();
    get_objects();
    ldout(cct, 0) << __func__ << " " << buckets.size() << " buckets, " 
      << objects.size() << " objects found" << dendl;

    // trigger DedupWorkers
    for (auto i = 0; i < num_workers; i++)
    {
      workers[i]->create("dedup_worker_" + i);
    }

    ldout(cct, 0) << __func__ << " " << workers.size() << " workers started" << dendl;
    // wait for all workers until they finish their job
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
  ldout(cct, 0) << "DedupProcessor::" << __func__ << dendl;
  for (auto& worker : workers) {
    if (worker.get()) {
      worker->stop();
    }
  }

  down_flag = true;
  ldout(cct, 0) << __func__ << " stop all DedupWorkers done" << dendl;
}

void RGWDedup::DedupProcessor::finalize()
{
  for (auto& worker : workers) {
    worker.reset();
  }
  workers.clear();
  ldout(cct, 0) << "DedupProcessor::" << __func__ << " done" << dendl;
}

bool RGWDedup::DedupProcessor::going_down()
{
  return down_flag;
}


// what dedup worker actually do
void* RGWDedup::DedupWorker::entry()
{
  ldout(cct, 0) << __func__ << " DedupWorker_" << id << " started" << dendl;
  for (int i = 0; i < 10; i++) {
    if (proc->going_down()) {
      break;
    }
    ldout(cct, 0) << "  DedupWorker_" << id << " working" << dendl;
    sleep(3);
  }
  ldout(cct, 0) << __func__ << " DedupWorker_" << id << " going down" << dendl;

  return nullptr;
}

void RGWDedup::DedupWorker::stop()
{
  // do something for stop DedupWorker
  ldout(cct, 0) << __func__ << " DedupWorker_" << id << " stopped" << dendl;
}

