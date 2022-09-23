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
  /*
  for (auto i = 0; i < num_workers; i++) {
    ldout(cct, 0) << __func__ << " RGWDedup::start_processor creating dedup_worker " << i << dendl;
    unique_ptr<DedupWorker> worker_ptr (new DedupWorker(this, cct, this, i));
    worker_ptr->create("dedup_worker_" + i);
    worker_threads.push_back(move(worker_ptr));
  }
  */

  // starts DedupProcessor
  proc.reset(new DedupProcessor(this, cct, this, store));
  proc->create("dedup_proc");
}

void RGWDedup::stop_processor()
{
/*  down_flag = true;
  if (worker_threads.size() > 0) {
    for (auto i = 0; i < worker_threads.size(); i++) {
      if (worker_threads[i].get()) {
        worker_threads[i]->stop();
        worker_threads[i]->join();
      }
      worker_thread[i].reset();
    }
  }*/
}
/*
RGWDedup::~RGWDedup()
{
  stop_processor();
  finalize();
}
*/

int RGWDedup::DedupProcessor::get_users()
{
  // get user list
  void* handle;
  string marker;
  //rgw::sal::RadosStore* rados_store = store->store
  int ret = store->meta_list_keys_init(dpp, "user", marker, &handle);
  if (ret < 0) {
    cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  bool truncated;
  uint64_t count = 0;
  list<string> keys;
  ret = store->meta_list_keys_next(dpp, handle, 10, keys, &truncated);
  ldout(cct, 0) << __func__ << " ret: " << ret << ", truncated: " << truncated
    << ", keys len: " << keys.size() << dendl;
  if (ret != -ENOENT) {
    if (keys.size() <= 0) {
       ldout(cct, 0) << __func__ << " no user exists" << dendl;
       return -1;
    }
    for (list<string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
      ldout(cct, 0) << "  " << *iter << dendl;
      ++count;
    }
  }
  store->meta_list_keys_complete(handle);

  return 0;
}

int RGWDedup::DedupProcessor::get_buckets()
{
  // get bucket list

  return 0;
}

void* RGWDedup::DedupProcessor::entry()
{
  // while (down_flag)
  if (!down_flag)
  {
    get_users();
    for (auto i = 0; i < num_workers; i++)
    {
      unique_ptr<DedupWorker> ptr(new DedupWorker(dpp, cct, i));
      ptr->create("dedup_worker_" + i);
      workers.emplace_back(move(ptr));
    }

    for (auto& w: workers)
    {
      w->join();
    }

    sleep(dedup_period);
  } // done while

  ldout(cct, 0) << __func__ << " RGWDedup loop done" << dendl;
  return nullptr;
}


// what dedup worker actually do
void *RGWDedup::DedupWorker::entry()
{
  ldout(cct, 0) << __func__ << " DedupWorker_" << id << " started" << dendl;

  return nullptr;
}

void RGWDedup::DedupWorker::stop()
{
  std::lock_guard l{lock};
  cond.notify_all();
}

