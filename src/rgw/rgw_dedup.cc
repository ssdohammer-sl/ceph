// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup.h"

#include "rgw_tools.h"
#include "include/scope_guard.h"
#include "include/rados/librados.hpp"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/refcount/cls_refcount_client.h"
#include "cls/version/cls_version_client.h"
#include "rgw_perf_counters.h"
#include "cls/lock/cls_lock_client.h"

#include <sstream>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace librados;

static string dedup_oid_prefix = "dedup";
static string dedup_index_lock_name = "dedup_process";

// init member variables
void RGWDedup::initialize(CephContext *_cct, RGWRados *_store)
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
}

bool RGWDedup::going_down()
{

}

// create DedupWorker threads
void RGWDedup::start_processor()
{
  for (auto i = 0; i < num_workers; i++) {
    ldpp_dout(this, 5) << "RGWDedup::start_processor creating dedup_worker " << i << dendl;
//    unique_ptr<Thread> worker_ptr (new DedupWorker());
//    worker_ptr->create("dedup_worker_" + i);
//    worker_threads.push_back(move(worker_ptr));
  }
  cout << num_workers << " threads are created" << endl;
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

unsigned RGWDedup::get_subsys() const
{
  return dout_subsys;
}

std::ostream& RGWDedup::gen_prefix(std::ostream& out) const
{
  return out << "RGWDedup: ";
}
/*
// what dedup worker actually do
void *RGWDedup::DedupWorker::entry()
{
  ldpp_dout(this, 5) << "DedupWorker_" << id << " started" << dendl;

  return nullptr;
}

void RGWDedup::DedupWorker::stop()
{
  std::lock_guard l{lock};
  cond.notify_all();
}
*/
