// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_DEDUP_H
#define CEPH_RGW_DEDUP_H


#include "include/types.h"
#include "include/rados/librados.hpp"
#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "rgw_common.h"
#include "rgw_sal.h"
#include "rgw_rados.h"
#include "cls/rgw/cls_rgw_types.h"

#include <atomic>

const int NUM_DEFAULT_WORKERS = 2;


class RGWDedup : public DoutPrefixProvider {
  CephContext *cct;
  RGWRados *store;

  class DedupWorker : public Thread {
    const DoutPrefixProvider *dpp;
    CephContext *cct;
    RGWDedup *dedup;
    ceph::mutex lock = ceph::make_mutex("DedupWorker");
    ceph::condition_variable cond;
    uint32_t id;
    bool run_dedup;
    std::atomic<bool> down_flag = { false };

  public:
    DedupWorker(const DoutPrefixProvider *_dpp, CephContext *_cct, RGWDedup *_dedup, uint32_t _id)
      : dpp(_dpp), cct(_cct), dedup(_dedup), id(_id) {}
    ~DedupWorker() {
      std::cout << "DedupWorker_" << id << " destructed" << std::endl;
    }
    void *entry() override;
    void stop();
  };

  uint32_t num_workers;
  uint32_t dedup_period;
  double sampling_ratio;
  uint32_t chunk_size;
  uint32_t chunk_dedup_threshold;
  string fp_algo;
  vector<std::unique_ptr<Thread>> worker_threads;

public:
  RGWDedup() 
    : cct(NULL), 
      store(NULL), 
      num_workers(DEFAULT_NUM_WORKERS), 
      worker(NULL)
  {}
  ~RGWDedup() {
    stop_processor();
    finalize();
  }

  void initialize(CephContext *_cct, RGWRados *_store);
  void finalize();
  int process();

  bool going_down();
  void start_processor();
  void stop_processor();

  CephContext *get_cct() const override { return store->ctx(); }
  std::ostream& gen_prefix(std::ostream& out) const;

};


#endif
