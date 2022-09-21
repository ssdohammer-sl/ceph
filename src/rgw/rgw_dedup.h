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

#include <string>
#include <atomic>
#include <sstream>

#define dout_subsys ceph_subsys_rgw

using namespace std;

const int DEFAULT_NUM_WORKERS = 2;
const int DEFAULT_DEDUP_PERIOD = 10;

class RGWDedup : public DoutPrefixProvider {
  CephContext* cct;
//  RGWRados* store;
  rgw::sal::Store* store;
  std::atomic<bool> run_dedup = { true };   // TODO: need to be false. use config

  class DedupWorker : public Thread {
    const DoutPrefixProvider* dpp;
    CephContext* cct;
    ceph::mutex lock = ceph::make_mutex("DedupWorker");
    ceph::condition_variable cond;
    uint32_t id;

//    RGWRados* store;
    rgw::sal::Store* store;

  public:
    DedupWorker(const DoutPrefixProvider* _dpp, CephContext* _cct, uint32_t _id)
      : dpp(_dpp), cct(_cct), id(_id) {}
    ~DedupWorker() {
      ldout(cct, 0) << "DedupWorker_" << id << " destructed" << dendl;
    }
    void *entry() override;
    void stop();

    friend class DedupDaemon;
  };

  class DedupProcessor : public Thread {
    const DoutPrefixProvider* dpp;
    CephContext* cct;
    RGWDedup* dedup;
    //RGWRados* store;
    rgw::sal::Store* store;

    std::atomic<bool> down_flag = { false };
    int num_workers = DEFAULT_NUM_WORKERS;
    int dedup_period = DEFAULT_DEDUP_PERIOD;
    list<unique_ptr<RGWDedup::DedupWorker>> workers;
    /*
    uint32_t dedup_period;
    double sampling_ratio;
    uint32_t chunk_size;
    uint32_t chunk_dedup_threshold;
    string fp_algo;
    */

  public:
    DedupProcessor(const DoutPrefixProvider* _dpp, CephContext* _cct, RGWDedup* _dedup,
                   rgw::sal::Store* _store)
      : dpp(_dpp), cct(_cct), dedup(_dedup), store(_store) {}
    ~DedupProcessor() {}
    void* entry() override;
    void stop();

    void get_users();

    friend class RGWDedup;
  };
  unique_ptr<DedupProcessor> proc;

public:
  RGWDedup() : cct(nullptr), store(nullptr) {}
  ~RGWDedup() override {}

  //void initialize(CephContext* _cct, RGWRados* _store);
  void initialize(CephContext* _cct, rgw::sal::Store* _store);
  void finalize();
  int process();

  bool going_down();
  void start_processor();
  void stop_processor();

  CephContext *get_cct() const override { return cct; }
  unsigned get_subsys() const override { return dout_subsys; }
  std::ostream& gen_prefix(std::ostream& out) const override { return out << "RGWDedup: "; }
};


#endif
