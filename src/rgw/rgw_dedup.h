// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_DEDUP_H
#define CEPH_RGW_DEDUP_H


#include <string>
#include <atomic>
#include <sstream>

#include "include/types.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "rgw_sal.h"


#define dout_subsys ceph_subsys_rgw

using namespace std;

const int DEFAULT_NUM_WORKERS = 2;
const int DEFAULT_DEDUP_PERIOD = 3;
const double DEFAULT_SAMPLING_RATIO = 0.5;
const int MAX_OBJ_SCAN_SIZE = 100;
const int MAX_BUCKET_SCAN_SIZE = 100;

class RGWDedup : public DoutPrefixProvider 
{
  CephContext* cct;
  rgw::sal::Store* store;

  class DedupProcessor;
  class DedupWorker : public Thread {
    const DoutPrefixProvider* dpp;
    CephContext* cct;
    uint32_t id;
    DedupProcessor* proc;
    rgw::sal::Store* store;
    bool is_run = { false };

  public:
    DedupWorker(const DoutPrefixProvider* _dpp, 
                CephContext* _cct, 
                uint32_t _id, 
                DedupProcessor* _proc)
      : dpp(_dpp), cct(_cct), id(_id), proc(_proc) {}
    ~DedupWorker() {
      ldout(cct, 0) << "DedupWorker_" << id << " destructed" << dendl;
    }
    void* entry() override;
    void stop();

    friend class DedupProcessor;
  };

  class DedupProcessor : public Thread {
    const DoutPrefixProvider* dpp;
    CephContext* cct;
    RGWDedup* dedup;
    rgw::sal::Store* store;

    std::atomic<bool> down_flag = { false };
    int num_workers = DEFAULT_NUM_WORKERS;
    int dedup_period = DEFAULT_DEDUP_PERIOD;
    vector<unique_ptr<RGWDedup::DedupWorker>> workers;
    list<string> buckets;
    list<rgw_bucket_dir_entry> objects;

    double sampling_ratio = DEFAULT_SAMPLING_RATIO;

  public:
    DedupProcessor(const DoutPrefixProvider* _dpp, 
                   CephContext* _cct,
		   RGWDedup* _dedup, 
                   rgw::sal::Store* _store)
      : dpp(_dpp), cct(_cct), dedup(_dedup), store(_store) 
    {}
    ~DedupProcessor() {}
    void* entry() override;
    void stop();
    void finalize();
    bool going_down();
    int initialize();

    int get_buckets();
    int get_objects();
    int process();

  private:
    vector<size_t> sample_objects();
    bufferlist read_object(string name);

    friend class RGWDedup;
  };
  unique_ptr<DedupProcessor> proc;

public:
  RGWDedup() : cct(nullptr), store(nullptr) {}
  ~RGWDedup() override;

  int initialize(CephContext* _cct, rgw::sal::Store* _store);
  void finalize();
  int process();

  void start_processor();
  void stop_processor();

  CephContext* get_cct() const override { return cct; }
  unsigned get_subsys() const override { return dout_subsys; }
  std::ostream& gen_prefix(std::ostream& out) const override { return out << "RGWDedup: "; }
};


#endif
