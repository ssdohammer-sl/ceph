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


extern const int DEFAULT_NUM_WORKERS;
extern const int DEFAULT_DEDUP_PERIOD;
extern const double DEFAULT_SAMPLING_RATIO;
extern const int MAX_OBJ_SCAN_SIZE;
extern const int MAX_BUCKET_SCAN_SIZE;
extern const string DEFAULT_CHUNK_ALGO;
extern const string DEFAULT_FP_ALGO;
extern const uint64_t DEFAULT_CHUNK_SIZE;
extern const int DEFAULT_CHUNK_DEDUP_THRESHOLD;

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
    bool is_run;

    vector<rgw_bucket_dir_entry> objects;
    vector<rgw::sal::Object*> objs;

  public:
    DedupWorker(const DoutPrefixProvider* _dpp, 
                CephContext* _cct, 
                uint32_t _id, 
                DedupProcessor* _proc)
      : dpp(_dpp), cct(_cct), id(_id), proc(_proc), is_run(false) {}
    ~DedupWorker() {
      ldout(cct, 0) << "DedupWorker_" << id << " destructed" << dendl;
    }
    void* entry() override;
    void stop();
    void append_obj(rgw_bucket_dir_entry obj) { objects.emplace_back(obj); }
    void append_sal_obj(rgw::sal::Object* obj) { objs.emplace_back(obj); }
    const size_t get_num_objs() { return objects.size(); }
    void clear_objs() { objects.clear(); objs.clear(); }
    void set_run(bool run) { is_run = run; }

    friend class DedupProcessor;
  };

  class DedupProcessor : public Thread {
    const DoutPrefixProvider* dpp;
    CephContext* cct;
    RGWDedup* dedup;
    rgw::sal::Store* store;

    bool down_flag;
    int num_workers;
    int dedup_period;
    vector<unique_ptr<RGWDedup::DedupWorker>> workers;
    vector<unique_ptr<rgw::sal::Bucket>> buckets;

    // TODO: need to clear up unnecessary object components
    vector<rgw_bucket_dir_entry> objects;
    vector<unique_ptr<rgw::sal::Object>> objs;

    double sampling_ratio;
    string chunk_algo;
    string fp_algo;
    uint64_t chunk_size;
    int chunk_dedup_threshold;

  public:
    DedupProcessor(const DoutPrefixProvider* _dpp, 
                   CephContext* _cct,
		   RGWDedup* _dedup, 
                   rgw::sal::Store* _store)
      : dpp(_dpp), cct(_cct), dedup(_dedup), store(_store), down_flag(true),
        num_workers(DEFAULT_NUM_WORKERS),
        dedup_period(DEFAULT_DEDUP_PERIOD),
        sampling_ratio(DEFAULT_SAMPLING_RATIO),
	chunk_algo(DEFAULT_CHUNK_ALGO),
	fp_algo(DEFAULT_FP_ALGO),
	chunk_size(DEFAULT_CHUNK_SIZE),
	chunk_dedup_threshold(DEFAULT_CHUNK_DEDUP_THRESHOLD) {}
    ~DedupProcessor() {}
    void* entry() override;
    void stop();
    void finalize();
    bool going_down();
    int initialize();

    int get_buckets();
    int get_objects();
    //int process(const rgw_bucket_dir_entry obj);
    int process(rgw::sal::Object* obj);
    void set_flag(bool flag) { down_flag = flag; }
    bool get_flag() { return down_flag; }

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

  void start_processor();
  void stop_processor();

  CephContext* get_cct() const override { return cct; }
  unsigned get_subsys() const override { return dout_subsys; }
  std::ostream& gen_prefix(std::ostream& out) const override { return out << "RGWDedup: "; }
};

#endif
