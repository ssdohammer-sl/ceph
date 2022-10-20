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
#include "rgw_dedup_proc.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;


class RGWDedup : public DoutPrefixProvider 
{
  CephContext* cct;
  rgw::sal::Store* store;
  unique_ptr<DedupProcessor> proc;

public:
  RGWDedup() : cct(nullptr), store(nullptr) {}
  ~RGWDedup() override;

  void initialize(CephContext* _cct, rgw::sal::Store* _store);
  void finalize();

  void start_processor();
  void stop_processor();

  CephContext* get_cct() const override { return cct; }
  unsigned get_subsys() const override { return dout_subsys; }
  std::ostream& gen_prefix(std::ostream& out) const override { return out << "RGWDedup: "; }
};

#endif
