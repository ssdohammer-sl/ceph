// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup.h"

using namespace std;


void RGWDedup::initialize(CephContext* _cct, rgw::sal::Store* _store)
{
  cct = _cct;
  store = _store;
  proc = make_unique<DedupProcessor>(this, cct, /*this,*/ store);
  proc->initialize();
}

void RGWDedup::finalize()
{
  // TODO: Destroy memory variables if needed
}

void RGWDedup::start_processor()
{
  if (proc.get()) {
    proc->set_flag(false);
    proc->create("dedup_proc");
  }
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
  if (proc.get()) {
    stop_processor();
  }
  finalize();
}

