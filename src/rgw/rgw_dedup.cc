// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp


#include "rgw_dedup.h"
#include "common/CDC.h"


using namespace std;

const int DEFAULT_NUM_WORKERS = 3;
const int DEFAULT_DEDUP_PERIOD = 3;
const double DEFAULT_SAMPLING_RATIO = 1.0;
const int MAX_OBJ_SCAN_SIZE = 100;
const int MAX_BUCKET_SCAN_SIZE = 100;


int RGWDedup::initialize(CephContext* _cct, rgw::sal::Store* _store)
{
  cct = _cct;
  store = _store;
  proc = make_unique<DedupProcessor>(this, cct, this, store); 
  return proc->initialize();
}

void RGWDedup::finalize()
{
  // TODO: Destroy memory variables if needed
}

void RGWDedup::start_processor()
{
  proc->set_flag(false);
  proc->create("dedup_proc");
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
  stop_processor();
  finalize();
}


int RGWDedup::DedupProcessor::initialize()
{
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

  // reserve DedupWorkers
  workers.reserve(num_workers);
  for (int i = 0; i < num_workers; ++i) {
    auto worker = std::make_unique<RGWDedup::DedupWorker>(dpp, cct, i, this);
    workers.emplace_back(std::move(worker));
  }

  return 0;
}

/* a main job of deduplication.
 * DedupWorkers call this function
 * to tell the objects which have benefits to dedup and dedup them.
 */
int RGWDedup::DedupProcessor::process()
{
  // TBD
  IoCtx ioctx;
  int ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()
              ->get_rados_handle()->ioctx_create2(obj.ver.pool, &ioctx);
  if (ret < 0) {
    ldout(cct, 0) << "Failed to get IoCtx. pool id: " << obj.ver.pool << dendl;
    return -1;
  }


  // read object
  //librados::ObjectReadOperation op;
  //auto ret = rgw_rados_operate(dpp, *(store->getRados()->get_));
  bufferlist bl;
  unique_ptr<rgw::sal::Object::ReadOp> read_op;
  ret = read_op->read(, , &bl, ,dpp);


  // chunking
  

  // calculate dedup ratio


  // update FPStore

  
  // dedup or flush if needed
  

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

    // sampling target objects from objects
    auto sampled_indexes = sample_objects();
    size_t num_objs_per_thread = sampled_indexes.size() / num_workers;
    int remain_objs = sampled_indexes.size() % num_workers;

    // clear allocated objects
    for (auto& worker : workers) {
      worker->clear_objs();
    }

    // allocate target objects to each DedupWorker
    vector<unique_ptr<RGWDedup::DedupWorker>>::iterator it = workers.begin();
    for (auto idx : sampled_indexes) {
      rgw_bucket_dir_entry* obj = &objects[idx];
      (*it)->append_obj(*obj);
      if ((*it)->get_num_objs() >= num_objs_per_thread) {
        // append remain object for even distribution if remain_objs exists
        if (remain_objs) {
          --remain_objs;
          continue;
        }
        ++it;
      }
    }

    // trigger DedupWorkers
    for (auto i = 0; i < num_workers; i++)
    {
      workers[i]->set_run(true);
      workers[i]->create("dedup_worker_" + i);
    }

    // all DedupWorkers synchronozed here
    for (auto& w: workers)
    {
      w->join();
    }

    if (down_flag) {
      break;
    }
    sleep(dedup_period);
  } // done while
  ldout(cct, 2) << __func__ << " DedupProcessor going down" << dendl;

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
  ldout(cct, 2) << "Stops all DedupWorkers done" << dendl;
}

void RGWDedup::DedupProcessor::finalize()
{
  for (auto& worker : workers) {
    worker.reset();
  }
  workers.clear();
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

  shuffle(indexes.begin(), indexes.end(), default_random_engine());
  size_t sampling_count = static_cast<double>(num_objs) * sampling_ratio;
  indexes.resize(sampling_count);

  return indexes;
}


void* RGWDedup::DedupWorker::entry()
{
  ldpp_dout(dpp, 2) << " DedupWorker_" << id << " started with "
    << objects.size() << " objects" << dendl;
  
  // FIXME: This is temporary code. Need to be replaced with DedupWorker's task
  for (auto obj : objects) {
    if (!is_run) {
      break;
    }

    ldout(cct, 0) << "DedupWorker_" << id << " objname: " << obj.key.name << dendl;
    proc->process(obj);

    sleep(2);
  }

  return nullptr;
}

void RGWDedup::DedupWorker::stop()
{
  is_run = false;
  ldout(cct, 2) << "DedupWorker " << id << " stop" << dendl;
}
