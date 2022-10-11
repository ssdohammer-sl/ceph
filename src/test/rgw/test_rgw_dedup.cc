// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"

#include "rgw/rgw_dedup.h"
#include "rgw/rgw_sal_rados.h"
#include "rgw/rgw_common.h"
#include "rgw/rgw_rados.h"

#include "common/dout.h"
#include "test_rgw_common.h"
//#include "global/global_context.h"
//#include "test/librados/test_shared.h"
//#include "common/ceph_argparse.h"
//#include "global/global_init.h"
//#include "include/rados/librados.hpp"
#include "test/librados/test_cxx.h"

//using namespace librados;

void append_head(list<rgw_obj>* objs, rgw_obj& head)
{
  objs->push_back(head);
}

static void gen_obj(test_rgw_env& env, uint64_t obj_size, uint64_t head_max_size,
                    uint64_t stripe_size, RGWObjManifest *manifest, 
                    const rgw_placement_rule& placement_rule, rgw_bucket *bucket, 
                    rgw_obj *head, RGWObjManifest::generator* gen,
                    list<rgw_obj>* test_objs)
{
  manifest->set_trivial_rule(head_max_size, stripe_size);

  test_rgw_init_bucket(bucket, "buck");

  *head = rgw_obj(*bucket, "oid");
  gen->create_begin(g_ceph_context, manifest, placement_rule, nullptr, *bucket, *head);

  append_head(test_objs, *head);
  cout << "test_objs.size()=" << test_objs->size() << std::endl;
  append_stripes(test_objs, *manifest, obj_size, stripe_size);

  cout << "test_objs.size()=" << test_objs->size() << std::endl;

  ASSERT_EQ((int)manifest->get_obj_size(), 0);
  ASSERT_EQ((int)manifest->get_head_size(), 0);
  ASSERT_EQ(manifest->has_tail(), false);

  uint64_t ofs = 0;
  list<rgw_obj>::iterator iter = test_objs->begin();

  while (ofs < obj_size) {
    rgw_raw_obj obj = gen->get_cur_obj(env.zonegroup, env.zone_params);
    cout << "obj=" << obj << std::endl;
    rgw_raw_obj test_raw = rgw_obj_select(*iter).get_raw_obj(env.zonegroup, env.zone_params);
    ASSERT_TRUE(obj == test_raw);

    ofs = std::min(ofs + gen->cur_stripe_max_size(), obj_size);
    gen->create_next(ofs);

  cout << "obj=" << obj << " *iter=" << *iter << std::endl;
  cout << "test_objs.size()=" << test_objs->size() << std::endl;
    ++iter;

  }

  if (manifest->has_tail()) {
    rgw_raw_obj obj = gen->get_cur_obj(env.zonegroup, env.zone_params);
    rgw_raw_obj test_raw = rgw_obj_select(*iter).get_raw_obj(env.zonegroup, env.zone_params);
    ASSERT_TRUE(obj == test_raw);
    ++iter;
  }
  ASSERT_TRUE(iter == test_objs->end());
  ASSERT_EQ(manifest->get_obj_size(), obj_size);
  ASSERT_EQ(manifest->get_head_size(), std::min(obj_size, head_max_size));
  ASSERT_EQ(manifest->has_tail(), (obj_size > head_max_size));
}

/*
rgw::sal::RadosStore*  create_store() {
  auto store = new rgw::sal::RadosStore;
  store->setRados(new RGWRados);
  return store;
}
*/
/*
auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
rgw::sal::RadosStore* store = create_store();
//rgw::sal::RadosStore store;
store->setRados(new RGWRados);
*/
// creates a rados client and temporary pool
/*
struct RadosEnv : public ::testing::Environment {
  static std::optional<std::string> pool_name;
public:
  static std::optional<librados::Rados> rados;

  void SetUp() override {
    rados.emplace();
    // create pool
    std::string name = "test_pool";
    ASSERT_EQ("", create_one_pool_pp(name, *rados));
    pool_name = name;
  }
  void TearDown() override {
    if (pool_name) {
      ASSERT_EQ(0, destroy_one_pool_pp(*pool_name, *rados));
    }
    rados.reset();
  }

  static int ioctx_create(librados::IoCtx& ioctx) {
    return rados->ioctx_create(pool_name->c_str(), ioctx);
  }
};
*/
//std::optional<std::string> RadosEnv::pool_name;
//std::optional<librados::Rados> RadosEnv::rados;
//auto* const rados_env = ::testing::AddGlobalTestEnvironment(new RadosEnv);

class RGWDedupTest : public ::testing::Test
{
protected:
  static librados::Rados rados;
  static librados::IoCtx ioctx;
  static std::string pool_name;
  static rgw::sal::RadosStore store;
  CephContext cct;
  DoutPrefix dp;

  static void SetUpTestCase() {
    pool_name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
  }
  static void TearDownTestCase() {
    ioctx.close();
    if (pool_name != "") {
      ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
    }
  }

  void SetUp() override {
    store.setRados(new RGWRados);
    /*
    rados.emplace();
    // create pool
    std::string name = "test_pool";
    ASSERT_EQ("", create_one_pool_pp(name, *rados));
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
    connect_cluster_pp(rados2);

    pool_name = name;
    */
  }
  void TearDown() override {
    /*
    if (pool_name) {
      ASSERT_EQ(0, destroy_one_pool_pp(*pool_name, *rados));
    }
    rados.reset();
    */
  }

  // use the test's name as the oid so different tests don't conflict
  std::string get_test_oid() const {
    return ::testing::UnitTest::GetInstance()->current_test_info()->name();
  }

  static int ioctx_create(librados::IoCtx& ioctx) {
    return rados.ioctx_create(pool_name.c_str(), ioctx);
  }
public:
  RGWDedupTest(): cct(CEPH_ENTITY_TYPE_CLIENT), dp(&cct, 1, "test rgw dedup: ") {}
  ~RGWDedupTest() override {}
};
std::string RGWDedupTest::pool_name;
librados::Rados RGWDedupTest::rados;
librados::IoCtx RGWDedupTest::ioctx;
rgw::sal::RadosStore RGWDedupTest::store;


TEST_F(RGWDedupTest, get_buckets)
{
  RGWDedup rgw_dedup;
  std::cout << "create RGWDedup object done" << std::endl;

  test_rgw_env env;
  RGWObjManifest manifest;
  rgw_bucket bkt1, bkt2;
  rgw_obj head;

  rgw_obj bkt1_obj1, bkt1_obj2;
  rgw_obj bkt2_obj1, bkt2_obj2, bkt2_obj3;
  
  test_rgw_init_bucket(bkt1, "test_bucket_1");
  test_rgw_init_bucket(bkt2, "test_bucket_2");

  int obj_size = 21 * 1024 * 1024 + 1000;
  int stripe_size = 4 * 1024 * 1024;
  int head_size = 512 * 1024;

  bufferlist bl;
  encode(old, bl);

  rgw_obj new_obj;
  rgw_raw_obj raw_obj;
  
  gen_obj(env, obj_size, head_size, stripe_size, &manifest,
          env.zonegroup.default_placement, &bkt1, &head, &gen, &objs);

  list<rgw_obj>::iterator iter;
  rgw_obj_select last_obj;

  int ret = rgw_dedup.initialize(&cct, &store);
  std::cout << "RGWDedup init done" << std::endl;
  ASSERT_EQ(ret, 0);
  rgw_dedup.start_processor();

  ASSERT_TRUE(true);
}

TEST_F(RGWDedupTest, get_objects)
{
  ASSERT_TRUE(true);
}


int main (int argc, char** argv) {
  //auto args = argv_to_vec(argc, argv);
  //common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
