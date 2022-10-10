// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>

#include "rgw/rgw_dedup.h"
#include "rgw/rgw_sal_rados.h"

//#include "test_rgw_common.h"
#include "test/librados/test_cxx.h"
#include "global/global_context.h"
//#include "test/librados/test_shared.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
//#include "include/rados/librados.hpp"


rgw::sal::RadosStore* create_store() {
  rgw::sal::RadosStore* store = new rgw::sal::RadosStore;
  store->setRados(new RGWRados);
  return store;
}

auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
rgw::sal::RadosStore* store = create_store();

// creates a rados client and temporary pool
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
std::optional<std::string> RadosEnv::pool_name;
std::optional<librados::Rados> RadosEnv::rados;

auto* const rados_env = ::testing::AddGlobalTestEnvironment(new RadosEnv);

class RGWDedupTest : public ::testing::Test
{
protected:
  static librados::IoCtx ioctx;

  static void SetUpTestSuite() {
    ASSERT_EQ(0, RadosEnv::ioctx_create(ioctx));
  }
  static void TearDownTestSuite() {
    ioctx.close();
  }

  // use the test's name as the oid so different tests don't conflict
  std::string get_test_oid() const {
    return ::testing::UnitTest::GetInstance()->current_test_info()->name();
  }
};
librados::IoCtx RGWDedupTest::ioctx;


TEST(RGWDedupTest, get_buckets)
{
  /*
  RGWDedup rgw_dedup;
  int ret = rgw_dedup.initialize(cct, store);
  ASSERT_EQ(ret, 0);
  rgw_dedup.start_processor();
*/
/*
  test_rgw_env env;
  RGWObjectManifest::generator gen;
  RGWObjManifest manifest;
  rgw_bucket bkt1, bkt2;
  rgw_obj head;

  rgw_obj bkt1_obj1, bkt1_obj2;
  rgw_obj bkt2_obj1, bkt2_obj2, bkt2_obj3;
  
  //test_rgw_init_bucket(bkt1, "test_bucket_1");
  //test_rgw_init_bucket(bkt2, "test_bucket_2");

  int obj_size = 21 * 1024 * 1024 + 1000;
  int stripe_size = 4 * 1024 * 1024;
  int head_size = 512 * 1024;

  old_rgw_bucket eb;
  test_rgw_init_old_bucket(&eb, "ebtest");
  old_rgw_obj old(eb, "testobj");

  bufferlist bl;
  encode(old, bl);

  rgw_obj new_obj;
  rgw_raw_obj raw_obj;
  
  try {
    auto iter = bl.cbegin();
    decode(new_obj
  } catch (buffer::error& err) {
    ASSERT_TRUE(false);
  }
*/
/*
  gen_obj(env, obj_size, head_size, stripe_size, &manifest,
          env.zonegroup.default_placement, &bucket, &head, &gen, &objs);

  list<rgw_obj>::iterator iter;
  rgw_obj_select last_obj;
*/

  ASSERT_TRUE(true);
}

TEST(RGWDedupTest, get_objects)
{
  ASSERT_TRUE(true);
}


int main (int argc, char** argv) {
  auto args = argv_to_vec(argc, argv);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
