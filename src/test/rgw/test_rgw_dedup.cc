// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"

#include "rgw/rgw_dedup_proc.h"
#include "rgw/rgw_sal_rados.h"
#include "common/dout.h"
#include "common/CDC.h"


auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
const DoutPrefix dp(cct, 1, "test rgw dedup: ");

class RGWDedupTest : public ::testing::Test
{
protected:
  rgw::sal::RadosStore store;

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  void SetUp() override {}
  void TearDown() override {}

  // use the test's name as the oid so different tests don't conflict
  //std::string get_test_oid() const {
    //return ::testing::UnitTest::GetInstance()->current_test_info()->name();
  //}

public:
  RGWDedupTest() {}
  ~RGWDedupTest() override {}
};


TEST_F(RGWDedupTest, sample_objects)
{
  DedupProcessor proc(&dp, cct, &store);
  int sampling_ratio = 0;
  EXPECT_EQ(-1, proc.set_sampling_ratio(sampling_ratio));
  sampling_ratio = 1000;
  EXPECT_EQ(-1, proc.set_sampling_ratio(sampling_ratio));
  sampling_ratio = 30;
  EXPECT_EQ(0, proc.set_sampling_ratio(sampling_ratio));

  int num_objs = 10;
  for (int i = 0; i < num_objs; ++i) {
    rgw_obj_key key("obj_ " + i);
    rgw::sal::RadosObject obj(&store, key);
    proc.append_obj(&obj);
  }
  EXPECT_EQ(num_objs, proc.get_num_objs());

  vector<size_t> sampled_idx = proc.sample_object_index();
  EXPECT_EQ(num_objs * sampling_ratio / 100, sampled_idx.size());

  sampling_ratio = 100;
  EXPECT_EQ(0, proc.set_sampling_ratio(sampling_ratio));
  sampled_idx.clear();
  sampled_idx = proc.sample_object_index();
  EXPECT_EQ(num_objs * sampling_ratio / 100, sampled_idx.size());
}

TEST_F(RGWDedupTest, do_chunking)
{
  DedupProcessor proc(&dp, cct, &store);
  uint64_t chunk_size = 0;
  EXPECT_EQ(-1, proc.set_chunk_size(chunk_size));

  // test fastcdc
  EXPECT_EQ(0, proc.set_chunk_algo("fastcdc"));

  chunk_size = 512; 
  EXPECT_EQ(0, proc.set_chunk_size(chunk_size));

  int data_size = 1024 * 4;
  bufferlist bl;
  generate_buffer(data_size, &bl);
  
  vector<tuple<bufferlist, ChunkInfo>> chunk_info;
  int ret = proc.do_chunking(bl, chunk_info);
  EXPECT_EQ(0, ret);

  std::cout << "fastcdc" << std::endl;
  int chunk_size_sum = 0;
  for (auto& chunk : chunk_info) {
    auto& chunk_boundary = std::get<1>(chunk);
    chunk_size_sum += chunk_boundary.second;
    std::cout << "  offset: " << chunk_boundary.first 
      << " len: " << chunk_boundary.second << std::endl;
  }
  EXPECT_EQ(data_size, chunk_size_sum);

  // test fixed
  EXPECT_EQ(0, proc.set_chunk_algo("fixed"));
  chunk_info.clear();
  ret = proc.do_chunking(bl, chunk_info);
  EXPECT_EQ(0, ret);
  
  std::cout << "fixed" << std::endl;
  chunk_size_sum = 0;
  for (auto& chunk : chunk_info) {
    auto& chunk_boundary = std::get<1>(chunk);
    chunk_size_sum += chunk_boundary.second;
    std::cout << "  offset: " << chunk_boundary.first 
      << " len: " << chunk_boundary.second << std::endl;
  }
  EXPECT_EQ(data_size, chunk_size_sum);
}

TEST_F(RGWDedupTest, generate_fingerprint)
{
  DedupProcessor proc(&dp, cct, &store);
  string fp_algo = "shashasha";
  EXPECT_EQ(-1, proc.set_fp_algo(fp_algo));

  // test sha1
  fp_algo = "sha1";
  EXPECT_EQ(0, proc.set_fp_algo(fp_algo));

  bufferlist chunk_1, chunk_2;
  string fp_chunk_1, fp_chunk_2;
  chunk_1.append("good morning");
  chunk_2.append("good morning");
  fp_chunk_1 = proc.generate_fingerprint(chunk_1);
  fp_chunk_2 = proc.generate_fingerprint(chunk_2);
  EXPECT_EQ(fp_chunk_1, fp_chunk_2);

  chunk_2.clear();
  chunk_2.append("good afternoon");
  fp_chunk_2 = proc.generate_fingerprint(chunk_2);
  EXPECT_NE(fp_chunk_1, fp_chunk_2);

  // test sha256
  fp_algo = "sha256";
  EXPECT_EQ(0, proc.set_fp_algo(fp_algo));

  chunk_1.clear();
  chunk_2.clear();
  chunk_1.append("good morning");
  chunk_2.append("good morning");
  fp_chunk_1 = proc.generate_fingerprint(chunk_1);
  fp_chunk_2 = proc.generate_fingerprint(chunk_2);
  EXPECT_EQ(fp_chunk_1, fp_chunk_2);

  chunk_2.clear();
  chunk_2.append("good afternoon");
  fp_chunk_2 = proc.generate_fingerprint(chunk_2);
  EXPECT_NE(fp_chunk_1, fp_chunk_2);

  // test sha512
  fp_algo = "sha512";
  EXPECT_EQ(0, proc.set_fp_algo(fp_algo));

  chunk_1.clear();
  chunk_2.clear();
  chunk_1.append("good morning");
  chunk_2.append("good morning");
  fp_chunk_1 = proc.generate_fingerprint(chunk_1);
  fp_chunk_2 = proc.generate_fingerprint(chunk_2);
  EXPECT_EQ(fp_chunk_1, fp_chunk_2);

  chunk_2.clear();
  chunk_2.append("good afternoon");
  fp_chunk_2 = proc.generate_fingerprint(chunk_2);
  EXPECT_NE(fp_chunk_1, fp_chunk_2);
}


int main (int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
