diff --git a/src/tools/ceph_dedup_tool.cc b/src/tools/ceph_dedup_tool.cc
index 62a64dd0eb..d47da5eb5e 100644
--- a/src/tools/ceph_dedup_tool.cc
+++ b/src/tools/ceph_dedup_tool.cc
@@ -514,7 +514,7 @@ void ChunkScrub::chunk_scrub_common()
   cout << "--done--" << std::endl;
 }
 
-#define DEBUG_OUT(x) if(debug==1){std::cout<<x;}else{}
+#define DEBUG_OUT(x) if (debug == 1) { std::cout << x << std::endl; } else {}
 static int make_manifest_object_and_flush(
   std::string& oid,
   IoCtx& io_ctx,
@@ -546,7 +546,7 @@ static int make_manifest_object_and_flush(
   }
 
   // set-chunk to make manifest object
-   ObjectReadOperation chunk_op;
+  ObjectReadOperation chunk_op;
   chunk_op.set_chunk(0, 4, chunk_io_ctx, temp_oid, 0,
       CEPH_OSD_OP_FLAG_WITH_REFERENCE);
   ret = io_ctx.operate(oid, &chunk_op, NULL);
@@ -577,6 +577,7 @@ public:
   SampleDedup(
     IoCtx& io_ctx,
     IoCtx& chunk_io_ctx,
+    IoCtx& cold_io_ctx,
     int n,
     int m,
     ObjectCursor& begin,
@@ -591,6 +592,7 @@ public:
     crawl_mode_t mode) :
     CrawlerThread(io_ctx, n, m, begin, end, report_period, num_objects),
     chunk_io_ctx(chunk_io_ctx),
+    cold_io_ctx(cold_io_ctx),
     mode(mode),
     sampling_ratio(sampling_ratio),
     chunk_dedup_threshold(chunk_dedup_threshold),
@@ -629,7 +631,7 @@ private:
     ObjectCursor end,
     size_t max_object_count);
   std::set<size_t> sample_object(size_t count);
-  void try_object_dedup_and_accumulate_result(ObjectItem& object);
+  void try_object_dedup_and_accumulate_result(ObjectItem& object, bool& is_unique);
   bool ok_to_dedup_all();
   void flush_duplicable_object(ObjectItem& object);
   AioCompletion* set_chunk_duplicated(chunk_t& chunk);
@@ -641,10 +643,13 @@ private:
   std::string generate_fingerprint(bufferlist chunk_data);
   bool check_whole_object_dedupable(size_t dedup_size, size_t total_size);
   bool is_dirty(ObjectItem& object);
+  bool is_hot(ObjectItem& object, bool& backeped, bool& deduped);
+  int make_object_cold(ObjectItem& object);
   AioCompletion* do_async_evict(string oid);
 
   Rados rados;
   IoCtx chunk_io_ctx;
+  IoCtx cold_io_ctx;
   crawl_mode_t mode;
   uint32_t sampling_ratio;
   std::list<chunk_t> duplicable_chunks;
@@ -728,7 +733,7 @@ void SampleDedup::crawl() {
     ObjectCursor shard_start;
     ObjectCursor shard_end;
     std::tie(shard_start, shard_end) = get_shard_boundary();
-    cout << "new iteration thread: " << n <<std::endl;
+    DEBUG_OUT("new iteration thread: " << n);
 
     for (ObjectCursor current_object = shard_start;
         current_object < shard_end;) {
@@ -745,11 +750,36 @@ void SampleDedup::crawl() {
       std::set<size_t> sampled_indexes = sample_object(objects.size());
       for (size_t index : sampled_indexes) {
         ObjectItem target = objects[index];
+
         // Only process dirty objects which are expected not processed yet
+        /* upstream original code
         if (is_dirty(target)) {
           try_object_dedup_and_accumulate_result(target);
         }
+        */
+
+        bool backedup = false;
+        bool deduped = false;
+        if (!is_hot(target, backedup, deduped)) {
+          DEBUG_OUT(" object is not hot : " << target.oid);
+
+          // target object is cold and not deduped
+          if (!deduped) {
+            bool is_unique = true;
+            try_object_dedup_and_accumulate_result(target, is_unique);
+
+            // target object is cold and unique
+            if (is_unique && !backedup) {
+              DEBUG_OUT(" cold object: " << target.oid);
+              make_object_cold(target);
+            }
+          }
+        }
+        else {
+          DEBUG_OUT("  object is hot " << target.oid);
+        }
       }
+      break;
     }
 
     map<std::string,AioCompletion*> set_chunk_completions;
@@ -761,6 +791,7 @@ void SampleDedup::crawl() {
       }
     }
 
+/*
     vector<AioCompletion*> evict_completions;
     for (auto& oid : oid_for_evict) {
       auto completion_iter = set_chunk_completions.find(oid);
@@ -778,16 +809,17 @@ void SampleDedup::crawl() {
       completion->wait_for_complete();
       delete completion;
     }
+*/
   } catch (std::exception& e) {
     cerr << "exception : " << e.what() << std::endl;
   }
-  cout << "done iteration thread: " << n <<std::endl;
+  DEBUG_OUT("done iteration thread: " << n);
 }
 
 AioCompletion* SampleDedup::do_async_evict(string oid) {
   ObjectReadOperation op_tier;
   AioCompletion* completion = rados.aio_create_completion();
-  DEBUG_OUT("evict " << oid << std::endl);
+  DEBUG_OUT("evict " << oid);
   op_tier.tier_evict();
   io_ctx.aio_operate(
       oid,
@@ -814,6 +846,44 @@ bool SampleDedup::is_dirty(ObjectItem& object) {
   return dirty;
 }
 
+bool SampleDedup::is_hot(ObjectItem& object, bool& backedup, bool& deduped) {
+  ObjectReadOperation op;
+  bool hot = false;
+  int r = -1;
+  op.is_hot(&hot, &r);
+  io_ctx.operate(object.oid, &op, NULL);
+  if (r == 1000) {
+    backedup = true;
+    DEBUG_OUT(__func__ << " object " << object.oid << " is already backedup ");
+  } else if (r == 2000) {
+    deduped = true;
+    DEBUG_OUT(__func__ << " object " << object.oid << " is already deduped ");
+  }
+  return hot;
+}
+
+int SampleDedup::make_object_cold(ObjectItem& object) {
+  bufferlist data = read_object(object);
+  ObjectWriteOperation wop;
+  wop.write_full(data);
+  int r = cold_io_ctx.operate(object.oid, &wop);
+  if (r < 0) {
+    cerr << __func__ << " write_full error: " << cpp_strerror(r) << std::endl;
+    return r;
+  }
+  ObjectReadOperation rop;
+  rop.set_chunk(0, data.length(), cold_io_ctx, object.oid, 0,
+      CEPH_OSD_OP_FLAG_WITH_REFERENCE);
+  r = io_ctx.operate(object.oid, &rop, NULL);
+  if (r < 0) {
+    cerr << " operate fail: " << cpp_strerror(r) << std::endl;
+    return r;
+  }
+  //oid_for_evict.insert(object.oid);
+
+  return r;
+}
+
 void SampleDedup::prepare_rados() {
   int ret = rados.init_with_context(g_ceph_context);
   if (ret < 0) {
@@ -882,7 +952,7 @@ std::set<size_t> SampleDedup::sample_object(size_t count) {
   return indexes;
 }
 
-void SampleDedup::try_object_dedup_and_accumulate_result(ObjectItem& object) {
+void SampleDedup::try_object_dedup_and_accumulate_result(ObjectItem& object, bool& is_unique) {
   bufferlist data = read_object(object);
   if (data.length() == 0) {
     cerr << __func__ << " skip object " << object.oid
@@ -917,11 +987,11 @@ void SampleDedup::try_object_dedup_and_accumulate_result(ObjectItem& object) {
       };
 
     DEBUG_OUT("check " << chunk_info.oid << " fp " << fingerprint << " "
-      << chunk_info.start << ", " << chunk_info.size << std::endl);
+      << chunk_info.start << ", " << chunk_info.size);
     if (fp_store.find(fingerprint)) {
       DEBUG_OUT("duplication oid " << chunk_info.oid <<  " "
         << chunk_info.fingerprint << " " << chunk_info.start << ", "
-        << chunk_info.size << std::endl);
+        << chunk_info.size);
       duplicated_size += chunk_data.length();
     }
     fp_store.add(chunk_info, duplicable_chunks);
@@ -930,12 +1000,13 @@ void SampleDedup::try_object_dedup_and_accumulate_result(ObjectItem& object) {
   size_t object_size = data.length();
 
   DEBUG_OUT("oid " << object.oid << " object_size " << object_size
-    << " dup size " << duplicated_size << std::endl);
+    << " dup size " << duplicated_size);
   // if the chunks in an object are duplicated higher than object_dedup_threshold,
   // try deduplicate whole object via tier_flush
   if (check_whole_object_dedupable(duplicated_size, object_size)) {
-    DEBUG_OUT("dedup object " << object.oid << std::endl);
+    DEBUG_OUT("dedup object " << object.oid);
     flush_duplicable_object(object);
+    is_unique = false;
   }
 
   total_duplicated_size += duplicated_size;
@@ -946,7 +1017,7 @@ void SampleDedup::try_object_dedup_and_accumulate_result(ObjectItem& object) {
 bufferlist SampleDedup::read_object(ObjectItem& object) {
   bufferlist whole_data;
   size_t offset = 0;
-  DEBUG_OUT("read object " << object.oid << std::endl);
+  DEBUG_OUT("read object " << object.oid);
   int ret  = -1;
   while (ret != 0) {
     bufferlist partial_data;
@@ -1016,7 +1087,7 @@ bool SampleDedup::check_whole_object_dedupable(
 void SampleDedup::flush_duplicable_object(ObjectItem& object) {
   ObjectReadOperation op;
   op.tier_flush();
-  DEBUG_OUT("try flush " << object.oid << " " << &flushed_objects << std::endl);
+  DEBUG_OUT("try flush " << object.oid << " " << &flushed_objects);
   {
     std::unique_lock lock(flushed_lock);
     flushed_objects.insert(object.oid);
@@ -1034,12 +1105,11 @@ AioCompletion* SampleDedup::set_chunk_duplicated(chunk_t& chunk) {
   {
     std::shared_lock lock(flushed_lock);
     if (flushed_objects.find(chunk.oid) != flushed_objects.end()) {
-      oid_for_evict.insert(chunk.oid);
+//      oid_for_evict.insert(chunk.oid);
       return nullptr;
     }
   }
-  DEBUG_OUT("set chunk " << chunk.oid << " fp " << chunk.fingerprint
-    << std::endl);
+  DEBUG_OUT("set chunk " << chunk.oid << " fp " << chunk.fingerprint);
 
   uint64_t size;
   time_t mtime;
@@ -1068,7 +1138,17 @@ AioCompletion* SampleDedup::set_chunk_duplicated(chunk_t& chunk) {
       completion,
       &op,
       NULL);
-  oid_for_evict.insert(chunk.oid);
+
+  completion->wait_for_complete();
+  if (completion->get_return_value() < 0 && ret == -ENOENT) {
+    DEBUG_OUT(__func__ << " set-chunk fail. remove chunk object");
+    ObjectWriteOperation op;
+    op.remove();
+    chunk_io_ctx.operate(chunk.fingerprint, &op);
+  }
+  completion->release();
+
+//  oid_for_evict.insert(chunk.oid);
   return completion;
 }
 
@@ -1776,6 +1856,15 @@ int make_crawling_daemon(const map<string, string> &opts,
     return -EINVAL;
   }
 
+  string cold_pool_name;
+  i = opts.find("cold-pool");
+  if (i != opts.end()) {
+    cold_pool_name = i->second.c_str();
+  } else {
+    cerr << "must specify --cold-pool" << std::endl;
+    return -EINVAL;
+  }
+
   unsigned max_thread = default_max_thread;
   i = opts.find("max-thread");
   if (i != opts.end()) {
@@ -1860,7 +1949,7 @@ int make_crawling_daemon(const map<string, string> &opts,
   }
 
   list<string> pool_names;
-  IoCtx io_ctx, chunk_io_ctx;
+  IoCtx io_ctx, chunk_io_ctx, cold_io_ctx;
   pool_names.push_back(base_pool_name);
   ret = rados.ioctx_create(base_pool_name.c_str(), io_ctx);
   if (ret < 0) {
@@ -1877,6 +1966,15 @@ int make_crawling_daemon(const map<string, string> &opts,
       << cpp_strerror(ret) << std::endl;
     return -EINVAL;
   }
+
+  ret = rados.ioctx_create(cold_pool_name.c_str(), cold_io_ctx);
+  if (ret < 0) {
+    cerr << "error opening cold pool "
+      << cold_pool_name << ": "
+      << cpp_strerror(ret) << std::endl;
+    return -EINVAL;
+  }
+
   bufferlist inbl;
   ret = rados.mon_command(
       make_pool_str(base_pool_name, "fingerprint_algorithm", fp_algo),
@@ -1899,12 +1997,19 @@ int make_crawling_daemon(const map<string, string> &opts,
     cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
     return ret;
   }
+
+    bool debug = false;
+    i = opts.find("debug");
+    if (i != opts.end()) {
+      debug = true;
+    }
   
   cout << "SampleRatio : " << sampling_ratio << std::endl
     << "Object Dedup Threshold : " << object_dedup_threshold << std::endl
     << "Chunk Dedup Threshold : " << chunk_dedup_threshold << std::endl
     << "Chunk Size : " << chunk_size << std::endl
-    << "Mode : " << ((crawl_mode == SampleDedup::crawl_mode_t::DEEP) ? "DEEP" : "SHALOW")
+    << "Mode : " << ((crawl_mode == SampleDedup::crawl_mode_t::DEEP) ? "DEEP" : "SHALOW") << std::endl
+    << ((debug == true) ? "DEBUG on" : "")
     << std::endl;
 
   while (true) {
@@ -1923,12 +2028,6 @@ int make_crawling_daemon(const map<string, string> &opts,
     }
     librados::pool_stat_t s = stats[base_pool_name];
 
-    bool debug = false;
-    i = opts.find("debug");
-    if (i != opts.end()) {
-      debug = true;
-    }
-
     estimate_threads.clear();
     SampleDedup::init(chunk_dedup_threshold);
     for (unsigned i = 0; i < max_thread; i++) {
@@ -1937,6 +2036,7 @@ int make_crawling_daemon(const map<string, string> &opts,
           new SampleDedup(
             io_ctx,
             chunk_io_ctx,
+            cold_io_ctx,
             i,
             max_thread,
             begin,
@@ -2010,6 +2110,8 @@ int main(int argc, const char **argv)
       opts["fingerprint-algorithm"] = val;
     } else if (ceph_argparse_witharg(args, i, &val, "--chunk-pool", (char*)NULL)) {
       opts["chunk-pool"] = val;
+    } else if (ceph_argparse_witharg(args, i, &val, "--cold-pool", (char*)NULL)) {
+      opts["cold-pool"] = val;
     } else if (ceph_argparse_witharg(args, i, &val, "--target-ref", (char*)NULL)) {
       opts["target-ref"] = val;
     } else if (ceph_argparse_witharg(args, i, &val, "--target-ref-pool-id", (char*)NULL)) {
