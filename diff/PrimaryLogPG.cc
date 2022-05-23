diff --git a/src/osd/PrimaryLogPG.cc b/src/osd/PrimaryLogPG.cc
index e91213bdee..4fb5a40a1b 100644
--- a/src/osd/PrimaryLogPG.cc
+++ b/src/osd/PrimaryLogPG.cc
@@ -2268,9 +2268,13 @@ void PrimaryLogPG::do_op(OpRequestRef& op)
     m->get_snapid() == CEPH_SNAPDIR ? head : m->get_hobj();
 
   // make sure LIST_SNAPS is on CEPH_SNAPDIR and nothing else
+  bool op_is_hot = false;
   for (vector<OSDOp>::iterator p = m->ops.begin(); p != m->ops.end(); ++p) {
     OSDOp& osd_op = *p;
 
+    if (osd_op.op.op == CEPH_OSD_OP_ISHOT) {
+      op_is_hot = true;
+    }
     if (osd_op.op.op == CEPH_OSD_OP_LIST_SNAPS) {
       if (m->get_snapid() != CEPH_SNAPDIR) {
 	dout(10) << "LIST_SNAPS with incorrect context" << dendl;
@@ -2354,7 +2358,13 @@ void PrimaryLogPG::do_op(OpRequestRef& op)
         in_hit_set = true;
     }
     if (!op->hitset_inserted) {
-      hit_set->insert(oid);
+      if (!op_is_hot) {
+        hit_set->insert(oid);
+      }
+      dout(20) << __func__ << " hit_set true " << oid
+        << " hit_set_start_stamp " << hit_set_start_stamp << " period "
+        << pool.info.hit_set_period << " stamp "
+        << m->get_recv_stamp() << dendl;
       op->hitset_inserted = true;
       if (hit_set->is_full() ||
           hit_set_start_stamp + pool.info.hit_set_period <= m->get_recv_stamp()) {
@@ -2533,14 +2543,27 @@ PrimaryLogPG::cache_result_t PrimaryLogPG::maybe_handle_manifest_detail(
   vector<OSDOp> ops = op->get_req<MOSDOp>()->ops;
   for (vector<OSDOp>::iterator p = ops.begin(); p != ops.end(); ++p) {
     OSDOp& osd_op = *p;
-    ceph_osd_op& op = osd_op.op;
-    if (op.op == CEPH_OSD_OP_SET_REDIRECT ||
-	op.op == CEPH_OSD_OP_SET_CHUNK ||
-	op.op == CEPH_OSD_OP_UNSET_MANIFEST ||
-	op.op == CEPH_OSD_OP_TIER_PROMOTE ||
-	op.op == CEPH_OSD_OP_TIER_FLUSH ||
-	op.op == CEPH_OSD_OP_TIER_EVICT ||
-	op.op == CEPH_OSD_OP_ISDIRTY) {
+    ceph_osd_op& c_osd_op = osd_op.op;
+    if (c_osd_op.op == CEPH_OSD_OP_SET_REDIRECT ||
+	c_osd_op.op == CEPH_OSD_OP_SET_CHUNK ||
+	c_osd_op.op == CEPH_OSD_OP_UNSET_MANIFEST ||
+	c_osd_op.op == CEPH_OSD_OP_TIER_PROMOTE ||
+	c_osd_op.op == CEPH_OSD_OP_TIER_FLUSH ||
+	c_osd_op.op == CEPH_OSD_OP_TIER_EVICT ||
+	c_osd_op.op == CEPH_OSD_OP_ISDIRTY ||
+        c_osd_op.op == CEPH_OSD_OP_ISHOT) {
+      if (c_osd_op.op == CEPH_OSD_OP_SET_CHUNK) {
+        if (!hit_set) {
+          hit_set_setup();
+        }
+        if (agent_state) {
+          if (agent_choose_mode(false, op)) {
+            return cache_result_t::NOOP;
+          }
+        } else {
+          agent_setup();
+        }
+      }
       return cache_result_t::NOOP;
     }
   }
@@ -6201,6 +6224,50 @@ int PrimaryLogPG::do_osd_ops(OpContext *ctx, vector<OSDOp>& ops)
       }
       break;
 
+    case CEPH_OSD_OP_ISHOT:
+      ++ctx->num_read;
+      {
+        bool is_hot = false;
+        is_hot = hit_set->contains(oi.soid);
+        dout(20) << "OP_ISHOT " << oi.soid << " is_hot = " << is_hot << dendl;
+        if (hit_set && !is_hot) {
+          for (map<time_t, HitSetRef>::reverse_iterator iter = 
+                agent_state->hit_set_map.rbegin();
+              iter != agent_state->hit_set_map.rend();
+              ++iter) {
+            if (iter->second->contains(soid)) {
+              is_hot = true;
+              break;
+            }
+          }
+        }
+        bool is_backedup = false;
+        bool is_deduped = false;
+        if (obs.oi.has_manifest() && obs.oi.manifest.is_chunked()) {
+          auto p = obs.oi.manifest.chunk_map.find(0);
+          if (p != obs.oi.manifest.chunk_map.end()) {
+            dout(20) << "  is_backedup: true" << dendl;
+            is_backedup = (obs.oi.soid.oid.name == obs.oi.manifest.chunk_map[0].oid.oid.name);
+          }
+          if (!is_backedup && obs.oi.manifest.chunk_map.size() > 0) {
+            dout(20) << "  is_deduped: true" << dendl;
+            is_deduped = true;
+          }
+        }
+        if (is_deduped && obs.oi.is_dirty()) {
+          dout(20) << "  is_deduped: false" << dendl;
+          is_deduped = false;
+        }
+        dout(20) << "ISHOT op done" << dendl;
+
+        encode(is_hot, osd_op.outdata);
+        encode(is_backedup, osd_op.outdata);
+        encode(is_deduped, osd_op.outdata);
+        ctx->delta_stats.num_rd++;
+        result = 0;
+      }
+      break;
+
     case CEPH_OSD_OP_UNDIRTY:
       ++ctx->num_write;
       result = 0;
@@ -7368,7 +7435,7 @@ int PrimaryLogPG::do_osd_ops(OpContext *ctx, vector<OSDOp>& ops)
 	  break;
 	}
 	if (!obs.oi.has_manifest()) {
-	  result = 0;
+	  result = -EINVAL;
 	  break;
 	}
 
@@ -10444,6 +10511,28 @@ struct C_Flush : public Context {
   }
 };
 
+struct C_Manifest_Flush : public Context {
+  PrimaryLogPGRef pg;
+  hobject_t oid;
+  epoch_t last_peering_reset;
+  ceph_tid_t tid;
+  utime_t start;
+  C_Manifest_Flush(PrimaryLogPG *p, hobject_t o, epoch_t lpr)
+    : pg(p), oid(o), last_peering_reset(lpr),
+      tid(0), start(ceph_clock_now())
+  {}
+
+  void finish(int r) override {
+    if (r == -ECANCELED) {
+      return;
+    }
+    std::scoped_lock locker { *pg };
+    if (last_peering_reset == pg->get_last_peering_reset()) {
+      pg->finish_flush(oid, tid, r);
+    }
+  }
+};
+
 int PrimaryLogPG::start_dedup(OpRequestRef op, ObjectContextRef obc)
 {
   const object_info_t& oi = obc->obs.oi;
@@ -10835,6 +10924,13 @@ int PrimaryLogPG::start_flush(
   }
 
   if (obc->obs.oi.has_manifest() && obc->obs.oi.manifest.is_chunked()) {
+    // flush current object to cold pool
+    if (obc->obs.oi.manifest.chunk_map[0].oid.oid.name == 
+        obc->obs.oi.soid.oid.name) {
+      flush_manifest(op, obc, obc->obs.oi.manifest.chunk_map[0].oid);
+      return -EINPROGRESS;
+    }
+
     int r = start_dedup(op, obc);
     if (r != -EINPROGRESS) {
       if (blocking)
@@ -10952,6 +11048,44 @@ int PrimaryLogPG::start_flush(
   return -EINPROGRESS;
 }
 
+void PrimaryLogPG::flush_manifest(
+    OpRequestRef op, ObjectContextRef obc, hobject_t& t_oid)
+{
+  const object_info_t& oi = obc->obs.oi;
+  const hobject_t& soid = oi.soid;
+  FlushOpRef fop(std::make_shared<FlushOp>());
+  fop->obc = obc;
+  fop->flushed_version = oi.user_version;
+  fop->blocking = true;
+  fop->op = op;
+
+  dout(10) << __func__ << " start" << dendl;
+
+  ObjectOperation o;
+  object_locator_t oloc(soid);
+  o.copy_from(soid.oid.name, soid.snap, oloc, oi.user_version,
+      CEPH_OSD_COPY_FROM_FLAG_FLUSH |
+      CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY | 
+      CEPH_OSD_COPY_FROM_FLAG_IGNORE_CACHE |
+      CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE,
+      LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL | 
+      LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
+  C_Manifest_Flush *fin = new C_Manifest_Flush(this, soid, get_last_peering_reset());
+
+  object_locator_t base_oloc(t_oid);
+  SnapContext snapc;
+  ceph_tid_t tid = osd->objecter->mutate(
+      soid.oid, base_oloc, o, snapc,
+      ceph::real_clock::from_ceph_timespec(oi.mtime),
+      CEPH_OSD_FLAG_IGNORE_OVERLAY | CEPH_OSD_FLAG_ENFORCE_SNAPC,
+      new C_OnFinisher(fin,
+        osd->get_objecter_finisher(get_pg_shard())));
+
+  fin->tid = tid;
+  fop->objecter_tid = tid;
+  flush_ops[soid] = fop;
+}
+
 void PrimaryLogPG::finish_flush(hobject_t oid, ceph_tid_t tid, int r)
 {
   dout(10) << __func__ << " " << oid << " tid " << tid
@@ -14665,10 +14799,14 @@ void PrimaryLogPG::agent_setup()
   ceph_assert(is_locked());
   if (!is_active() ||
       !is_primary() ||
-      state_test(PG_STATE_PREMERGE) ||
+      state_test(PG_STATE_PREMERGE) 
+#if 0
+      ||
       pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE ||
       pool.info.tier_of < 0 ||
-      !get_osdmap()->have_pg_pool(pool.info.tier_of)) {
+      !get_osdmap()->have_pg_pool(pool.info.tier_of)
+#endif
+      ) {
     agent_clear();
     return;
   }
@@ -14731,8 +14869,10 @@ bool PrimaryLogPG::agent_work(int start_max, int agent_flush_quota)
 
   agent_load_hit_sets();
 
+#if 0
   const pg_pool_t *base_pool = get_osdmap()->get_pg_pool(pool.info.tier_of);
   ceph_assert(base_pool);
+#endif
 
   int ls_min = 1;
   int ls_max = cct->_conf->osd_pool_default_cache_max_evict_check_size;
@@ -14796,6 +14936,7 @@ bool PrimaryLogPG::agent_work(int start_max, int agent_flush_quota)
       continue;
     }
 
+#if 0
     // be careful flushing omap to an EC pool.
     if (!base_pool->supports_omap() &&
 	obc->obs.oi.is_omap()) {
@@ -14803,7 +14944,13 @@ bool PrimaryLogPG::agent_work(int start_max, int agent_flush_quota)
       osd->logger->inc(l_osd_agent_skip);
       continue;
     }
+#endif
+
+    if (obc->obs.oi.has_manifest() && obc->obs.oi.manifest.is_chunked()) {
+      dout(20) << __func__ << " agent evict" << dendl;
+    }
 
+#if 0
     if (agent_state->evict_mode != TierAgentState::EVICT_MODE_IDLE &&
 	agent_maybe_evict(obc, false))
       ++started;
@@ -14812,6 +14959,8 @@ bool PrimaryLogPG::agent_work(int start_max, int agent_flush_quota)
       ++started;
       --agent_flush_quota;
     }
+#endif
+
     if (started >= start_max) {
       // If finishing early, set "next" to the next object
       if (++p != ls.end())
@@ -15142,11 +15291,13 @@ bool PrimaryLogPG::agent_choose_mode(bool restart, OpRequestRef op)
   // they cannot be flushed.
   uint64_t unflushable = info.stats.stats.sum.num_objects_hit_set_archive;
 
+#if 0
   // also exclude omap objects if ec backing pool
   const pg_pool_t *base_pool = get_osdmap()->get_pg_pool(pool.info.tier_of);
   ceph_assert(base_pool);
   if (!base_pool->supports_omap())
     unflushable += info.stats.stats.sum.num_objects_omap;
+#endif
 
   uint64_t num_user_objects = info.stats.stats.sum.num_objects;
   if (num_user_objects > unflushable)
@@ -15162,12 +15313,14 @@ bool PrimaryLogPG::agent_choose_mode(bool restart, OpRequestRef op)
 
   // also reduce the num_dirty by num_objects_omap
   int64_t num_dirty = info.stats.stats.sum.num_objects_dirty;
+#if 0
   if (!base_pool->supports_omap()) {
     if (num_dirty > info.stats.stats.sum.num_objects_omap)
       num_dirty -= info.stats.stats.sum.num_objects_omap;
     else
       num_dirty = 0;
   }
+#endif
 
   dout(10) << __func__
 	   << " flush_mode: "
@@ -15325,6 +15478,9 @@ bool PrimaryLogPG::agent_choose_mode(bool restart, OpRequestRef op)
       });
     agent_state->evict_mode = evict_mode;
   }
+
+  agent_state->evict_mode = TierAgentState::EVICT_MODE_SOME;
+
   uint64_t old_effort = agent_state->evict_effort;
   if (evict_effort != agent_state->evict_effort) {
     dout(5) << __func__ << " evict_effort "
@@ -15344,6 +15500,7 @@ bool PrimaryLogPG::agent_choose_mode(bool restart, OpRequestRef op)
     }
   } else {
     if (restart || old_idle) {
+      dout(0) << " enable cache agent: " << info.pgid << dendl;
       osd->agent_enable_pg(this, agent_state->evict_effort);
     } else if (old_effort != agent_state->evict_effort) {
       osd->agent_adjust_pg(this, old_effort, agent_state->evict_effort);
