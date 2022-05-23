diff --git a/src/osd/PrimaryLogPG.h b/src/osd/PrimaryLogPG.h
index b92c46bf4a..7d847acce9 100644
--- a/src/osd/PrimaryLogPG.h
+++ b/src/osd/PrimaryLogPG.h
@@ -973,6 +973,7 @@ protected:
   bool agent_work(int max, int agent_flush_quota) override;
   bool agent_maybe_flush(ObjectContextRef& obc);  ///< maybe flush
   bool agent_maybe_evict(ObjectContextRef& obc, bool after_flush);  ///< maybe evict
+  void flush_manifest(OpRequestRef op, ObjectContextRef obc, hobject_t& t_oid);
 
   void agent_load_hit_sets();  ///< load HitSets, if needed
 
@@ -1365,6 +1366,7 @@ protected:
   bool is_present_clone(hobject_t coid);
 
   friend struct C_Flush;
+  friend struct C_Manifest_Flush;
 
   // -- cls_gather --
   std::map<hobject_t, CLSGatherOp> cls_gather_ops;
