diff --git a/src/mon/OSDMonitor.cc b/src/mon/OSDMonitor.cc
index 3e59384a38..0ae2c6a205 100644
--- a/src/mon/OSDMonitor.cc
+++ b/src/mon/OSDMonitor.cc
@@ -8256,8 +8256,11 @@ int OSDMonitor::prepare_command_pool_set(const cmdmap_t& cmdmap,
   }
 
   if (!p.is_tier() &&
-      (var == "hit_set_type" || var == "hit_set_period" ||
+      (
+#if 0
+       var == "hit_set_type" || var == "hit_set_period" ||
        var == "hit_set_count" || var == "hit_set_fpp" ||
+#endif
        var == "target_max_objects" || var == "target_max_bytes" ||
        var == "cache_target_full_ratio" || var == "cache_target_dirty_ratio" ||
        var == "cache_target_dirty_high_ratio" || var == "use_gmt_hitset" ||
