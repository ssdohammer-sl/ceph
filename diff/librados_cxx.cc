diff --git a/src/librados/librados_cxx.cc b/src/librados/librados_cxx.cc
index 75e3e797a9..700674d896 100644
--- a/src/librados/librados_cxx.cc
+++ b/src/librados/librados_cxx.cc
@@ -374,6 +374,13 @@ void librados::ObjectReadOperation::is_dirty(bool *is_dirty, int *prval)
   o->is_dirty(is_dirty, prval);
 }
 
+void librados::ObjectReadOperation::is_hot(bool *is_hot, int *prval)
+{
+  ceph_assert(impl);
+  ::ObjectOperation *o = &impl->o;
+  o->is_hot(is_hot, prval);
+}
+
 int librados::IoCtx::omap_get_vals(const std::string& oid,
                                    const std::string& orig_start_after,
                                    const std::string& filter_prefix,
