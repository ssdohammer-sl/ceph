diff --git a/src/include/rados/librados.hpp b/src/include/rados/librados.hpp
index 7c883c3364..ee1721746a 100644
--- a/src/include/rados/librados.hpp
+++ b/src/include/rados/librados.hpp
@@ -760,6 +760,7 @@ inline namespace v14_2_0 {
      * updates.
      */
     void tier_evict();
+    void is_hot(bool *isdirty, int *prval);
   };
 
   /* IoCtx : This is a context in which we can perform I/O.
