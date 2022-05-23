diff --git a/src/osdc/Objecter.h b/src/osdc/Objecter.h
index 582ce8c197..97b8d91a87 100644
--- a/src/osdc/Objecter.h
+++ b/src/osdc/Objecter.h
@@ -1183,6 +1183,38 @@ struct ObjectOperation {
     }
   };
 
+  struct C_ObjectOperation_ishot : public Context {
+    ceph::buffer::list bl;
+    bool *pishot;
+    int *prval;
+    C_ObjectOperation_ishot(bool *p, int *r)
+      : pishot(p), prval(r) {}
+    void finish(int r) override {
+      using ceph::decode;
+      if (r < 0)
+        return;
+      try {
+        auto p = bl.cbegin();
+        bool ishot;
+        bool isbackedup;
+        bool isdeduped;
+        decode(ishot, p);
+        decode(isbackedup, p);
+        decode(isdeduped, p);
+        if (pishot)             // object is hot
+          *pishot = ishot;
+        if (isbackedup) {       // object is already backed up
+          *prval = 1000;
+        } else if (isdeduped) { // object is already deduped
+          *prval = 2000;
+        }
+      } catch (const ceph::buffer::error& e) {
+        if (prval)
+          *prval = -EIO;
+      }
+    }
+  };
+
   void is_dirty(bool *pisdirty, int *prval) {
     add_op(CEPH_OSD_OP_ISDIRTY);
     unsigned p = ops.size() - 1;
@@ -1193,6 +1225,16 @@ struct ObjectOperation {
     set_handler(h);
   }
 
+  void is_hot(bool *pisdirty, int *prval) {
+    add_op(CEPH_OSD_OP_ISHOT);
+    unsigned p = ops.size() - 1;
+    out_rval[p] = prval;
+    C_ObjectOperation_ishot *h =
+      new C_ObjectOperation_ishot(pisdirty, prval);
+    out_bl[p] = &h->bl;
+    set_handler(h);
+  }
+
   struct C_ObjectOperation_hit_set_ls : public Context {
     ceph::buffer::list bl;
     std::list< std::pair<time_t, time_t> > *ptls;
