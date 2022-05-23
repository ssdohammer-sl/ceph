diff --git a/src/include/rados.h b/src/include/rados.h
index ae4ab59de8..53f706ddcb 100644
--- a/src/include/rados.h
+++ b/src/include/rados.h
@@ -331,6 +331,7 @@ extern const char *ceph_osd_state_name(int s);
 	f(UNSET_MANIFEST, __CEPH_OSD_OP(WR, DATA, 42),	"unset-manifest")   \
 	f(TIER_FLUSH, __CEPH_OSD_OP(CACHE, DATA, 43),	"tier-flush")	    \
 	f(TIER_EVICT, __CEPH_OSD_OP(CACHE, DATA, 44),	"tier-evict")	    \
+	f(ISHOT,      __CEPH_OSD_OP(RD, DATA, 46),	"is-hot")	    \
 									    \
 	/** attrs **/							    \
 	/* read */							    \
