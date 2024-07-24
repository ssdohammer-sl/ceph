#!/bin/bash

set -x

die() {
  echo "$@"
  exit1
}

do_run() {
  if [ "$1" == "--tee" ]; then
    shift
    tee_out="$1"
    shift
    "$@" | tee $tee_out
  else
    "$@"
  fi
}

run_expect_succ() {
  echo "RUN_EXPECT_SUCC: " $@
  do_run "$@"
  [ $? -ne 0 ] && die "expected success, but got failure! cmd: $@"
}

run() {
  echo "RUN: " $@
  do_run "$@"
}

if [ -n "$CEPH_BIN" ]; then
  # CMake env
  RADOS_TOOL="$CEPH_BIN/rados"
  CEPH_TOOL="$CEPH_BIN/ceph"
  DEDUP_TOOL="$CEPH_BIN/ceph-dedup-tool"
  DEDUP_DAEMON="$CEPH_BIN/ceph-dedup-daemon"
else
   # executables should be installed by the QA env
   RADOS_TOOL=$(which rados)
   CEPH_TOOL=$(which ceph)
   DEDUP_TOOL=$(which ceph-dedup-tool)
   DEDUP_DAEMON=$(which ceph-dedup-daemon)
fi

POOL=dedup_pool
OBJ=test_rados_obj

[ -x "$RADOS_TOOL" ] || die "couldn't find $RADOS_TOOL binary to test"
[ -x "$CEPH_TOOL" ] || die "couldn't find $CEPH_TOOL binary to test"


function test_hot_cold_dedup()
{
  CHUNK_POOL=dedup_chunk_pool
  $CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it
  $CEPH_TOOL osd pool delete $CHUNK_POOL $CHUNK_POOL --yes-i-really-really-mean-it

  sleep 2

  run_expect_succ "$CEPH_TOOL" osd pool create "$POOL" 8
  run_expect_succ "$CEPH_TOOL" osd pool create "$CHUNK_POOL" 8
  run_expect_succ "$CEPH_TOOL" osd pool set "$POOL" dedup_tier $CHUNK_POOL
  run_expect_succ "$CEPH_TOOL" osd pool set "$POOL" hit_set_type explicit_object
  run_expect_succ "$CEPH_TOOL" osd pool set "$POOL" hit_set_count 1
  run_expect_succ "$CEPH_TOOL" osd pool set "$POOL" hit_set_period 10

  # 5 dedupable objects
  CONTENTS_1="HIHIHIHIHI"
  echo $CONTENTS_1 > foo
  for num in `seq 1 5`; do
    $RADOS_TOOL -p $POOL put foo_num ./foo
  done

  sleep 1

  # hot object detection test
  # execute dedup-daemon while expecting all objects are still hot
  #$DEDUP_DAEMON --pool $POOL --chunk-pool $CHUNK_POOL --chunk-algorithm fastcdc --fingerprint-algorithm sha1 --chunk-dedup-threshold 2 --sampling-ratio 100 --run-once --max-thread 1

  sleep 2
  PID=$(pidof ceph-dedup-daemon)
  COUNT=1
  while [ -n "$PID" ] && [ $COUNT -le 30 ]; do
    sleep 15
    PID=$(pidof ceph-dedup-daemon)
    ((COUNT++))
  done

  NUM_CHUNK_POOL_OBJS=$("$CEPH_TOOL" df | grep "$CHUNK_POOL")
  echo $NUM_CHUNK_POOL_OBJS
}

test_hot_cold_dedup


$CEPH_TOOL osd pool delete $POOL $POOL --yes-i-really-really-mean-it

echo "SUCCESS!"
exit 0

