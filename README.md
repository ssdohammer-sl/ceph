# TiDedup
*TiDedup* is a cluster-level deduplication architecture for Ceph which provides selective crawling for effective storage resource management.
In this branch, the changelist for *TiDedup*, which has been accepted by USENIX ATC’23, has been rebased onto ceph/main branch to provide the explanation in detail.

For more details on the internals of *TiDedup*, please refer to the paper.


## Getting Started
*TiDedup* is Ceph's default deduplication and it aims to explore ways to reduce storage data efficiently by scanning and analyzing objects.

All designs and features that introduce in the paper have been merged in Ceph mainline except for two small parts which are object hot-cold decision, and memory utilization optimization. These features are almost ready for upstream. So we add two commits at this branch.

There are two ways to use deduplication in Ceph.

 1. **Use mainline's deduplication**:
		- Download the latest Ceph release and enable a deduplication feature. The detailed steps can be referred to at the following document link:

    > [ceph/deduplication.rst at main · ceph/ceph · GitHub](https://github.com/ceph/ceph/blob/main/doc/dev/deduplication.rst)

 2. **Use this git repository**:
                - Check out the current branch that contains those two new optimizations and enable a deduplication feature.


### Prerequisites
Ceph cluster must be provisioned and running prior to enabling *TiDedup*. When the Ceph cluster becomes available, users can use *TiDedup* by executing *ceph-dedup-tool*. If the Ceph cluster is started from Ceph mainline, you need to check *ceph-test* package which is including *ceph-dedup-tool* is installed.

#### Start Ceph cluster
We just introduced how to deploy and use development cluster configure, called "vstart", to demonstrate how deduplication works. If you are interested in a large Ceph cluster deployment, please refer to orchestration tools (cephadm, or ansible) in order to deploy Ceph cluster on your cluster.
If you want to get more information about starting a Ceph cluster, you can also refer to the original README of Ceph mainline. In this branch, the original README file has been backed up as README.og.

 - Deploying a development cluster by using vstart

		# ./do_cmake.sh && cd build
		# ninja && ninja install
		# MON=1 OSD=3 MGR=1 ../src/vstart.sh -d -n
		# ceph -s
		  cluster:
		    id:     7d33d38d-e8cf-4bb4-91bc-c21403c87eea
		    health: HEALTH_OK

		  services:
		    mon: 1 daemons, quorum a (age 29s)
		    mgr: x(active, since 24s)
		    osd: 3 osds: 3 up (since 1.21026s), 3 in (since 12s)

		  data:
		    pools:   0 pools, 0 pgs
		    objects: 0 objects, 0 B
		    usage:   3.0 GiB used, 300 GiB / 303 GiB avail
		    pgs:	  1 active+clean

    > [Deploying a development cluster — Ceph Documentation](https://docs.ceph.com/en/latest/dev/dev_cluster_deployment/)

- Deploying a cluster using orchestration tool

  > [Deploying a new Ceph cluster — Ceph Documentation](https://docs.ceph.com/en/latest/cephadm/install/#cephadm-install-distros)

#### Wrap up Ceph cluster
Following command supports wrapping up vstart Ceph cluster.

```
  # ../src/stop.sh
```


### Pool setup
Before starts *TiDedup*, three-steps to setup pools are left. 

1. create a base and a chunk pool

		# ceph osd pool create base
		# ceph osd pool create chunk

2. set dedup tier on the base pool

		# ceph osd pool set base dedup_tier chunk
		# ceph osd pool set base dedup_chunk_algorithm fastcdc
		# ceph osd pool set base dedup_cdc_chunk_size 65536
		# ceph osd pool set base fingerprint_algorithm sha1

3. set hit_set info on osds

		# ceph osd pool set base hit_set_type bloom
		# ceph osd pool set base hit_set_count 10
		# ceph osd pool set base hit_set_period 60

    * To adjust the duration for which objects stay in the hot state, you need to modify the parameters related to the hit_set. The detailed information about the parameters (hit_set_type, hit_set_count, and hit_set_period) can be found in the following link.

      > [Pools — Ceph Documentation](https://docs.ceph.com/en/latest/rados/operations/pools/#hit-set-count)


### Prepare dataset
Import dataset that users want to deduplicate through any interfaces Ceph provides (e.g., RBD, CephFS, RGW)



## Detailed Instructions
Users can use *TiDedup* with a *sample-dedup*, a *chunk-scrub*, and a *chunk-repair* operations of *ceph-dedup-tool*.
If *ceph-dedup-tool* is not installed and *ceph-dedup-tool* command cannot be executed, the users have to install *ceph-test* package.

To start *TiDedup*, make crawling threads which crawls objects in base pool and deduplicate them based on their deduplication efficiency.

To provide users with high flexibility, we have enabled necessary operations through *ceph-dedup-tool*, and we recommend using the following operations freely by using any types of scripts.


### sample-dedup
A  *sample-dedup* operation periodically scans objects in base-pool and determine whether deduplicate object or not. When *sample-dedup* deduplicate an object, it stored in a *base-pool* demotes to a *chunk-pool* which is a lower tier of the *base-pool*.

	# ceph-dedup-tool --op sample-dedup 
            --pool <pool name> 
            --chunk-pool <pool name> 
            --chunk-algorithm <algorithm> 
            --fingerprint-algorithm <algorithm> 
            --max-threads <num threads> 
            --sampling-ratio <ratio> 
            --daemon --loop

- **pool**: base pool name
- **chunk-pool**: chunk pool name
- **chunk-algorithm**: chunking algorithm used by dedup-tool (fastcdc, fixed)
- **fingerprint-algorithm**: fingerprint algorithm used by dedup-tool (sha1, sha128, sha256) (optional, default sha1)
- **chunk-size**: chunk size (optional, default 8192)
- **max-threads**: the number of working thread runs concurrently (optional, default 2)
- **sampling-ratio**: indicates how much to sample objects (range of [0, 100])
- **wakeup-period**: set the sleep time between periods (optional, default 100 sec)
- **chunk-dedup-threshold**: set the thresohld for chunk dedup
- **fpmap-capacity**: set the maximum capacity of a fingerprint map (optional, defulat 1GB)
- **daemon**: execute sample dedup in a daemon mode (optional)
- **loop**: execute sample dedup in a loop until terminated (optional)


### chunk-scrub
A *chunk-scrub* operation identifies reference mismatches between a metadata object and a chunk object. This is inevitable process for Ceph cluster because Ceph uses a false-positive based reference counting design.

	# ceph-dedup-tool --op chunk-scrub 
            --chunk-pool <pool name> 
            --max-threads <num threads>

- **chunk-pool**: chunk pool name
- **max-threads**: the number of working thread runs concurrently


### chunk-repair
A *chunk-repair* operation fixes mismatched reference between a metadata object and a chunk object.

	# ceph-dedup-tool --op chunk-repair 
            --chunk-pool <pool name> 
            --object <oid> 
            --target-ref <oid> 
            --target-ref-pool-id <pool id>

- **chunk-pool**: chunk pool name
- **object**: chunk object name
- **target-ref**: metadata object name
- **target-ref-pool-id**: base-pool id



## Example results
For an example run, we created five 1GB-sized files which contain same contents to make contents redundancy artificially by using dd command (dd if=/dev/urandom of=/test-file-0X bs=1g count=1).

1. Import these files into Ceph cluster by using rbd import command.

		# rbd import --image-format 2 test-file-01 base/volume-01
		# rbd import --image-format 2 test-file-02 base/volume-02
		# rbd import --image-format 2 test-file-03 base/volume-03
		# rbd import --image-format 2 test-file-04 base/volume-04
		# rbd import --image-format 2 test-file-05 base/volume-05

2. Check storage utilization

		# bin/ceph df
		--- RAW STORAGE ---
		CLASS     SIZE    AVAIL    USED  RAW USED  %RAW USED
		ssd    303 GiB  285 GiB  18 GiB    18 GiB       5.94
		TOTAL  303 GiB  285 GiB  18 GiB    18 GiB       5.94

		--- POOLS ---
		POOL   ID  PGS   STORED  OBJECTS     USED  %USED  MAX AVAIL
		.mgr    1    1  577 KiB        2  1.7 MiB      0     94 GiB
		base    2   32  5.0 GiB    1.31k   15 GiB   5.05     94 GiB
		chunk   3   32      0 B        0      0 B      0     94 GiB

3. Run *TiDedup*

		# ceph-dedup-tool --op sample-dedup 
                    --pool base --chunk-pool chunk --chunk-algorithm fastcdc 
                    --fingerprint-algorithm sha1 --max-thread 4 --sampling-ratio 100 
                    --chunk-size 65536 --chunk-dedup-threshold 2 --wakeup-period 100 
                    --fpmap-capacity 100000000 --loop --daemon

4. Dedulicated result

		# bin/ceph df
		--- RAW STORAGE ---
		CLASS     SIZE    AVAIL     USED  RAW USED  %RAW USED
		ssd    303 GiB  294 GiB  9.1 GiB   9.1 GiB       3.00
		TOTAL  303 GiB  294 GiB  9.1 GiB   9.1 GiB       3.00

		--- POOLS ---
		POOL   ID  PGS   STORED  OBJECTS     USED  %USED  MAX AVAIL
		.mgr    1    1  577 KiB        2  1.7 MiB      0     97 GiB
		base    2   32  2.7 KiB    1.33k  384 KiB      0     97 GiB
		chunk   3   32  2.0 GiB   13.35k  6.1 GiB   2.05     97 GiB

After *TiDedup* starts, storage usage is gradually reduced, and as shown in the above output, 5GB of data size is reduced to 2GB.

- While examples provided here focused on block interface (RBD), the same principle can be applied for object (RGW) and file (CephFS) interfaces as well. It means setting dedup-tier on their data pool as described above makes *TiDedup* available.


## Experiments in the paper
Unfortunately, we cannot open the dataset because it contains sensitive personal information, we have added the commands and explanations that we used to perform the evaluations in the paper.

### Space svaing on factory data
This evaluation is to find out how *TiDedup* can remove duplicates from real-world datasets using four types of factory data workloads.
As shown in the graph, we measured the possible amount of duplicate removal by estimating the dedup ratio daily for the accumulated dataset for 16 days.

After a Ceph cluster is ready and all datasets are prepared, the following command is run daily on the accumulated dataset.

```
  ceph-dedup-tool --op estimate 
    --pool base --chunk-size 16384 --chunk-algorithm fastcdc 
    --fingerprint-algorithm sha1 --max-thread 96
```

### YCSB throughput
A throughput, a read latency, and a write latency measured in this evaluation were extracted by processing each metric every 5 seconds from the YCSB log generated from four client nodes.

Before run YCSB, we used following command for each client node.

```
  ycsb load mongodb-async -s -P workloads/workloada 
    -p mongodb.url='mognodb://localhost://27939/tidedup'
    -p status.interval=5 -threads 16
```

To run YCSB, we used following command for each client node. This command will run for 1200 seconds.

```
  ycsb run mongodb-async -s -P workloads/workloada 
    -p mongodb.url='mongoldb://localhost:27030/tidedup'
    -p status.interval 5 -threads 16
```

While YCSB is running, *TiDedup* and Fixed runs twice for each, once in incremental mode and once in full mode.

- Incremental mode

```
  ceph-dedup-tool --op sample-dedup 
    --pool base --chunk-pool chunk --max-thread 96 
    --chunk-dedup-threshold 2 --fingerprint-algorithm sha1 
    --chunk-size 16384 --sampling-ratio 50 --loop --wakeup-period 5
```

- Full mode

```
  ceph-dedup-tool --op sample-dedup 
    --pool base --chunk-pool chunk --max-thread 96 
    --chunk-dedup-threshold 2 --fingerprint-algorithm sha1 
    --chunk-size 16384 --sampling-ratio 100 --loop --wakeup-period 1
```

### Space saving depending on the average chunk size
This evaluation shows how much *TiDedup* can actually reduce data duplication for datasets with the same dedup ratio for different chunk sizes.

We used fio command to create data with a constant dedup ratio by giving *dedupe_percentage* option.

```
  fio --name=space__saving --ioengine rbd --clientname=admin 
    --pool=base --rbdname=vol --rw=write bs=16k --size=10g 
    --dedupe-percentage=50 --iodepth=16
```

Start *TiDedup* with varying its *chunk_size* option.

```
  for chunk_size in [8KB, 16KB, 32KB, 64KB]:
    ceph-dedup-tool --op sample-dedup
      --pool base --chunk-pool chunk --max-thread 64
      --chunk-dedup-threshold 2 --fingerprint-algorithm sha1
      --chunk-size chunk_size --sampling-ratio 100 --loop --wakeup-period 1
```

Check elapsed time until the dedup ratio is saturated. After dedup work is done, get metadata usage size from (total pool usage - written data size).


### Recovery performance
This evaluation is to find out how *TiDedup* degrades its performance during Ceph cluster is under recovery state.

To create block device for each client node, use following rbd command and mount it to each node’s file-system.

```
  rbd create vol --size 2TB --pool base
  rbd map vol --pool base --name client.admin
  mkfs.xfs -f /dev/rbd0
  mount /dev/rbd0 /mnt/rbd0
```

After then, generate foreground I/O to a Ceph cluster, by using fio command.

```
  fio --name=recovery_test --ioengine=rbd --clientname=admin 
    --pool=base --rbdname=vol --rw=randrw --rwmixwrite=20 
    --bs=16k --time_based=1 --run-time=600  --iodepth=64
```

*TiDedup* runs when fio is started until the evaluation is over with following command.

```
  ceph-dedup-tool --op sample-dedup 
    --pool base --chunk-pool chunk --max-thread 64 
    --chunk-dedup-threshold 2 --fingerprint-algorithm sha1 
    --chunk-size 16384 --sampling-ratio 100 --loop --wakeup-period 1
```

### Scrub time
This evaluation compares scrub time between *TiDedup* and Fixed as the number of chunk objects increases.

Scrub time can be estimated by completing several steps in advance.
Create 100, 1000, 10000, 100000, and 1000000 4MB objects in each testcase using fio, and dedup all objects.

```
  for data_size in [400MB, 4GB, 40GB, 400GB, 4TB]:
    fio --name=scrub-test --ioengine=rbd --clientname=admin 
      --pool=base --rbdname=vol --rw=write --bs=128k 
      --size=data_size --iodepth=32
```

Dedup for all the metadata objects in base pool.

```
  for metadata_object in all_objects:
    ceph-dedup-tool --op object-dedup --pool base 
      --object metadata_object --chunk-pool chunk 
      --fingerprint-algorithm sha1 --dedup-cdc-chunk-size 16384 
```

Make all chunk object inconsistent. For every chunk object make its reference mismatched by running following command. This command intentionally injects dummy object which doesn’t actually exist.

```
  for chunk-obj in all-chunk-objects:
    ceph-dedeup-tool --op chunk-get-reef --chunk-pool chunk 
      --object chunk-obj --target-ref dummy-obj --target-ref-pool-id 3
```

When above steps are ready, we can estimate the running time while following *chunk-scrub* command is running.

```
  ceph-dedup-tool --op chunk-scrub --chunk-pool chunk --max-thread 16
```


## Future work
As a future work for *TiDedup*, we are currently researching and developing a deduplication method for rados gateway (RGW) layer in Ceph.

This work called *RGWDedup* is based on *TiDedup*. It supports a deduplication method that leverages the characteristics of RGW and cloud workloads better way. We expect that RGWDedup has advantages of scalability, efficiency, and usability than current deduplication for Ceph.

RGWDedup is not a direct replacement for *TiDedup* but rather a feature that provides speialized functionality within the RGW layer. If you need deduplication for general purposes, it is recommended to use *TiDedup*.

> https://github.com/samsungceph/ceph/tree/wip-rgw-dedup-v2
