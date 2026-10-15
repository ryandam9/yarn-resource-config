# yarn_spark_settings.py

Reports, in plain words, why a Spark app on YARN may get no resources: containers too big for
YARN or for the nodes, a full ApplicationMaster pool, Oozie launchers that block Spark jobs, user
limits in a queue, or nodes without room. It reads the YARN, Capacity Scheduler, Spark and Oozie
config files and, on Amazon EMR, the live state of the ResourceManager.

The report starts with the findings. Each finding says what is wrong (or fine), what happens
because of it, and what to do. Then come tables of what one Spark app asks for, the queues, the
nodes and the settings behind the findings, and a short list of the terms used.

Background: [Why your Spark app gets no YARN resources](../docs/yarn_resources_explained_by_claude.html).

## The script is read-only

- It reads the config files and the EMR info files on the host.
- It sends only HTTP GET requests to the ResourceManager REST API: `/ws/v1/cluster/metrics`,
  `/ws/v1/cluster/scheduler`, `/ws/v1/cluster/nodes` and `/ws/v1/cluster/apps` (running and waiting
  apps). For running Spark apps it also reads the active stages from the Spark UI through the
  ResourceManager proxy (`/proxy/<app id>/api/v1/applications/<app id>/stages`).
- It does not submit apps, change queues or settings, start YARN containers or write files.
- `--all` hides the values of properties that look like secrets (passwords, secret keys, tokens)
  and passwords inside addresses such as `jdbc:mysql://user:password@host`, because the output
  can end up in shared logs.

## What you need

| Need | Details |
|------|---------|
| Python 3.6 or later | Only the standard library. The script stops with a message on older versions. EMR 6 has Python 3.7 and EMR 7 has Python 3.9. |
| The cluster's config files | `/etc/hadoop/conf` (`yarn-site.xml`, `capacity-scheduler.xml`), `/etc/spark/conf/spark-defaults.conf`, `/etc/oozie/conf/oozie-site.xml`. A missing file is reported and skipped. |
| Read access to those files | Most are readable by any user on EMR. Some, such as `oozie-site.xml` or `/mnt/var/lib/info`, can be readable only by their service user. The script then lists them under **COULD NOT READ** and continues. Run it as the `hadoop` user (or with `sudo`) to include them. |
| For live data: access to the ResourceManager | Port 8088 (8090 with `yarn.http.policy=HTTPS_ONLY`) on the primary node. The script does not use an HTTP proxy for these requests. |
| For live data on a Kerberos cluster | Run `kinit` first, and have `curl` installed. If the plain request fails, the script retries with `curl --negotiate`. |

## Where to run it

### On the EMR primary node (recommended)

Everything works without options. The script finds the config files, sees that it is on EMR
(from `/mnt/var/lib/info`) and reads live data from the ResourceManager.

Connect with SSH or Session Manager, copy the script, then:

```bash
python3 yarn_spark_settings.py
```

### As an EMR step (no SSH needed)

Upload the script to S3 and add a step. The step streams the script into `python3`, so nothing
is written on the node:

```bash
aws s3 cp yarn_spark_settings.py s3://<bucket>/tools/
aws emr add-steps --cluster-id j-XXXXXXXX --steps \
  'Type=CUSTOM_JAR,Name=yarn-settings,ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=[bash,-c,"aws s3 cp s3://<bucket>/tools/yarn_spark_settings.py - | python3 -"]'
```

Add options after `python3 -`, for example `python3 - --all`.

The step runs the copy in S3. After you update the script, upload it to S3 again.

The output goes to the step's stdout log:
`s3://<log-bucket>/<prefix>/j-XXXXXXXX/steps/s-XXXXXXXX/stdout.gz` (EMR uploads it a few
minutes after the step ends), or **Steps > View logs** in the EMR console.

The step runs on the primary node, not in a YARN container, and ends in a few seconds. It
takes a step slot: with the default step concurrency of 1 it waits behind running steps, and
other steps wait for it.

The cluster's EC2 instance profile needs `s3:GetObject` on the script's location (the default
`EMR_EC2_DefaultRole` has it).

### On an edge node

This works if the edge node has a copy of the cluster's client configs and can reach the
ResourceManager. Things to know:

- **Turn live data on yourself.** The edge node has no `/mnt/var/lib/info`, so the script does
  not detect EMR. Add `--live`. If the copied `yarn-site.xml` does not name the ResourceManager,
  give it with `--rm`.
- **Open port 8088.** EMR's managed security group for the primary node does not allow other
  instances by default. Allow the edge node's security group on port 8088.
- **The "Amazon EMR" section is missing.** It comes from `/mnt/var/lib/info`.
- **`capacity-scheduler.xml` is often not on an edge node.** Clients do not need it, so the
  queue section from the files shows NOT FOUND. The live queue table still shows each queue's
  capacity, AM limit and user-limit factor.
- **The Spark values are the edge node's.** That is what spark-submit uses for jobs submitted
  from the edge node, but it can differ from the primary node's copy.
- **Ignore `yarn.nodemanager.resource.memory-mb` in the file.** No NodeManager runs on the edge
  node. The live node list shows the real node sizes.

```bash
python3 yarn_spark_settings.py --live --rm http://<primary-node-private-dns>:8088
```

### Other Hadoop clusters

Point the script at the config directories, or set `HADOOP_CONF_DIR` (or `YARN_CONF_DIR`),
`SPARK_CONF_DIR` and `OOZIE_CONF_DIR`:

```bash
python3 yarn_spark_settings.py --hadoop-conf /opt/hadoop/etc/hadoop \
    --spark-conf /opt/spark/conf --live --rm http://rm-host:8088
```

### Not with spark-submit or an Oozie workflow

Run it as a plain Python program, not as a Spark app or an Oozie action:

- **The job would use the resources that the script diagnoses.**
  - `spark-submit --deploy-mode cluster` starts a YARN ApplicationMaster container.
  - An Oozie shell or Spark action starts an Oozie launcher AM.
  - Both use the queue's AM pool and cluster memory. When the cluster is short of resources
    (the problem you want to look at), the job itself waits in ACCEPTED and never prints
    anything.
- **It would run on the wrong node.** The container runs on a core or task node. That node's
  `yarn-site.xml` describes that node only, it may have no `spark-defaults.conf` or
  `capacity-scheduler.xml`, and the script reports "not the primary node".
- **No benefit in client mode.** `spark-submit --deploy-mode client` only runs the Python file
  on the local host, the same as `python3`, with the extra start-up time of spark-submit.

To run it on a schedule, use an EMR step, or cron on the primary or edge node.

## Options

For a quick live view without the rest of the report, Hadoop's own `yarn top` also lists the running apps; `--watch` adds the queue limits, Spark stages and why apps wait.

| Option | Meaning |
|--------|---------|
| `--live` | Read live queue and node usage from the ResourceManager. Default on EMR. |
| `--no-live` | Do not contact the ResourceManager. Read the files only. |
| `--rm URL` | ResourceManager web address, for example `http://ip-10-0-0-1:8088`. By default the script takes it from `yarn-site.xml` (including ResourceManager HA). |
| `--all` | Also list every property that the Settings table does not show, so every property in the files appears in the report (secret values hidden). |
| `--color auto\|always\|never` | Colour the report. `auto` (the default) colours it only on a terminal, so files, pipes and EMR step logs stay plain text. `NO_COLOR=1` turns colour off. |
| `--queue NAME` | Start the report with a one-screen **queue card** for this queue: can a new job run now, and if not, why (see below). Give it more than once for more queues. Needs live data. |
| `--details` | Also print the balance sheets (every number behind the queue cards, step by step). Without `--queue` they are always printed. |
| `--watch SECONDS` | Show only the Queues, the queue room overview and the Apps, redrawn every SECONDS (2 or more), like `yarn top`. Needs live data. Ctrl-C stops it. |
| `--hadoop-conf DIR` | Directory with `yarn-site.xml` and `capacity-scheduler.xml`. Default: `$HADOOP_CONF_DIR`, `$YARN_CONF_DIR`, then `/etc/hadoop/conf`. |
| `--spark-conf DIR` | Directory with `spark-defaults.conf`. Default: `$SPARK_CONF_DIR`, then `/etc/spark/conf`. |
| `--oozie-conf DIR` | Directory with `oozie-site.xml`. Default: `$OOZIE_CONF_DIR`, then `/etc/oozie/conf`. |

### Colours

On a terminal the report uses colour so that the parts are easy to tell apart:

| Part | Colour |
|------|--------|
| Config files, paths and URLs | teal |
| Property names (`spark.executor.memory`, `yarn.scheduler...`) | magenta |
| Values and memory sizes | bold |
| `(default)` and `not set` | dim |
| `[PROBLEM]` and `[WARNING]` with their titles | red |
| `[NOTE]` | blue |
| `[OK]` and "What to do" | green |
| Section titles | bold blue |
| Explanations | your terminal's normal text colour |

## What the report shows

| Part | Content |
|------|---------|
| Header | Time, EMR cluster ID, release and instance groups, whether this host is the primary node, and where the live data comes from. |
| Could not read | Only when something failed: each config file that could not be read and each ResourceManager address that did not answer, with the reason and what to do. The rest of the report then leaves out what depends on it. |
| Findings | `[PROBLEM]` (apps fail or wait now), `[WARNING]` (apps can wait or block), `[NOTE]` (good to know), `[OK]`. Each has an explanation with the numbers and a "What to do" line. |
| What one Spark app asks YARN for | Executor and ApplicationMaster containers: heap, overhead, other memory, the size YARN gives, vcores, and the number of executors. |
| Queues | The queue tree, one frame per parent queue: each frame holds the children of one parent, with their share of that parent ("Of parent") and, under the frame, the sum of those shares (green with ✓ when it is 100%, red when not). Frames next to each other have different colours. For each leaf queue: guaranteed and maximum share of the cluster, the most one user can use, the AM pool use, used memory, running and waiting apps. Marks the queue that Spark and the Oozie launchers use. Below the table, "What the columns mean" explains each column in plain words, and an example reads the numbers of Spark's queue: what it always gets, how far it can grow, what one user can use and how many more apps can start. |
| Queue room in numbers | Live only. A table with each leaf queue's guaranteed and maximum memory, used memory, room now, free AM pool and whether a new Spark app can start now with spark-submit and, when oozie-site.xml is found, from an Oozie workflow (if not, the first reason). Then a balance sheet for the queues that matter (see below). |
| Apps | Live only: the apps that run or wait now, grouped by queue and user. Per user: running and waiting apps, the share of the cluster they use against the one-user limit, and their ApplicationMaster memory against the user's AM limit. Per app: ID, name, state, run time, containers, memory, vcores and what it does now (for Spark, the active stage and its finished/total tasks; for waiting apps, YARN's reason). For each running app that asks for more containers than YARN gives, or that has waiting tasks but asks for nothing, it says why: a container bigger than any node, the user limit (all apps of one account share it), the queue maximum, older apps first in a FIFO queue, memory spread over nodes, a full cluster, or Spark's own `maxExecutors` / `spark.executor.instances`. YARN counts limits per user, so all apps of one service account share one user limit. If YARN ACLs hide apps from the user who runs the script, the section says so. |
| Nodes | Live only: memory and vcores of each node, used and free, containers and state. |
| Settings | The settings behind the findings, with a plain label, the value (or the default used) and the property name. Under `capacity-scheduler.xml` it also lists the settings of each queue, such as `yarn.scheduler.capacity.root.<queue>.user-limit-factor`; see the example below. |
| Terms | Container, ApplicationMaster, AM pool, overhead and the other terms used in the report. |

The checks:

- **Container size**: the executor and ApplicationMaster containers (heap + overhead + off-heap +
  Python worker memory) against the largest container YARN allows.
- **Executors on nodes**: an executor must fit on a node; how many fit per node and in the
  cluster; whether a node has room right now; nodes that are not usable.
- **AM pool**: how many Spark apps can run at the same time in Spark's queue before new apps wait
  in ACCEPTED; whether apps wait now because the pool is full.
- **Oozie launchers**: whether launchers share the Spark queue's AM pool, and how many workflows
  fit before they can block each other. A PROBLEM when the launcher and the Spark AM together are
  bigger than one user's AM limit in that queue: then a Spark action from Oozie can never start.
- **User limit**: queues where one user cannot use the part of the queue that other queues leave
  idle, with the `user-limit-factor` that fixes it.
- **Spark's own limits** (`maxExecutors`, `spark.executor.instances`), a missing or stopped queue,
  and apps that wait in any queue (with the likely reason).

Notes:
- `spark-submit` options (`--conf`, `--executor-memory`, ...) change the sizes for one app. The
  report uses `spark-defaults.conf`.
- On EMR, Spark uses `spark.emr.default.executor.memory` and `spark.emr.default.executor.cores`
  when `spark.executor.memory` and `spark.executor.cores` are not set. The report does the same.
- Without live data, the report uses the node size from this host's `yarn-site.xml` and shows the
  AM pool as a share of the cluster instead of in MB.

### Queue settings

Under `capacity-scheduler.xml`, the Settings table lists each queue by name. A queue that does
not set a value shows the default, or the cluster-wide value that it inherits. A parent queue
shows only its share and state, because only leaf queues run apps. Example from a test config with
three queues:

```text
  Queue root.etl
  Guaranteed share of the parent          yarn.scheduler.capacity.root.etl.capacity                                   40%
  Most share of the parent                yarn.scheduler.capacity.root.etl.maximum-capacity                           80%
  State                                   yarn.scheduler.capacity.root.etl.state                                      RUNNING (default)
  One user: x guaranteed share            yarn.scheduler.capacity.root.etl.user-limit-factor                          2
  Smallest share per active user          yarn.scheduler.capacity.root.etl.minimum-user-limit-percent                 100% (default)
  AM pool share                           yarn.scheduler.capacity.root.etl.maximum-am-resource-percent                0.3
  Largest container: memory               yarn.scheduler.capacity.root.etl.maximum-allocation-mb                      cluster limit (default)
  Most running apps                       yarn.scheduler.capacity.root.etl.max-parallel-apps                          no limit (default)

  Queue root.analytics  (parent queue: its children share it)
  Guaranteed share of the parent          yarn.scheduler.capacity.root.analytics.capacity                             20%
  Most share of the parent                yarn.scheduler.capacity.root.analytics.maximum-capacity                     100% (default)
  State                                   yarn.scheduler.capacity.root.analytics.state                                RUNNING (default)
```

Other queue properties, such as `acl_submit_applications` or `ordering-policy`, are listed with
`--all`.
### Nested queues

With nested queues, each parent gets its own frame, in tree order, so you can see at once which
queues share one parent and whether their shares add up to 100%. Example from a test config with
three levels and one mistake (in colour, each frame has its own colour and the sum line is green or
red):

```
    Queue                          Of parent  Guaranteed  Max   One user  AM pool used  Used  Run  Wait
  ┌─ children of root ─────────────────────────────────────────────────────────────────────────────────
  │ root.default (Spark, Oozie)    40%        40%         80%   80%       20%-40%       -     -    -
  │ root.etl (parent)              40%        40%         80%   -         -             -     -    -
  │ root.analytics (parent)        20%        20%         100%  -         -             -     -    -
  └─ 40% + 40% + 20% = 100% of root ✓
  ┌─ children of root.etl ─────────────────────────────────────────────────────────────────────────────
  │ root.etl.daily                 2w (67%)   27%         80%   27%       13%-40%       -     -    -
  │ root.etl.hourly                1w (33%)   13%         80%   13%       7%-40%        -     -    -
  └─ weights 2w + 1w: each queue gets its weight / 3 of root.etl
  ┌─ children of root.analytics ───────────────────────────────────────────────────────────────────────
  │ root.analytics.adhoc (parent)  50%        10%         100%  -         -             -     -    -
  │ root.analytics.reports         50%        10%         100%  10%       5%-50%        -     -    -
  └─ 50% + 50% = 100% of root.analytics ✓
  ┌─ children of root.analytics.adhoc ─────────────────────────────────────────────────────────────────
  │ root.analytics.adhoc.team_a    50%        5%          100%  5%        2%-50%        -     -    -
  │ root.analytics.adhoc.team_b    30%        3%          100%  3%        2%-50%        -     -    -
  │ root.analytics.adhoc.team_c    10%        1%          100%  1%        0%-50%        -     -    -
  └─ 50% + 30% + 10% = 90% of root.analytics.adhoc, not 100%: check the capacities of these queues
```

- **Of parent** is the `capacity` as written, a share of the parent queue. **Guaranteed** is the
  share of the whole cluster (Of parent x the parent's Guaranteed).
- Weights (`2w`, `1w`) are shown with the share they give. Capacities in absolute resources
  (`[memory=...]`) are shown as written; their sum is not checked.
- Without `capacity-scheduler.xml` (edge node), the tree comes from the live ResourceManager data.
- When the terminal cannot show box-drawing characters (for example `LANG=C`), the frames use
  `+`, `-` and `|`.

### Queue card (`--queue NAME`)

The answer for one queue, on one screen. It comes first in the report.

- **The answer:** ✗ a new job would wait (and why), ! it would start but get no executor (and why),
  or ✓ it can start now and get up to N executors right away.
- **1. Job-starter (AM) limit, what is available:** every job first starts one job starter (its
  ApplicationMaster). For the user's AM limit and the queue's AM pool: a gauge, the GB free and the
  room for N more job starters. Their size is known, so this is a real count.
- **2. Memory, what is available:** for the queue's maximum, the user's share of the queue and the
  cluster: a gauge, the GB free and how many executors of the configured size that is; then the
  largest free space on one node, and how many executors a new job can get right now (placed on the
  real nodes, after its job starter).
- **3. In short:** jobs running now, room for job starters, free memory. It also splits the user's
  memory share over the most jobs its AM limit allows: when each job would get less than one
  executor, the AM limit lets more jobs start than memory can feed. Oozie (launcher first) gets its
  own line.
- **4. What you can do:** numbered steps, with the setting and a suggested value where that helps.

The card does not say how many new jobs fit: each job asks for its own number of executors, so it
shows what is free and what one executor needs. There is one card for each user with jobs in the
queue (or one for a new user). The balance sheets with every number are there with `--details`.

Example: the service account's job-starter limit is full, and there is no executor room either:

```
QUEUE root.default  ·  user svc_spark
─────────────────────────────────────

  ✗ A new Spark job would WAIT now (state ACCEPTED).
    • Why: svc_spark already runs 3 jobs, and its job-starter limit has only 0.00 GB left; one job
      starter needs 0.88 GB.
    ✗ And even if it could start, it would get no executor now: the queue has only 3.12 GB of memory
      left, and one executor needs 5.12 GB.

  1. Job-starter (AM) limit: what is available
    • Every job first starts one job starter (its ApplicationMaster) of 0.88 GB.
    ✗ svc_spark's job starters           [██████████] 100%  0.00 GB free of 2.62 GB  room for 0 more
    ✓ job starters of the whole queue    [███▊░░░░░░]  39%  7.38 GB free of 12.00 GB  room for 8 more

  2. Memory: what is available
    • Then a job needs executors to do the work: 5.12 GB each (spark-defaults.conf). A job that
      starts but gets no executor does nothing.
    ✗ memory of the queue (its max)      [████████▋░]  87%  3.12 GB free of 24.00 GB  = 0 executors
    ✓ svc_spark's share of the queue     [███████▊░░]  79%  5.12 GB free of 24.00 GB  = 1 executor
    ✗ memory of the whole cluster        [████████▋░]  87%  3.12 GB free of 24.00 GB  = 0 executors
    ✗ largest free space on one node                        1.87 GB  (an executor needs 5.12 GB)
    ✗ Executors a new job can get right now: 0 (placed on the real nodes, after its job starter).

  3. In short
    • svc_spark runs 3 jobs here now.
    ✗ Job starters: room for 0 more.
    ✗ Memory: 3.12 GB free for svc_spark in this queue, up to 0 executors for a new job now.
    • If svc_spark ran 3 jobs (the most its job-starter limit allows), each would have about 7.12 GB
      for executors (its share of the queue, 24.00 GB, split 3 ways).
    ✗ From Oozie: NEVER. Its launcher (2.00 GB) and the job starter (0.88 GB) together are more than
      svc_spark's job-starter limit (2.62 GB).

  4. What you can do
    1. Wait: when one of svc_spark's jobs ends, its job starter frees up.
    2. Let svc_spark start more jobs: yarn.scheduler.capacity.root.default.user-limit-factor 1 ->
       1.7 gives a job-starter limit of about 4.46 GB (room for about 2 more). It also raises
       svc_spark's share of the queue.
    3. Wait until other containers end. For more room for good, add nodes to the cluster.
    4. For Oozie: give the launchers their own queue (oozie.launcher.default.queue).

  All numbers in detail (the balance sheet): run again with --details.
```

Example: a job can start and get 2 executors now:

```
QUEUE root.default  ·  user svc_spark
─────────────────────────────────────

  ✓ A new Spark job can START now and get up to 2 executors (5.12 GB each) right away.
    • More than that: as memory frees up (after its own job starter, svc_spark's share of the queue
      has only 2.88 GB left, and one executor needs 5.12 GB).

  1. Job-starter (AM) limit: what is available
    • Every job first starts one job starter (its ApplicationMaster) of 0.88 GB.
    ✓ svc_spark's job starters           [███▎░░░░░░]  33%  1.75 GB free of 2.62 GB  room for 2 more
    ✓ job starters of the whole queue    [▋░░░░░░░░░]   7%  11.12 GB free of 12.00 GB  room for 12 more

  2. Memory: what is available
    • Then a job needs executors to do the work: 5.12 GB each (spark-defaults.conf). A job that
      starts but gets no executor does nothing.
    ✓ memory of the queue (its max)      [██▋░░░░░░░]  27%  16.00 GB free of 22.00 GB  = 3 executors
    ✓ svc_spark's share of the queue     [███░░░░░░░]  30%  14.00 GB free of 20.00 GB  = 2 executors
    ✓ memory of the whole cluster        [██▌░░░░░░░]  25%  18.00 GB free of 24.00 GB  = 3 executors
    ✓ largest free space on one node                        9.00 GB  (an executor needs 5.12 GB)
    ✓ Executors a new job can get right now: 2 (placed on the real nodes, after its job starter).

  3. In short
    • svc_spark runs 1 job here now.
    ✓ Job starters: room for 2 more.
    ✓ Memory: 14.00 GB free for svc_spark in this queue, up to 2 executors for a new job now.
    • If svc_spark ran 3 jobs (the most its job-starter limit allows), each would have about 5.79 GB
      for executors (its share of the queue, 20.00 GB, split 3 ways).
    ✗ From Oozie: NEVER. Its launcher (2.00 GB) and the job starter (0.88 GB) together are more than
      svc_spark's job-starter limit (2.62 GB).

  4. What you can do
    1. For Oozie: give the launchers their own queue (oozie.launcher.default.queue).

  All numbers in detail (the balance sheet): run again with --details.
```

### Queue room in numbers (balance sheets)

With live data, each chosen queue gets a balance sheet in GB and vcores. It starts with what the
queue may use, then subtracts what is used, then shows the result:

- **Room left now:** queue max minus what the queue uses, the same for each parent queue (all
  queues under a parent share its maximum), and the free memory of the cluster. The room is the
  smallest of these, and the line names which one sets it.
- **AM pool:** the pool minus the AMs of the running apps, and how many more Spark AMs fit.
- **Users:** each user's limit, use and AM use. YARN counts these per user, so all apps of one
  service account share them.
- **Can a new Spark app start here now?** Each check that YARN makes before an app starts, ✓ or
  ✗, then YES (with how many executors fit at once, packed on the real nodes) or NO. It is done
  per user twice:
  - **with spark-submit:** one Spark AM.
  - **from an Oozie workflow:** first the Oozie launcher (`oozie.launcher.default.memory.mb`,
    in `oozie.launcher.default.queue`), which stays until the Spark job ends, then the Spark AM.
    The launcher's memory is taken off the AM pool, the user's AM limit, the user limit and the
    room (of its queue, its parents and the cluster) before the Spark AM is checked. The verdict
    says which step fails. If the two together never fit in one user's AM limit, it says so and
    how to fix it.

  Running launchers need no special handling: they are YARN apps, so the used memory, AM memory
  and user numbers from the ResourceManager already include them.

  When the answer is ✗ NO (or ✓ YES, but no executor fits), a **Why** block explains the limit
  that stops the app, as short lists rather than paragraphs, for a reader who does not know
  YARN's rules:
  - **Why:** which limit it is: a stopped queue, the queue's AM pool, the user's AM limit, the
    queue or a parent queue at its maximum, a full cluster, no node with room, or the user limit.
  - **What this limit is:** one bullet per fact.
  - **The numbers:** how the limit is worked out and what uses it now, one bullet each; the line
    that is not enough has a ✗.
  - **Set by:** one bullet per property.
  - **What you can do:** numbered steps.
  - **Also not enough:** other limits that would stop the app next, each with a ✗.

  Each kind of limit is explained in full once per sheet; later it says "see above".

Example: a spark-submit that waits because the service account's AM limit is full:

```
      As user svc_spark, with spark-submit:
        ✓ Spark AM: queue root.default is RUNNING
        ✓ Spark AM: AM pool of root.default: 7.38 GB free, needs 0.88 GB
        ✗ Spark AM: AM limit of user svc_spark: 0.00 GB free, needs 0.88 GB
        ✓ Spark AM: room for queue root.default now: 3.12 GB, needs 0.88 GB
        ✓ Spark AM: the most free on one node: 1.87 GB (a container must fit on one node)
        ✓ Spark AM: user limit of svc_spark in root.default: 5.12 GB left (limit 24.00 GB, uses 18.88 GB)
        ✗ NO: it waits in ACCEPTED.
          • ACCEPTED means: YARN has the app, but does not start it yet.
        Why: User svc_spark has reached its AM limit in queue root.default
          What this limit is:
            • YARN limits the AMs of each user, not only of the whole queue.
            • All apps of one account share this limit.
            • So one service account can block itself, while the AM pool still has room for other
              users.
          The numbers:
            • User AM limit: 2.62 GB (YARN's figure, userAMResourceLimit)
            • Worked out by YARN from maximum-am-resource-percent, user-limit-factor and
              minimum-user-limit-percent
            • svc_spark's AMs now: 2.62 GB (3 running apps)
            ✗ Free: 0.00 GB, but the Spark AM needs 0.88 GB
          Set by:
            • yarn.scheduler.capacity.root.default.maximum-am-resource-percent
            • yarn.scheduler.capacity.root.default.user-limit-factor
            • yarn.scheduler.capacity.root.default.minimum-user-limit-percent
          What you can do:
            1. Wait until one of this user's apps ends.
            2. Or raise user-limit-factor of queue root.default.
            3. Or raise maximum-am-resource-percent of queue root.default.
            4. Or run some apps under another account: each user has its own AM limit.
```

Example of the Oozie check, where the launcher fits but the Spark AM then does not:

```
      As user svc_spark, from an Oozie workflow: first the launcher (2,048 MB, queue root.default),
      then the Spark AM in the same queue:
        ✓ launcher: queue root.default is RUNNING
        ✓ launcher: AM pool of root.default: 12.00 GB free, needs 2.00 GB
        ✓ launcher: AM limit of user svc_spark: 2.62 GB free, needs 2.00 GB
        ✓ launcher: room for queue root.default now: 18.00 GB, needs 2.00 GB
        ✓ launcher: the most free on one node: 9.00 GB (a container must fit on one node)
        ✓ launcher: user limit of svc_spark in root.default: 10.80 GB left (limit 16.80 GB, uses 6.00 GB)
        ✓ Spark AM: queue root.default is RUNNING
        ✓ Spark AM: AM pool of root.default: 10.00 GB free after the 2.00 GB above, needs 0.88 GB
        ✗ Spark AM: AM limit of user svc_spark: 0.62 GB free after the 2.00 GB above, needs 0.88 GB
        ✓ Spark AM: room for queue root.default now: 16.00 GB, needs 0.88 GB
        ✓ Spark AM: the most free on one node: 9.00 GB (a container must fit on one node)
        ✓ Spark AM: user limit of svc_spark in root.default: 8.80 GB left (limit 16.80 GB, uses 8.00 GB)
        ✗ NO: it can NEVER start.
          • The launcher and the Spark AM together need 2.88 GB.
          • That is more than the AM limit of one user (2.62 GB), even when nothing else runs.
          • So the launcher starts, and then waits for good.
          • Best fix: give the launchers their own queue (oozie.launcher.default.queue).
        Why: User svc_spark has reached its AM limit in queue root.default
          What this limit is:
            • YARN limits the AMs of each user, not only of the whole queue.
            • All apps of one account share this limit.
            • So one service account can block itself, while the AM pool still has room for other
              users.
          The numbers:
            • User AM limit: 2.62 GB (YARN's figure, userAMResourceLimit)
            • Worked out by YARN from maximum-am-resource-percent, user-limit-factor and
              minimum-user-limit-percent
            • svc_spark's AMs now: 0.00 GB (1 running app)
            • Taken by the step above: 2.00 GB
            ✗ Free: 0.62 GB, but the Spark AM needs 0.88 GB
          Set by:
            • yarn.scheduler.capacity.root.default.maximum-am-resource-percent
            • yarn.scheduler.capacity.root.default.user-limit-factor
            • yarn.scheduler.capacity.root.default.minimum-user-limit-percent
          What you can do:
            1. Wait until one of this user's apps ends.
            2. Or raise user-limit-factor of queue root.default.
            3. Or raise maximum-am-resource-percent of queue root.default.
            4. Or run some apps under another account: each user has its own AM limit.
```

Example (live test data, Hadoop 3 with effective resources):

```
  Queue root.default (Spark, Oozie)
                                                                  Memory    vcores
      Cluster for YARN, all running nodes                       24.00 GB         8
      Guaranteed: 83% of the cluster (capacity)                 20.00 GB         6
      Max: 92% of the cluster (maximum-capacity)                22.00 GB         7

    Room left now
      Queue max                                                 22.00 GB         7
    − Used by the apps of this queue                             6.00 GB         2
                                                             ─────────── ─────────
    = (a) Left under the queue max                              16.00 GB         5
      (b) Free in the whole cluster now                         18.00 GB         2
                                                             ─────────── ─────────
    = Room for this queue now: the smallest of (a)(b)           16.00 GB         2  ◀ set by the queue maximum
      vcores are shown, but YARN does not limit by them here: the resource calculator counts memory
      only (yarn.scheduler.capacity.resource-calculator).

    AM pool: each new app needs room for its ApplicationMaster here
      AM pool (maximum-am-resource-percent 50%)                 12.00 GB
    − Used by the AMs of the running apps                        0.88 GB
                                                             ───────────
    = Free in the AM pool                                       11.12 GB
      That is room for 12 more Spark AMs of 896 MB (one AM per app).
      Each user may use at most 2.62 GB of the AM pool (userAMResourceLimit).

    Users with apps in this queue (the user limit is per user, not per app)
      User       Run / wait  User limit  Used     Left      AM limit  AM used  AM left
      svc_spark  1 / 0       20.00 GB    6.00 GB  14.00 GB  2.62 GB   0.88 GB  1.75 GB

    Can a new Spark app start here now?
      As user svc_spark:
      ✓ the queue is RUNNING
      ✓ AM pool: 11.12 GB free, one AM needs 0.88 GB
      ✓ AM limit of user svc_spark: 1.75 GB free, one AM needs 0.88 GB
      ✓ room for the queue now: 16.00 GB, one AM needs 0.88 GB
      ✓ the most free on one node: 9.00 GB (a container must fit on one node)
      ✓ user limit of svc_spark: 14.00 GB left (limit 20.00 GB, uses 6.00 GB)
      YES: it starts now, and about 2 executors of 5,248 MB fit right away.
```

Notes:
- The numbers are a snapshot. "NO" means a new app waits in ACCEPTED now, not that it never runs.
- With the default resource calculator, YARN counts memory only. The vcores are shown, but they do
  not limit anything, and the sheet says so.
- YARN's own figures are used when it sends them (`effectiveMinResource`, `effectiveMaxResource`);
  otherwise the script works them out from the percentages.
- The user limit is the ceiling `user-limit-factor` x guaranteed, at most the queue max. YARN's
  `userResourceLimit` is not used: above the queue's guaranteed share YARN recomputes it on every
  scheduling pass as about "used + one container", so it is not a ceiling.

## Example output

A run on emr-7.14.0 (1 primary + 2 core m5.xlarge, Hadoop, Spark and Oozie) while a Spark
job was running. The cluster had two queues: `default` (70%) and `etl` (30%, AM pool 20%).
Everything else is EMR's own config.

```text
YARN and Spark resource report
==============================
Created:   2026-10-13 12:26 UTC
Cluster:   j-1ABCDEFGHIJKL, emr-7.14.0 (master: 1 x m5.xlarge, core: 2 x m5.xlarge)
This host: primary node
Live data: from the ResourceManager at http://ip-10-0-0-1.ec2.internal:8088

FINDINGS
--------
Summary: 3 warnings, 1 note, 3 OK.

[WARNING] Right now no node has room for another executor
          The most free memory on one node is 1,024 MB; an executor needs 10,434 MB. New executors
          wait until running containers end.
          What to do: Wait, add nodes (or turn on managed scaling), or make executors smaller.

[WARNING] Oozie launchers and Spark apps share queue root.default
          If you run Spark from Oozie workflows: each Oozie Spark action keeps a launcher of 2,048
          MB running in YARN until its Spark job ends, and the Spark job adds its own AM of 896 MB.
          Both count against queue root.default's AM pool. The pool (12,288 MB) holds about 4 such
          workflows at once. If more start together, launchers can fill the pool, so their Spark
          jobs never start (a deadlock).
          What to do: Send the launchers to another queue: oozie.launcher.default.queue in
          oozie-site.xml, or <launcher><queue> in the workflow action.

[WARNING] In queue root.default one user can use only 70% of the cluster
          The queue can grow to 100% of the cluster when other queues are idle, but
          user-limit-factor 1 stops one user at 1 x the queue's guaranteed 70%. YARN checks the
          limit before it starts each container, so a user can go over it by up to one container. If
          one user runs the apps, they wait while part of the cluster stays idle.
          What to do: Set yarn.scheduler.capacity.root.default.user-limit-factor to 1.43 or more.

[NOTE]    In queue root.etl one user can use only 30% of the cluster
          The queue can grow to 100% of the cluster when other queues are idle, but
          user-limit-factor 1 stops one user at 1 x the queue's guaranteed 30%. YARN checks the
          limit before it starts each container, so a user can go over it by up to one container. If
          one user runs the apps, they wait while part of the cluster stays idle.
          What to do: Set yarn.scheduler.capacity.root.etl.user-limit-factor to 3.33 or more.

[OK]      Each Spark container fits under YARN's per-container limit
          Executor 10,434 MB and ApplicationMaster 896 MB; the limit is 12,288 MB
          (yarn.scheduler.maximum-allocation-mb).

[OK]      Executors fit on the nodes
          A node gives YARN 12,288 MB (the largest node, from the ResourceManager) and an executor
          takes 10,434 MB, so a node holds 1 executor. The 2 running nodes hold about 2 executors in
          all, when nothing else runs.

[OK]      Queue root.default can run about 13 Spark apps at the same time
          Only part of a queue may hold ApplicationMasters: 50% of queue root.default
          (maximum-am-resource-percent). Each Spark app has one AM of 896 MB (client mode: a small
          AM; the driver runs where you start spark-submit). The pool is 12,288 MB now, so about 13
          apps fit; more wait in ACCEPTED.


WHAT ONE SPARK APP ASKS YARN FOR
--------------------------------
From spark-defaults.conf (deploy mode: client). Options given to spark-submit (--conf,
--executor-memory, ...) change these for one app.

  Container          Heap      Overhead  Other  Size in YARN  vcores
  Executor           9,486 MB  948 MB    -      10,434 MB     4
  ApplicationMaster  512 MB    384 MB    -      896 MB        1

  Heap of the executor: spark.executor.memory. Overhead: 10% of heap, at least 384 MB. Size in YARN
  is the total rounded up to a multiple of yarn.scheduler.minimum-allocation-mb.
  Number of executors per app: between 0 and no limit (dynamic allocation).

QUEUES
------
How YARN shares the cluster between the queues. Each frame holds the children of one parent queue;
their shares of the parent must add up to 100%. The columns are explained below the table.

    Queue                        Of parent  Guaranteed  Max   One user  AM pool used      Used       Run  Wait
  ┌─ children of root ────────────────────────────────────────────────────────────────────────────────────────
  │ root.default (Spark, Oozie)  70%        70%         100%  70%       896 of 12,288 MB  23,424 MB  1    0
  │ root.etl                     30%        30%         100%  30%       0 of 4,915 MB     0 MB       0    0
  └─ 70% + 30% = 100% of root ✓

  (Spark) = the queue in spark.yarn.queue; (Oozie) = the queue of the Oozie launchers. Guaranteed,
  Max and One user are shares of the whole cluster.

NODES
-----
Cluster memory for YARN: 24,576 MB, free 1,152 MB. Apps: 1 running, 0 waiting.

  Node              YARN memory  Used       Free      vcores used  Containers  State
  ip-10-0-0-2       12,288 MB    11,264 MB  1,024 MB  1 of 4       1           RUNNING
  ip-10-0-0-3       12,288 MB    12,160 MB  128 MB    2 of 4       2           RUNNING

SETTINGS
--------
The settings behind the findings. "(default)" means that the file does not have the setting, so
YARN, Spark or Oozie uses its default.

  Description                             Setting                                              Value

  /etc/hadoop/conf/yarn-site.xml
  Largest container: memory               yarn.scheduler.maximum-allocation-mb                 12288 MB
  Largest container: vcores               yarn.scheduler.maximum-allocation-vcores             128
  Memory is rounded up to a multiple of   yarn.scheduler.minimum-allocation-mb                 1 MB
  Memory per node for YARN (this host)    yarn.nodemanager.resource.memory-mb                  12288 MB
  vcores per node for YARN (this host)    yarn.nodemanager.resource.cpu-vcores                 4
  Node labels on                          yarn.node-labels.enabled                             false
  Node label for ApplicationMasters       yarn.node-labels.am.default-node-label-expression    (empty)

  /etc/hadoop/conf/capacity-scheduler.xml
  Containers are sized by                 yarn.scheduler.capacity.resource-calculator          memory only
  AM pool of each queue (default)         yarn.scheduler.capacity.maximum-am-resource-percent  0.5 (50%)
  Most apps (running + waiting)           yarn.scheduler.capacity.maximum-applications         10000
  Most running apps                       yarn.scheduler.capacity.max-parallel-apps            not set
  Most running apps per user              yarn.scheduler.capacity.user.max-parallel-apps       not set

  /etc/spark/conf/spark-defaults.conf
  Deploy mode                             spark.submit.deployMode                              client (default)
  Queue                                   spark.yarn.queue                                     default (default)
  Executor heap                           spark.executor.memory                                9486M
  Executor heap (EMR default)             spark.emr.default.executor.memory                    9486M
  Executor overhead                       spark.executor.memoryOverhead                        10% of heap, min 384 MB (default)
  Executor vcores                         spark.executor.cores                                 4
  Executor vcores (EMR default)           spark.emr.default.executor.cores                     4
  Python worker memory per executor       spark.executor.pyspark.memory                        not set
  Off-heap memory on                      spark.memory.offHeap.enabled                         false (default)
  Dynamic allocation                      spark.dynamicAllocation.enabled                      true
  Fewest executors                        spark.dynamicAllocation.minExecutors                 0 (default)
  Most executors                          spark.dynamicAllocation.maxExecutors                 no limit (default)
  Executors (without dynamic allocation)  spark.executor.instances                             2 (default)
  Driver heap                             spark.driver.memory                                  2048M
  AM heap in client mode                  spark.yarn.am.memory                                 512m (default)

  /etc/oozie/conf/oozie-site.xml
  Launcher memory                         oozie.launcher.default.memory.mb                     2048 MB (default)
  Launcher vcores                         oozie.launcher.default.vcores                        1 (default)
  Launcher queue                          oozie.launcher.default.queue                         default (default)

TERMS
-----
  Container               A share of one node's memory and vcores that YARN gives to a process. Each
                          Spark executor runs in one container.

  ApplicationMaster (AM)  The first container of each YARN app; it asks YARN for the other
                          containers. In Spark cluster mode the driver runs in it.

  AM pool                 The part of a queue that ApplicationMasters may use
                          (maximum-am-resource-percent). When it is full, new apps wait in ACCEPTED
                          even if the cluster has free memory.

  Overhead                Memory that Spark adds to the heap for the JVM itself, threads and
                          buffers.

  Guaranteed / Max        A queue always gets its guaranteed share; it can borrow up to its maximum
                          when other queues are idle.

  One user                The most of the cluster that one user's apps can use in the queue
                          (user-limit-factor x guaranteed share).

  Oozie launcher          A small YARN app that Oozie starts for each action; it stays until the
                          action (for example the Spark job) ends.
```

## Checks (for changes to the script)

The script has type hints and is checked with ruff and mypy (settings in `pyproject.toml`). It
must keep running on Python 3.6, so the hints use the `typing` module (`List[str]`, not `list[str]`).

```bash
pip install ruff mypy vermin
ruff check scripts/yarn_spark_settings.py
ruff format scripts/yarn_spark_settings.py
mypy                                   # strict
vermin --no-tips --violations -t=3.6- scripts/yarn_spark_settings.py
```

The `yarn-spark-settings` workflow runs the same checks on every pull request that changes the script.

## Error messages

Problems are listed under **COULD NOT READ** at the top of the report. The report still runs and
leaves out what depends on the missing source.

| Message | What to do |
|---------|------------|
| `... was not found at ...` | The config is in another directory: use `--hadoop-conf`, `--spark-conf`, `--oozie-conf` or the `*_CONF_DIR` variables. A missing `oozie-site.xml` is fine if you do not use Oozie. On an edge node `capacity-scheduler.xml` is often missing; the live data still shows the queues. |
| `No permission to read ...` | Run the script as a user that can read the file (on EMR the `hadoop` user), or have it made readable. |
| `... is not valid XML ...` | The file is damaged or cut off. Check it with `xmllint --noout <file>`. |
| `Connection refused` | Nothing listens there: the ResourceManager is down, or the port is wrong (8088; 8090 with HTTPS). |
| `Unknown host name` | Fix the host in `--rm` or in `yarn.resourcemanager.hostname`. |
| `No answer within 15 seconds` | A firewall or security group blocks the port. On EMR, allow this host on port 8088 in the primary node's security group. |
| `asks for Kerberos login (HTTP 401)` | Run `kinit` (check with `klist`); `curl` must be installed. |
| `refused access (HTTP 403)` | The user may not read the REST API. Check `yarn.admin.acl` and the web UI ACLs. |
| `HTTP 404` / `not JSON` | The address is not a ResourceManager web address, or a proxy login page answers. |
| `This ResourceManager is the standby one` | Give the active ResourceManager with `--rm`. |
| `This script needs Python 3.6 or later` | Run it with a newer `python3`. |
| `No permission to read the EMR cluster information` | The user cannot read `/mnt/var/lib/info`. The header shows the host as an EMR node with an unknown role, and live data stays on. The rest of the report is not affected. |
| No "Cluster" line in the header | The host has no `/mnt/var/lib/info` (not an EMR node). Live data is then off unless you add `--live`. |
