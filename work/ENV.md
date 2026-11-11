# Environment (verified 2026-11-11)
Machine: 4 vCPU, 15 GiB RAM, disk 252G (~30G free on /), Ubuntu 24.04, Python 3.13.18 (system pip, root).
Proxy: Apache archive, Maven Central, conda-forge, PyPI reachable. Blocked (403 CONNECT): fastdl.mongodb.org, micro.mamba.pm, drive.google.com. conda-forge has NO kafka/hadoop/hive packages.

| Component | Works | How installed | Version |
|---|---|---|---|
| Java | yes | preinstalled (apt) /usr/lib/jvm/java-21-openjdk-amd64; JDK8 via `micromamba create -n jdk8 -c conda-forge openjdk=8` | 21.0.12 ; 8u472 |
| pyspark | yes (local[*], UI :4040 returned HTTP 200 during job) | `pip install --ignore-installed wheel setuptools; pip install pyspark` (plain install failed: debian setuptools "AttributeError: install_layout") | 4.3.0 (warns: pandas 3.0 not fully supported) |
| MongoDB server | yes (insert/find/index/explain OK) | micromamba from `https://conda.anaconda.org/conda-forge/linux-64/micromamba-2.9.0-0.tar.bz2` (tools/mm), then `micromamba create -n mongo -c conda-forge mongodb` | mongod 8.3.7 |
| pymongo | yes | pip | 4.19.0 |
| Kafka | yes (KRaft, topic create/produce/consume OK) | `curl -O https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz`; runs on JDK21 | 3.7.0 |
| Hadoop | yes (hadoop fs, local MapReduce wordcount OK) | `https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz`; JDK21 | 3.3.6 |
| Hive | yes, only with JDK8 (embedded Derby, create/insert/select OK; with JDK21 the CLI fails with a reflection stack trace) | `https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz`; `schematool -dbType derby -initSchema` in a work dir | 3.1.3 |
| Chromium + playwright | yes, screenshot OK (work/test_shot.png) | pip playwright upgraded to 1.65 (its bundled revision is missing), so launch with explicit path (below) | Chromium 153 (build 1243), playwright 1.65.0 |
| pandas / pyarrow / matplotlib / python-docx / pymupdf / Pillow | yes | pip | 3.0.5 / 26.0.1 / 3.11.2 / 1.2.0 / 2.1 / 12.3.0 |
| LibreOffice soffice | yes, docx->pdf OK (`soffice --headless --convert-to pdf f.docx`) | preinstalled | 24.2.7.2 |

## Setup lines
    source /home/user/AVD/work/env_setup.sh        # exports vars, installs anything missing
    source /home/user/AVD/work/env_setup.sh start  # also starts mongod (127.0.0.1:27017, dbpath work/mongo_data) and Kafka (localhost:9092, KRaft, log work/kafka.log)
    export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
    export PATH=$JAVA_HOME/bin:/home/user/AVD/work/mamba/envs/mongo/bin:$PATH
    mongod --dbpath /home/user/AVD/work/mongo_data --bind_ip 127.0.0.1 --port 27017 --fork --logpath /home/user/AVD/work/mongo_data/mongod.log
    # Hive: JAVA_HOME=/home/user/AVD/work/mamba/envs/jdk8 HADOOP_HOME=/home/user/AVD/work/tools/hadoop-3.3.6 /home/user/AVD/work/tools/apache-hive-3.1.3-bin/bin/hive -e "..."
    # Playwright python:
    p.chromium.launch(executable_path="/opt/pw-browsers/chromium-1243/chrome-linux64/chrome", args=["--no-sandbox"])
Tarballs/tools live in /home/user/AVD/work/tools. mongod and Kafka were left running at time of writing.
