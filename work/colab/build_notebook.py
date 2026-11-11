# Генератор PZ3_Kafka_Streaming.ipynb (код-файли беруться з pz3_code/)
import nbformat as nbf, os
HERE = os.path.dirname(os.path.abspath(__file__))
def code_file(n): return open(os.path.join(HERE, "pz3_code", n), encoding="utf-8").read()
cells = []
def md(s): cells.append(nbf.v4.new_markdown_cell(s.strip("\n")))
def code(s): cells.append(nbf.v4.new_code_cell(s.strip("\n")))
def wf(name, comment=None):
    cells.append(nbf.v4.new_code_cell(f"%%writefile /content/pz3/code/{name}\n" + code_file(name).rstrip("\n")))

md(r"""
# ПЗ3. Потокова обробка: Apache Kafka + Spark Structured Streaming
Датасет: Bitcoin Historical Data (1-хвилинні свічки BTC/USD). Записник виконує **всі завдання 1–8** методички у порядку методички.
Єдиний ручний крок – підключення Google Drive. Далі: **Runtime → Run all** (орієнтовно 20–25 хв).

| Завдання | Розділ |
|---|---|
| 1. Підготовка Kafka, topic `bigdata-topic` | §2 |
| 2. Producer із затримкою | §3 |
| 3–5. Spark Structured Streaming: отримання, обробка, вікна | §4–§5 |
| 6. Виявлення аномалій | §4 (потік) і §7 (калібрування k на всій історії) |
| 7. Збереження результатів (parquet/csv + checkpoint) | §5–§6 |
| 8. Аналіз закономірностей | §8 |

Скріншоти робить сам записник: вивід реальних команд у вигляді термінального PNG і Spark UI через headless Chromium. Усе зберігається у `MyDrive/АВД/colab_results/pz3/`.
""")

md("## 0. Підключення Drive, налаштування, допоміжні функції")
code(r'''
from google.colab import drive
drive.mount('/content/drive')
''')
code(r'''
import os, sys, subprocess, json, time, glob, shutil, textwrap, re, platform
from contextlib import contextmanager
import numpy as np, pandas as pd
from PIL import Image, ImageDraw, ImageFont

# ---------------- параметри (можна змінювати) ----------------
TOPIC          = "bigdata-topic"
# періоди відтворення (UTC, межі включно): COVID-крах, травневий крах 2021, колапс FTX
PERIODS        = "2020-03-09:2020-03-16,2021-05-17:2021-05-22,2022-11-06:2022-11-12"
DELAY_MS       = 10          # затримка між повідомленнями producer, мс
WATERMARK      = "10 minutes"
TRIGGER_SEC    = 10
K_STREAM       = 5           # початкове k у потоці; обране за даними k визначається у §7
KAFKA_VERSION  = "3.9.1"; SCALA = "2.13"
PYSPARK_VER    = "3.5.3"
DRIVE_ROOT     = "/content/drive/MyDrive/АВД"
OUT_DRIVE      = f"{DRIVE_ROOT}/colab_results/pz3"
# --------------------------------------------------------------
BASE="/content/pz3"; CODE=f"{BASE}/code"; DATA=f"{BASE}/data"; LOGS=f"{BASE}/logs"
SHOTS=f"{BASE}/screenshots"; RES=f"{BASE}/results"; JOB_OUT=f"{BASE}/job_out"; CKPT=f"{BASE}/ckpt"
FLAG=f"{BASE}/producer_done.json"
for d in (CODE, DATA, LOGS, SHOTS, RES): os.makedirs(d, exist_ok=True)
BOOT="localhost:9092"
CHECKS = {}      # завдання -> (True/False, повідомлення)
TIMES  = []      # (етап, секунди)
SHOT_LIST = []

@contextmanager
def timed(name):
    t=time.time(); print(f"--> {name}")
    try: yield
    finally:
        TIMES.append((name, round(time.time()-t,1))); print(f"<-- {name}: {time.time()-t:.1f} с")

def sync_drive():
    """Копіює результати на Drive (викликається кілька разів, щоб частковий результат не втрачався)."""
    try:
        os.makedirs(OUT_DRIVE, exist_ok=True)
        for sub in ("screenshots","results","logs"):
            if os.path.isdir(f"{BASE}/{sub}"):
                shutil.copytree(f"{BASE}/{sub}", f"{OUT_DRIVE}/{sub}", dirs_exist_ok=True)
        if os.path.isdir(JOB_OUT):
            shutil.copytree(JOB_OUT, f"{OUT_DRIVE}/results/job_out", dirs_exist_ok=True,
                            ignore=shutil.ignore_patterns("scored_parts"))
        for f in ("versions.txt","runtimes.csv","summary.txt"):
            if os.path.exists(f"{BASE}/{f}"): shutil.copy(f"{BASE}/{f}", f"{OUT_DRIVE}/{f}")
    except Exception as e:
        print("УВАГА: не вдалося синхронізувати з Drive:", e)

def write_summary():
    lines=["ПЗ3 – чекліст завдань", "="*50]
    for k,(ok,msg) in CHECKS.items(): lines.append(f"[{'OK' if ok else 'FAIL'}] {k}: {msg}")
    open(f"{BASE}/summary.txt","w",encoding="utf-8").write("\n".join(lines)+"\n")
    pd.DataFrame(TIMES, columns=["stage","seconds"]).to_csv(f"{BASE}/runtimes.csv", index=False)
    sync_drive()
    return "\n".join(lines)

def check(name, ok, msg=""):
    """М'яка перевірка: фіксує OK/FAIL і продовжує."""
    CHECKS[name]=(bool(ok), msg); print(("[OK]   " if ok else "[FAIL] ")+name+(": "+msg if msg else ""))

def require(ok, msg):
    """Критична перевірка: при невдачі зберігає звіт і зупиняє виконання з чітким повідомленням."""
    if not ok:
        CHECKS["КРИТИЧНА ПОМИЛКА"]=(False,msg); print(write_summary())
        raise RuntimeError("ПЗ3 ЗУПИНЕНО: "+msg)

# ---- термінальний PNG зі справжнім виводом команди ----
ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
def _font(sz=15):
    for p in ("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf","/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf"):
        if os.path.exists(p): return ImageFont.truetype(p, sz)
    return ImageFont.load_default()
def term_png(cmd, text, path, width=118, max_lines=70):
    text = ANSI.sub("", text).replace("\t","    ")
    lines=[]
    for ln in text.splitlines():
        lines += textwrap.wrap(ln, width, drop_whitespace=False, replace_whitespace=False) or [""]
    if len(lines)>max_lines:
        cut=len(lines)-max_lines
        lines = lines[:max_lines//2] + [f"[... пропущено {cut} рядків для скріншота; повний вивід у logs/ ...]"] + lines[-max_lines//2:]
    cl = textwrap.wrap("$ "+cmd, width, subsequent_indent="  ", break_long_words=True) or ["$"]
    f=_font(15); cw=int(f.getlength("M")); lh=21; pad=14
    img=Image.new("RGB",(cw*width+2*pad, lh*(len(cl)+len(lines)+1)+2*pad),(30,30,30))
    dr=ImageDraw.Draw(img); y=pad
    for l in cl: dr.text((pad,y),l,font=f,fill=(120,220,120)); y+=lh
    for l in lines: dr.text((pad,y),l,font=f,fill=(212,212,212)); y+=lh
    img.save(path)

def sh(cmd, shot=None, timeout=300, env=None, quiet=False, log=None):
    """Виконує команду в bash. Повертає (код, вивід). shot='ім'я' -> PNG з реальним виводом."""
    e=os.environ.copy(); 
    if env: e.update(env)
    try:
        p=subprocess.run(cmd, shell=True, executable="/bin/bash", capture_output=True, text=True, timeout=timeout, env=e)
        out=(p.stdout or "")+(p.stderr or ""); rc=p.returncode
    except subprocess.TimeoutExpired as ex:
        out=(ex.stdout.decode() if isinstance(ex.stdout,bytes) else (ex.stdout or ""))+"\n[TIMEOUT]"; rc=124
    if not quiet: print(out[-3000:] if len(out)>3000 else out)
    if log: open(f"{LOGS}/{log}","w",encoding="utf-8").write(f"$ {cmd}\n{out}")
    if shot:
        try:
            term_png(cmd, out, f"{SHOTS}/{shot}.png"); SHOT_LIST.append(shot)
        except Exception as ex: print("скріншот не створено:", ex)
    return rc, out

t_global=time.time()
print("Готово. Результати:", OUT_DRIVE)
''')

md("## 1. Дані, Java, Kafka (KRaft), PySpark")
code(r'''
# --- пошук і розпакування архіву з датасетом
with timed("Дані: пошук і розпакування archive.zip"):
    zip_path = f"{DRIVE_ROOT}/archive.zip"
    if not os.path.exists(zip_path):
        found = glob.glob("/content/drive/MyDrive/**/archive.zip", recursive=True)
        require(found, "archive.zip не знайдено на Drive: покладіть файл у MyDrive/АВД/archive.zip")
        zip_path = found[0]
    print("архів:", zip_path)
    sh(f"rm -rf {DATA}/* && unzip -o -q '{zip_path}' -d {DATA}", quiet=True)
    csvs = sorted(glob.glob(f"{DATA}/**/*.csv", recursive=True), key=os.path.getsize, reverse=True)
    require(csvs, "у архіві немає csv-файлу")
    CSV = csvs[0]
    print("CSV:", CSV, f"{os.path.getsize(CSV)/1e6:.0f} МБ")
    ts_all = pd.read_csv(CSV, usecols=["Timestamp"])["Timestamp"].astype("int64")
    print("рядків:", len(ts_all), "| від", pd.to_datetime(ts_all.min(),unit="s",utc=True), "до", pd.to_datetime(ts_all.max(),unit="s",utc=True))
    EXPECTED = {}
    for it in PERIODS.split(","):
        s,e = it.split(":")
        lo=int(pd.Timestamp(s,tz="UTC").timestamp()); hi=int((pd.Timestamp(e,tz="UTC")+pd.Timedelta(days=1)).timestamp())
        EXPECTED[it]=int(((ts_all>=lo)&(ts_all<hi)).sum())
    EXPECTED_TOTAL=sum(EXPECTED.values())
    print("рядків у періодах відтворення:", EXPECTED, "| разом", EXPECTED_TOTAL)
    pd.DataFrame([{"period":k,"rows_in_csv":v} for k,v in EXPECTED.items()]).to_csv(f"{RES}/replay_periods.csv", index=False)
    del ts_all
    require(EXPECTED_TOTAL>0, "жоден із періодів відтворення не містить даних")
''')
code(r'''
# --- Java 17
with timed("Встановлення OpenJDK 17"):
    sh("apt-get -qq update && apt-get -qq install -y openjdk-17-jdk-headless", quiet=True, timeout=600)
    jh = sorted(glob.glob("/usr/lib/jvm/java-17-openjdk*"))
    require(jh, "OpenJDK 17 не встановлено (apt-get)")
    os.environ["JAVA_HOME"]=jh[0]; os.environ["PATH"]=jh[0]+"/bin:"+os.environ["PATH"]
    print(sh("java -version", quiet=True)[1])
''')
code(r'''
# --- Kafka 3.9.x (режим KRaft, без ZooKeeper)
with timed("Завантаження Kafka"):
    KAFKA=None
    for ver_ in (KAFKA_VERSION, "3.9.0", "3.8.1"):          # запасні версії, якщо основної немає на дзеркалах
        tgz=f"kafka_{SCALA}-{ver_}.tgz"; cand=f"/content/kafka_{SCALA}-{ver_}"
        if os.path.isdir(cand): KAFKA=cand; break
        for base in ("https://dlcdn.apache.org/kafka","https://archive.apache.org/dist/kafka"):
            url=f"{base}/{ver_}/{tgz}"; print("спроба:", url)
            sh(f"wget -q -O /content/{tgz} {url}", quiet=True, timeout=600)
            if os.path.exists(f"/content/{tgz}") and os.path.getsize(f"/content/{tgz}")>50e6:
                sh(f"tar -xzf /content/{tgz} -C /content", quiet=True); KAFKA=cand; break
        if KAFKA: break
    require(KAFKA, "не вдалося завантажити Kafka ні з dlcdn.apache.org, ні з archive.apache.org")
    require(os.path.isdir(KAFKA), "Kafka не розпаковано")
    print("Kafka:", KAFKA)
''')
code(r'''
# --- pip-пакети: pyspark (версія = версії пакета spark-sql-kafka), confluent-kafka, playwright
with timed("pip: pyspark, confluent-kafka, playwright"):
    sh(f"pip install -q pyspark=={PYSPARK_VER} confluent-kafka pyarrow pillow playwright", quiet=True, timeout=900)
    import importlib.metadata as im
    SPARK_V = im.version("pyspark")
    KAFKA_PKG = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_V}"
    print("pyspark", SPARK_V, "| пакет:", KAFKA_PKG, "| confluent-kafka", im.version("confluent-kafka"))
    UI_OK = False
    rc,_ = sh("playwright install --with-deps chromium", quiet=True, timeout=900)
    UI_OK = (rc==0); print("playwright/chromium встановлено:", UI_OK)
''')
code(r'''
# --- ініціалізація сховища KRaft і запуск брокера у фоні
with timed("Запуск Kafka (KRaft)"):
    cfg = f"{KAFKA}/config/kraft/server.properties"
    if not os.path.exists(cfg): cfg = f"{KAFKA}/config/server.properties"
    sh(f"rm -rf /content/kafka-logs; sed -i 's#^log.dirs=.*#log.dirs=/content/kafka-logs#' {cfg}", quiet=True)
    cid = sh(f"{KAFKA}/bin/kafka-storage.sh random-uuid", quiet=True)[1].strip().splitlines()[-1]
    sh(f"{KAFKA}/bin/kafka-storage.sh format -t {cid} -c {cfg}",
       shot="01_kafka_storage_format", log="kafka_format.log")
    kenv = os.environ.copy(); kenv["KAFKA_HEAP_OPTS"]="-Xmx1g -Xms1g"
    klog = open(f"{LOGS}/kafka_server.log","w")
    KPROC = subprocess.Popen([f"{KAFKA}/bin/kafka-server-start.sh", cfg], stdout=klog, stderr=subprocess.STDOUT, env=kenv)
    up=False
    for i in range(45):
        time.sleep(2)
        if KPROC.poll() is not None: break
        rc,_ = sh(f"{KAFKA}/bin/kafka-broker-api-versions.sh --bootstrap-server {BOOT}", quiet=True, timeout=30)
        if rc==0: up=True; break
    require(up, "брокер Kafka не запустився за 90 с. Кінець логу:\n"+sh(f"tail -n 25 {LOGS}/kafka_server.log", quiet=True)[1])
    print("Брокер Kafka працює (KRaft, один вузол: broker+controller).")
''')
code(r'''
# --- версії та конфігурація машини
def _o(c): return sh(c, quiet=True)[1].strip()
ver = [
 "Python: "+platform.python_version(),
 "Java: "+_o("java -version 2>&1 | head -n1"),
 "Kafka: "+_o(f"{KAFKA}/bin/kafka-topics.sh --version | head -n1")+f" (Scala {SCALA}, режим KRaft)",
 "PySpark: "+SPARK_V, "Пакет Kafka-конектора: "+KAFKA_PKG,
 "confluent-kafka: "+im.version("confluent-kafka"), "pandas: "+pd.__version__, "numpy: "+np.__version__,
 "CPU: "+_o("lscpu | grep 'Model name' | sed 's/.*: *//'")+f" | потоків: {os.cpu_count()}",
 "RAM: "+_o("grep MemTotal /proc/meminfo"), "Дата запуску: "+time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
 f"Параметри: TOPIC={TOPIC}; PERIODS={PERIODS}; DELAY_MS={DELAY_MS}; WATERMARK={WATERMARK}; TRIGGER={TRIGGER_SEC}s"]
open(f"{BASE}/versions.txt","w",encoding="utf-8").write("\n".join(ver)+"\n")
print("\n".join(ver)); sync_drive()
''')

md(r"""
## 2. Завдання 1. Підготовка Kafka та topic `bigdata-topic`
Кількість partition = **1**. Обґрунтування: (1) Kafka гарантує порядок лише всередині partition, а аналіз за часом події й правило аномалій (ковзне вікно 60 хв) потребують хронологічного порядку; (2) при ключі = дата повідомлення різних діб потрапили б у різні partition, і Spark читав би їх паралельно – доба, що відстає, виходила б за 10-хвилинний watermark та відкидалася б як запізніла; (3) навантаження ~100 повідомлень/с на порядки менше за пропускну здатність одного partition; (4) брокер один, тому replication-factor = 1.
""")
code(r'''
KT = f"{KAFKA}/bin/kafka-topics.sh --bootstrap-server {BOOT}"
with timed("Завдання 1: створення topic"):
    sh(f"{KT} --delete --topic {TOPIC} --if-exists; sleep 2", quiet=True)
    sh(f"{KT} --create --topic {TOPIC} --partitions 1 --replication-factor 1", shot="02_topic_create", log="topic_create.log")
    sh(f"{KT} --list", shot="03_topic_list")
    rc,desc = sh(f"{KT} --describe --topic {TOPIC}", shot="04_topic_describe", log="topic_describe.log")
    check("Завдання 1: topic створено", rc==0 and TOPIC in desc and "PartitionCount: 1" in desc, "kafka-topics --describe")
    require(TOPIC in desc, f"topic {TOPIC} не створено; вивід: {desc}")
''')

md(r"""
## 3. Завдання 2. Producer
Producer (`producer.py`, confluent-kafka) читає CSV, вибирає свічки заданих періодів і послідовно надсилає їх у Kafka: **1 повідомлення = 1 ринкова хвилина**, `key` = дата UTC, `value` = JSON `{ts, open, high, low, close, volume}`. Затримка між повідомленнями (`DELAY_MS`) імітує потік (прискорення ≈ 6000× відносно реального часу при 10 мс). Періоди – волатильні епізоди, щоб відтворення було коротким (≈30 тис. повідомлень).
""")
wf("anomaly_rule.py")
wf("producer.py")
code(r'''
# --- smoke-тест producer на окремому topic + console consumer (доказ отримання повідомлень)
with timed("Завдання 2: smoke-тест producer і console consumer"):
    sh(f"{KT} --create --topic bigdata-smoke --partitions 1 --replication-factor 1 --if-not-exists", quiet=True)
    rc,out = sh(f"python3 {CODE}/producer.py --csv '{CSV}' --topic bigdata-smoke --max-rows 30 --delay-ms 20 --periods '{PERIODS}'",
                shot="05_producer_smoke", log="producer_smoke.log", timeout=300)
    check("Завдання 2: producer відправляє повідомлення", rc==0 and "завершено" in out, "smoke-тест 30 повідомлень")
    cc = (f"{KAFKA}/bin/kafka-console-consumer.sh --bootstrap-server {BOOT} --topic bigdata-smoke --from-beginning "
          f"--max-messages 6 --timeout-ms 20000 --group pz3-demo --property print.key=true --property print.timestamp=true")
    rc,out = sh(cc, shot="06_console_consumer_smoke", log="console_consumer_smoke.log", timeout=60)
    check("Завдання 2: console consumer отримав повідомлення", out.count('"ts"')>=6, "bigdata-smoke")
    sh(f"{KAFKA}/bin/kafka-consumer-groups.sh --bootstrap-server {BOOT} --describe --group pz3-demo", shot="07_consumer_group_describe")
''')

md(r"""
## 4. Завдання 3–6. Spark Structured Streaming-застосунок
`streaming_job.py` виконує: **(3)** читання з Kafka (`readStream`, `maxOffsetsPerTrigger`); **(4)** декодування (bytes→string→JSON з явною схемою), перетворення типів (`Long`/`Double`, `unix → timestamp`), фільтрацію некоректних свічок (null, `high<low`, `close<=0`, `volume<0`), групування за добою та статистичні агрегати; **(5)** tumbling-вікна 15 хв і 1 год за **часом події** (`ts`), а не часом обробки: OHLC (`open` – значення з найранішою подією у вікні, `close` – з найпізнішою через `min/max(struct(event_time, …))`), `sum(volume)`, `count`; **(6)** правило аномалій у `foreachBatch` зі станом (буфер 61 хв у pandas); **(7)** parquet-приймачі з checkpoint (append), memory-приймачі (update) для перевірки, console-приймач для демонстрації.

**Watermark = 10 хв.** Дані з одного partition приходять упорядковано, тож запізнілих подій немає; 10 хв допускають дрібні розбіжності (повторна доставка, зміщення між кількома producer) і менші за найкоротше вікно (15 хв). Ціна: parquet-приймач (append) випускає вікно лише коли watermark перетне його кінець, тобто із запізненням на 10 хв події-часу. Тому останні вікна останнього періоду в parquet не потрапляють; їх повний стан береться з memory-приймача (update).

**Вибір реалізації аномалій.** Ковзна медіана/MAD не є алгебраїчним агрегатом і погано ложиться на віконні агрегати Structured Streaming, тому використано `foreachBatch` зі станом у драйвері (буфер останніх 61 записів). Та сама функція `score()` застосовується офлайн (перевірка збігу у §7). Правило: `|r_t| > k·1,4826·MAD_60(r)` **АБО** `V_t > k_v·median_60(V)`, де `r_t = ln(C_t/C_{t-1})`, вікно – 60 попередніх хвилин без поточної.
""")
wf("streaming_job.py")
wf("ui_shot.py")

md(r"""
## 5. Запуск потоку (завдання 2–7 разом)
Спочатку стартує Spark-застосунок (чекає на дані), потім producer. Під час роботи знімаються: console consumer на `bigdata-topic`, зміщення (offsets) у Kafka, Spark UI (Jobs, Structured Streaming).
""")
code(r'''
def tail(path, n=25): return sh(f"tail -n {n} '{path}'", quiet=True)[1]
with timed("Потокова обробка (Spark + producer)"):
    sh(f"rm -rf {CKPT} {JOB_OUT} {FLAG}", quiet=True)
    jenv = os.environ.copy()
    jlog = open(f"{LOGS}/streaming_job.log","w")
    JOB = subprocess.Popen([sys.executable, f"{CODE}/streaming_job.py", "--bootstrap", BOOT, "--topic", TOPIC,
        "--out", JOB_OUT, "--ckpt", CKPT, "--done-flag", FLAG, "--kafka-pkg", KAFKA_PKG, "--watermark", WATERMARK,
        "--trigger-seconds", str(TRIGGER_SEC), "--k-stream", str(K_STREAM)], stdout=jlog, stderr=subprocess.STDOUT, env=jenv)
    started=False
    for i in range(120):   # до 10 хв (завантаження jar-пакетів Maven при першому запуску)
        time.sleep(5)
        if JOB.poll() is not None: break
        if "STREAMING_STARTED" in open(f"{LOGS}/streaming_job.log",errors="ignore").read(): started=True; break
    require(started, "Spark-застосунок не запустив потоки. Кінець логу:\n"+tail(f"{LOGS}/streaming_job.log", 40))
    print("Spark-запити запущено.")
    plog = open(f"{LOGS}/producer.log","w")
    PROD = subprocess.Popen([sys.executable, f"{CODE}/producer.py", "--csv", CSV, "--topic", TOPIC, "--periods", PERIODS,
        "--delay-ms", str(DELAY_MS), "--stats", f"{RES}/producer_stats.json", "--done-flag", FLAG], stdout=plog, stderr=subprocess.STDOUT)
    t0=time.time(); shot_done=set()
    while True:
        time.sleep(15); el=time.time()-t0
        lp=tail(f"{LOGS}/producer.log",1).strip(); lj=[l for l in tail(f"{LOGS}/streaming_job.log",40).splitlines() if l.startswith("[job]") or l.startswith("[anomaly]")]
        print(f"t={el:4.0f}с | {lp} | {lj[-1] if lj else ''}")
        if el>=45 and "kafka" not in shot_done:
            shot_done.add("kafka")
            sh(f"{KAFKA}/bin/kafka-console-consumer.sh --bootstrap-server {BOOT} --topic {TOPIC} --from-beginning --max-messages 8 "
               f"--timeout-ms 15000 --property print.key=true --property print.timestamp=true", shot="08_console_consumer_main", log="console_consumer_main.log", timeout=60)
            sh(f"{KAFKA}/bin/kafka-get-offsets.sh --bootstrap-server {BOOT} --topic {TOPIC} --time -1 || {KAFKA}/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server {BOOT} --topic {TOPIC}",
               shot="09_kafka_end_offsets")
        if el>=75 and "ui" not in shot_done and UI_OK:
            shot_done.add("ui")
            for url,nm in (("jobs/","10_spark_ui_jobs"),("StreamingQuery/","11_spark_ui_streaming")):
                try:
                    rc,o = sh(f"python3 {CODE}/ui_shot.py http://localhost:4040/{url} {SHOTS}/{nm}.png", quiet=True, timeout=90)
                    print("Spark UI", url, "->", "OK" if rc==0 else "не вдалося: "+o[-200:])
                    if rc==0: SHOT_LIST.append(nm)
                except Exception as ex: print("Spark UI знімок пропущено:", ex)
        if PROD.poll() is not None and JOB.poll() is not None: break
        if el>1500:
            PROD.kill(); JOB.kill(); break
    print("producer rc =", PROD.returncode, "| job rc =", JOB.returncode)
    jlog.close(); plog.close()
    print(tail(f"{LOGS}/producer.log",3)); print(tail(f"{LOGS}/streaming_job.log",12))
    require(PROD.returncode==0, "producer завершився з помилкою:\n"+tail(f"{LOGS}/producer.log",15))
    require(os.path.exists(f"{JOB_OUT}/job_summary.json"), "Spark-застосунок не зберіг job_summary.json. Кінець логу:\n"+tail(f"{LOGS}/streaming_job.log",40))
JSUM = json.load(open(f"{JOB_OUT}/job_summary.json")); PSTAT = json.load(open(f"{RES}/producer_stats.json"))
print(JSUM); sync_drive()
check("Завдання 2: потік відтворено повністю", PSTAT["sent"]==EXPECTED_TOTAL and PSTAT["delivery_errors"]==0,
      f"надіслано {PSTAT['sent']} з {EXPECTED_TOTAL} рядків CSV, {PSTAT['msg_per_s']} повід./с")
check("Завдання 3: Spark прочитав усі повідомлення з Kafka", JSUM["status"]=="ok" and all(v>=PSTAT["sent"] for v in JSUM["input_totals"].values()),
      f"прочитано {JSUM['input_totals']}")
''')
code(r'''
# --- докази: вивід консольного приймача, структура sink-каталогів і checkpoint
log = open(f"{LOGS}/streaming_job.log",errors="ignore").read()
bids = sorted({int(x) for x in re.findall(r"^Batch: (\d+)$", log, flags=re.M)})
print("пакети console-приймача:", bids[:10], "...")
if bids:
    b = bids[len(bids)//2]
    sh(f"grep -A 14 -m1 '^Batch: {b}$' {LOGS}/streaming_job.log", shot="12_console_sink_batch")
sh(f"grep '^\\[anomaly\\]' {LOGS}/streaming_job.log | head -n 25", shot="13_foreachbatch_anomaly_log")
sh(f"cd {BASE} && ls -la job_out job_out/parquet/win15m | head -n 30 && echo && ls ckpt && echo && find ckpt/win15m -maxdepth 2 | head -n 12", shot="14_sinks_and_checkpoint")
''')

md("## 6. Завдання 5 і 7. Перевірка віконних агрегацій та збережених результатів")
code(r'''
with timed("Перевірка вікон проти pandas"):
    # 1) parquet-приймач (append) не порожній
    w15_sink = pd.read_parquet(f"{JOB_OUT}/parquet/win15m"); w1h_sink = pd.read_parquet(f"{JOB_OUT}/parquet/win1h")
    check("Завдання 7: parquet-приймач вікон непорожній", len(w15_sink)>0 and len(w1h_sink)>0, f"15 хв: {len(w15_sink)} вікон, 1 год: {len(w1h_sink)} вікон")
    require(len(w15_sink)>0, "parquet-приймач порожній: жодне вікно не закрилося watermark-ом")
    # 2) еталон: pandas.resample по тих самих рядках CSV
    lo_hi=[]
    for it in PERIODS.split(","):
        s,e=it.split(":"); lo_hi.append((int(pd.Timestamp(s,tz="UTC").timestamp()), int((pd.Timestamp(e,tz="UTC")+pd.Timedelta(days=1)).timestamp())))
    parts=[]
    for ch in pd.read_csv(CSV, chunksize=1_000_000):
        m=False
        for lo,hi in lo_hi: m = m | ((ch["Timestamp"]>=lo)&(ch["Timestamp"]<hi))
        if m.any(): parts.append(ch[m])
    ref = pd.concat(parts).dropna(); ref = ref[(ref.High>=ref.Low)&(ref.Close>0)&(ref.Volume>=0)]
    ref["dt"]=pd.to_datetime(ref.Timestamp.astype("int64"),unit="s")
    ref=ref.sort_values("dt").set_index("dt")
    def ref_win(freq):
        r=ref.resample(freq).agg({"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum","Timestamp":"count"})
        r=r[r.Timestamp>0]; r.columns=["open","high","low","close","volume","n_candles"]; r.index.name="win_start"; return r.reset_index()
    def compare(spark_df, ref_df, label):
        s=spark_df.copy(); s["win_start"]=pd.to_datetime(s["win_start"]).dt.tz_localize(None) if pd.to_datetime(s["win_start"]).dt.tz is not None else pd.to_datetime(s["win_start"])
        m=s.merge(ref_df,on="win_start",how="outer",suffixes=("_spark","_ref"),indicator=True)
        both=m[m._merge=="both"]
        bad=0
        for c in ("open","high","low","close","volume"):
            bad+=int((~np.isclose(both[c+"_spark"],both[c+"_ref"],rtol=1e-9,atol=1e-9)).sum())
        bad+=int((both["n_candles_spark"]!=both["n_candles_ref"]).sum())
        return {"window":label,"spark_windows":len(s),"reference_windows":len(ref_df),"only_spark":int((m._merge=="left_only").sum()),
                "only_reference":int((m._merge=="right_only").sum()),"value_mismatches":bad}
    w15_all=pd.read_csv(f"{JOB_OUT}/win15m_all.csv"); w1h_all=pd.read_csv(f"{JOB_OUT}/win1h_all.csv")
    cmpdf=pd.DataFrame([compare(w15_all,ref_win("15min"),"15 хв (memory, update)"), compare(w1h_all,ref_win("1h"),"1 год (memory, update)"),
                        compare(w15_sink,ref_win("15min"),"15 хв (parquet, append)"), compare(w1h_sink,ref_win("1h"),"1 год (parquet, append)")])
    cmpdf.to_csv(f"{RES}/window_check_vs_pandas.csv",index=False); print(cmpdf.to_string(index=False))
    # у append-приймачі відсутні лише останні вікна (watermark ще не перетнув їх кінець)
    full_ok = (cmpdf.loc[:1,"only_spark"].sum()==0 and cmpdf.loc[:1,"only_reference"].sum()==0 and cmpdf.loc[:1,"value_mismatches"].sum()==0)
    sink_ok = (cmpdf.loc[2:,"only_spark"].sum()==0 and cmpdf.loc[2:,"value_mismatches"].sum()==0)
    check("Завдання 5: OHLC/volume/count вікон збігаються з pandas.resample", full_ok, "memory-приймач, 15 хв і 1 год")
    check("Завдання 7: вікна у parquet збігаються з еталоном", sink_ok, f"не випущено watermark-ом: 15 хв {cmpdf.loc[2,'only_reference']}, 1 год {cmpdf.loc[3,'only_reference']} вікон")
    check("Завдання 4: сума n_candles = кількість коректних повідомлень", int(w15_all.n_candles.sum())==len(ref) and int(w1h_all.n_candles.sum())==len(ref),
          f"{int(w15_all.n_candles.sum())} vs {len(ref)} (запізнілих подій немає)")
''')
code(r'''
# --- таблиці: агрегати за вікнами, добова статистика, сліди сховища (скріншоти реального виводу)
sh(f"python3 -c \"import pandas as pd; pd.set_option('display.width',200); d=pd.read_parquet('{JOB_OUT}/parquet/win15m').sort_values('win_start'); print(d.head(10).to_string(index=False)); print(len(d),'вікон 15 хв у parquet')\"", shot="15_parquet_win15m")
sh(f"python3 -c \"import pandas as pd; pd.set_option('display.width',200); d=pd.read_parquet('{JOB_OUT}/parquet/win1h').sort_values('win_start'); print(d.head(10).to_string(index=False)); print(len(d),'вікон 1 год у parquet')\"", shot="16_parquet_win1h")
sh(f"cat {JOB_OUT}/daily_stats.csv", shot="17_daily_stats")
sh(f"cat {RES}/window_check_vs_pandas.csv", shot="18_window_check")
sh(f"ls -la {JOB_OUT}/*.csv {JOB_OUT}/*.parquet", shot="19_saved_files")
''')
code(r'''
# --- пропускна здатність, тривалість мікробатчів, затримка
prog=pd.read_csv(f"{JOB_OUT}/progress.csv"); bst=pd.read_csv(f"{JOB_OUT}/batch_stats.csv")
prog.to_csv(f"{RES}/batch_durations_all_queries.csv",index=False); bst.to_csv(f"{RES}/latency_per_batch.csv",index=False)
a_=prog[(prog["query"]=="anomaly")&(prog.num_input_rows>0)]
thr=pd.DataFrame([{
 "producer_msg_per_s":PSTAT["msg_per_s"],"producer_seconds":PSTAT["seconds"],"messages":PSTAT["sent"],"delay_ms":PSTAT["delay_ms"],
 "spark_input_rows_per_s_mean":a_.input_rows_per_s.mean(),"spark_processed_rows_per_s_mean":a_.processed_rows_per_s.mean(),
 "spark_processed_rows_per_s_max":a_.processed_rows_per_s.max(),"microbatches_nonempty":len(a_),
 "trigger_ms_mean":a_.trigger_execution_ms.mean(),"trigger_ms_p95":a_.trigger_execution_ms.quantile(.95),"trigger_ms_max":a_.trigger_execution_ms.max(),
 "latency_mean_s":np.average(bst.latency_mean_s,weights=bst.n_raw),"latency_p95_s_max_over_batches":bst.latency_p95_s.max(),"latency_max_s":bst.latency_max_s.max(),
 "invalid_records":int(bst.n_invalid.sum()),"raw_records":int(bst.n_raw.sum())}]).round(3)
thr.to_csv(f"{RES}/throughput_latency.csv",index=False)
sh(f"python3 -c \"import pandas as pd; print(pd.read_csv('{RES}/throughput_latency.csv').T.to_string(header=False))\"", shot="20_throughput_latency")
check("Завдання 4: відфільтровано некоректні записи (лічильник)", True, f"некоректних: {int(bst.n_invalid.sum())} із {int(bst.n_raw.sum())}")
print(write_summary()[-400:])
''')

md(r"""
## 7. Завдання 6. Калібрування правила аномалій на всій історії
`anomaly_analysis.py` застосовує ту саму функцію `score()` до **всієї історії** (pandas/numpy), рахує кількість спрацювань для k ∈ {3, 5, 7, 10} (k_v = k), обирає k за «бюджетом» (частка позначених хвилин ≤ 1 %, тобто не більше ~14 сигналів на добу), показує реальні дати з найбільшою кількістю аномалій і звіряє результат потокової реалізації з офлайн-реалізацією на тих самих хвилинах.
""")
wf("anomaly_analysis.py")
code(r'''
with timed("Завдання 6: офлайн-калібрування k на всій історії"):
    os.makedirs(f"{RES}/anomaly",exist_ok=True)
    rc,out = sh(f"python3 {CODE}/anomaly_analysis.py --csv '{CSV}' --out {RES}/anomaly --scored {JOB_OUT}/scored.parquet",
                shot="21_anomaly_analysis", log="anomaly_analysis.log", timeout=1200, quiet=True)
    print(out[-6000:])
    require(rc==0 and "ANALYSIS_DONE" in out, "anomaly_analysis.py завершився з помилкою")
cnt=pd.read_csv(f"{RES}/anomaly/anomaly_counts_per_k.csv"); chosen=json.load(open(f"{RES}/anomaly/chosen_k.json"))
sh(f"cat {RES}/anomaly/anomaly_counts_per_k.csv", shot="22_anomaly_counts_per_k")
sh(f"head -n 16 {RES}/anomaly/top_dates.csv", shot="23_anomaly_top_dates")
sh(f"cat {RES}/anomaly/known_events_check.csv {RES}/anomaly/stream_vs_offline.csv", shot="24_known_events_stream_vs_offline")
svo=pd.read_csv(f"{RES}/anomaly/stream_vs_offline.csv").iloc[0]
check("Завдання 6: правило аномалій реалізовано (потік + офлайн)", len(cnt)==4 and (svo.filter(like="flag_mismatch")==0).all(),
      f"обрано k={chosen['k']}; розбіжностей прапорців потік/офлайн: {int(svo.filter(like='flag_mismatch').iloc[0])}")
an=pd.read_parquet(f"{RES}/anomaly/anomalies_stream_final.parquet")
check("Завдання 7: аномалії збережено (parquet+csv)", len(an)>0 and os.path.exists(f"{RES}/anomaly/anomalies_stream_final.csv"), f"{len(an)} аномалій при k={chosen['k']}")
from IPython.display import display, Image as IPImage
for f in ("anomaly_counts_per_k.png","stream_price_anomalies.png"):
    p=f"{RES}/anomaly/figures/{f}"
    if os.path.exists(p): shutil.copy(p, f"{SHOTS}/fig_{f}"); display(IPImage(p))
sync_drive()
''')

md("## 8. Завдання 8. Закономірності, виявлені при потоковій обробці")
code(r'''
w1h=pd.read_csv(f"{JOB_OUT}/win1h_all.csv",parse_dates=["win_start"]); w15=pd.read_csv(f"{JOB_OUT}/win15m_all.csv",parse_dates=["win_start"])
daily=pd.read_csv(f"{JOB_OUT}/daily_stats.csv"); sc=pd.read_parquet(f"{JOB_OUT}/scored.parquet")
w1h["range_pct"]=100*(w1h.high-w1h.low)/w1h.open; w15["range_pct"]=100*(w15.high-w15.low)/w15.open
w1h["hour"]=w1h.win_start.dt.hour; w1h["period"]=(w1h.win_start.diff()>pd.Timedelta(days=2)).cumsum()+1
by_hour=w1h.groupby("hour").agg(mean_range_pct=("range_pct","mean"),mean_volume=("volume","mean")).round(4)
by_hour.to_csv(f"{RES}/pattern_by_hour_utc.csv")
daily["range_pct"]=100*(daily.max_high-daily.min_low)/daily.avg_close
per_period=w1h.groupby("period").agg(from_=("win_start","min"),to_=("win_start","max"),mean_range_pct=("range_pct","mean"),max_range_pct=("range_pct","max"),volume=("volume","sum")).round(3)
per_period.to_csv(f"{RES}/pattern_by_period.csv")
daily.sort_values("range_pct",ascending=False).to_csv(f"{RES}/pattern_days_by_range.csv",index=False)
rho=w15[["range_pct","volume"]].corr(method="spearman").iloc[0,1]
k_=chosen["k"]; scs=sc[sc.scored]; scs=scs.assign(hour=pd.to_datetime(scs.ts,unit="s").dt.hour, date=pd.to_datetime(scs.ts,unit="s").dt.strftime("%Y-%m-%d"))
an_by_day=scs[scs[f"flag_k{k_}"]].groupby("date").size().sort_values(ascending=False)
an_by_hour=scs[scs[f"flag_k{k_}"]].groupby("hour").size()
top_day=daily.sort_values("range_pct",ascending=False).iloc[0]
require(len(an_by_day)>0, "у потоці немає жодної аномалії при обраному k – перевірте дані/параметри")
lines=[
 f"1. Волатильність за годинами UTC: максимум середнього діапазону 1-год вікна о {int(by_hour.mean_range_pct.idxmax()):02d}:00 ({by_hour.mean_range_pct.max():.3f} %), мінімум о {int(by_hour.mean_range_pct.idxmin()):02d}:00 ({by_hour.mean_range_pct.min():.3f} %).",
 f"2. Обсяг найбільший о {int(by_hour.mean_volume.idxmax()):02d}:00 UTC (середній обсяг вікна {by_hour.mean_volume.max():.1f} BTC), найменший о {int(by_hour.mean_volume.idxmin()):02d}:00 ({by_hour.mean_volume.min():.1f} BTC).",
 f"3. Кореляція Спірмена між діапазоном і обсягом 15-хв вікон: {rho:.3f}.",
 f"4. Найвищий добовий діапазон (max high – min low відносно середнього close): {top_day.trade_date}, {top_day.range_pct:.2f} %.",
 f"5. Аномалій (k={k_}) у відтвореному потоці: {int(an_by_day.sum())} із {len(scs)} оцінених хвилин ({100*an_by_day.sum()/len(scs):.2f} %); найбільше за датою: {an_by_day.index[0]} ({int(an_by_day.iloc[0])}).",
 f"6. Частка аномалій у три найбільш аномальні години доби: {100*an_by_hour.sort_values(ascending=False).head(3).sum()/an_by_hour.sum():.1f} % (години UTC: {list(an_by_hour.sort_values(ascending=False).head(3).index)}).",
 "7. Середній діапазон 1-год вікна за періодами (таблиця pattern_by_period.csv): "+"; ".join(f"період {i}: {r.mean_range_pct:.3f} %" for i,r in per_period.iterrows())+"."]
open(f"{RES}/findings.txt","w",encoding="utf-8").write("\n".join(lines)+"\n"); print("\n".join(lines))
sh(f"cat {RES}/pattern_by_period.csv && head -n 8 {RES}/pattern_days_by_range.csv", shot="25_patterns")
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
fig,ax=plt.subplots(1,2,figsize=(11,3.8))
ax[0].bar(by_hour.index,by_hour.mean_range_pct); ax[0].set_xlabel("година UTC"); ax[0].set_ylabel("середній діапазон 1-год вікна, %"); ax[0].set_title("Волатильність за годинами")
ax[1].bar(by_hour.index,by_hour.mean_volume,color="tab:orange"); ax[1].set_xlabel("година UTC"); ax[1].set_ylabel("середній обсяг, BTC"); ax[1].set_title("Обсяг за годинами")
fig.tight_layout(); fig.savefig(f"{SHOTS}/fig_pattern_by_hour.png",dpi=150); plt.show()
check("Завдання 8: закономірності обчислено й збережено", True, "findings.txt, pattern_*.csv")
''')

md("## 9. Підсумок: чекліст, версії, час, збереження на Drive")
code(r'''
TIMES.append(("ВСЬОГО (від початку §0)", round(time.time()-t_global,1)))
for f in glob.glob(f"{JOB_OUT}/parquet"): pass
check("Скріншоти створено", len(glob.glob(f"{SHOTS}/*.png"))>=15, f"{len(glob.glob(f'{SHOTS}/*.png'))} PNG; Spark UI: {'так' if any(s.startswith('10_') for s in SHOT_LIST) else 'не знято (необов’язково)'}")
txt=write_summary(); print(txt)
print("\nУсі файли на Drive:", OUT_DRIVE)
sh(f"find '{OUT_DRIVE}' -maxdepth 2 | head -n 60", quiet=False)
print("Загальний час:", round((time.time()-t_global)/60,1), "хв")
''')

nb = nbf.v4.new_notebook(); nb.cells = cells
nb.metadata = {"colab": {"provenance": [], "name": "PZ3_Kafka_Streaming.ipynb"}, "kernelspec": {"name": "python3", "display_name": "Python 3"}, "language_info": {"name": "python"}}
nbf.validate(nb); nbf.write(nb, os.path.join(HERE, "PZ3_Kafka_Streaming.ipynb"))
print("cells:", len(cells))
