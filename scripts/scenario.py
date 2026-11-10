"""Этапы 3 и 4. Две копии координатора, пауза лидера посреди расчета, возобновление и проверка итога.

Аргументы RUN и FENCING, например scenario.py stage3_nofence 0 или scenario.py stage4_fence 1.
"""
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime

from kazoo.client import KazooClient

from common import HW3_PAIRS_KEY, INPUT_BUCKET, LAB, OUT_BUCKET, ROOT, ZK_HOSTS, s3_client

RUN, FENCING = sys.argv[1], sys.argv[2]
PAUSE_AFTER_SPLITS = 8
PAUSE_SECONDS = 40
LOGS = LAB / 'logs' / RUN


def say(msg):
    print(f'{datetime.now():%Y-%m-%d %H:%M:%S.%f}'[:-3] + f' scenario {msg}', flush=True)


def wait_line(name, text, timeout=120):
    path = LOGS / f'{name}.log'
    end = time.time() + timeout
    while time.time() < end:
        if path.exists():
            for line in path.read_text().splitlines():
                if text in line:
                    return line
        time.sleep(0.05)
    raise SystemExit(f'{name} не дождался строки {text}')


def log_time(line):
    return datetime.strptime(line[:23], '%Y-%m-%d %H:%M:%S.%f').timestamp()


def reset(zk, s3):
    if zk.exists(ROOT):
        zk.delete(ROOT, recursive=True)
    zk.create(f'{ROOT}/election', makepath=True)
    zk.create(f'{ROOT}/epoch', b'0')
    zk.create(f'{ROOT}/checkpoint', json.dumps({'done': [], 'epoch': 0}).encode())
    zk.create(f'{ROOT}/result', b'{}')
    if OUT_BUCKET not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
        s3.create_bucket(Bucket=OUT_BUCKET)
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=OUT_BUCKET, Prefix=f'{RUN}/'):
        for o in page.get('Contents', []):
            s3.delete_object(Bucket=OUT_BUCKET, Key=o['Key'])
    shutil.rmtree(LOGS, ignore_errors=True)
    LOGS.mkdir(parents=True)
    stat = zk.get(f'{ROOT}/result')[1]
    say(f'сброс состояния {ROOT} эпоха=0 чекпоинт пустой указатель версия={stat.version}')


def start(name):
    env = dict(os.environ, NAME=name, RUN=RUN, FENCING=FENCING)
    p = subprocess.Popen([sys.executable, str(LAB / 'scripts' / 'coordinator.py')], env=env,
                         stdout=subprocess.DEVNULL, stderr=(LOGS / f'{name}.stderr').open('w'))
    say(f'запустил {name} pid={p.pid} FENCING={FENCING}')
    return p


def main():
    print(f'$ .venv/bin/python scripts/scenario.py {RUN} {FENCING}', flush=True)
    zk = KazooClient(hosts=ZK_HOSTS, timeout=10.0)
    zk.start(timeout=15)
    s3 = s3_client()
    reset(zk, s3)

    a = start('coord-a')
    wait_line('coord-a', 'стал лидером')
    b = start('coord-b')
    wait_line('coord-b', 'жду лидерства')
    while len(json.loads(zk.get(f'{ROOT}/checkpoint')[0])['done']) < PAUSE_AFTER_SPLITS:
        time.sleep(0.05)
    os.kill(a.pid, signal.SIGSTOP)
    paused = time.time()
    done = json.loads(zk.get(f'{ROOT}/checkpoint')[0])['done']
    say(f'приостановил лидера coord-a SIGSTOP чекпоинт готово={len(done)}')

    leader_line = wait_line('coord-b', 'стал лидером')
    wait_line('coord-b', 'записал результат')
    time.sleep(max(0.0, paused + PAUSE_SECONDS - time.time()))
    os.kill(a.pid, signal.SIGCONT)
    say(f'возобновил coord-a SIGCONT пауза={time.time() - paused:.1f}с')
    for p in (a, b):
        p.wait(timeout=180)
    for f in LOGS.glob('*.stderr'):
        if not f.read_text().strip():
            f.unlink()

    say(f'смена лидера через {log_time(leader_line) - paused:.2f}с от паузы coord-a до строки стал лидером у coord-b')
    for name in ('coord-a', 'coord-b'):
        text = (LOGS / f'{name}.log').read_text()
        say(f'{name} строк записал_результат={text.count("записал результат")} BadVersionError={text.count("BadVersionError")}')
    data, stat = zk.get(f'{ROOT}/result')
    say(f'указатель {ROOT}/result версия={stat.version} данные={data.decode()}')

    hw3 = s3.get_object(Bucket=INPUT_BUCKET, Key=HW3_PAIRS_KEY)['Body'].read()
    hw3_sha = hashlib.sha256(hw3).hexdigest()
    say(f'ДЗ 3 s3://{INPUT_BUCKET}/{HW3_PAIRS_KEY} строк={hw3.count(b"\n") - 1} sha256={hw3_sha[:16]}')
    for o in s3.list_objects_v2(Bucket=OUT_BUCKET, Prefix=f'{RUN}/result-').get('Contents', []):
        body = s3.get_object(Bucket=OUT_BUCKET, Key=o['Key'])['Body'].read()
        sha = hashlib.sha256(body).hexdigest()
        say(f'результат s3://{OUT_BUCKET}/{o["Key"]} строк={body.count(b"\n") - 1} sha256={sha[:16]} совпадает_с_ДЗ_3={sha == hw3_sha}')
    zk.stop()


if __name__ == '__main__':
    main()
