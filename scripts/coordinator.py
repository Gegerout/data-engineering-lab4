"""Координатор расчета пар навыков варианта. Выбор лидера, чекпоинт и публикация результата через ZooKeeper.

Параметры через окружение. NAME имя копии, RUN имя запуска, FENCING=1 включает проверку эпохи.
"""
import csv
import hashlib
import io
import json
import logging
import math
import os
import re
import threading
from collections import Counter
from datetime import datetime
from itertools import combinations

from kazoo.client import KazooClient

from common import (CHUNK, INPUT_BUCKET, LAB, OUT_BUCKET, RAW_KEY, ROOT, VARIANT_ROLE, ZK_HOSTS,
                    in_variant, s3_client, skills_of)

NAME = os.environ['NAME']
RUN = os.environ['RUN']
FENCING = os.environ.get('FENCING') == '1'
LOG = LAB / 'logs' / RUN / f'{NAME}.log'
ROLE_BYTES = VARIANT_ROLE.encode()
EXTRA = 256 * 1024
negotiated = {}


def log(msg):
    line = f'{datetime.now():%Y-%m-%d %H:%M:%S.%f}'[:-3] + f' {NAME} {msg}'
    print(line, flush=True)
    with LOG.open('a') as f:
        f.write(line + '\n')


class KazooLog(logging.Handler):
    """Из лога kazoo берем согласованный таймаут сессии, предупреждения переносим в журнал."""

    def emit(self, record):
        msg = record.getMessage()
        m = re.search(r'negotiated session timeout: (\d+)', msg)
        if m:
            negotiated['ms'] = int(m.group(1))
        if record.levelno >= logging.WARNING:
            log(f'kazoo {msg}')


def get_range(s3, lo, hi):
    return s3.get_object(Bucket=INPUT_BUCKET, Key=RAW_KEY, Range=f'bytes={lo}-{hi}')['Body'].read()


def read_split(s3, start, end, size):
    """Сплит от байта start-1 до конца последней строки, начатой до end, как в ДЗ 3."""
    base = start - 1 if start > 0 else 0
    hi = end - 1
    buf = bytearray(get_range(s3, base, hi))
    while hi < size - 1 and buf.find(b'\n', end - 1 - base) == -1:
        lo, hi = hi + 1, min(hi + EXTRA, size - 1)
        buf += get_range(s3, lo, hi)
    return bytes(buf), base


def owned_lines(buf, base, start, end):
    p = 0
    if start > 0:
        p = buf.find(b'\n') + 1  # хвост строки предыдущего сплита
        if p == 0:
            return
    while p < len(buf) and base + p < end:
        nl = buf.find(b'\n', p)
        nl = len(buf) if nl == -1 else nl
        yield base + p, buf[p:nl]
        p = nl + 1


def map_split(s3, i, size):
    start, end = i * CHUNK, min((i + 1) * CHUNK, size)
    for attempt in range(1, 4):
        try:
            buf, base = read_split(s3, start, end, size)
            break
        except Exception as e:
            log(f'сплит {i} ошибка чтения {type(e).__name__} попытка={attempt}')
    else:
        raise SystemExit(f'split {i} unreadable')
    pairs = Counter()
    for pos, line in owned_lines(buf, base, start, end):
        if ROLE_BYTES not in line:
            continue
        row = next(csv.reader([line.decode('utf-8')]))
        if pos == 0 and row[0] == 'source':
            continue
        if in_variant(row):
            pairs.update(combinations(skills_of(row), 2))
    return pairs


def split_key(i):
    return f'{RUN}/splits/split-{i:03d}.json'


def wait_for_leadership(zk):
    """Эфемерный последовательный узел, watch только на предыдущего кандидата."""
    me = zk.create(f'{ROOT}/election/n_', NAME.encode(), ephemeral=True, sequence=True)
    mine = me.rsplit('/', 1)[1]
    log(f'кандидат узел={me}')
    while True:
        kids = sorted(zk.get_children(f'{ROOT}/election'))
        idx = kids.index(mine)
        if idx == 0:
            return
        prev = f'{ROOT}/election/{kids[idx - 1]}'
        gone = threading.Event()
        if zk.exists(prev, watch=lambda event: gone.set()):
            log(f'жду лидерства слежу_за={prev}')
            gone.wait()


def finish(zk, listener):
    zk.remove_listener(listener)
    zk.stop()
    log('завершил работу')


def main():
    LOG.parent.mkdir(parents=True, exist_ok=True)
    kz_log = logging.getLogger('kazoo.client')
    kz_log.addHandler(KazooLog())
    kz_log.setLevel(5)  # уровень BLATHER, на нем kazoo пишет согласованный таймаут
    kz_log.propagate = False
    log(f'старт pid={os.getpid()} fencing={int(FENCING)} zk={ZK_HOSTS}')
    zk = KazooClient(hosts=ZK_HOSTS, timeout=10.0)
    def on_state(state):
        log(f'состояние сессии {state}' + (f' сессия={hex(zk.client_id[0])}' if state == 'CONNECTED' and zk.client_id else ''))
    zk.add_listener(on_state)
    zk.start(timeout=15)
    log(f'сессия={hex(zk.client_id[0])} таймаут_сессии_мс={negotiated.get("ms")}')
    s3 = s3_client()
    size = s3.head_object(Bucket=INPUT_BUCKET, Key=RAW_KEY)['ContentLength']
    n = math.ceil(size / CHUNK)

    wait_for_leadership(zk)
    data, st = zk.get(f'{ROOT}/epoch')
    epoch = int(data) + 1
    epoch_version = zk.set(f'{ROOT}/epoch', str(epoch).encode(), version=st.version).version
    log(f'стал лидером эпоха={epoch} версия_узла_эпохи={epoch_version}')

    done = json.loads(zk.get(f'{ROOT}/checkpoint')[0])['done']
    log(f'чекпоинт готово={len(done)} из {n} сплитов {done}')
    for i in range(n):
        if i in done:
            continue
        pairs = map_split(s3, i, size)
        body = json.dumps(sorted([a, b, c] for (a, b), c in pairs.items()), ensure_ascii=False)
        s3.put_object(Bucket=OUT_BUCKET, Key=split_key(i), Body=body.encode())
        done.append(i)
        zk.retry(zk.set, f'{ROOT}/checkpoint', json.dumps({'done': sorted(done), 'epoch': epoch}).encode())
        log(f'сплит {i} готов пар_в_сплите={len(pairs)} чекпоинт готово={len(done)} из {n}')

    total = Counter()
    for i in range(n):
        for a, b, c in json.loads(s3.get_object(Bucket=OUT_BUCKET, Key=split_key(i))['Body'].read()):
            total[(a, b)] += c
    rows = sorted(total.items(), key=lambda kv: (-kv[1], kv[0]))
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['skill_1', 'skill_2', 'vacancies'])
    for (a, b), v in rows:
        w.writerow([a, b, v])
    body = out.getvalue().encode()
    key = f'{RUN}/result-{NAME}-epoch{epoch}.csv'
    s3.put_object(Bucket=OUT_BUCKET, Key=key, Body=body)
    (a, b), v = rows[0]
    log(f'расчет готов пар={len(rows)} сумма={sum(total.values())} первая={a} + {b} {v} '
        f'sha256={hashlib.sha256(body).hexdigest()[:16]} файл=s3://{OUT_BUCKET}/{key}')

    pointer = json.dumps({'result': f's3://{OUT_BUCKET}/{key}', 'coordinator': NAME, 'epoch': epoch}).encode()
    if FENCING:
        def publish():
            tx = zk.transaction()
            tx.check(f'{ROOT}/epoch', epoch_version)
            tx.set_data(f'{ROOT}/result', pointer)
            return tx.commit()
        res = zk.retry(publish)
        if isinstance(res[0], Exception):
            current = int(zk.retry(zk.get, f'{ROOT}/epoch')[0])
            log(f'запись отклонена {type(res[0]).__name__} моя_эпоха={epoch} текущая_эпоха={current}')
            s3.delete_object(Bucket=OUT_BUCKET, Key=key)
            log(f'удалил неопубликованный файл s3://{OUT_BUCKET}/{key}')
            finish(zk, on_state)
            return
        stat = res[1]
    else:
        stat = zk.retry(zk.set, f'{ROOT}/result', pointer)
    log(f'записал результат указатель={ROOT}/result версия={stat.version} эпоха={epoch}')
    finish(zk, on_state)


if __name__ == '__main__':
    main()
