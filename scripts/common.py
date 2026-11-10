"""Подключение к RustFS и ZooKeeper, условие варианта и разбор навыков из ДЗ 3."""
import re
from pathlib import Path

import boto3
from botocore.config import Config

LAB = Path(__file__).resolve().parents[1]
S3_ENDPOINT = 'http://127.0.0.1:9010'
INPUT_BUCKET = 'hh-lake'
RAW_KEY = 'raw/it_vacancies_2024.csv'
HW3_PAIRS_KEY = 'results/mapreduce/skills_k2.csv'
OUT_BUCKET = 'zk-lab'
ZK_HOSTS = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
ROOT = '/lab4'
CHUNK = 32 * 1024 * 1024

COLUMNS = ['source', 'vacancy_id', 'status', 'snapshot_date', 'published_at', 'area',
           'employer_id', 'employer_inn', 'employer_ogrn', 'employer_name', 'name', 'role',
           'sphere', 'salary_from', 'salary_to', 'currency', 'gross', 'experience', 'schedule',
           'employment', 'key_skills', 'description', 'url']
COL = {c: i for i, c in enumerate(COLUMNS)}

# Вариант 9, то же условие, что в ДЗ 3
VARIANT_ROLE = 'Программист, разработчик'
_B = '[^a-zа-яё0-9]'
VARIANT_NAME_RE = (f'(^|{_B})(android|андроид|мобильн|flutter|флаттер|xamarin|swift|react.native|'
                   f'objective.c|kotlin multiplatform)|(^|{_B})(ios|айос|mobile|ionic|kmp)({_B}|$)')
_name_re = re.compile(VARIANT_NAME_RE)


def env():
    return dict(line.split('=', 1) for line in (LAB / '.env').read_text().splitlines() if '=' in line)


def s3_client():
    e = env()
    return boto3.client(
        's3', endpoint_url=S3_ENDPOINT, region_name='us-east-1',
        aws_access_key_id=e['RUSTFS_ACCESS_KEY'], aws_secret_access_key=e['RUSTFS_SECRET_KEY'],
        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'},
                      retries={'max_attempts': 3}, read_timeout=20))


def in_variant(row):
    return row[COL['role']] == VARIANT_ROLE and _name_re.search(row[COL['name']].lower()) is not None


def skills_of(row):
    """Навыки через точку с запятой, trim, нижний регистр, без повторов, по возрастанию."""
    raw = row[COL['key_skills']]
    if not raw:
        return []
    return sorted({s.strip(' ').lower() for s in raw.split(';')} - {''})
