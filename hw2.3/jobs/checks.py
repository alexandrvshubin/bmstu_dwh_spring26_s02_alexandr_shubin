import argparse
import json
from collections import Counter
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
from airflow.models import DagRun, TaskInstance
from airflow.utils.session import create_session
from pyspark.sql import Window, functions as F
from pipeline import ATTRS, META, BUCKET, session, events, campaigns, build_recipients, table_path, execute

DAG = 'hw23_scd2_campaigns'
DAYS = ['2026-01-23', '2026-01-24', '2026-01-25']
TABLES = ['user_scd2', 'campaign_recipients']
TASKS = ['build_user_scd2', 'build_campaign_recipients']
ROOT = 'results/demo'

def client():
    import os
    return boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT'])

def airflow_runs():
    result = []
    with create_session() as db:
        runs = db.query(DagRun).filter(DagRun.dag_id == DAG).order_by(DagRun.execution_date).all()
        assert len(runs) == 3
        for run, day in zip(runs, DAYS):
            assert run.state == 'success' and run.run_type == 'scheduled'
            assert run.data_interval_start.date().isoformat() == day
            assert run.data_interval_end - run.data_interval_start == timedelta(days=1)
            item = {'run_id': run.run_id, 'type': run.run_type, 'state': run.state,
                    'start': run.data_interval_start.isoformat(), 'end': run.data_interval_end.isoformat(), 'tasks': {}}
            for task_id in TASKS:
                task = db.query(TaskInstance).filter_by(dag_id=DAG, run_id=run.run_id, task_id=task_id).one()
                assert task.state == 'success'
                item['tasks'][task_id] = {'state': task.state, 'try_number': task.try_number,
                                          'start': task.start_date.isoformat(), 'end': task.end_date.isoformat()}
            assert item['tasks'][TASKS[1]]['start'] >= item['tasks'][TASKS[0]]['end']
            result.append(item)
    return result

def same(a, b):
    return not a.exceptAll(b.select(a.columns)).limit(1).count() and not b.select(a.columns).exceptAll(a).limit(1).count()

def save_before(s):
    runs = airflow_runs()
    for day in DAYS:
        for table in TABLES:
            df = s.read.parquet(table_path(ROOT, table, day))
            df.write.mode('overwrite').parquet(table_path('checks/before', table, day))
    client().put_object(Bucket=BUCKET, Key='checks/before/airflow.json', Body=json.dumps(runs).encode())
    print('Сохранены копии всех шести партиций и состояние Airflow до повтора.', flush=True)

def expected_history(source, end):
    history = {}
    for e in sorted(source, key=lambda x: (x.user_id, x.event_time)):
        if e.event_time >= end:
            continue
        versions = history.setdefault(e.user_id, [])
        state = tuple(e[k] for k in ATTRS)
        if not versions or versions[-1][1] != state:
            versions.append((e.event_time, state))
    rows = []
    for uid, versions in history.items():
        for i, (start, state) in enumerate(versions):
            end_time = versions[i + 1][0] if i + 1 < len(versions) else None
            rows.append((uid, *state, start, end_time, end_time is None))
    return rows

def verify_day(s, day, source, campaign_rows, run):
    start = datetime.fromisoformat(day)
    end = start + timedelta(days=1)
    scd = s.read.parquet(table_path(ROOT, 'user_scd2', day))
    rec = s.read.parquet(table_path(ROOT, 'campaign_recipients', day))
    history = expected_history(source, end)
    business = ['user_id', *ATTRS, 'valid_from', 'valid_to', 'is_current']
    assert Counter(tuple(r) for r in scd.select(business).collect()) == Counter(history)
    assert not scd.groupBy('user_id', 'valid_from').count().filter('count > 1').count()
    current = scd.groupBy('user_id').agg(F.sum(F.col('is_current').cast('int')).alias('n'))
    assert not current.filter('n != 1').count()
    w = Window.partitionBy('user_id').orderBy('valid_from')
    ordered = scd.withColumn('next_start', F.lead('valid_from').over(w)).withColumn('previous_state', F.lag(F.struct(*ATTRS)).over(w))
    assert not ordered.filter(~F.col('valid_to').eqNullSafe(F.col('next_start'))).count()
    assert not ordered.filter(F.col('previous_state').eqNullSafe(F.struct(*ATTRS))).count()
    assert not scd.filter((F.col('valid_from') >= F.lit(end)) | (F.col('valid_to') <= F.col('valid_from'))).count()
    expected = []
    for c in campaign_rows:
        if start <= c.send_at < end:
            for uid, fio, email, login, valid_from, valid_to, _ in history:
                if email.rsplit('@', 1)[-1].lower() == c.email_domain.lower() and valid_from <= c.send_at and (valid_to is None or c.send_at < valid_to):
                    expected.append((c.campaign_id, c.campaign_name, c.send_at, c.email_domain, uid, fio, email, login, valid_from, valid_to))
    cols = ['campaign_id', 'campaign_name', 'send_at', 'email_domain', 'user_id', *ATTRS, 'user_valid_from', 'user_valid_to']
    assert Counter(tuple(r) for r in rec.select(cols).collect()) == Counter(expected)
    assert not rec.groupBy('campaign_id', 'user_id').count().filter('count > 1').count()
    print(f'{day}: SCD2={scd.count()}, пользователей={scd.select("user_id").distinct().count()}, получателей={rec.count()}; история, ключи и интервалы OK.')
    rec.groupBy('campaign_id').agg(F.count('*').alias('rows'), F.countDistinct('user_id').alias('users')).orderBy('campaign_id').show(truncate=False)
    for table, df, task in [('user_scd2', scd, TASKS[0]), ('campaign_recipients', rec, TASKS[1])]:
        date_col = 'snapshot_date' if table == 'user_scd2' else 'calculation_date'
        allowed_null = 'valid_to' if table == 'user_scd2' else 'user_valid_to'
        expected_types = {k: 'string' for k in df.columns}
        for k in ['user_id', 'campaign_id']:
            if k in expected_types: expected_types[k] = 'bigint'
        for k in ['valid_from', 'valid_to', 'send_at', 'user_valid_from', 'user_valid_to', *META[3:]]:
            if k in expected_types: expected_types[k] = 'timestamp'
        expected_types[date_col] = 'date'
        if table == 'user_scd2': expected_types['is_current'] = 'boolean'
        assert dict(df.dtypes) == expected_types
        assert not df.filter(' OR '.join(f'`{k}` IS NULL' for k in df.columns if k != allowed_null)).count()
        assert not df.filter(F.col(date_col) != F.lit(start.date())).count()
        meta = df.select(META).distinct().collect()
        assert len(meta) == 1
        m = meta[0]
        assert (m._dag_id, m._run_id, m._task_id) == (DAG, run['run_id'], task)
        assert (m._data_interval_start, m._data_interval_end) == (start, end)
        task_info = run['tasks'][task]
        assert datetime.fromisoformat(task_info['start']).replace(tzinfo=None) <= m._loaded_at <= datetime.fromisoformat(task_info['end']).replace(tzinfo=None)
        print(f'{table}: _dag_id={m._dag_id}; _run_id={m._run_id}; _task_id={m._task_id}; interval=[{start}, {end}); rows={df.count()}; min/max(_loaded_at)={m._loaded_at}; NULL метаполей=0.')

def verify_repeat(s, before, after):
    for day in DAYS:
        for table in TABLES:
            old = s.read.parquet(table_path('checks/before', table, day))
            new = s.read.parquet(table_path(ROOT, table, day))
            cols = [c for c in old.columns if c not in META] if day == DAYS[0] else old.columns
            assert same(old.select(cols), new.select(cols))
            if day == DAYS[0]:
                a, b = old.select(META).first(), new.select(META).first()
                assert a._run_id == b._run_id and b._loaded_at > a._loaded_at
            print(f'Повтор: {day} {table}, EXCEPT ALL в обе стороны=0; сравнение ' + ('всех бизнес-полей и даты.' if day == DAYS[0] else 'всех полей, включая метаполя.'))
    for old, new in zip(before, after):
        print(f'Airflow {new["run_id"]}, type={new["type"]}, state={new["state"]}, interval=[{new["start"]}, {new["end"]})')
        for task in TASKS:
            a, b = old['tasks'][task], new['tasks'][task]
            if old['run_id'] == before[0]['run_id']:
                assert b['start'] > a['end']
            else:
                assert a == b
            print(f'  {task}: до={a}; после={b}')

def fixtures(s):
    expected = {101: [1001], 102: [1002], 103: [1001], 104: [], 105: [1001, 1002],
                106: [1003], 107: [], 108: [1002], 109: [1001], 110: [1004],
                111: [1004, 1005], 112: [1001], 113: [], 114: [], 115: [1001], 116: [1001]}
    found = {}
    source = events(s, 'fixtures').collect()
    for day in ['2026-01-01', '2026-01-22', *DAYS, '2026-01-26', '2026-01-27']:
        start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
        ctx = {'dag_id': 'fixture_check', 'run_id': day, 'start': start, 'end': start + timedelta(days=1), 'task_id': TASKS[0]}
        sp = execute(s, 'scd2', ctx, 'fixtures', 'checks/fixtures')
        ctx['task_id'] = TASKS[1]
        rp = execute(s, 'recipients', ctx, 'fixtures', 'checks/fixtures')
        h, r = s.read.parquet(sp), s.read.parquet(rp)
        assert Counter(tuple(x) for x in h.select('user_id', *ATTRS, 'valid_from', 'valid_to', 'is_current').collect()) == Counter(expected_history(source, ctx['end'].replace(tzinfo=None)))
        for row in r.collect():
            found.setdefault(row.campaign_id, []).append(row.user_id)
        if day == '2026-01-25':
            assert h.filter('user_id = 1001').count() == 5
            assert r.filter('campaign_id = 115').first().user_login == 'alice_new'
            assert r.filter('campaign_id = 116').first().user_login == 'alice'
            assert not h.filter('user_id = 1005').count()
            assert r.filter('campaign_id = 115').first().email_domain == 'MAIL.EXAMPLE'
        if day in ['2026-01-01', '2026-01-27']:
            assert r.count() == 0 and len(r.columns) == 17
            s.createDataFrame([(1,)], 'stale int').write.mode('overwrite').parquet(rp)
            execute(s, 'recipients', ctx, 'fixtures', 'checks/fixtures')
            empty = s.read.parquet(rp)
            assert empty.count() == 0 and empty.schema == r.schema
            print(f'Пустая витрина {day}: читаемый Parquet, полная схема, прежние строки удалены.')
        if day == '2026-01-01':
            assert h.count() == 0 and len(h.columns) == 14
            s.createDataFrame([(1,)], 'stale int').write.mode('overwrite').parquet(sp)
            ctx['task_id'] = TASKS[0]
            execute(s, 'scd2', ctx, 'fixtures', 'checks/fixtures')
            empty = s.read.parquet(sp)
            assert empty.count() == 0 and empty.schema == h.schema
            print('Пустая SCD2: читаемый Parquet, полная схема, прежние строки удалены.')
    assert {k: sorted(found.get(k, [])) for k in expected} == expected
    print('Fixtures: campaign_id -> ожидаемые и фактические user_id')
    for key, value in expected.items(): print(key, value, sorted(found.get(key, [])))
    print('Проверены смена домена, valid_from/valid_to, дата до первого события, регистр, неизменное состояние, A→B→A, смена ФИО и логина, границы суток.')

def export_results():
    for day in DAYS:
        for table in TABLES:
            column = 'snapshot_date' if table == 'user_scd2' else 'calculation_date'
            directory = Path('/opt/airflow/results') / table / f'{column}={day}'
            for old_file in directory.glob('*.parquet'):
                old_file.unlink()
    c = client()
    for page in c.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=ROOT + '/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                target = Path('/opt/airflow/results') / obj['Key'][len(ROOT) + 1:]
                target.parent.mkdir(parents=True, exist_ok=True)
                c.download_file(BUCKET, obj['Key'], str(target))

def verify(s):
    print('ДЗ 2.3. Данные demo, 6000 событий, 100 пользователей. Все времена UTC.')
    print('Airflow 2.10.5; Spark ' + s.version + '; Python 3.11; Java 17; Hadoop 3.3.4; RustFS 1.0.4; local[2], driver/executor 1 GiB.')
    print(f'Входы s3://{BUCKET}/raw/demo/; выходы s3://{BUCKET}/{ROOT}/. Выгрузка Parquet приложена в results/.')
    runs = airflow_runs()
    before = json.loads(client().get_object(Bucket=BUCKET, Key='checks/before/airflow.json')['Body'].read())
    src, camps = events(s, 'demo'), campaigns(s, 'demo')
    source, campaign_rows = src.collect(), camps.collect()
    assert len(source) == 6000 and len({r.user_id for r in source}) == 100
    print('Диапазон event_time:', min(r.event_time for r in source), max(r.event_time for r in source))
    for day, run in zip(DAYS, runs): verify_day(s, day, source, campaign_rows, run)
    verify_repeat(s, before, runs)
    print('План JOIN: BroadcastHashJoin. Справочник — 25 строк до фильтрации; broadcast позволяет не перераспределять SCD2 по пяти доменам. AQE отключён, поэтому физический оператор не меняется при выполнении.')
    start = datetime(2026, 1, 23)
    build_recipients(s.read.parquet(table_path(ROOT, TABLES[0], DAYS[0])), camps, start, start + timedelta(days=1)).explain()
    fixtures(s)
    export_results()
    print('ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--phase', choices=['before', 'after'], default='after')
    args = parser.parse_args()
    spark = session('hw23-checks')
    try:
        if args.phase == 'before':
            save_before(spark)
        else:
            Path('/opt/airflow/results').mkdir(exist_ok=True)
            with open('/opt/airflow/results/Проверки.txt', 'w') as output, redirect_stdout(output):
                verify(spark)
            print('ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ. results/Проверки.txt', flush=True)
    finally:
        spark.stop()
