import os
import sys
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession, Window, functions as F

EVENT_SCHEMA = 'event_time timestamp,user_id long,user_fio string,user_email string,user_login string'
CAMPAIGN_SCHEMA = 'campaign_id long,email_domain string,send_at timestamp,campaign_name string'
ATTRS = ['user_fio', 'user_email', 'user_login']
META = ['_dag_id', '_run_id', '_task_id', '_data_interval_start', '_data_interval_end', '_loaded_at']
BUCKET = os.getenv('S3_BUCKET', 'hw23')

def session(name):
    jars = '/opt/airflow/s3-jars/'
    s = (SparkSession.builder.master(os.getenv('SPARK_MASTER', 'local[2]')).appName(name)
         .config('spark.driver.memory', '1g').config('spark.executor.memory', '1g')
         .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.shuffle.partitions', '4')
         .config('spark.sql.adaptive.enabled', 'false').config('spark.ui.enabled', 'false')
         .config('spark.ui.showConsoleProgress', 'false')
         .config('spark.jars', jars + 'hadoop-aws-3.3.4.jar,' + jars + 'aws-java-sdk-bundle-1.12.262.jar')
         .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
         .config('spark.hadoop.fs.s3a.endpoint', os.environ['S3_ENDPOINT'])
         .config('spark.hadoop.fs.s3a.endpoint.region', 'us-east-1')
         .config('spark.hadoop.fs.s3a.path.style.access', 'true')
         .config('spark.hadoop.fs.s3a.connection.ssl.enabled', str(os.environ['S3_ENDPOINT'].startswith('https://')).lower())
         .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
         .getOrCreate())
    s.sparkContext.setLogLevel('ERROR')
    return s

def events(s, dataset):
    return s.read.schema(EVENT_SCHEMA).option('header', True).option('mode', 'FAILFAST').csv(f's3a://{BUCKET}/raw/{dataset}/events/*.csv')

def campaigns(s, dataset):
    return s.read.schema(CAMPAIGN_SCHEMA).option('header', True).option('mode', 'FAILFAST').csv(f's3a://{BUCKET}/raw/{dataset}/campaigns.csv')

def build_scd2(source, end):
    w = Window.partitionBy('user_id').orderBy('event_time')
    state = F.struct(*ATTRS)
    history = source.filter(F.col('event_time') < F.lit(end)).dropDuplicates()
    changed = history.withColumn('_previous', F.lag(state).over(w))
    changed = changed.filter(F.col('_previous').isNull() | ~state.eqNullSafe(F.col('_previous')))
    return (changed.withColumn('valid_from', F.col('event_time'))
            .withColumn('valid_to', F.lead('event_time').over(w))
            .withColumn('is_current', F.col('valid_to').isNull())
            .select('user_id', *ATTRS, 'valid_from', 'valid_to', 'is_current'))

def build_recipients(scd, source, start, end):
    u = scd.alias('u')
    c = F.broadcast(source.filter((F.col('send_at') >= F.lit(start)) & (F.col('send_at') < F.lit(end)))).alias('c')
    condition = ((F.lower(F.substring_index(F.col('u.user_email'), '@', -1)) == F.lower(F.col('c.email_domain')))
                 & (F.col('u.valid_from') <= F.col('c.send_at'))
                 & (F.col('u.valid_to').isNull() | (F.col('c.send_at') < F.col('u.valid_to'))))
    return u.join(c, condition, 'inner').select(
        'c.campaign_id', 'c.campaign_name', 'c.send_at', 'c.email_domain',
        'u.user_id', 'u.user_fio', 'u.user_email', 'u.user_login',
        F.col('u.valid_from').alias('user_valid_from'), F.col('u.valid_to').alias('user_valid_to'))

def table_path(root, table, day):
    column = 'snapshot_date' if table == 'user_scd2' else 'calculation_date'
    return f's3a://{BUCKET}/{root}/{table}/{column}={day}'

def add_metadata(df, context):
    values = {
        '_dag_id': context['dag_id'], '_run_id': context['run_id'], '_task_id': context['task_id'],
        '_data_interval_start': context['start'], '_data_interval_end': context['end'],
        '_loaded_at': datetime.now(timezone.utc),
    }
    for key, value in values.items():
        df = df.withColumn(key, F.lit(value))
    return df

def execute(s, stage, context, dataset='demo', root='results/demo'):
    start, end = context['start'], context['end']
    assert end - start == timedelta(days=1)
    day = start.date()
    if stage == 'scd2':
        table, date_col = 'user_scd2', 'snapshot_date'
        df = build_scd2(events(s, dataset), end)
    else:
        table, date_col = 'campaign_recipients', 'calculation_date'
        scd = s.read.parquet(table_path(root, 'user_scd2', day))
        df = build_recipients(scd, campaigns(s, dataset), start, end)
    df = add_metadata(df.withColumn(date_col, F.lit(day)), context)
    path = table_path(root, table, day)
    df.write.mode('overwrite').parquet(path)
    if stage == 'recipients' and dataset == 'demo':
        df.explain(mode='formatted')
    print(f'RESULT task={context["task_id"]} run={context["run_id"]} path={path} rows={s.read.parquet(path).count()}', flush=True)
    return path

if __name__ == '__main__':
    context = {
        'dag_id': os.environ['HW_DAG_ID'], 'run_id': os.environ['HW_RUN_ID'],
        'task_id': os.environ['HW_TASK_ID'],
        'start': datetime.fromisoformat(os.environ['HW_START']),
        'end': datetime.fromisoformat(os.environ['HW_END']),
    }
    spark = session(context['task_id'])
    try:
        execute(spark, sys.argv[1], context)
    finally:
        spark.stop()
