"""Databricks equivalents of Snowflake data.sql and costs.sql."""
from datetime import datetime
from functools import reduce
from pyspark.sql import functions as F


def data_report(pipeline, display):
    for key in pipeline.t:
        frame=pipeline.table(key)
        if key=='bronze':
            order=[F.expr('try_cast(trade_time_us as bigint)'),F.expr('try_cast(trade_id as bigint)')]
        elif key=='trades':order=['trade_timestamp_utc','trade_id']
        elif key=='baseline':order=['symbol','load_mode']
        else:order=[F.col('data').desc(),'symbol']
        if key.startswith('sma_'):frame=frame.where(F.col(key).isNotNull())
        print(pipeline.t[key]);display(frame.orderBy(*order).limit(10))
    # Exact counts intentionally scan all tables, as in the original data.sql.
    display(reduce(lambda a,b:a.unionByName(b),[
        pipeline.table(k).agg(F.count('*').alias('total_rows')).withColumn('table_name',F.lit(t)).select('table_name','total_rows')
        for k,t in pipeline.t.items()]))


def cost_report(pipeline, display, start, end, currency):
    spark, c = pipeline.spark, pipeline.c
    lo=datetime.fromisoformat(start.replace('Z','+00:00'))
    hi=datetime.fromisoformat(end.replace('Z','+00:00'))
    if lo.tzinfo is None or hi.tzinfo is None or lo>=hi:
        raise ValueError('Cost window requires timezone-aware ISO timestamps, start < end.')
    rows=[]
    for key,table in pipeline.t.items():
        d=spark.sql(f'DESCRIBE DETAIL {table}').first().asDict()
        rows.append((table,int(d['numFiles']),int(d['sizeInBytes'])))
    display(spark.createDataFrame(rows,'table_name string, active_files long, active_bytes long'))
    print('Active Delta snapshot sizes only: excludes obsolete files, time travel and provider storage charges.')
    if c['evaluation']['include_exact_counts']:
        display(reduce(lambda a,b:a.unionByName(b),[
            pipeline.table(k).agg(F.count('*').alias('rows')).withColumn('table_name',F.lit(t)) for k,t in pipeline.t.items()]))
    def walk(path):
        for item in pipeline.dbutils.fs.ls(path):
            if item.isDir():yield from walk(item.path)
            else:yield item
    files=list(walk(c['volume']))
    display(spark.createDataFrame([(len(files),sum(f.size for f in files))],'volume_file_count long, volume_bytes long'))
    display(pipeline.table('summary').groupBy('symbol').agg(F.min('data').alias('first_trading_date'),
        F.max('data').alias('last_trading_date'),F.count('*').alias('summary_rows'),F.countDistinct('data').alias('trading_days')))
    from databricks.sdk import WorkspaceClient
    from job_sync import JOB_KEYS, render_settings, resolve_job
    client=WorkspaceClient()
    ids=[str(j) for k in JOB_KEYS if (j:=resolve_job(client,c,k,render_settings(c,k))) is not None]
    if not ids:
        print('No deployed jobs resolved. Deploy first for billing and runtime reports.');return
    def optional(label, action):
        try:
            print(label);action()
        except Exception as error:
            # Missing system tables/permissions are common in Free Edition.
            # Always report the error; do not turn unavailable costs into zero.
            print(f'UNAVAILABLE: {label}: {type(error).__name__}: {error}')
    workspace_id = c['evaluation'].get('workspace_id')
    if not workspace_id:
        try:
            workspace_id = str(client.get_workspace_id())
        except Exception as error:
            print(f'Workspace ID unavailable: {error}. Set databricks.evaluation.workspace_id in config.json.');return
    def billing():
        usage=spark.table('system.billing.usage').where(
            (F.col('usage_start_time')>=F.lit(lo)) & (F.col('usage_start_time')<F.lit(hi)) &
            F.col('usage_metadata.job_id').isin(ids) & (F.col('workspace_id')==str(workspace_id))).alias('u')
        prices=spark.table('system.billing.list_prices').where(F.col('currency_code')==currency).alias('p')
        priced=usage.join(prices,(F.col('u.sku_name')==F.col('p.sku_name')) &
            (F.col('u.cloud')==F.col('p.cloud')) & (F.col('u.usage_unit')==F.col('p.usage_unit')) &
            (F.col('u.usage_start_time')>=F.col('p.price_start_time')) &
            (F.col('p.price_end_time').isNull() | (F.col('u.usage_end_time')<=F.col('p.price_end_time'))),'left').select(
                'u.workspace_id',F.col('u.usage_metadata.job_id').alias('job_id'),
                F.col('u.usage_metadata.job_run_id').alias('job_run_id'),'u.usage_date','u.sku_name','u.usage_unit',
                'u.usage_quantity',(F.col('u.usage_quantity')*F.col('p.pricing.effective_list.default')).alias('list_cost'),
                F.col('p.pricing.effective_list.default').isNull().cast('int').alias('unpriced_records'))
        # Keep retractions/restatements: signed quantities reconcile corrected billing.
        display(priced.groupBy('workspace_id','job_id','job_run_id','usage_unit').agg(
            F.sum('usage_quantity').alias('usage_quantity'),F.sum('list_cost').alias('priced_list_cost'),
            F.sum('unpriced_records').alias('unpriced_records')))
        display(priced.groupBy('usage_date','sku_name','usage_unit').agg(
            F.sum('usage_quantity').alias('usage_quantity'),F.sum('list_cost').alias('priced_list_cost'),
            F.sum('unpriced_records').alias('unpriced_records')))
        print(f'List-price estimate in {currency}; not invoice/net negotiated cost. Unpriced records mean incomplete totals. '
              'Window selects usage_start_time; use whole-hour boundaries. Daily and run totals overlap: do not add them. '
              'Shared/all-purpose compute without job attribution, cloud infrastructure and storage are excluded.')
    optional('Job usage and list-price estimate',billing)
    def timelines(table, task=False):
        frame=spark.table(table).where((F.col('workspace_id')==str(workspace_id)) & F.col('job_id').cast('string').isin(ids) &
            (F.col('period_start_time')<F.lit(hi)) & (F.col('period_end_time')>F.lit(lo)))
        group=['workspace_id','job_id','run_id']+(['job_run_id','task_key'] if task else [])
        display(frame.groupBy(*group).agg(F.min('period_start_time').alias('run_first_seen'),
            F.max('period_end_time').alias('run_last_seen'),
            F.sum(F.greatest(F.lit(0),F.least(F.col('period_end_time'),F.lit(hi)).cast('double')-
                  F.greatest(F.col('period_start_time'),F.lit(lo)).cast('double'))).alias('seconds_in_window'),
            F.collect_set('result_state').alias('result_states')))
    optional('Job run timeline',lambda:timelines('system.lakeflow.job_run_timeline'))
    optional('Task run timeline',lambda:timelines('system.lakeflow.job_task_run_timeline',True))
