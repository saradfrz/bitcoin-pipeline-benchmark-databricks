"""Delta Lake I/O and orchestration operations."""
from datetime import date, datetime, timedelta, timezone
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from configuration import landing_scope
from transforms import RAW_COLUMNS, normalize, changed_days, summarize, sma, cvd, vwap


class Pipeline:
    def __init__(self, spark, dbutils, config):
        self.spark, self.dbutils, self.c = spark, dbutils, config
        self.t = config['qualified_tables']
        spark.conf.set('spark.sql.session.timeZone','UTC')

    def table(self, key):
        return self.spark.table(self.t[key])

    def setup_namespaces(self):
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.c['catalog']}")
        for schema in self.c['schemas'].values():
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.c['catalog']}.{schema}")
        self.spark.sql(f"CREATE VOLUME IF NOT EXISTS {self.c['catalog']}.{self.c['schemas']['bronze']}.{self.c['volume_name']}")
        self.dbutils.fs.mkdirs(self.c['landing'])
        self.dbutils.fs.mkdirs(self.c['volume']+'/zipped')

    def setup_bronze(self):
        columns = ', '.join(f'{name} STRING' for name in RAW_COLUMNS)
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.t['bronze']} ({columns}, symbol STRING, source_file STRING, ingested_at TIMESTAMP) USING DELTA")

    def setup_silver(self):
        empty = normalize(self.table('bronze').limit(0))
        empty.write.format('delta').mode('ignore').saveAsTable(self.t['trades'])
        summarize(empty.withColumn('data',F.to_date('trade_timestamp_utc'))).write.format('delta').mode('ignore').saveAsTable(self.t['summary'])
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.t['baseline']} (
            symbol STRING, load_mode STRING, lo DATE, hi DATE, summary_hi DATE,
            validated_at TIMESTAMP, job_run_id STRING) USING DELTA""")

    def setup_gold(self):
        daily = self.table('summary').limit(0)
        for key, df in [('sma_50d',sma(daily,None,50,True)),('sma_200d',sma(daily,None,200,True)),
                        ('cvd',cvd(daily,None,True)),('vwap',vwap(daily,None,True))]:
            df.write.format('delta').mode('ignore').saveAsTable(self.t[key])

    def setup(self):
        self.setup_namespaces()
        self.setup_bronze()
        self.setup_silver()
        self.setup_gold()

    def landing_check(self, mode):
        files = [(f.name,f.size) for f in self.dbutils.fs.ls(self.c['landing']) if not f.isDir()]
        scope = landing_scope(files,self.c['symbol'])
        scope['summary_hi'] = ((datetime.now(timezone.utc).date()-timedelta(days=1)).isoformat()
                               if mode=='delta' else scope['hi'])
        if scope['summary_hi'] < scope['lo']:
            raise ValueError('INVALID_SCOPE')
        self.dbutils.jobs.taskValues.set(key='scope',value=scope)
        print(scope)
        return scope

    def reset_full(self, scope):
        if not scope or not scope.get('lo') or not scope.get('hi'):
            raise ValueError('FULL reset requires successful landing validation task output.')
        # A retry is safe; only run in the full graph, before Bronze.
        for key, table in self.t.items():
            if key != 'baseline':
                self.spark.sql(f'TRUNCATE TABLE {table}')
        # The full load reuses Auto Loader so the next delta has a valid checkpoint.
        self.dbutils.fs.rm(self.c['checkpoint'],True)

    def bronze(self):
        schema = ', '.join(f'{name} STRING' for name in RAW_COLUMNS)
        source = (self.spark.readStream.format('cloudFiles').option('cloudFiles.format','csv')
            .option('header','false').option('mode','FAILFAST')
            .option('cloudFiles.includeExistingFiles','true')
            .option('cloudFiles.maxFilesPerTrigger',self.c['max_files_per_trigger'])
            .option('pathGlobFilter',self.c['symbol']+'-trades-*.csv').schema(schema).load(self.c['landing'])
            .select(*RAW_COLUMNS,F.lit(self.c['symbol']).alias('symbol'),F.col('_metadata.file_path').alias('source_file'),
                    F.current_timestamp().alias('ingested_at')))
        query = (source.writeStream.format('delta').outputMode('append')
            .option('checkpointLocation',self.c['checkpoint']).trigger(availableNow=True).toTable(self.t['bronze']))
        query.awaitTermination()

    def silver_trades(self):
        # Match Snowflake's file-level append semantics, including its limitations.
        unseen = self.table('bronze').join(self.table('trades').select('source_file').distinct(),'source_file','left_anti')
        self.aligned(normalize(unseen),'trades').write.format('delta').mode('append').saveAsTable(self.t['trades'])

    def aligned(self, frame, key):
        return frame.select(*[F.col(field.name).cast(field.dataType).alias(field.name) for field in self.table(key).schema])

    def merge(self, frame, key, full=False):
        frame = self.aligned(frame,key)
        if full:
            # All Gold models rebuild the full daily result, including CVD/VWAP.
            frame.write.format('delta').mode('overwrite').saveAsTable(self.t[key])
        else:
            (DeltaTable.forName(self.spark,self.t[key]).alias('t').merge(frame.alias('s'),
                't.symbol = s.symbol AND t.data = s.data').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

    def silver_summary(self):
        trades = self.table('trades').where((F.col('symbol')==self.c['symbol']) & F.col('trade_timestamp_utc').isNotNull()).withColumn('data',F.to_date('trade_timestamp_utc'))
        days = changed_days(trades,self.table('summary'))
        self.merge(summarize(trades.join(days,['symbol','data'],'inner')),'summary')

    def dq_after_summary(self, scope, mode, job_run_id):
        lo, hi = date.fromisoformat(scope['lo']), date.fromisoformat(scope['summary_hi'])
        if lo > hi:
            raise ValueError('INVALID_SCOPE')
        row = self.table('summary').where((F.col('symbol')==self.c['symbol']) & F.col('data').between(F.lit(lo),F.lit(hi))).agg(
            F.count('*').alias('rows'),F.countDistinct('data').alias('days')).first()
        if row.rows != row.days or row.days != (hi-lo).days+1:
            raise ValueError(f'BAD_SUMMARY: expected one row per day for {lo}..{hi}; got {row}')
        # Audit of a successful summary check, never an upstream prerequisite.
        baseline = self.spark.createDataFrame([(self.c['symbol'],mode,lo,date.fromisoformat(scope['hi']),hi,job_run_id)],
            'symbol string, load_mode string, lo date, hi date, summary_hi date, job_run_id string').withColumn('validated_at',F.current_timestamp())
        (DeltaTable.forName(self.spark,self.t['baseline']).alias('t').merge(baseline.alias('s'),
            't.symbol=s.symbol AND t.load_mode=s.load_mode').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        print('PASS')

    def gold(self, key, mode):
        daily = self.table('summary').where(F.col('symbol')==self.c['symbol'])
        previous, full = self.table(key), mode=='full'
        result = (sma(daily,previous,int(key.split('_')[1][:-1]),full) if key.startswith('sma_') else
                  cvd(daily,previous,full) if key=='cvd' else vwap(daily,previous,full))
        self.merge(result,key,full)

    def prune(self):
        cutoff = datetime.now(timezone.utc).date()-timedelta(days=self.c['retention_days'])
        # Remove expired input files first, chronologically. Keep checkpoint metadata.
        for folder, suffix in [('unzipped','.csv'),('zipped','.zip')]:
            import re
            pattern = re.compile(rf"{self.c['symbol']}-trades-(\d{{4}}-\d{{2}}-\d{{2}}){re.escape(suffix)}")
            for item in sorted(self.dbutils.fs.ls(self.c['volume']+'/'+folder),key=lambda f:f.name):
                match = pattern.fullmatch(item.name)
                if match and date.fromisoformat(match[1]) < cutoff:
                    self.dbutils.fs.rm(item.path)
        cutoff_us = int((datetime.combine(cutoff,datetime.min.time(),tzinfo=timezone.utc)-datetime(1970,1,1,tzinfo=timezone.utc)).total_seconds()*1_000_000)
        for key in self.t:
            if key=='baseline':
                continue
            condition = (F.expr(f'try_cast(trim(trade_time_us) as bigint) < {cutoff_us}') if key=='bronze' else
                         F.col('trade_timestamp_utc')<F.lit(cutoff).cast('timestamp') if key=='trades' else F.col('data')<F.lit(cutoff))
            DeltaTable.forName(self.spark,self.t[key]).delete((F.col('symbol')==self.c['symbol']) & condition)
        # Separate Delta table commits: rerun this task after a partial failure.
        print(f'Pruned dates strictly before {cutoff}; no VACUUM performed.')
