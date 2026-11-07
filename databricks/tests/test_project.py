from pathlib import Path
from datetime import date, timedelta
import sys
import pytest
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'databricks/src'))
from configuration import date_range, landing_scope, load_config
from job_sync import render_settings


def test_dates_and_landing():
    assert date_range('full','','','2026-01-01',date(2026,11,6)) == (date(2026,1,1),date(2026,11,5))
    assert date_range('delta','','','2026-01-01',date(2026,11,6)) == (date(2026,11,5),date(2026,11,5))
    assert date_range('delta','2026-02-01','2026-02-03','2026-01-01',date(2026,11,6))[0]==date(2026,2,1)
    with pytest.raises(ValueError):date_range('delta','2026-11-07','','2026-01-01',date(2026,11,6))
    assert landing_scope([('BTCUSDT-trades-2026-01-01.csv',1),('BTCUSDT-trades-2026-01-02.csv',2)],'BTCUSDT')['hi']=='2026-01-02'
    for files in [[],[('BTCUSDT-trades-2026-02-30.csv',1)], [('BTCUSDT-trades-2026-01-01.csv',0)],
                  [('BTCUSDT-trades-2026-01-01.csv',1)]*2,
                  [('BTCUSDT-trades-2026-01-01.csv',1),('BTCUSDT-trades-2026-01-03.csv',1)]]:
        with pytest.raises(ValueError):landing_scope(files,'BTCUSDT')


def test_yaml_graphs():
    config=load_config(ROOT)
    for key in ['full','delta','ingestion_full','ingestion_delta']:
        settings=render_settings(config,key)
        assert settings['name']==key
        assert settings['max_concurrent_runs']==1
        assert len(settings['tasks'])==(10 if key in ('full','delta') else 1)
        assert 'schedule' not in settings
        if key=='delta':assert len(settings['tasks'][-1]['depends_on'])==4
        if key=='full':assert settings['tasks'][1]['task_key']=='FULL_RESET_AFTER_LANDING_CHECK'


@pytest.fixture(scope='module')
def spark():
    from pyspark.sql import SparkSession
    s=(SparkSession.builder.master('local[2]').appName('binance-tests')
       .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2')
       .config('spark.sql.session.timeZone','UTC').getOrCreate())
    s.sparkContext.setLogLevel('ERROR')
    yield s
    s.stop()


def test_summary_backfill(spark):
    from transforms import normalize, summarize, changed_days
    from pyspark.sql import functions as F
    raw=spark.createDataFrame([
      ('2','12','2','24','1767225600000002','true','true','BTCUSDT','day1'),
      ('1','10','1','10','1767225600000001','false','true','BTCUSDT','day1'),
      ('3','20','3','60','1767312000000001','false','true','BTCUSDT','day2')],
      'trade_id string, price string, quantity string, quote_quantity string, trade_time_us string, is_buyer_maker string, is_best_match string, symbol string, source_file string').withColumn('ingested_at',F.current_timestamp())
    trades=normalize(raw).withColumn('data',F.to_date('trade_timestamp_utc'))
    summary=summarize(trades).cache()
    rows=summary.orderBy('data').collect()
    assert len(rows)==2
    assert rows[0].preco_abertura==10 and rows[0].preco_fechamento==12
    assert rows[0].quantidade_trades==2 and rows[0].delta_btc==-1
    assert changed_days(trades,summary).count()==0
    missing=summary.where(F.col('data')!=date(2026,1,1))
    days=changed_days(trades,missing)
    assert days.first().data==date(2026,1,1)
    rebuilt=summarize(trades.join(days,['symbol','data']))
    assert rebuilt.first().asDict()==rows[0].asDict()
    # A new trade on an existing date must also flag the day.
    assert changed_days(trades.unionByName(trades.where("trade_id=1")),summary).count()==1
    summary.unpersist()


def test_gold_incremental_matches_full(spark):
    from transforms import sma, cvd, vwap
    from pyspark.sql import functions as F
    daily=spark.createDataFrame([('BTCUSDT',date(2026,1,1)+timedelta(days=i),float(i+1),float((-1)**i),2.,float(2*(i+1))) for i in range(205)],
       'symbol string, data date, preco_fechamento double, delta_btc double, volume_total_btc double, volume_total_usdt double')
    d=date(2026,2,1)
    changed=daily.withColumn('preco_fechamento',F.when(F.col('data')==d,999.).otherwise(F.col('preco_fechamento'))).withColumn(
       'delta_btc',F.when(F.col('data')==d,5.).otherwise(F.col('delta_btc'))).withColumn(
       'volume_total_usdt',F.when(F.col('data')==d,100.).otherwise(F.col('volume_total_usdt')))
    for model in [lambda a,b,f:sma(a,b,50,f),lambda a,b,f:sma(a,b,200,f),cvd,vwap]:
        old=model(daily,None,True).cache();old.count()
        incremental=model(changed,old,False)
        rows=incremental.collect()
        actual={r.data:r.asDict() for r in rows}
        expected={r.data:r.asDict() for r in model(changed,None,True).collect()}
        assert actual and all(v==expected[k] for k,v in actual.items())
        combined={r.data:r.asDict() for r in old.collect()};combined.update(actual)
        assert combined==expected
        hole=old.where(F.col('data')!=d)
        assert any(r.data==d for r in model(daily,hole,False).collect())
        old.unpersist()


def test_sync_preserves_ids_and_sends_complete_settings():
    from types import SimpleNamespace
    from job_sync import sync_jobs
    config=load_config(ROOT)
    calls=[]
    class Jobs:
        def list(self,name):
            return [SimpleNamespace(job_id=42,settings=SimpleNamespace(name=name,tags={'project':'di-pipe-eng'}))] if name=='full' else []
        def list_runs(self,**kw):return []
    class Api:
        def do(self,method,path,body):
            calls.append((path,body));return {'job_id':100+len(calls)}
    client=SimpleNamespace(jobs=Jobs(),api_client=Api(),config=SimpleNamespace(host='https://example.invalid'))
    result=sync_jobs(config,client)
    assert result['full']==42
    assert calls[0][0]=='/api/2.2/jobs/reset'
    assert len(calls[0][1]['new_settings']['tasks'])==10
    assert len([p for p,b in calls if p.endswith('/create')])==3


def test_sync_validates_every_target_before_mutation():
    from types import SimpleNamespace
    from job_sync import sync_jobs
    config=load_config(ROOT)
    class Jobs:
        def list(self,name):
            if name=='delta':return [SimpleNamespace(job_id=1,settings=SimpleNamespace(name=name,tags={}))]
            return []
    client=SimpleNamespace(jobs=Jobs())
    with pytest.raises(ValueError,match='not tagged'):
        sync_jobs(config,client)
