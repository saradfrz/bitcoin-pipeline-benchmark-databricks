"""PySpark transformations; no notebook globals or I/O."""
from pyspark.sql import functions as F, Window

RAW_COLUMNS = ['trade_id','price','quantity','quote_quantity','trade_time_us','is_buyer_maker','is_best_match']


def normalize(raw):
    def num(name, dtype):
        return F.expr(f'try_cast(trim({name}) as {dtype})').alias(name)
    def boolean(name):
        return F.when(F.lower(F.trim(name)) == 'true', True).when(F.lower(F.trim(name)) == 'false', False).alias(name)
    result = raw.select(num('trade_id','bigint'), *[num(n,'decimal(38,8)') for n in ['price','quantity','quote_quantity']],
                        num('trade_time_us','bigint'), boolean('is_buyer_maker'), boolean('is_best_match'),
                        F.trim('symbol').alias('symbol'), 'source_file','ingested_at')
    return result.withColumn('trade_timestamp_utc', F.when(F.col('trade_time_us').between(-62135596800000000,253402300799999999),
                            F.expr('timestamp_micros(trade_time_us)'))).select(
        'trade_id','price','quantity','quote_quantity','trade_time_us','trade_timestamp_utc',
        'is_buyer_maker','is_best_match','symbol','source_file','ingested_at')


def changed_days(trades, summary):
    counts = trades.groupBy('symbol','data').count()
    return counts.alias('t').join(summary.select('symbol','data','quantidade_trades').alias('s'), ['symbol','data'],'left').where(
        F.col('count') != F.coalesce(F.col('quantidade_trades'),F.lit(0))).select('symbol','data')


def summarize(trades):
    opening = Window.partitionBy('symbol','data').orderBy('trade_time_us','trade_id')
    closing = Window.partitionBy('symbol','data').orderBy(F.col('trade_time_us').desc(),F.col('trade_id').desc())
    t = trades.withColumn('_open',F.row_number().over(opening)).withColumn('_close',F.row_number().over(closing))
    result = t.groupBy('symbol','data').agg(
        F.min('trade_timestamp_utc').alias('primeiro_timestamp'), F.max('trade_timestamp_utc').alias('ultimo_timestamp'),
        F.max(F.when(F.col('_open')==1,F.col('trade_id'))).alias('primeiro_trade_id'),
        F.max(F.when(F.col('_close')==1,F.col('trade_id'))).alias('ultimo_trade_id'),
        F.max(F.when(F.col('_open')==1,F.col('price'))).alias('preco_abertura'),
        F.max('price').alias('preco_maximo'), F.min('price').alias('preco_minimo'),
        F.max(F.when(F.col('_close')==1,F.col('price'))).alias('preco_fechamento'),
        F.avg('price').alias('preco_medio_simples'), F.sum('quantity').alias('volume_total_btc'),
        F.sum('quote_quantity').alias('volume_total_usdt'),
        *[F.sum(F.when(F.col('is_buyer_maker')==seller,F.col(col)).otherwise(0)).alias(alias)
          for col, seller, alias in [('quantity',False,'volume_comprador_btc'),('quantity',True,'volume_vendedor_btc'),
              ('quote_quantity',False,'volume_comprador_usdt'),('quote_quantity',True,'volume_vendedor_usdt')]],
        F.count('*').alias('quantidade_trades'),
        F.sum(F.when(F.col('is_buyer_maker')==False,1).otherwise(0)).alias('trades_compradores'),
        F.sum(F.when(F.col('is_buyer_maker')==True,1).otherwise(0)).alias('trades_vendedores'))
    return (result.withColumn('delta_btc',F.col('volume_comprador_btc')-F.col('volume_vendedor_btc'))
        .withColumn('delta_usdt',F.col('volume_comprador_usdt')-F.col('volume_vendedor_usdt'))
        .withColumn('vwap_diario',F.when(F.col('volume_total_btc')!=0,F.col('volume_total_usdt')/F.col('volume_total_btc')))
        .withColumn('desequilibrio_percentual',F.when(F.col('volume_total_btc')!=0,100*F.col('delta_btc')/F.col('volume_total_btc'))))


def sma(daily, previous, n, full=False):
    daily = daily.select('symbol','data','preco_fechamento')
    changed = daily if full else daily.alias('s').join(previous.alias('g'),['symbol','data'],'left').where(
        F.col('g.preco_fechamento').isNull() | ~F.col('s.preco_fechamento').eqNullSafe(F.col('g.preco_fechamento'))).select('symbol','data','s.preco_fechamento')
    affected = daily.alias('d').join(changed.alias('c'),
        (F.col('d.symbol')==F.col('c.symbol')) & F.col('d.data').between(F.col('c.data'),F.date_add(F.col('c.data'),n-1)), 'left_semi')
    joined = affected.alias('a').join(daily.alias('h'),(F.col('a.symbol')==F.col('h.symbol')) &
        F.col('h.data').between(F.date_sub(F.col('a.data'),n-1),F.col('a.data')))
    return joined.groupBy('a.symbol','a.data','a.preco_fechamento').agg(
        F.when(F.count('h.preco_fechamento')==n,F.avg('h.preco_fechamento')).cast('decimal(38,10)').alias(f'sma_{n}d'))


def cvd(daily, previous, full=False):
    daily = daily.select('symbol','data',F.col('delta_btc').cast('decimal(38,8)').alias('delta_btc'))
    if full:
        affected = daily.withColumn('_seed',F.lit(0).cast('decimal(38,8)'))
    else:
        first = daily.alias('s').join(previous.alias('g'),['symbol','data'],'left').where(
            F.col('g.cvd_btc').isNull() | ~F.col('s.delta_btc').eqNullSafe(F.col('g.delta_btc'))
        ).groupBy('symbol').agg(F.min('data').alias('_first'))
        seed = first.alias('f').join(previous.alias('g'),(F.col('f.symbol')==F.col('g.symbol')) & (F.col('g.data')<F.col('_first')),'left')
        seed = seed.withColumn('_rank',F.row_number().over(Window.partitionBy('f.symbol').orderBy(F.col('g.data').desc())))
        seed = seed.where('_rank = 1').select(F.col('f.symbol').alias('symbol'),'_first',F.coalesce('g.cvd_btc',F.lit(0)).alias('_seed'))
        affected = daily.join(seed,'symbol').where(F.col('data')>=F.col('_first'))
    window = Window.partitionBy('symbol').orderBy('data').rowsBetween(Window.unboundedPreceding,Window.currentRow)
    return affected.select('symbol','data','delta_btc',(F.col('_seed')+F.sum('delta_btc').over(window)).cast('decimal(38,8)').alias('cvd_btc'))


def vwap(daily, previous, full=False):
    cols = ['symbol','data','volume_total_btc','volume_total_usdt']
    affected = daily.select(*cols)
    if not full:
        affected = affected.alias('s').join(previous.alias('g'),['symbol','data'],'left').where(
            F.col('g.volume_total_btc').isNull() | ~F.col('s.volume_total_btc').eqNullSafe(F.col('g.volume_total_btc')) |
            ~F.col('s.volume_total_usdt').eqNullSafe(F.col('g.volume_total_usdt'))).select('symbol','data','s.volume_total_btc','s.volume_total_usdt')
    return affected.withColumn('vwap_diario',F.when(F.col('volume_total_btc')!=0,F.col('volume_total_usdt')/F.col('volume_total_btc')).cast('decimal(38,8)'))
