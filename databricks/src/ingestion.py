"""Widget-driven API archive ingestion, shared by both ingestion notebooks."""
from configuration import date_range
from binance_downloader import BinanceTradeDownloader


def run_ingestion(dbutils, config, mode):
    lo, hi = date_range(mode, '', '', config['ingestion']['full_start_date'])
    # Job parameters use blank defaults; dates resolve at RUN time, not deployment.
    dbutils.widgets.text('start_date',lo.isoformat(),'Start date (YYYY-MM-DD)')
    dbutils.widgets.text('end_date',hi.isoformat(),'End date (YYYY-MM-DD)')
    lo, hi = date_range(mode,dbutils.widgets.get('start_date').strip(),dbutils.widgets.get('end_date').strip(),config['ingestion']['full_start_date'])
    if lo.year < 2025:
        raise ValueError('This microsecond timestamp contract supports Binance Spot archives from 2025 onward.')
    options = config['ingestion']
    downloader = BinanceTradeDownloader(start_date=lo.strftime('%d/%m/%Y'),end_date=hi.strftime('%d/%m/%Y'),
        volume=config['volume'],symbol=config['symbol'],sleep_seconds=options['sleep_seconds'],
        max_retries=options['max_retries'],timeout_seconds=options['timeout_seconds'])
    files = downloader.run()
    expected = (hi-lo).days+1
    if len(files) != expected:
        raise RuntimeError(f'INCOMPLETE_INGESTION: expected {expected} daily CSVs, found {len(files)}; retry after unavailable archives are published.')
    print(f'Available: {expected} CSV files, {lo} through {hi}. Run the processing job separately.')
