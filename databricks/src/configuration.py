"""Configuration shared by notebook entry points and importable modules."""
import json
import re
from pathlib import Path
from datetime import date, datetime, timedelta, timezone


def load_config(root):
    root = Path(root).resolve()
    cfg = json.loads((root / 'config.json').read_text())
    c = cfg['databricks']
    for value in [c['catalog'], *c['schemas'].values(), c['volume_name'], *c['tables'].values()]:
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value):
            raise ValueError(f'Invalid SQL identifier: {value}')
    if c['symbol'] != 'BTCUSDT':
        raise ValueError('This project implements the BTCUSDT data contract.')
    if c['retention_days'] < 200:
        raise ValueError('Retention must cover the 200-day indicator.')
    c = dict(c, repo_root=str(root), workspace_root=cfg.get('databricks_project_root') or str(root / 'databricks'))
    c['volume'] = f"/Volumes/{c['catalog'].lower()}/{c['schemas']['bronze'].lower()}/{c['volume_name'].lower()}"
    c['landing'] = c['volume'] + '/unzipped'
    c['checkpoint'] = c['volume'] + '/_checkpoints/btcusdt_csv_bronze_v2'
    c['qualified_tables'] = {
        k: f"{c['catalog']}.{c['schemas'][('bronze' if k == 'bronze' else 'silver' if k in ('trades','summary') else 'orchestration' if k == 'baseline' else 'gold')]}.{v}"
        for k,v in c['tables'].items()
    }
    return c


def date_range(mode, start, end, full_start, today=None):
    today = today or datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    lo = date.fromisoformat(start) if start else (date.fromisoformat(full_start) if mode == 'full' else yesterday)
    hi = date.fromisoformat(end) if end else yesterday
    if mode not in ('full','delta') or lo > hi or hi > yesterday:
        raise ValueError('Use full/delta and an inclusive range ending no later than yesterday UTC.')
    return lo, hi


def landing_scope(files, symbol):
    """Validate a single listing, ignoring ZIP/partial/unrelated files."""
    days = []
    pattern = re.compile(rf'{re.escape(symbol)}-trades-(\d{{4}}-\d{{2}}-\d{{2}})\.csv')
    for name, size in files:
        if not name.startswith(symbol + '-trades-') or not name.endswith('.csv'):
            continue
        match = pattern.fullmatch(name)
        if not match or size <= 0:
            raise ValueError('BAD_ARCHIVE: invalid name or empty CSV: ' + name)
        days.append(date.fromisoformat(match[1]))
    if not days:
        raise ValueError('EMPTY_LANDING: no CSV archives')
    if len(days) != len(set(days)):
        raise ValueError('DUPLICATE_DAY')
    lo, hi = min(days), max(days)
    if len(days) != (hi-lo).days+1:
        raise ValueError('MISSING_DAY: landing coverage is not contiguous')
    return {'lo':lo.isoformat(), 'hi':hi.isoformat()}
