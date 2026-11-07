from pathlib import Path
import sys
import pytest
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'databricks/src'))
from project_validation import validate_workspace_names
from configuration import load_config
from job_sync import render_settings

@pytest.mark.parametrize('other', ['ingestion.py', 'ingestion'])
def test_detects_notebook_collision(tmp_path, other):
    (tmp_path/'ingestion.ipynb').write_text('{}')
    if other.endswith('.py'):
        (tmp_path/other).write_text('# Databricks notebook source\n')
    else:
        (tmp_path/other).mkdir()
    with pytest.raises(ValueError, match='collision'):
        validate_workspace_names(tmp_path)

def test_current_tree_has_no_collisions():
    validate_workspace_names(ROOT)

def test_remote_workspace_path_validates_local_sources():
    config = load_config(ROOT)
    config['workspace_root'] = '/Workspace/Users/example/separate-repo/databricks'
    settings = render_settings(config, 'delta')
    assert settings['tasks'][0]['notebook_task']['notebook_path'].startswith(config['workspace_root'])
