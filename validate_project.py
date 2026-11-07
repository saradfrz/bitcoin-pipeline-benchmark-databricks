"""Run locally: python validate_project.py (requires PyYAML). No workspace writes."""
import ast
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'databricks/src'))
from configuration import load_config
from job_sync import JOB_KEYS, render_settings
from project_validation import validate_workspace_names

def main():
    validate_workspace_names(ROOT)
    sources = list((ROOT / 'databricks').rglob('*.py'))
    for path in sources:
        ast.parse(path.read_text(), filename=str(path))
    config = load_config(ROOT)
    for key in JOB_KEYS:
        settings = render_settings(config, key)
        print(f'{key}: {len(settings["tasks"])} tasks; notebook paths and dependencies valid')
    print(f'PASS: {len(sources)} Python files; no workspace name collisions')

if __name__ == '__main__':
    main()
