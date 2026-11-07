"""Validate workspace asset names before deploying or cloning into Databricks."""
from pathlib import Path

IGNORED = {'.git', '__pycache__', '.venv', '.pytest_cache'}

def is_notebook(path):
    if path.suffix == '.ipynb':
        return True
    return path.suffix in {'.py', '.sql', '.scala', '.r', '.R'} and 'Databricks notebook source' in path.read_text(encoding='utf-8').split('\n', 1)[0]

def validate_workspace_names(root):
    root = Path(root)
    for folder in [root, *[p for p in root.rglob('*') if p.is_dir()]]:
        if any(part in IGNORED for part in folder.relative_to(root).parts):
            continue
        entries = [p for p in folder.iterdir() if p.name not in IGNORED]
        for notebook in [p for p in entries if p.is_file() and is_notebook(p)]:
            name = notebook.stem
            for other in entries:
                if other == notebook:
                    continue
                other_name = other.stem if other.is_file() and is_notebook(other) else other.name
                if name == other_name:
                    raise ValueError(f'Workspace name collision: {notebook.relative_to(root)} and {other.relative_to(root)}')
