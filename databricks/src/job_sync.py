"""Synchronize full job settings from Git-versioned YAML; retain existing IDs."""
from pathlib import Path
import json
import re
import yaml
from project_validation import validate_workspace_names, is_notebook

JOB_KEYS = ('full','delta','ingestion_full','ingestion_delta')


def render_settings(config, key):
    validate_workspace_names(config['repo_root'])
    path = Path(config['repo_root'])/'databricks/orchestration'/f'{key}.yml'
    text = path.read_text()
    values = {'databricks_project_root':config['workspace_root'],
              'environment_version':config['jobs']['environment_version'],
              'performance_target':config['jobs']['performance_target']}
    # Replace only ${...}, leaving Databricks {{...}} task references intact.
    for k,v in values.items():
        text = text.replace('${'+k+'}', str(v))
    if re.search(r'\$\{[^}]+\}',text):
        raise ValueError(f'Unresolved YAML variable in {path}')
    settings = yaml.safe_load(text)['resources']['jobs'][key]
    keys = [t['task_key'] for t in settings['tasks']]
    if len(keys)!=len(set(keys)):
        raise ValueError('Duplicate task keys')
    seen=set()
    ancestors = {}
    for task in settings['tasks']:
        if any(d['task_key'] not in seen for d in task.get('depends_on',[])):
            raise ValueError('Tasks must be in dependency order')
        dependencies = {d['task_key'] for d in task.get('depends_on', [])}
        ancestors[task['task_key']] = dependencies | set().union(*(ancestors[d] for d in dependencies))
        for producer in re.findall(r'\{\{tasks\.([A-Za-z0-9_]+)\.values\.', json.dumps(task)):
            if producer not in ancestors[task['task_key']]:
                raise ValueError(f'Task value producer {producer} must be an upstream dependency')
        seen.add(task['task_key'])
        relative = Path(task['notebook_task']['notebook_path']).relative_to(config['workspace_root'])
        source = Path(config['repo_root']) / 'databricks' / (str(relative) + '.py')
        if not source.is_file() or not is_notebook(source):
            raise FileNotFoundError(f'Missing source notebook: {source}')
    return settings


def resolve_job(client, config, key, settings):
    job_id=config['jobs']['ids'].get(key)
    if job_id is not None:
        client.jobs.get(int(job_id))  # Validate access/existence before reset.
        return int(job_id)
    matches=[j for j in client.jobs.list(name=settings['name']) if j.settings and j.settings.name==settings['name']]
    if len(matches)>1:
        raise ValueError(f'Ambiguous name {settings["name"]}; set jobs.ids.{key} in config.json.')
    if matches:
        tags=matches[0].settings.tags or {}
        if tags.get('project')!='di-pipe-eng':
            raise ValueError(f'Existing {key} job is not tagged for this project; explicitly set its ID to adopt it.')
        return matches[0].job_id
    return None


def sync_jobs(config, client=None):
    if client is None:
        from databricks.sdk import WorkspaceClient
        client=WorkspaceClient()
    # Validate every file/target before changing any job.
    plan=[(k,render_settings(config,k)) for k in JOB_KEYS]
    plan=[(k,s,resolve_job(client,config,k,s)) for k,s in plan]
    for key,settings,job_id in plan:
        if job_id and next(iter(client.jobs.list_runs(job_id=job_id,active_only=True)),None):
            raise RuntimeError(f'{key} has an active run; redeploy only when all jobs are idle.')
    ids={}
    for key,settings,job_id in plan:
        if job_id is None:
            job_id=client.api_client.do('POST','/api/2.2/jobs/create',body=settings)['job_id']
        else:
            client.api_client.do('POST','/api/2.2/jobs/reset',body={'job_id':job_id,'new_settings':settings})
        ids[key]=job_id
        print(f'{key}: {client.config.host.rstrip("/")}/#job/{job_id}')
    print('Copy these IDs into config.json databricks.jobs.ids and commit:')
    print(json.dumps(ids,indent=2))
    return ids
