#!/usr/bin/env python3
"""Run inside the trainer. Add project tasks to the specified existing DAG."""
import argparse
import shutil
from pathlib import Path


def main():
    from airflow.models import DagBag
    from airflow.configuration import conf
    parser = argparse.ArgumentParser()
    parser.add_argument('--orders-dag-id',required=True)
    args = parser.parse_args()
    root = Path(conf.get('core','dags_folder')).resolve()
    source = Path(__file__).resolve().parents[1] / 'src' / 'dags'
    bag = DagBag(dag_folder=str(root),include_examples=False)
    dag = bag.get_dag(args.orders_dag_id)
    if dag is None:
        raise SystemExit('DAG not found. Check airflow dags list and import errors first.')
    if dag.dag_id.startswith('project5_delivery_'):
        raise SystemExit('Choose the order/fact DAG, not a project5 child DAG.')
    if dag.schedule_interval is None:
        raise SystemExit('The order DAG needs a daily (or more frequent) schedule. Configure it before installation.')
    target = Path(dag.fileloc).resolve()
    if root not in target.parents:
        raise SystemExit('DAG file must be inside dags_folder')
    original = target.read_text()
    marker = '# PROJECT5_COURIER_EXTENSION'
    if marker in original:
        raise SystemExit('Extension already installed; source files were not changed.')
    backup = target.with_suffix('.py.project5-backup')
    if backup.exists():
        raise SystemExit('Backup exists. Review the earlier installation before continuing.')
    for name in ('project5','project5_delivery_dags.py'):
        src,dst = source/name,root/name
        if src.is_dir():
            shutil.copytree(src,dst,dirs_exist_ok=True)
        else:
            shutil.copy2(src,dst)
    shutil.copy2(target,backup)
    extension = '''\n\n# PROJECT5_COURIER_EXTENSION
from airflow import DAG as _Project5DAG
from project5.integration import extend_orders_dag as _extend_project5
for _project5_object in list(globals().values()):
    if isinstance(_project5_object, _Project5DAG) and _project5_object.dag_id == %r:
        _extend_project5(_project5_object)
        break
else:
    raise RuntimeError('Project5: target DAG object was not found in module globals')
''' % args.orders_dag_id
    target.write_text(original+extension)
    # Reparse in a fresh process: imports already cached by DagBag must not mask errors.
    import subprocess,sys
    check = '''from airflow.models import DagBag
b=DagBag(dag_folder=sys.argv[1], include_examples=False)
assert not b.import_errors, b.import_errors
assert b.get_dag(sys.argv[2]).has_task('project5_build_ledger')
print('DAG import and extension: OK')'''
    result = subprocess.run([sys.executable,'-c','import sys\n'+check,str(root),args.orders_dag_id])
    if result.returncode:
        shutil.copy2(backup,target)
        raise SystemExit('DAG parse failed; original DAG restored. Inspect import errors.')
    print('Extended DAG:',args.orders_dag_id)
    print('Original file backup:',backup)
    print('Keep its order and bonus source DAGs running. Unpause both project5 child DAGs.')


if __name__ == '__main__':
    main()
