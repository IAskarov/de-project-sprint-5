"""Check graph wiring without importing or starting an Airflow installation."""
import importlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'/'dags'))


class FakeDAG:
    current=None
    def __init__(self,*args,**kwargs): self.tasks=[]; self.max_active_runs=9
    def get_task(self,name): return next(t for t in self.tasks if t.task_id==name)
    @property
    def leaves(self): return [t for t in self.tasks if not t.down]
    def has_task(self,name): return any(t.task_id==name for t in self.tasks)
    def __enter__(self): FakeDAG.current=self; return self
    def __exit__(self,*args): FakeDAG.current=None


class Operator:
    def __init__(self,task_id,**kwargs):
        self.task_id=task_id; self.kwargs=kwargs; self.down=[]; self.up=[]
        FakeDAG.current.tasks.append(self)
    def __rshift__(self,other):
        if isinstance(other,list):
            for task in other: self >> task
            return other
        self.down.append(other); other.up.append(self); return other
    def __rrshift__(self,other):
        for task in other: task >> self
        return self


class GraphTests(unittest.TestCase):
    def test_all_original_leaves_and_dimensions_before_facts(self):
        modules={}
        for name in ['airflow','airflow.operators','airflow.operators.python',
                     'airflow.operators.trigger_dagrun','airflow.sensors','airflow.sensors.external_task']:
            modules[name]=types.ModuleType(name)
        modules['airflow.operators.python'].PythonOperator=Operator
        modules['airflow.operators.trigger_dagrun'].TriggerDagRunOperator=Operator
        modules['airflow.sensors.external_task'].ExternalTaskSensor=Operator
        with patch.dict(sys.modules,modules):
            sys.modules.pop('project5.integration',None)
            extend=importlib.import_module('project5.integration').extend_orders_dag
            dag=FakeDAG()
            with dag:
                a=Operator('orders'); b=Operator('product_sales'); c=Operator('bonus_report')
                a >> b
            extend(dag)
            tasks={t.task_id:t for t in dag.tasks}
            facts=tasks['project5_load_facts']
            self.assertEqual({t.task_id for t in facts.up},
                             {'product_sales','bonus_report','project5_wait_dimensions'})
            self.assertEqual(facts.down,[tasks['project5_build_ledger']])
            self.assertEqual(tasks['project5_wait_stg'].kwargs['mode'],'reschedule')
            self.assertFalse(tasks['project5_trigger_stg'].kwargs['wait_for_completion'])
            self.assertEqual(dag.max_active_runs,1)
            count=len(dag.tasks)
            extend(dag)
            self.assertEqual(len(dag.tasks),count)
        sys.modules.pop('project5.integration',None)

    def test_fresh_pipeline_creates_schema_before_delivery_trigger(self):
        modules={}
        for name in ['airflow','airflow.operators','airflow.operators.python',
                     'airflow.operators.trigger_dagrun','airflow.sensors','airflow.sensors.external_task']:
            modules[name]=types.ModuleType(name)
        modules['airflow'].DAG=FakeDAG
        modules['airflow.operators.python'].PythonOperator=Operator
        modules['airflow.operators.trigger_dagrun'].TriggerDagRunOperator=Operator
        modules['airflow.sensors.external_task'].ExternalTaskSensor=Operator
        with patch.dict(sys.modules,modules):
            sys.modules.pop('project5.integration',None)
            spec=importlib.util.spec_from_file_location('baseline',Path(__file__).resolve().parents[1]/'src/dags/project5_orders_dag.py')
            module=importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            dag=module.project5_orders
            trigger=dag.get_task('project5_trigger_stg')
            self.assertIn(dag.get_task('init_order_schema'),trigger.up)
            facts=dag.get_task('project5_load_facts')
            self.assertIn(dag.get_task('load_order_facts'),facts.up)
        sys.modules.pop('project5.integration',None)
