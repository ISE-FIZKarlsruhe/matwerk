from __future__ import annotations
import sys, os
sys.path.append(os.path.dirname(__file__))

import requests

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.sdk import DAG

CQ_MD_URL="https://raw.githubusercontent.com/ISE-FIZKarlsruhe/matwerk/refs/heads/main/docs/general_queries.md"

with DAG(
    "matwerk_cq_tester",
    description="Tests the matwerk competecy questions",
    tags=["matwerk"],
) as dag:


    @task()
    def init_data_dir(ti=None):
        ctx = get_current_context()
        sharedfs = Variable.get("matwerk_sharedfs")
        rid = ctx["dag_run"].run_id
        run_dir = os.path.join(sharedfs, "runs", ctx["dag"].dag_id, rid)
        if not os.path.exists(run_dir):
            os.makedirs(run_dir)

        out_path = os.path.join(run_dir, "markdown.md")

        r = requests.get(CQ_MD_URL, timeout=60)
        r.raise_for_status()
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(r.text)
        if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            raise RuntimeError(f"file not written: {out_path}")

        ti.xcom_push(key="run_dir", value=run_dir)


    @task 
    def test_queries(ti=None):
        ctx = get_current_context()
        sharedfs = Variable.get("matwerk_sharedfs")
        rid = ctx["dag_run"].run_id
        run_dir = os.path.join(sharedfs, "runs", ctx["dag"].dag_id, rid)
        md_path = os.path.join(run_dir, "markdown.md")
        virtuoso=Variable.get("matwerk-virtuoso_sparql_ro")
        
        with open(md_path, "r") as f:
            for token in f.read().split("```"):
                if token.startswith("sparql"):
                    token = token[6:]
                    print ("Testing query: ")
                    print (token)
                    headers = {"Accept":"application/sparql-results+json", "Content-Type":"application/x-www-form-urlencoded"}
                    payload ={"format":"json", "query": token}

                    session = requests.Session()
                    r    = session.post(virtuoso,headers=headers,data=payload)
                    #print ("RESPONSE: ", r.reason)
                    r.raise_for_status()
                    num_results =len(r.json()["results"]["bindings"])
                    print ("NUM RESULTS: ", num_results)
                    if num_results<=0: 
                        raise Exception('Query does not return any result')

    init_data_dir() >> test_queries()

