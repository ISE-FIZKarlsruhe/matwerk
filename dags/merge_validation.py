from __future__ import annotations

import os
import shutil
from glob import glob

import requests

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.exceptions import AirflowFailException
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


DAG_ID = "merge_validation"
OUT_TTL = "spreadsheets_asserted.ttl"
GRAPH_BASE = "https://purls.helmholtz-metadaten.de/msekg/merge_validation/"

@dag(
    schedule=None,
    catchup=False,
    dag_id=DAG_ID,
    tags=["kg", "spreadsheets", "production"],
)
def merge_validation():

    # -----------------------------
    # XCom / directory init 
    # -----------------------------
    @task
    def init_data_dir(ti=None):
        ctx = get_current_context()
        run_id = ctx["dag_run"].run_id
        run_id_safe = run_id.replace(":", "_").replace("+", "_").replace("/", "_")

        data_dir = os.path.join(Variable.get("sharedfs"), "runs", ctx["dag"].dag_id, run_id_safe)
        print("Creating directory for run:", data_dir)
        ti.xcom_push(key="datadir", value=data_dir)

    @task
    def init_dirs(ti=None):
        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        os.makedirs(os.path.join(data_dir, "_shapes"), exist_ok=True)
        os.makedirs(os.path.join(data_dir, "components"), exist_ok=True)

    # -----------------------------
    # Shapes download (push filenames to XCom)
    # -----------------------------
    @task
    def fetch_shapes(ti=None):
        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        shapes_dir = os.path.join(data_dir, "_shapes")

        api = "https://api.github.com/repos/ISE-FIZKarlsruhe/matwerk/contents/shapes?ref=main"
        items = requests.get(api).json()

        for it in items:
            if it.get("type") != "file":
                continue
            name = it.get("name", "")
            dl = it.get("download_url", "")
            if not name or not dl:
                continue
            out = os.path.join(shapes_dir, name)
            r = requests.get(dl)
            r.raise_for_status()
            with open(out, "wb") as f:
                f.write(r.content)

        shape_files = sorted([os.path.basename(p) for p in glob(os.path.join(shapes_dir, "*.ttl"))])
        sparql_files = sorted([os.path.basename(p) for p in glob(os.path.join(shapes_dir, "*.sparql"))])

        print("Downloaded SHACL shapes:", shape_files)
        print("Downloaded SPARQL queries:", sparql_files)

        ti.xcom_push(key="shape_files", value=shape_files)
        ti.xcom_push(key="sparql_files", value=sparql_files)

    # -----------------------------
    # Stage components (push component basenames to XCom)
    # -----------------------------
    @task()
    def stage_components(ti=None):
        source_run_dir = Variable.get("last_sucessfull_spreadsheet_run")

        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        components_dir = os.path.join(data_dir, "components")

        print("Staging OWL components from:", source_run_dir)

        owls = glob(os.path.join(source_run_dir, "*.owl"))
        print("Found OWLs:", len(owls))
        if not owls:
            raise AirflowFailException(f"No OWLs found in {source_run_dir}")

        component_names: list[str] = []
        for f in owls:
            base = os.path.basename(f)
            name, _ = os.path.splitext(base)
            component_names.append(name)
            shutil.copyfile(f, os.path.join(components_dir, f"{name}.owl"))

        component_names.sort()
        print("Staged OWL components to:", components_dir)
        print("Component names:", component_names)

        ti.xcom_push(key="component_names", value=component_names)

    # -----------------------------
    # ROBOT command templates 
    # -----------------------------
    def robotMergeCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        ttl = os.path.join(DATA_DIR, OUT_TTL)

        cmd = (
            f"{ROBOT} merge --include-annotations true"
            f" --inputs '{os.path.join(DATA_DIR, 'components', '*.owl')}'"
            f" --output '{ttl}'"
        )
        return cmd.replace(DATA_DIR, XCOM_DATADIR)

    def robotHermitExplainCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        ttl = os.path.join(DATA_DIR, OUT_TTL)
        explain_md = os.path.join(DATA_DIR, "hermit_inconsistency.md")

        cmd = (
            f"{ROBOT} explain --reasoner hermit --input '{ttl}' "
            f"-M inconsistency --explanation '{explain_md}'"
        )
        return cmd.replace(DATA_DIR, XCOM_DATADIR)


    def robotVerifySparqlCmdTemplate() -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = "DATA_DIR"
        XCOM_DATADIR = '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'

        ttl = os.path.join(DATA_DIR, OUT_TTL)

        cmd = (
            "{% for q in ti.xcom_pull(task_ids='fetch_shapes', key='sparql_files') %}\n"
            f"echo 'Running ROBOT verify with: {os.path.join(DATA_DIR, '_shapes', '{{ q }}')}'\n"
            f"{ROBOT} verify --input \"{ttl}\" --queries '{os.path.join(DATA_DIR, '_shapes', '{{ q }}')}' -vvv\n"
            "{% endfor %}\n"
        )
        return cmd.replace(DATA_DIR, XCOM_DATADIR)
        
    def isvalid(filename: str, ti=None):
        DATA_DIR = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        path = os.path.join(DATA_DIR, filename)
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            s = f.read()
        if s.strip() != "No explanations found.":
            print(s)
            raise ValueError("Ontology inconsistent or explanation not empty")
        
    # -----------------------------
    # SHACL validation
    # -----------------------------
    @task
    def shacl_validate(ti=None):
        from pyshacl import validate

        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        shape_files = ti.xcom_pull(task_ids="fetch_shapes", key="shape_files")

        ttl_path = os.path.join(data_dir, OUT_TTL)

        any_bad = False
        for shp_name in shape_files:
            shp = os.path.join(data_dir, "_shapes", shp_name)
            print("Running SHACL:", shp)
            conforms, _, results_text = validate(
                data_graph=ttl_path,
                shacl_graph=shp,
                data_graph_format="turtle",
                shacl_graph_format="turtle",
                inference="rdfs",
                abort_on_error=False,
                meta_shacl=False,
                debug=False,
            )
            print(results_text or "")
            if not conforms:
                any_bad = True

        if any_bad:
            raise AirflowFailException("SHACL validation failed (see logs)")

    # -----------------------------
    # Virtuoso load
    # -----------------------------
    @task
    def load_merge_to_virtuoso(ti=None):
        from requests.auth import HTTPDigestAuth

        ctx = get_current_context()
        run_id_safe = ctx["dag_run"].run_id.replace(":", "_").replace("+", "_").replace("/", "_")
        graph = GRAPH_BASE + run_id_safe

        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        ttl_path = os.path.join(data_dir, OUT_TTL)

        vcrud = Variable.get("virtuoso_crud")
        vsparql = Variable.get("virtuoso_sparql")
        vuser = Variable.get("virtuoso_user")
        vpass = Variable.get("virtuoso_pass")

        if not (vcrud and vsparql and vuser and vpass):
            raise AirflowFailException("Missing virtuoso_* variables (crud/sparql/user/pass)")

        delete_first = True
        auth = HTTPDigestAuth(vuser, vpass)

        if delete_first:
            q = f"drop silent graph <{graph}>"
            url = (
                vsparql.rstrip("/")
                + "?default-graph-uri=&query="
                + requests.utils.quote(q)
                + "&format=text%2Fhtml&timeout=0&signal_void=on"
            )
            print("Dropping graph:", graph)
            r = requests.get(url, auth=auth, timeout=60)
            print("DROP status:", r.status_code)
            if r.status_code != 200:
                print(r.text[:1000])
                raise AirflowFailException("DROP GRAPH failed")

        print("Uploading TTL to graph:", graph)
        data = open(ttl_path, "rb").read()
        url = vcrud.rstrip("/") + "?graph=" + requests.utils.quote(graph, safe="")
        r = requests.post(
            url,
            data=data,
            headers={"Content-Type": "text/turtle"},
            auth=auth,
            timeout=(10, 600),
        )
        print("UPLOAD status:", r.status_code)
        if r.status_code not in (200, 201, 204):
            print(r.text[:1000])
            raise AirflowFailException("Virtuoso upload failed")

        print("Virtuoso load done")
    
    # -----------------------------
    # DAG
    # -----------------------------
    wait = EmptyOperator(task_id="wait_for_inputs")

    init = init_data_dir()
    dirs = init_dirs()
    shapes = fetch_shapes()
    staged = stage_components()

    init >> dirs >> [shapes, staged] >> wait

    robot_merge = BashOperator(task_id="robot_merge_and_convert", bash_command=robotMergeCmdTemplate(),)

    robot_hermit_explain = BashOperator(task_id="robot_hermit_explain", bash_command=robotHermitExplainCmdTemplate(),)

    robot_hermit_valid = PythonOperator(task_id="robot_hermit_valid", python_callable=isvalid, op_kwargs={"filename": "hermit_inconsistency.md"},)

    robot_verify = BashOperator(task_id="robot_verify_sparql", bash_command=robotVerifySparqlCmdTemplate(),)

    shacl = shacl_validate()
    load = load_merge_to_virtuoso()

    wait >> robot_merge

    robot_merge >> robot_hermit_explain >> robot_hermit_valid

    robot_merge >> [robot_verify, shacl]
    [robot_hermit_valid, robot_verify, shacl] >> load



merge_validation()
