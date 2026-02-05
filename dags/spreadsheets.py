from __future__ import annotations

import os
import requests

from airflow.sdk import dag, task, Variable, get_current_context
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

SUCCESFULL_RUN_VARIABLE_NAME = "last_sucessfull_spreadsheet_run"

"""
Airflow DAG: process_spreadsheets

Builds ontology modules from Google Sheets TSV templates, validates each generated OWL
using ROBOT explain (inconsistency explanations).

All outputs are written into:
  {{ var.value.sharedfs }}/runs/spreadsheet_asserted/<run_id>/
"""
@dag(
    schedule=None,
    catchup=False,
    tags=["kg", "spreadsheets", "production"],
)
def process_spreadsheets():

    tsv_gids = [
        ("req_1", "394894036"),
        ("req_2", "0"),
        ("agent", "2077140060"),
        ("role", "1425127117"),
        ("process", "1169992315"),
        ("city", "1469482382"),
        ("people", "1666156492"),
        ("organization", "447157523"),
        ("dataset", "1079878268"),
        ("publication", "1747331228"),
        ("software", "1275685399"),
        ("dataportal", "923160190"),
        ("instrument", "2015927839"),
        ("largescalefacility", "370181939"),
        ("metadata", "278046522"),
        ("matwerkta", "1489640604"),
        ("matwerkiuc", "281962521"),
        ("matwerkpp", "606786541"),
        ("temporal", "1265818056"),
        ("event", "638946284"),
        ("collaboration", "266847052"),
        ("service", "130394813"),
        ("sparql_endpoints", "1732373290"),
        ("fdos", "152649677"),
        ("ontologies", "2006199416"),
        ("materials", "497166822"),
    ]

    """
    This is the inital task.
    It creates a local working directory for this dag run. (later we will setup an object store)
    The path (bucket name) is stored via xcom, so subsequent tasks can access it. 
    """
    @task
    def init_data_dir(ti=None):
        context = get_current_context()
        RUN_ID = context['dag_run'].run_id
        DAG_ID = context['task'].dag_id
        DATA_DIR = os.path.join(Variable.get("sharedfs"), "runs", DAG_ID, RUN_ID)
        print("Creating directory for run: ", DATA_DIR)
        os.makedirs(DATA_DIR)
        ti.xcom_push(key="datadir", value=DATA_DIR)

    @task 
    def retrieve_ontology(ti=None):
        data_dir = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        print ("Working in datadir ", data_dir)
        out_path = os.path.join(data_dir, "ontology.owl")
        ONTOLOGY=Variable.get("matwerk_ontology")
        r = requests.get(ONTOLOGY, timeout=60)
        r.raise_for_status()
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(r.text)
        if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            raise RuntimeError(f"ontology.owl not written: {out_path}")

    def retrieve_csv_impl(data_dir: str, name: str, gid: str):
        out_path = os.path.join(data_dir, f"{name}.tsv")
        url = (
            "https://docs.google.com/spreadsheets/d/e/"
            "2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE"
            f"/pub?gid={gid}&single=true&output=tsv"
        )
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(r.text)
        if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            raise RuntimeError(f"TSV not written: {out_path}")

    waitForCsv = EmptyOperator(task_id="waitForCsv")

    # we have multiple dependencies on that, that's why we need to instantiate
    init = init_data_dir()

    # Download all TSVs
    for name, gid in tsv_gids:
        retrieve_csv = PythonOperator(
            task_id=f"retrieve_csv_{name}",
            python_callable=retrieve_csv_impl,
            op_kwargs={"name": name, "gid": gid, "data_dir": '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}'},
        )
        init >> retrieve_csv >> waitForCsv

    # Download ontology.owl
    init >> retrieve_ontology() >> waitForCsv

    def isvalid(filename: str, ti=None):
        DATA_DIR = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        path = os.path.join(DATA_DIR, filename)
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            s = f.read()
        if s.strip() != "No explanations found.":
            print(s)
            raise ValueError("Ontology inconsistent or explanation not empty")

    def robotCmdTemplate(inputs: list[str], name: str) -> str:
        ROBOT = "{{ var.value.robotcmd }}"
        DATA_DIR = 'DATA_DIR'
        insert = ""
        for i in inputs:
            insert += f" -i '{os.path.join(DATA_DIR, i + '.owl')}'"
        cmd = (
            f"{ROBOT} merge --include-annotations true {insert} "
            f"template --merge-before --template '{os.path.join(DATA_DIR, name + '.tsv')}' "
            f"--output '{os.path.join(DATA_DIR, name + '.owl')}' -vvv"
        )
        cmd += " && "
        cmd += (
            f"{ROBOT} explain --reasoner hermit --input '{os.path.join(DATA_DIR, name + '.owl')}' "
            f"-M inconsistency --explanation '{os.path.join(DATA_DIR, name + '.md')}'"
        )
        return cmd.replace("DATA_DIR", '{{ ti.xcom_pull(task_ids="init_data_dir", key="datadir") }}')

    '''
    The last task to be executed. 
    If all other tasks where successfull, we write the DATA_DIR to a global variable. 
    '''
    @task
    def tear_down(ti=None):
        print ("Storing successful run id in variable")
        DATA_DIR = ti.xcom_pull(task_ids="init_data_dir", key="datadir")
        Variable.set(SUCCESFULL_RUN_VARIABLE_NAME, DATA_DIR)

    teardown = tear_down()

    # ---- ROBOT chain (same dependencies as your original DAG) ----

    robot_req_1 = BashOperator(task_id="robot_req_1", bash_command=robotCmdTemplate(["ontology"], "req_1"))
    robot_req_1_valid = PythonOperator(task_id="robot_req_1_valid", python_callable=isvalid, op_kwargs={"filename": f"req_1.md"})
    waitForCsv >> robot_req_1 >> robot_req_1_valid >> teardown

    robot_req_2 = BashOperator(task_id="robot_req_2", bash_command=robotCmdTemplate(["req_1"], "req_2"))
    robot_req_2_valid = PythonOperator(task_id="robot_req_2_valid", python_callable=isvalid, op_kwargs={"filename": f"req_2.md"})
    robot_req_1_valid >> robot_req_2 >> robot_req_2_valid >> teardown

    robot_agent = BashOperator(task_id="robot_agent", bash_command=robotCmdTemplate(["req_2"], "agent"))
    robot_agent_valid = PythonOperator(task_id="robot_agent_valid", python_callable=isvalid, op_kwargs={"filename": f"agent.md"})
    robot_req_2_valid >> robot_agent >> robot_agent_valid >> teardown

    robot_role = BashOperator(task_id="robot_role", bash_command=robotCmdTemplate(["agent"], "role"))
    robot_role_valid = PythonOperator(task_id="robot_role_valid", python_callable=isvalid, op_kwargs={"filename": f"role.md"})
    robot_agent_valid >> robot_role >> robot_role_valid >> teardown

    robot_process = BashOperator(task_id="robot_process", bash_command=robotCmdTemplate(["agent"], "process"))
    robot_process_valid = PythonOperator(task_id="robot_process_valid", python_callable=isvalid, op_kwargs={"filename": f"process.md"})
    robot_role_valid >> robot_process >> robot_process_valid >> teardown

    robot_city = BashOperator(task_id="robot_city", bash_command=robotCmdTemplate(["req_2"], "city"))
    robot_city_valid = PythonOperator(task_id="robot_city_valid", python_callable=isvalid, op_kwargs={"filename": f"city.md"})
    robot_req_2_valid >> robot_city >> robot_city_valid >> teardown

    robot_ontologies = BashOperator(task_id="robot_ontologies", bash_command=robotCmdTemplate(["req_1", "req_2"], "ontologies"))
    robot_ontologies_valid = PythonOperator(task_id="robot_ontologies_valid", python_callable=isvalid, op_kwargs={"filename": f"ontologies.md"})
    robot_city_valid >> robot_ontologies >> robot_ontologies_valid >> teardown

    robot_materials = BashOperator(task_id="robot_materials", bash_command=robotCmdTemplate(["req_1", "req_2"], "materials"))
    robot_materials_valid = PythonOperator(task_id="robot_materials_valid", python_callable=isvalid, op_kwargs={"filename": f"materials.md"})
    robot_ontologies_valid >> robot_materials >> robot_materials_valid >> teardown

    robot_organization = BashOperator(task_id="robot_organization", bash_command=robotCmdTemplate(["city"], "organization"))
    robot_organization_valid = PythonOperator(task_id="robot_organization_valid", python_callable=isvalid, op_kwargs={"filename": f"organization.md"})
    robot_city_valid >> robot_organization >> robot_organization_valid >> teardown

    robot_people = BashOperator(task_id="robot_people", bash_command=robotCmdTemplate(["organization"], "people"))
    robot_people_valid = PythonOperator(task_id="robot_people_valid", python_callable=isvalid, op_kwargs={"filename": f"people.md"})
    robot_organization_valid >> robot_people >> robot_people_valid >> teardown

    robot_dataset = BashOperator(task_id="robot_dataset", bash_command=robotCmdTemplate(["organization", "process"], "dataset"))
    robot_dataset_valid = PythonOperator(task_id="robot_dataset_valid", python_callable=isvalid, op_kwargs={"filename": f"dataset.md"})
    [robot_organization_valid, robot_process_valid] >> robot_dataset >> robot_dataset_valid >> teardown

    robot_publication = BashOperator(task_id="robot_publication", bash_command=robotCmdTemplate(["agent", "process"], "publication"))
    robot_publication_valid = PythonOperator(task_id="robot_publication_valid", python_callable=isvalid, op_kwargs={"filename": f"publication.md"})
    [robot_agent_valid, robot_process_valid] >> robot_publication >> robot_publication_valid >> teardown

    robot_software = BashOperator(task_id="robot_software", bash_command=robotCmdTemplate(["agent", "process"], "software"))
    robot_software_valid = PythonOperator(task_id="robot_software_valid", python_callable=isvalid, op_kwargs={"filename": f"software.md"})
    [robot_agent_valid, robot_process_valid] >> robot_software >> robot_software_valid >> teardown

    robot_dataportal = BashOperator(task_id="robot_dataportal", bash_command=robotCmdTemplate(["agent", "process"], "dataportal"))
    robot_dataportal_valid = PythonOperator(task_id="robot_dataportal_valid", python_callable=isvalid, op_kwargs={"filename": f"dataportal.md"})
    [robot_organization_valid, robot_process_valid, robot_publication_valid] >> robot_dataportal >> robot_dataportal_valid >> teardown

    robot_instrument = BashOperator(task_id="robot_instrument", bash_command=robotCmdTemplate(["organization", "publication"], "instrument"))
    robot_instrument_valid = PythonOperator(task_id="robot_instrument_valid", python_callable=isvalid, op_kwargs={"filename": f"instrument.md"})
    [robot_organization_valid, robot_publication_valid] >> robot_instrument >> robot_instrument_valid >> teardown

    robot_largescalefacility = BashOperator(task_id="robot_largescalefacility", bash_command=robotCmdTemplate(["agent", "process", "organization", "publication"], "largescalefacility"))
    robot_largescalefacility_valid = PythonOperator(task_id="robot_largescalefacility_valid", python_callable=isvalid, op_kwargs={"filename": f"largescalefacility.md"})
    [robot_process_valid, robot_agent_valid, robot_organization_valid, robot_publication_valid] >> robot_largescalefacility >> robot_largescalefacility_valid >> teardown

    robot_metadata = BashOperator(task_id="robot_metadata", bash_command=robotCmdTemplate(["process", "organization", "publication"], "metadata"))
    robot_metadata_valid = PythonOperator(task_id="robot_metadata_valid", python_callable=isvalid, op_kwargs={"filename": f"metadata.md"})
    [robot_process_valid, robot_organization_valid, robot_publication_valid] >> robot_metadata >> robot_metadata_valid >> teardown

    robot_matwerkta = BashOperator(task_id="robot_matwerkta", bash_command=robotCmdTemplate(["process", "organization"], "matwerkta"))
    robot_matwerkta_valid = PythonOperator(task_id="robot_matwerkta_valid", python_callable=isvalid, op_kwargs={"filename": f"matwerkta.md"})
    [robot_process_valid, robot_organization_valid] >> robot_matwerkta >> robot_matwerkta_valid >> teardown

    robot_matwerkiuc = BashOperator(task_id="robot_matwerkiuc", bash_command=robotCmdTemplate(["matwerkta"], "matwerkiuc"))
    robot_matwerkiuc_valid = PythonOperator(task_id="robot_matwerkiuc_valid", python_callable=isvalid, op_kwargs={"filename": f"matwerkiuc.md"})
    robot_matwerkta_valid >> robot_matwerkiuc >> robot_matwerkiuc_valid >> teardown

    robot_matwerkpp = BashOperator(task_id="robot_matwerkpp", bash_command=robotCmdTemplate(["matwerkiuc", "role"], "matwerkpp"))
    robot_matwerkpp_valid = PythonOperator(task_id="robot_matwerkpp_valid", python_callable=isvalid, op_kwargs={"filename": f"matwerkpp.md"})
    robot_matwerkiuc_valid >> robot_matwerkpp >> robot_matwerkpp_valid >> teardown

    robot_temporal = BashOperator(task_id="robot_temporal", bash_command=robotCmdTemplate(["organization", "publication", "process"], "temporal"))
    robot_temporal_valid = PythonOperator(task_id="robot_temporal_valid", python_callable=isvalid, op_kwargs={"filename": f"temporal.md"})
    [robot_organization_valid, robot_publication_valid, robot_process_valid] >> robot_temporal >> robot_temporal_valid >> teardown

    robot_event = BashOperator(task_id="robot_event", bash_command=robotCmdTemplate(["organization", "temporal", "publication", "process"], "event"))
    robot_event_valid = PythonOperator(task_id="robot_event_valid", python_callable=isvalid, op_kwargs={"filename": f"event.md"})
    [robot_organization_valid, robot_temporal_valid, robot_publication_valid, robot_process_valid] >> robot_event >> robot_event_valid >> teardown

    robot_collaboration = BashOperator(task_id="robot_collaboration", bash_command=robotCmdTemplate(["organization", "temporal", "process"], "collaboration"))
    robot_collaboration_valid = PythonOperator(task_id="robot_collaboration_valid", python_callable=isvalid, op_kwargs={"filename": f"collaboration.md"})
    [robot_organization_valid, robot_temporal_valid, robot_process_valid] >> robot_collaboration >> robot_collaboration_valid >> teardown

    robot_service = BashOperator(task_id="robot_service", bash_command=robotCmdTemplate(["organization", "temporal", "process"], "service"))
    robot_service_valid = PythonOperator(task_id="robot_service_valid", python_callable=isvalid, op_kwargs={"filename": f"service.md"})
    [robot_organization_valid, robot_temporal_valid, robot_process_valid] >> robot_service >> robot_service_valid >> teardown

    robot_sparql_endpoints = BashOperator(task_id="robot_sparql_endpoints", bash_command=robotCmdTemplate(["organization", "temporal", "process"], "sparql_endpoints"))
    robot_sparql_endpoints_valid = PythonOperator(task_id="robot_sparql_endpoints_valid", python_callable=isvalid, op_kwargs={"filename": f"sparql_endpoints.md"})
    [robot_organization_valid, robot_temporal_valid, robot_process_valid] >> robot_sparql_endpoints >> robot_sparql_endpoints_valid  >> teardown

    robot_fdos = BashOperator(task_id="robot_fdos", bash_command=robotCmdTemplate(["organization", "temporal", "process", "dataset"], "fdos"))
    robot_fdos_valid = PythonOperator(task_id="robot_fdos_valid", python_callable=isvalid, op_kwargs={"filename": f"fdos.md"})
    [robot_organization_valid, robot_temporal_valid, robot_process_valid, robot_dataset_valid] >> robot_fdos >> robot_fdos_valid  >> teardown





process_spreadsheets() 
