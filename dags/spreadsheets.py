from __future__ import annotations

import os
import requests

from airflow.sdk import dag, Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

"""
Airflow DAG: process_spreadsheets

Build ontology modules from Google Sheets TSV templates, validate each generated OWL
using ROBOT explain (inconsistency explanations).

All outputs are written into:
  {{ var.value.sharedfs }}/runs/spreadsheet_asserted/<run_id>/
"""

DAG_ID = "process_spreadsheets"

RUN_ID_SAFE = "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
RUN_DIR = "{{ var.value.sharedfs }}/runs/spreadsheet_asserted/" + RUN_ID_SAFE


@dag(
    schedule=None,
    catchup=False,
    tags=["kg", "spreadsheets"],
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
    ]

    init_run_dir = BashOperator(
        task_id="init_run_dir",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'
        mkdir -p "{RUN_DIR}"
        """,
    )

    def retrieve_ontology_impl(data_dir: str):
        os.makedirs(data_dir, exist_ok=True)
        out_path = os.path.join(data_dir, "ontology.owl")
        url = "https://raw.githubusercontent.com/ISE-FIZKarlsruhe/mwo/refs/tags/v3.0.0/mwo.owl"
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(r.text)
        if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            raise RuntimeError(f"ontology.owl not written: {out_path}")

    def retrieve_csv_impl(name: str, gid: str, data_dir: str):
        os.makedirs(data_dir, exist_ok=True)
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

    retrieve_ontology = PythonOperator(
        task_id="retrieve_ontology",
        python_callable=retrieve_ontology_impl,
        op_kwargs={"data_dir": RUN_DIR},
    )

    waitForCsv = EmptyOperator(task_id="waitForCsv")

    # Download all TSVs
    for name, gid in tsv_gids:
        retrieve_csv = PythonOperator(
            task_id=f"retrieve_csv_{name}",
            python_callable=retrieve_csv_impl,
            op_kwargs={"name": name, "gid": gid, "data_dir": RUN_DIR},
        )
        init_run_dir >> retrieve_csv >> waitForCsv

    # Download ontology.owl
    init_run_dir >> retrieve_ontology >> waitForCsv

    # Fast guard before ROBOT starts
    check_inputs = BashOperator(
        task_id="check_inputs",
        bash_command=f"""
        set -euo pipefail
        test -s "{RUN_DIR}/ontology.owl"
        test -s "{RUN_DIR}/req_1.tsv"
        """,
    )

    def isvalid(path: str):
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            s = f.read()
        if s.strip() != "No explanations found.":
            print(s)
            raise ValueError("Ontology inconsistent or explanation not empty")

    def robotCmdTemplate(inputs: list[str], name: str) -> str:
        DATA = RUN_DIR
        ROBOT = "{{ var.value.robotcmd }}"
        insert = ""
        for i in inputs:
            insert += f" -i {os.path.join(DATA, i + '.owl')}"
        cmd = (
            f"{ROBOT} merge --include-annotations true {insert} "
            f"template --merge-before --template {os.path.join(DATA, name + '.tsv')} "
            f"--output {os.path.join(DATA, name + '.owl')}"
        )
        cmd += " && "
        cmd += (
            f"{ROBOT} explain --reasoner hermit --input {os.path.join(DATA, name + '.owl')} "
            f"-M inconsistency --explanation {os.path.join(DATA, name + '.md')}"
        )
        return cmd

    # ---- ROBOT chain (same dependencies as your original DAG) ----

    robot_req_1 = BashOperator(task_id="robot_req_1", bash_command=robotCmdTemplate(["ontology"], "req_1"))
    robot_req_1_valid = PythonOperator(task_id="robot_req_1_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/req_1.md"})
    waitForCsv >> check_inputs >> robot_req_1 >> robot_req_1_valid

    robot_req_2 = BashOperator(task_id="robot_req_2", bash_command=robotCmdTemplate(["req_1"], "req_2"))
    robot_req_2_valid = PythonOperator(task_id="robot_req_2_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/req_2.md"})
    robot_req_1_valid >> robot_req_2 >> robot_req_2_valid

    robot_agent = BashOperator(task_id="robot_agent", bash_command=robotCmdTemplate(["req_2"], "agent"))
    robot_agent_valid = PythonOperator(task_id="robot_agent_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/agent.md"})
    robot_req_2_valid >> robot_agent >> robot_agent_valid

    robot_role = BashOperator(task_id="robot_role", bash_command=robotCmdTemplate(["agent"], "role"))
    robot_role_valid = PythonOperator(task_id="robot_role_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/role.md"})
    robot_agent_valid >> robot_role >> robot_role_valid

    robot_process = BashOperator(task_id="robot_process", bash_command=robotCmdTemplate(["agent"], "process"))
    robot_process_valid = PythonOperator(task_id="robot_process_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/process.md"})
    robot_role_valid >> robot_process >> robot_process_valid

    robot_city = BashOperator(task_id="robot_city", bash_command=robotCmdTemplate(["req_2"], "city"))
    robot_city_valid = PythonOperator(task_id="robot_city_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/city.md"})
    robot_req_2_valid >> robot_city >> robot_city_valid

    robot_organization = BashOperator(task_id="robot_organization", bash_command=robotCmdTemplate(["city"], "organization"))
    robot_organization_valid = PythonOperator(task_id="robot_organization_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/organization.md"})
    robot_city_valid >> robot_organization >> robot_organization_valid

    robot_people = BashOperator(task_id="robot_people", bash_command=robotCmdTemplate(["organization"], "people"))
    robot_people_valid = PythonOperator(task_id="robot_people_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/people.md"})
    robot_organization_valid >> robot_people >> robot_people_valid

    robot_dataset = BashOperator(task_id="robot_dataset", bash_command=robotCmdTemplate(["organization", "process"], "dataset"))
    robot_dataset_valid = PythonOperator(task_id="robot_dataset_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/dataset.md"})
    [robot_organization_valid, robot_process_valid] >> robot_dataset >> robot_dataset_valid

    robot_publication = BashOperator(task_id="robot_publication", bash_command=robotCmdTemplate(["agent", "process"], "publication"))
    robot_publication_valid = PythonOperator(task_id="robot_publication_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/publication.md"})
    [robot_agent_valid, robot_process_valid] >> robot_publication >> robot_publication_valid

    robot_software = BashOperator(task_id="robot_software", bash_command=robotCmdTemplate(["agent", "process"], "software"))
    robot_software_valid = PythonOperator(task_id="robot_software_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/software.md"})
    [robot_agent_valid, robot_process_valid] >> robot_software >> robot_software_valid

    robot_dataportal = BashOperator(task_id="robot_dataportal", bash_command=robotCmdTemplate(["agent", "process"], "dataportal"))
    robot_dataportal_valid = PythonOperator(task_id="robot_dataportal_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/dataportal.md"})
    [robot_organization_valid, robot_process_valid, robot_publication_valid] >> robot_dataportal >> robot_dataportal_valid

    robot_instrument = BashOperator(task_id="robot_instrument", bash_command=robotCmdTemplate(["organization", "publication"], "instrument"))
    robot_instrument_valid = PythonOperator(task_id="robot_instrument_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/instrument.md"})
    [robot_organization_valid, robot_publication_valid] >> robot_instrument >> robot_instrument_valid

    robot_largescalefacility = BashOperator(
        task_id="robot_largescalefacility",
        bash_command=robotCmdTemplate(["agent", "process", "organization", "publication"], "largescalefacility"),
    )
    robot_largescalefacility_valid = PythonOperator(
        task_id="robot_largescalefacility_valid",
        python_callable=isvalid,
        op_kwargs={"path": f"{RUN_DIR}/largescalefacility.md"},
    )
    [robot_process_valid, robot_agent_valid, robot_organization_valid, robot_publication_valid] >> robot_largescalefacility >> robot_largescalefacility_valid

    robot_metadata = BashOperator(task_id="robot_metadata", bash_command=robotCmdTemplate(["process", "organization", "publication"], "metadata"))
    robot_metadata_valid = PythonOperator(task_id="robot_metadata_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/metadata.md"})
    [robot_process_valid, robot_organization_valid, robot_publication_valid] >> robot_metadata >> robot_metadata_valid

    robot_matwerkta = BashOperator(task_id="robot_matwerkta", bash_command=robotCmdTemplate(["process", "organization"], "matwerkta"))
    robot_matwerkta_valid = PythonOperator(task_id="robot_matwerkta_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/matwerkta.md"})
    [robot_process_valid, robot_organization_valid] >> robot_matwerkta >> robot_matwerkta_valid

    robot_matwerkiuc = BashOperator(task_id="robot_matwerkiuc", bash_command=robotCmdTemplate(["matwerkta"], "matwerkiuc"))
    robot_matwerkiuc_valid = PythonOperator(task_id="robot_matwerkiuc_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/matwerkiuc.md"})
    robot_matwerkta_valid >> robot_matwerkiuc >> robot_matwerkiuc_valid

    robot_matwerkpp = BashOperator(task_id="robot_matwerkpp", bash_command=robotCmdTemplate(["matwerkiuc", "role"], "matwerkpp"))
    robot_matwerkpp_valid = PythonOperator(task_id="robot_matwerkpp_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/matwerkpp.md"})
    robot_matwerkiuc_valid >> robot_matwerkpp >> robot_matwerkpp_valid

    robot_temporal = BashOperator(task_id="robot_temporal", bash_command=robotCmdTemplate(["organization", "publication", "process"], "temporal"))
    robot_temporal_valid = PythonOperator(task_id="robot_temporal_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/temporal.md"})
    [robot_organization_valid, robot_publication_valid, robot_process_valid] >> robot_temporal >> robot_temporal_valid

    robot_event = BashOperator(task_id="robot_event", bash_command=robotCmdTemplate(["organization", "temporal", "publication", "process"], "event"))
    robot_event_valid = PythonOperator(task_id="robot_event_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/event.md"})
    [robot_organization_valid, robot_temporal_valid, robot_publication_valid, robot_process_valid] >> robot_event >> robot_event_valid

    robot_collaboration = BashOperator(task_id="robot_collaboration", bash_command=robotCmdTemplate(["organization", "temporal", "process"], "collaboration"))
    robot_collaboration_valid = PythonOperator(task_id="robot_collaboration_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/collaboration.md"})
    [robot_organization_valid, robot_temporal_valid, robot_process_valid] >> robot_collaboration >> robot_collaboration_valid

    robot_service = BashOperator(task_id="robot_service", bash_command=robotCmdTemplate(["organization", "temporal", "process"], "service"))
    robot_service_valid = PythonOperator(task_id="robot_service_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/service.md"})
    [robot_organization_valid, robot_temporal_valid, robot_process_valid] >> robot_service >> robot_service_valid

    robot_sparql_endpoints = BashOperator(task_id="robot_sparql_endpoints", bash_command=robotCmdTemplate(["organization", "temporal", "process"], "sparql_endpoints"))
    robot_sparql_endpoints_valid = PythonOperator(task_id="robot_sparql_endpoints_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/sparql_endpoints.md"})
    [robot_organization_valid, robot_temporal_valid, robot_process_valid] >> robot_sparql_endpoints >> robot_sparql_endpoints_valid

    robot_fdos = BashOperator(task_id="robot_fdos", bash_command=robotCmdTemplate(["organization", "temporal", "process", "dataset"], "fdos"))
    robot_fdos_valid = PythonOperator(task_id="robot_fdos_valid", python_callable=isvalid, op_kwargs={"path": f"{RUN_DIR}/fdos.md"})
    [robot_organization_valid, robot_temporal_valid, robot_process_valid, robot_dataset_valid] >> robot_fdos >> robot_fdos_valid


process_spreadsheets()
