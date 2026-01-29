from __future__ import annotations

import os
import requests

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Variable
from pathlib import Path

"""
Airflow DAG: process_spreadsheets

This DAG builds and validates a set of ontology modules from Google Sheets
published as TSV templates. Each template is processed using the ROBOT
toolchain to generate an OWL file and verify logical consistency.

Extended workflow adds:
- merge_and_save_spreadsheets (merge all component OWLs into a TTL)
- verify_sparql (ROBOT verify with SPARQL queries)
- shacl2/shacl3/shacl4 (pyshacl validation)
- final_consistency_gate (checks all inconsistency reports + SHACL results)
- publish (copies artifacts to an output folder)
"""

# Assumes repo layout: <repo_root>/dags/<this_file>
REPO_ROOT = str(Path(__file__).resolve().parents[1])

RUN_ID_SAFE = "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
VALIDATION_DIR = "{{ var.value.sharedfs }}/validation/process_spreadsheets/" + RUN_ID_SAFE
PUBLISH_ROOT = "{{ var.value.sharedfs }}/output/process_spreadsheets/" + RUN_ID_SAFE

ROBOT_JAVA_ARGS = "-Xmx16G -Dfile.encoding=UTF-8"


@dag(
    schedule=None,
    catchup=False,
)
def process_spreadsheets():
    tsv_gids = []
    tsv_gids.append(("req_1", "394894036"))
    tsv_gids.append(("req_2", "0"))
    tsv_gids.append(("agent", "2077140060"))
    tsv_gids.append(("role", "1425127117"))
    tsv_gids.append(("process", "1169992315"))
    tsv_gids.append(("city", "1469482382"))
    tsv_gids.append(("people", "1666156492"))
    tsv_gids.append(("organization", "447157523"))
    tsv_gids.append(("dataset", "1079878268"))
    tsv_gids.append(("publication", "1747331228"))
    tsv_gids.append(("software", "1275685399"))
    tsv_gids.append(("dataportal", "923160190"))
    tsv_gids.append(("instrument", "2015927839"))
    tsv_gids.append(("largescalefacility", "370181939"))
    tsv_gids.append(("metadata", "278046522"))
    tsv_gids.append(("matwerkta", "1489640604"))
    tsv_gids.append(("matwerkiuc", "281962521"))
    tsv_gids.append(("matwerkpp", "606786541"))
    tsv_gids.append(("temporal", "1265818056"))
    tsv_gids.append(("event", "638946284"))
    tsv_gids.append(("collaboration", "266847052"))
    tsv_gids.append(("service", "130394813"))
    tsv_gids.append(("sparql_endpoints", "1732373290"))
    tsv_gids.append(("fdos", "152649677"))

    COMPONENT_NAMES = [name for name, _ in tsv_gids]

    @task()
    def retrieveOntology():
        DATA = Variable.get("sharedfs")
        data_path = os.path.join(DATA, "ontology.owl")
        url = "https://raw.githubusercontent.com/ISE-FIZKarlsruhe/mwo/refs/tags/v3.0.0/mwo.owl"
        response = requests.request("GET", url)
        with open(data_path, "w") as file:
            file.write(response.text)

    def retrieveCsv(name, gid):
        DATA = Variable.get("sharedfs")
        data_path = os.path.join(DATA, name + ".tsv")
        url = (
            "https://docs.google.com/spreadsheets/d/e/"
            "2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub"
            "?gid=" + gid + "&single=true&output=tsv"
        )
        response = requests.request("GET", url)
        with open(data_path, "w") as file:
            file.write(response.text)

    waitForCsv = EmptyOperator(task_id="waitForCsv")

    for name, gid in tsv_gids:
        retrieve_csv = PythonOperator(
            task_id=f"retrieve_csv_{name}",
            python_callable=retrieveCsv,
            op_kwargs={"name": name, "gid": gid},
        )
        retrieve_csv >> waitForCsv

    retrieveOntology() >> waitForCsv

    def isvalid(name):
        """
        Exact sentinel check (robust to CRLF + trailing newline).
        """
        DATA = Variable.get("sharedfs")
        md_path = os.path.join(DATA, name + ".md")
        owl_path = os.path.join(DATA, name + ".owl")

        if not (os.path.isfile(owl_path) and os.path.getsize(owl_path) > 0):
            raise ValueError(f"Missing/empty OWL: {owl_path}")
        if not (os.path.isfile(md_path) and os.path.getsize(md_path) > 0):
            raise ValueError(f"Missing/empty MD: {md_path}")

        with open(md_path, "rb") as f:
            text = f.read().decode("utf-8", errors="replace").replace("\r", "")
        text = text.rstrip("\n")
        if text != "No explanations found.":
            print(text)
            raise ValueError(f"Inconsistency detected; report {md_path} != exact sentinel.")

    def robotCmdTemplate(inputs, name):
        insert = ""
        DATA = "{{ var.value.sharedfs }}"
        ROBOT = "{{ var.value.robotcmd }}"
        for i in inputs:
            token = " -i " + os.path.join(DATA, i + ".owl")
            insert = insert + token
        cmd = (
            ROBOT
            + " merge --include-annotations true  "
            + insert
            + " template --merge-before  --template  "
            + os.path.join(DATA, name + ".tsv")
            + " --output "
            + os.path.join(DATA, name + ".owl")
        )
        cmd += " && "
        cmd += (
            ROBOT
            + " explain --reasoner hermit --input "
            + os.path.join(DATA, name + ".owl")
            + " -M inconsistency --explanation "
            + os.path.join(DATA, name + ".md")
        )
        return cmd

    valid_tasks = []

    #######
    robot_req_1 = BashOperator(
        task_id="robot_req_1",
        bash_command=robotCmdTemplate(["ontology"], "req_1"),
    )
    robot_req_1_valid = PythonOperator(
        task_id="robot_req_1_valid",
        python_callable=isvalid,
        op_kwargs={"name": "req_1"},
    )
    waitForCsv >> robot_req_1 >> robot_req_1_valid
    valid_tasks.append(robot_req_1_valid)

    #######
    robot_req_2 = BashOperator(
        task_id="robot_req_2",
        bash_command=robotCmdTemplate(["req_1"], "req_2"),
    )
    robot_req_2_valid = PythonOperator(
        task_id="robot_req_2_valid",
        python_callable=isvalid,
        op_kwargs={"name": "req_2"},
    )
    robot_req_1_valid >> robot_req_2 >> robot_req_2_valid
    valid_tasks.append(robot_req_2_valid)

    #######
    robot_agent = BashOperator(
        task_id="robot_agent",
        bash_command=robotCmdTemplate(["req_2"], "agent"),
    )
    robot_agent_valid = PythonOperator(
        task_id="robot_agent_valid",
        python_callable=isvalid,
        op_kwargs={"name": "agent"},
    )
    robot_req_2_valid >> robot_agent >> robot_agent_valid
    valid_tasks.append(robot_agent_valid)

    ########
    robot_role = BashOperator(
        task_id="robot_role",
        bash_command=robotCmdTemplate(["agent"], "role"),
    )
    robot_role_valid = PythonOperator(
        task_id="robot_role_valid",
        python_callable=isvalid,
        op_kwargs={"name": "role"},
    )
    robot_agent_valid >> robot_role >> robot_role_valid
    valid_tasks.append(robot_role_valid)

    ########
    robot_process = BashOperator(
        task_id="robot_process",
        bash_command=robotCmdTemplate(["agent"], "process"),
    )
    robot_process_valid = PythonOperator(
        task_id="robot_process_valid",
        python_callable=isvalid,
        op_kwargs={"name": "process"},
    )
    robot_role_valid >> robot_process >> robot_process_valid
    valid_tasks.append(robot_process_valid)

    #########
    robot_city = BashOperator(
        task_id="robot_city",
        bash_command=robotCmdTemplate(["req_2"], "city"),
    )
    robot_city_valid = PythonOperator(
        task_id="robot_city_valid",
        python_callable=isvalid,
        op_kwargs={"name": "city"},
    )
    robot_req_2_valid >> robot_city >> robot_city_valid
    valid_tasks.append(robot_city_valid)

    ########
    robot_organization = BashOperator(
        task_id="robot_organization",
        bash_command=robotCmdTemplate(["city"], "organization"),
    )
    robot_organization_valid = PythonOperator(
        task_id="robot_organization_valid",
        python_callable=isvalid,
        op_kwargs={"name": "organization"},
    )
    robot_city_valid >> robot_organization >> robot_organization_valid
    valid_tasks.append(robot_organization_valid)

    ########
    robot_people = BashOperator(
        task_id="robot_people",
        bash_command=robotCmdTemplate(["organization"], "people"),
    )
    robot_people_valid = PythonOperator(
        task_id="robot_people_valid",
        python_callable=isvalid,
        op_kwargs={"name": "people"},
    )
    robot_organization_valid >> robot_people >> robot_people_valid
    valid_tasks.append(robot_people_valid)

    ########
    robot_dataset = BashOperator(
        task_id="robot_dataset",
        bash_command=robotCmdTemplate(["organization", "process"], "dataset"),
    )
    robot_dataset_valid = PythonOperator(
        task_id="robot_dataset_valid",
        python_callable=isvalid,
        op_kwargs={"name": "dataset"},
    )
    [robot_organization_valid, robot_process_valid] >> robot_dataset >> robot_dataset_valid
    valid_tasks.append(robot_dataset_valid)

    #######
    robot_publication = BashOperator(
        task_id="robot_publication",
        bash_command=robotCmdTemplate(["agent", "process"], "publication"),
    )
    robot_publication_valid = PythonOperator(
        task_id="robot_publication_valid",
        python_callable=isvalid,
        op_kwargs={"name": "publication"},
    )
    [robot_agent_valid, robot_process_valid] >> robot_publication >> robot_publication_valid
    valid_tasks.append(robot_publication_valid)

    ########
    robot_software = BashOperator(
        task_id="robot_software",
        bash_command=robotCmdTemplate(["agent", "process"], "software"),
    )
    robot_software_valid = PythonOperator(
        task_id="robot_software_valid",
        python_callable=isvalid,
        op_kwargs={"name": "software"},
    )
    [robot_agent_valid, robot_process_valid] >> robot_software >> robot_software_valid
    valid_tasks.append(robot_software_valid)

    ######
    robot_dataportal = BashOperator(
        task_id="robot_dataportal",
        bash_command=robotCmdTemplate(["agent", "process"], "dataportal"),
    )
    robot_dataportal_valid = PythonOperator(
        task_id="robot_dataportal_valid",
        python_callable=isvalid,
        op_kwargs={"name": "dataportal"},
    )
    [robot_organization_valid, robot_process_valid, robot_publication_valid] >> robot_dataportal >> robot_dataportal_valid
    valid_tasks.append(robot_dataportal_valid)

    #######
    robot_instrument = BashOperator(
        task_id="robot_instrument",
        bash_command=robotCmdTemplate(["organization", "publication"], "instrument"),
    )
    robot_instrument_valid = PythonOperator(
        task_id="robot_instrument_valid",
        python_callable=isvalid,
        op_kwargs={"name": "instrument"},
    )
    [robot_organization_valid, robot_publication_valid] >> robot_instrument >> robot_instrument_valid
    valid_tasks.append(robot_instrument_valid)

    ########
    robot_largescalefacility = BashOperator(
        task_id="robot_largescalefacility",
        bash_command=robotCmdTemplate(["agent", "process", "organization", "publication"], "largescalefacility"),
    )
    robot_largescalefacility_valid = PythonOperator(
        task_id="robot_largescalefacility_valid",
        python_callable=isvalid,
        op_kwargs={"name": "largescalefacility"},
    )
    [robot_process_valid, robot_agent_valid, robot_organization_valid, robot_publication_valid] >> robot_largescalefacility >> robot_largescalefacility_valid
    valid_tasks.append(robot_largescalefacility_valid)

    ########
    robot_metadata = BashOperator(
        task_id="robot_metadata",
        bash_command=robotCmdTemplate(["process", "organization", "publication"], "metadata"),
    )
    robot_metadata_valid = PythonOperator(
        task_id="robot_metadata_valid",
        python_callable=isvalid,
        op_kwargs={"name": "metadata"},
    )
    [robot_process_valid, robot_organization_valid, robot_publication_valid] >> robot_metadata >> robot_metadata_valid
    valid_tasks.append(robot_metadata_valid)

    #########
    robot_matwerkta = BashOperator(
        task_id="robot_matwerkta",
        bash_command=robotCmdTemplate(["process", "organization"], "matwerkta"),
    )
    robot_matwerkta_valid = PythonOperator(
        task_id="robot_matwerkta_valid",
        python_callable=isvalid,
        op_kwargs={"name": "matwerkta"},
    )
    [robot_process_valid, robot_organization_valid] >> robot_matwerkta >> robot_matwerkta_valid
    valid_tasks.append(robot_matwerkta_valid)

    #########
    robot_matwerkiuc = BashOperator(
        task_id="robot_matwerkiuc",
        bash_command=robotCmdTemplate(["matwerkta"], "matwerkiuc"),
    )
    robot_matwerkiuc_valid = PythonOperator(
        task_id="robot_matwerkiuc_valid",
        python_callable=isvalid,
        op_kwargs={"name": "matwerkiuc"},
    )
    robot_matwerkta_valid >> robot_matwerkiuc >> robot_matwerkiuc_valid
    valid_tasks.append(robot_matwerkiuc_valid)

    ##########
    robot_matwerkpp = BashOperator(
        task_id="robot_matwerkpp",
        bash_command=robotCmdTemplate(["matwerkiuc", "role"], "matwerkpp"),
    )
    robot_matwerkpp_valid = PythonOperator(
        task_id="robot_matwerkpp_valid",
        python_callable=isvalid,
        op_kwargs={"name": "matwerkpp"},
    )
    robot_matwerkiuc_valid >> robot_matwerkpp >> robot_matwerkpp_valid
    valid_tasks.append(robot_matwerkpp_valid)

    ##########
    robot_temporal = BashOperator(
        task_id="robot_temporal",
        bash_command=robotCmdTemplate(["organization", "publication", "process"], "temporal"),
    )
    robot_temporal_valid = PythonOperator(
        task_id="robot_temporal_valid",
        python_callable=isvalid,
        op_kwargs={"name": "temporal"},
    )
    [robot_organization_valid, robot_publication_valid, robot_process_valid] >> robot_temporal >> robot_temporal_valid
    valid_tasks.append(robot_temporal_valid)

    ##########
    robot_event = BashOperator(
        task_id="robot_event",
        bash_command=robotCmdTemplate(["organization", "temporal", "publication", "process"], "event"),
    )
    robot_event_valid = PythonOperator(
        task_id="robot_event_valid",
        python_callable=isvalid,
        op_kwargs={"name": "event"},
    )
    [robot_organization_valid, robot_temporal_valid, robot_publication_valid, robot_process_valid] >> robot_event >> robot_event_valid
    valid_tasks.append(robot_event_valid)

    ##########
    robot_collaboration = BashOperator(
        task_id="robot_collaboration",
        bash_command=robotCmdTemplate(["organization", "temporal", "process"], "collaboration"),
    )
    robot_collaboration_valid = PythonOperator(
        task_id="robot_collaboration_valid",
        python_callable=isvalid,
        op_kwargs={"name": "collaboration"},
    )
    [robot_organization_valid, robot_temporal_valid, robot_process_valid] >> robot_collaboration >> robot_collaboration_valid
    valid_tasks.append(robot_collaboration_valid)

    ##########
    robot_service = BashOperator(
        task_id="robot_service",
        bash_command=robotCmdTemplate(["organization", "temporal", "process"], "service"),
    )
    robot_service_valid = PythonOperator(
        task_id="robot_service_valid",
        python_callable=isvalid,
        op_kwargs={"name": "service"},
    )
    [robot_organization_valid, robot_temporal_valid, robot_process_valid] >> robot_service >> robot_service_valid
    valid_tasks.append(robot_service_valid)

    ##########
    robot_sparql_endpoints = BashOperator(
        task_id="robot_sparql_endpoints",
        bash_command=robotCmdTemplate(["organization", "temporal", "process"], "sparql_endpoints"),
    )
    robot_sparql_endpoints_valid = PythonOperator(
        task_id="robot_sparql_endpoints_valid",
        python_callable=isvalid,
        op_kwargs={"name": "sparql_endpoints"},
    )
    [robot_organization_valid, robot_temporal_valid, robot_process_valid] >> robot_sparql_endpoints >> robot_sparql_endpoints_valid
    valid_tasks.append(robot_sparql_endpoints_valid)

    ###########
    robot_fdos = BashOperator(
        task_id="robot_fdos",
        bash_command=robotCmdTemplate(["organization", "temporal", "process", "dataset"], "fdos"),
    )
    robot_fdos_valid = PythonOperator(
        task_id="robot_fdos_valid",
        python_callable=isvalid,
        op_kwargs={"name": "fdos"},
    )
    [robot_organization_valid, robot_temporal_valid, robot_process_valid, robot_dataset_valid] >> robot_fdos >> robot_fdos_valid
    valid_tasks.append(robot_fdos_valid)

    # ---------------------------------------------------------------------
    # New tasks (ported from server_spreadsheets_asserted), but NOT docker-specific.
    # They operate on the same sharedfs directory your original DAG already uses.
    # ---------------------------------------------------------------------

    def robot_prelude_bash() -> str:
        # Works whether robotcmd is "robot" or "java -jar ...".
        # Sanitizes whitespace/newlines to prevent accidental splitting.
        return f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'
        export ROBOT_JAVA_ARGS="{ROBOT_JAVA_ARGS}"
        ROBOT_CMD="{{{{ var.value.robotcmd }}}}"
        ROBOT_CMD="$(echo "$ROBOT_CMD" | tr '\\r\\n' ' ' | xargs)"
        if [ -z "$ROBOT_CMD" ]; then
          echo "[ERROR] robotcmd Airflow Variable is empty."
          exit 1
        fi
        read -r -a ROBOT_ARR <<< "$ROBOT_CMD"
        echo "Using ROBOT_CMD: $ROBOT_CMD"
        """

    merge_and_save_spreadsheets = BashOperator(
        task_id="merge_and_save_spreadsheets",
        bash_command=f"""
        {robot_prelude_bash()}

        DATA="{{{{ var.value.sharedfs }}}}"
        OUT_TTL="$DATA/spreadsheets_asserted.ttl"

        # Merge all generated component OWLs (skip base ontology.owl)
        "${{ROBOT_ARR[@]}}" merge --include-annotations true \\
          $(for f in "$DATA"/*.owl; do
              [ -e "$f" ] || continue
              [ "$(basename "$f")" = "ontology.owl" ] && continue
              echo -n " -i \\"$f\\""
            done) \\
          --output "$OUT_TTL"

        test -s "$OUT_TTL"
        """,
    )

    verify_sparql = BashOperator(
        task_id="verify_sparql",
        bash_command=f"""
        {robot_prelude_bash()}

        mkdir -p "{VALIDATION_DIR}"

        DATA_TTL="{{{{ var.value.sharedfs }}}}/spreadsheets_asserted.ttl"
        test -s "$DATA_TTL"

        QUERIES="{REPO_ROOT}/shapes/verify1.sparql"
        test -f "$QUERIES"

        "${{ROBOT_ARR[@]}}" verify --input "$DATA_TTL" --queries "$QUERIES" \\
          --output-dir "{VALIDATION_DIR}" -vvv > "{VALIDATION_DIR}/verify_sparql.md"

        test -s "{VALIDATION_DIR}/verify_sparql.md"
        """,
    )

    def shacl_task(n: int) -> BashOperator:
        return BashOperator(
            task_id=f"shacl{n}",
            bash_command=f"""
            set -eEuo pipefail
            IFS=$'\\n\\t'
            mkdir -p "{VALIDATION_DIR}"

            DATA="{{{{ var.value.sharedfs }}}}/spreadsheets_asserted.ttl"
            SHAPE="{REPO_ROOT}/shapes/shape{n}.ttl"
            OUT="{VALIDATION_DIR}/shape{n}.md"

            test -s "$DATA"
            test -f "$SHAPE"

            command -v pyshacl >/dev/null 2>&1 || (echo "[ERROR] pyshacl not installed" && exit 1)
            pyshacl -s "$SHAPE" -d "$DATA" > "$OUT" || true
            test -s "$OUT"
            """,
        )

    shacl2 = shacl_task(2)
    shacl3 = shacl_task(3)
    shacl4 = shacl_task(4)

    final_consistency_gate = BashOperator(
        task_id="final_consistency_gate",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'

        DATA="{{{{ var.value.sharedfs }}}}"

        # Re-check all component inconsistency reports are exact sentinel.
        bad=0
        for name in {" ".join(COMPONENT_NAMES)}; do
          md="$DATA/$name.md"
          owl="$DATA/$name.owl"
          if [ ! -s "$owl" ]; then
            echo "❌ Missing/empty OWL: $owl"
            bad=1
            continue
          fi
          if [ ! -s "$md" ]; then
            echo "❌ Missing/empty MD: $md"
            bad=1
            continue
          fi
          content="$(tr -d '\\r' < "$md" | sed -e :a -e '/^\\n*$/{{;$d;N;ba' -e '}}')"
          if [ "$content" != "No explanations found." ]; then
            echo "❌ Inconsistency found in: $md"
            cat "$md" || true
            bad=1
          fi
        done
        [ "$bad" -eq 0 ] || exit 1

        # SHACL gate: fail if common "non-conformance" tokens appear.
        VAL_DIR="{VALIDATION_DIR}"
        test -d "$VAL_DIR"
        if grep -RInE "(Conforms:\\s*False|SHACL\\s*Violation|Violations|violation)" "$VAL_DIR"/*.md 2>/dev/null; then
          echo "FAIL: SHACL violations detected."
          exit 1
        fi
        """,
    )

    publish = BashOperator(
        task_id="publish",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'

        DATA="{{{{ var.value.sharedfs }}}}"
        DEST="{PUBLISH_ROOT}"

        mkdir -p "$DEST"

        # Copy main artifacts
        cp -a "$DATA"/*.owl "$DEST/" 2>/dev/null || true
        cp -a "$DATA"/*.tsv "$DEST/" 2>/dev/null || true
        cp -a "$DATA"/*.md  "$DEST/" 2>/dev/null || true
        cp -a "$DATA"/spreadsheets_asserted.ttl "$DEST/" 2>/dev/null || true

        # Copy validation reports
        if [ -d "{VALIDATION_DIR}" ]; then
          mkdir -p "$DEST/validation"
          cp -a "{VALIDATION_DIR}/." "$DEST/validation/"
        fi

        echo "Published artifacts to: $DEST"
        """,
    )

    # Wire the new tasks exactly as requested:
    #   merge_and_save_spreadsheets >> verify_sparql
    #   merge_and_save_spreadsheets >> [shacl2, shacl3, shacl4]
    #   [verify_sparql, shacl2, shacl3, shacl4] >> final_consistency_gate >> publish
    valid_tasks >> merge_and_save_spreadsheets
    merge_and_save_spreadsheets >> verify_sparql
    merge_and_save_spreadsheets >> [shacl2, shacl3, shacl4]
    [verify_sparql, shacl2, shacl3, shacl4] >> final_consistency_gate >> publish


process_spreadsheets()
