from __future__ import annotations

from datetime import datetime
from pathlib import Path
import os

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow.models import Variable


# Assumes repo layout: <repo_root>/dags/<this_file>
REPO_ROOT = str(Path(__file__).resolve().parents[1])

RUN_ID_SAFE = "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
RUN_DIR = "{{ var.value.sharedfs }}/runs/server_spreadsheets_asserted/" + RUN_ID_SAFE

# Publish location on server filesystem
PUBLISH_ROOT = "{{ var.value.sharedfs }}/output/server_spreadsheets_asserted/" + RUN_ID_SAFE

# Read robotcmd at parse-time; strip whitespace/newlines to prevent "robot" running alone.
ROBOT_CMD = Variable.get("robotcmd", default_var="robot")
ROBOT_CMD = " ".join(ROBOT_CMD.replace("\r", " ").replace("\n", " ").split()).strip()

# Hard default to avoid VARIABLE_NOT_FOUND
ROBOT_JAVA_ARGS = "-Xmx16G -Dfile.encoding=UTF-8"


def validate_md_exact_sentinel(md_path: str, owl_path: str) -> None:
    if not (os.path.isfile(owl_path) and os.path.getsize(owl_path) > 0):
        raise ValueError(f"Missing/empty OWL: {owl_path}")
    if not (os.path.isfile(md_path) and os.path.getsize(md_path) > 0):
        raise ValueError(f"Missing/empty MD: {md_path}")

    with open(md_path, "rb") as f:
        text = f.read().decode("utf-8", errors="replace").replace("\r", "")
    text = text.rstrip("\n")
    if text != "No explanations found.":
        raise ValueError(f"Inconsistency detected; report {md_path} != exact sentinel.")


with DAG(
    dag_id="server_spreadsheets_asserted",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1},
    tags=["kg", "spreadsheets"],
) as dag:

    init_run_dir = BashOperator(
        task_id="init_run_dir",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'
        mkdir -p "{RUN_DIR}/data/components/reasoner"
        mkdir -p "{RUN_DIR}/data/validation"
        """,
    )

    TSVS = [
        {"name": "req_1", "gid": "394894036"},
        {"name": "req_2", "gid": "0"},
        {"name": "agent", "gid": "2077140060"},
        {"name": "role", "gid": "1425127117"},
        {"name": "process", "gid": "1169992315"},
        {"name": "city", "gid": "1469482382"},
        {"name": "people", "gid": "1666156492"},
        {"name": "organization", "gid": "447157523"},
        {"name": "dataset", "gid": "1079878268"},
        {"name": "publication", "gid": "1747331228"},
        {"name": "software", "gid": "1275685399"},
        {"name": "dataportal", "gid": "923160190"},
        {"name": "instrument", "gid": "2015927839"},
        {"name": "largescalefacility", "gid": "370181939"},
        {"name": "metadata", "gid": "278046522"},
        {"name": "matwerkta", "gid": "1489640604"},
        {"name": "matwerkiuc", "gid": "281962521"},
        {"name": "matwerkpp", "gid": "606786541"},
        {"name": "temporal", "gid": "1265818056"},
        {"name": "event", "gid": "638946284"},
        {"name": "collaboration", "gid": "266847052"},
        {"name": "service", "gid": "130394813"},
        {"name": "sparql_endpoints", "gid": "1732373290"},
        {"name": "FDOs", "gid": "152649677"},
    ]

    download_ontology_and_tsvs = BashOperator(
        task_id="download_ontology_and_tsvs",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'
        mkdir -p "{RUN_DIR}/data/components"

        curl -fsSL \\
          "https://raw.githubusercontent.com/ISE-FIZKarlsruhe/mwo/refs/tags/v3.0.0/mwo.owl" \\
          -o "{RUN_DIR}/data/components/ontology.owl"
        test -s "{RUN_DIR}/data/components/ontology.owl"

        for item in {" ".join([f"{t['name']}::{t['gid']}" for t in TSVS])}; do
          name="${{item%%::*}}"
          gid="${{item##*::}}"
          url="https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=${{gid}}&single=true&output=tsv"
          curl -fsSL "$url" -o "{RUN_DIR}/data/components/${{name}}.tsv"
          test -s "{RUN_DIR}/data/components/${{name}}.tsv"
        done
        """,
    )

    COMPONENT_DEPS = {
        "req_1": [],
        "req_2": ["req_1"],
        "agent": ["req_2"],
        "role": ["req_2", "agent"],
        "process": ["req_2", "agent", "role"],
        "city": ["req_1", "req_2"],
        "organization": ["req_1", "req_2", "city"],
        "people": ["req_1", "req_2", "organization"],
        "publication": ["req_2", "agent", "process"],
        "software": ["req_1", "req_2", "publication", "process"],
        "dataportal": ["req_1", "req_2", "publication", "organization", "process"],
        "dataset": ["req_1", "req_2", "organization", "agent", "role", "process"],
        "instrument": ["req_1", "req_2", "publication", "organization", "agent", "role", "process"],
        "largescalefacility": ["req_1", "req_2", "publication", "organization", "agent", "role", "process"],
        "metadata": ["req_1", "req_2", "publication", "organization", "process"],
        "matwerkta": ["req_1", "req_2", "organization", "agent", "role", "process"],
        "matwerkiuc": ["req_1", "req_2", "matwerkta", "organization", "agent", "role", "process"],
        "matwerkpp": ["req_1", "req_2", "matwerkta", "matwerkiuc", "organization", "agent", "role", "process"],
        "temporal": ["req_1", "req_2", "publication", "organization", "process"],
        "event": ["req_1", "req_2", "organization", "temporal", "agent", "role", "process"],
        "collaboration": ["req_1", "req_2", "temporal", "organization", "process"],
        "service": ["req_1", "req_2", "organization", "temporal", "agent", "role", "process"],
        "sparql_endpoints": ["req_1", "req_2", "organization", "temporal", "agent", "role", "process"],
        "FDOs": ["req_1", "req_2", "organization", "temporal", "agent", "role", "process", "dataset"],
    }

    def robot_prelude() -> str:
        # Convert ROBOT_CMD into argv[] safely (handles "java -jar ...", etc.)
        return f"""
        export ROBOT_JAVA_ARGS="{ROBOT_JAVA_ARGS}"
        ROBOT_CMD="{ROBOT_CMD}"
        # Split ROBOT_CMD into an argv array
        read -r -a ROBOT_ARR <<< "$ROBOT_CMD"
        echo "Using ROBOT_CMD: $ROBOT_CMD"
        """

    def robot_build_cmd(name: str, deps: list[str]) -> str:
        if name == "req_1":
            inputs = f'-i "{RUN_DIR}/data/components/ontology.owl"'
        else:
            inputs = " ".join([f'-i "{RUN_DIR}/data/components/{d}.owl"' for d in deps])

        return f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'
        {robot_prelude()}

        mkdir -p "{RUN_DIR}/data/components/reasoner"

        "${{ROBOT_ARR[@]}}" merge --include-annotations true {inputs} \\
          template --merge-before --template "{RUN_DIR}/data/components/{name}.tsv" \\
          --output "{RUN_DIR}/data/components/{name}.owl"

        test -s "{RUN_DIR}/data/components/{name}.owl"

        "${{ROBOT_ARR[@]}}" explain --reasoner hermit --input "{RUN_DIR}/data/components/{name}.owl" \\
          -M inconsistency --explanation "{RUN_DIR}/data/components/reasoner/{name}_inconsistency.md"

        test -s "{RUN_DIR}/data/components/reasoner/{name}_inconsistency.md"
        """

    with TaskGroup(group_id="build_components") as build_components:
        build_tasks = {}
        validate_tasks = {}

        for name, deps in COMPONENT_DEPS.items():
            build = BashOperator(
                task_id=f"build_{name}",
                bash_command=robot_build_cmd(name, deps),
            )
            validate = PythonOperator(
                task_id=f"validate_{name}",
                python_callable=validate_md_exact_sentinel,
                op_kwargs={
                    "md_path": f"{RUN_DIR}/data/components/reasoner/{name}_inconsistency.md",
                    "owl_path": f"{RUN_DIR}/data/components/{name}.owl",
                },
            )
            build >> validate
            build_tasks[name] = build
            validate_tasks[name] = validate

        for name, deps in COMPONENT_DEPS.items():
            for dep in deps:
                validate_tasks[dep] >> build_tasks[name]

    merge_and_save_spreadsheets = BashOperator(
        task_id="merge_and_save_spreadsheets",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'
        {robot_prelude()}

        OUT_TTL="{RUN_DIR}/data/spreadsheets_asserted.ttl"

        "${{ROBOT_ARR[@]}}" merge --include-annotations true \\
          $(for f in "{RUN_DIR}/data/components"/*.owl; do
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
        set -eEuo pipefail
        IFS=$'\\n\\t'
        {robot_prelude()}

        mkdir -p "{RUN_DIR}/data/validation"
        DATA_TTL="{RUN_DIR}/data/spreadsheets_asserted.ttl"
        test -s "$DATA_TTL"

        QUERIES="{REPO_ROOT}/shapes/verify1.sparql"
        test -f "$QUERIES"

        "${{ROBOT_ARR[@]}}" verify --input "$DATA_TTL" --queries "$QUERIES" \\
          --output-dir "{RUN_DIR}/data/validation/" -vvv > "{RUN_DIR}/data/validation/verify_sparql.md"

        test -s "{RUN_DIR}/data/validation/verify_sparql.md"
        """,
    )

    def shacl_task(n: int) -> BashOperator:
        return BashOperator(
            task_id=f"shacl_shape{n}",
            bash_command=f"""
            set -eEuo pipefail
            IFS=$'\\n\\t'
            mkdir -p "{RUN_DIR}/data/validation"

            DATA="{RUN_DIR}/data/spreadsheets_asserted.ttl"
            SHAPE="{REPO_ROOT}/shapes/shape{n}.ttl"
            OUT="{RUN_DIR}/data/validation/shape{n}.md"

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

        RUN_DIR="{RUN_DIR}"

        REPORT_DIR="$RUN_DIR/data/components/reasoner"
        test -d "$REPORT_DIR"

        bad=0
        for f in "$REPORT_DIR"/*.md; do
          [ -e "$f" ] || continue
          content="$(tr -d '\\r' < "$f" | sed -e :a -e '/^\\n*$/{{;$d;N;ba' -e '}}')"
          if [ "$content" != "No explanations found." ]; then
            echo "❌ Inconsistency found in: $f"
            cat "$f" || true
            bad=1
          fi
        done
        [ "$bad" -eq 0 ] || exit 1

        VAL_DIR="$RUN_DIR/data/validation"
        test -d "$VAL_DIR"
        if grep -RInE "(Conforms:\\s*False|SHACL\\s*Violation|Violations|violation)" "$VAL_DIR"/*.md 2>/dev/null; then
          echo "FAIL: SHACL violations detected."
          exit 1
        fi
        """,
    )

    publish = BashOperator(
        task_id="publish_run_artifacts",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'

        SRC="{RUN_DIR}"
        DEST="{PUBLISH_ROOT}"

        mkdir -p "$DEST"
        cp -a "$SRC/." "$DEST/"

        echo "Published run artifacts to: $DEST"
        """,
    )

    init_run_dir >> download_ontology_and_tsvs >> build_components >> merge_and_save_spreadsheets
    merge_and_save_spreadsheets >> verify_sparql
    merge_and_save_spreadsheets >> [shacl2, shacl3, shacl4]
    [verify_sparql, shacl2, shacl3, shacl4] >> final_consistency_gate >> publish
