from __future__ import annotations

from datetime import datetime
from pathlib import Path
import os

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Repo root = dags/.. (assumes dags folder inside repo)
REPO_ROOT = str(Path(__file__).resolve().parents[1])

RUN_ID_SAFE = "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
RUN_DIR = "{{ var.value.sharedfs }}/runs/server_spreadsheets_asserted/" + RUN_ID_SAFE

OUT_ROOT = "{{ var.value.output_dir if var.value.output_dir is defined else (var.value.sharedfs ~ '/output') }}"
ROBOT = "{{ var.value.robotcmd }}"  # same pattern as your server DAG
ROBOT_JAVA_ARGS_DEFAULT = "-Xmx16G -Dfile.encoding=UTF-8"


def _validate_md_exact_sentinel(md_path: str, owl_path: str) -> None:
    # Strong validation: OWL exists+nonempty, MD exists+nonempty, and MD content is exact sentinel
    if not (os.path.isfile(owl_path) and os.path.getsize(owl_path) > 0):
        raise ValueError(f"Missing/empty OWL: {owl_path}")
    if not (os.path.isfile(md_path) and os.path.getsize(md_path) > 0):
        raise ValueError(f"Missing/empty MD: {md_path}")

    with open(md_path, "rb") as f:
        raw = f.read()
    # normalize CRLF and trim trailing blank lines
    text = raw.decode("utf-8", errors="replace").replace("\r", "")
    text = text.rstrip("\n")
    if text != "No explanations found.":
        raise ValueError(f"Inconsistency detected, expected exact sentinel in {md_path}.")


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
        RUN_DIR="{RUN_DIR}"
        mkdir -p "$RUN_DIR/data/components/reasoner"
        mkdir -p "$RUN_DIR/data/validation"
        """,
    )

    TSVS = [
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
        ("FDOs", "152649677"),
    ]

    download_ontology_and_tsvs = BashOperator(
        task_id="download_ontology_and_tsvs",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'

        RUN_DIR="{RUN_DIR}"
        mkdir -p "$RUN_DIR/data/components"

        # base ontology
        curl -fsSL \
          "https://raw.githubusercontent.com/ISE-FIZKarlsruhe/mwo/refs/tags/v3.0.0/mwo.owl" \
          -o "$RUN_DIR/data/components/ontology.owl"
        test -s "$RUN_DIR/data/components/ontology.owl"

        # TSV templates
        {chr(10).join([
            f'''
        curl -fsSL "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid={gid}&single=true&output=tsv" \
          -o "$RUN_DIR/data/components/{name}.tsv"
        test -s "$RUN_DIR/data/components/{name}.tsv"
            '''.rstrip()
            for name, gid in TSVS
        ])}
        """,
    )

    COMPONENT_DEPS: dict[str, list[str]] = {
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

    def _robot_build_cmd(name: str, deps: list[str]) -> str:
        # Inputs: req_1 uses base ontology; others use dep OWLs
        if name == "req_1":
            inputs = f'-i "$RUN_DIR/data/components/ontology.owl"'
        else:
            inputs = " ".join([f'-i "$RUN_DIR/data/components/{d}.owl"' for d in deps])

        return f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'

        RUN_DIR="{RUN_DIR}"

        # Optional: allow overriding java args via Airflow Variable ROBOT_JAVA_ARGS
        ROBOT_JAVA_ARGS_VAR="{{{{ var.value.ROBOT_JAVA_ARGS if var.value.ROBOT_JAVA_ARGS is defined else '' }}}}"
        if [ -n "$ROBOT_JAVA_ARGS_VAR" ]; then
          export ROBOT_JAVA_ARGS="$ROBOT_JAVA_ARGS_VAR"
        else
          export ROBOT_JAVA_ARGS="{ROBOT_JAVA_ARGS_DEFAULT}"
        fi

        mkdir -p "$RUN_DIR/data/components/reasoner"

        {ROBOT} merge --include-annotations true {inputs} \\
          template --merge-before --template "$RUN_DIR/data/components/{name}.tsv" \\
          --output "$RUN_DIR/data/components/{name}.owl"
        test -s "$RUN_DIR/data/components/{name}.owl"

        {ROBOT} explain --reasoner hermit --input "$RUN_DIR/data/components/{name}.owl" \\
          -M inconsistency --explanation "$RUN_DIR/data/components/reasoner/{name}_inconsistency.md"
        test -s "$RUN_DIR/data/components/reasoner/{name}_inconsistency.md"
        """

    with TaskGroup(group_id="build_components") as build_components:
        build_tasks = {}
        validate_tasks = {}

        for name, deps in COMPONENT_DEPS.items():
            build = BashOperator(
                task_id=f"build_{name}",
                bash_command=_robot_build_cmd(name, deps),
            )

            validate = PythonOperator(
                task_id=f"validate_{name}",
                python_callable=_validate_md_exact_sentinel,
                op_kwargs={
                    "md_path": f"{RUN_DIR}/data/components/reasoner/{name}_inconsistency.md",
                    "owl_path": f"{RUN_DIR}/data/components/{name}.owl",
                },
            )

            build >> validate
            build_tasks[name] = build
            validate_tasks[name] = validate

        # Dependency wiring: downstream builds wait on upstream validates
        for name, deps in COMPONENT_DEPS.items():
            for dep in deps:
                validate_tasks[dep] >> build_tasks[name]

    merge_and_save_spreadsheets = BashOperator(
        task_id="merge_and_save_spreadsheets",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'

        RUN_DIR="{RUN_DIR}"

        OUT_TTL="$RUN_DIR/data/spreadsheets_asserted.ttl"

        # Merge all component OWLs except ontology.owl
        INPUTS=""
        for f in "$RUN_DIR/data/components"/*.owl; do
          [ "$(basename "$f")" = "ontology.owl" ] && continue
          INPUTS="$INPUTS -i \\"$f\\""
        done

        {ROBOT} merge --include-annotations true $INPUTS --output "$OUT_TTL"
        test -s "$OUT_TTL"

        # Graph IRI
        TS="$(date +%s%3N)"
        GRAPH_BASE="https://purls.helmholtz-metadaten.de/msekg/"
        G="$GRAPH_BASE$TS"
        echo "$G" > "$RUN_DIR/data/spreadsheets_graph_iri.txt"

        # Provenance
        NOW_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        DAG_ID="{{{{ dag.dag_id }}}}"
        RUN_ID="{{{{ dag_run.run_id }}}}"
        TASK_ID="{{{{ ti.task_id }}}}"
        LOG_URL="{{{{ ti.log_url }}}}"

        PROV_TTL="$RUN_DIR/data/spreadsheets_provenance.ttl"
        cat > "$PROV_TTL" <<EOF
@prefix dct: <http://purl.org/dc/terms/> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix mse: <https://purls.helmholtz-metadaten.de/msekg/vocab/> .

[] a prov:Activity ;
  dct:created "$NOW_ISO"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;
  mse:airflowDagId "$DAG_ID" ;
  mse:airflowRunId "$RUN_ID" ;
  mse:airflowTaskId "$TASK_ID" ;
  mse:producedGraph <$G> ;
  mse:producedFile "spreadsheets_asserted.ttl" ;
  rdfs:seeAlso <$LOG_URL> .
EOF
        test -s "$PROV_TTL"
        """,
    )

    verify_sparql = BashOperator(
        task_id="verify_sparql",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'

        RUN_DIR="{RUN_DIR}"

        DATA_TTL="$RUN_DIR/data/spreadsheets_asserted.ttl"
        test -s "$DATA_TTL"

        QUERIES="{REPO_ROOT}/shapes/verify1.sparql"
        test -f "$QUERIES"

        {ROBOT} verify --input "$DATA_TTL" --queries "$QUERIES" \\
          --output-dir "$RUN_DIR/data/validation/" -vvv > "$RUN_DIR/data/validation/verify_sparql.md"

        test -s "$RUN_DIR/data/validation/verify_sparql.md"
        """,
    )

    shacl_shape2 = BashOperator(
        task_id="shacl_shape2",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'

        RUN_DIR="{RUN_DIR}"
        DATA="$RUN_DIR/data/spreadsheets_asserted.ttl"
        SHAPE="{REPO_ROOT}/shapes/shape2.ttl"
        OUT="$RUN_DIR/data/validation/shape2.md"

        test -s "$DATA"
        test -f "$SHAPE"

        command -v pyshacl >/dev/null 2>&1 || (echo "[ERROR] pyshacl not installed" && exit 1)
        pyshacl -s "$SHAPE" -d "$DATA" > "$OUT" || true
        test -s "$OUT"
        """,
    )
    shacl_shape3 = BashOperator(
        task_id="shacl_shape3",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'

        RUN_DIR="{RUN_DIR}"
        DATA="$RUN_DIR/data/spreadsheets_asserted.ttl"
        SHAPE="{REPO_ROOT}/shapes/shape3.ttl"
        OUT="$RUN_DIR/data/validation/shape3.md"

        test -s "$DATA"
        test -f "$SHAPE"

        command -v pyshacl >/dev/null 2>&1 || (echo "[ERROR] pyshacl not installed" && exit 1)
        pyshacl -s "$SHAPE" -d "$DATA" > "$OUT" || true
        test -s "$OUT"
        """,
    )
    shacl_shape4 = BashOperator(
        task_id="shacl_shape4",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'

        RUN_DIR="{RUN_DIR}"
        DATA="$RUN_DIR/data/spreadsheets_asserted.ttl"
        SHAPE="{REPO_ROOT}/shapes/shape4.ttl"
        OUT="$RUN_DIR/data/validation/shape4.md"

        test -s "$DATA"
        test -f "$SHAPE"

        command -v pyshacl >/dev/null 2>&1 || (echo "[ERROR] pyshacl not installed" && exit 1)
        pyshacl -s "$SHAPE" -d "$DATA" > "$OUT" || true
        test -s "$OUT"
        """,
    )

    final_consistency_gate = BashOperator(
        task_id="final_consistency_gate",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'

        RUN_DIR="{RUN_DIR}"
        REPORT_DIR="$RUN_DIR/data/components/reasoner"
        VAL_DIR="$RUN_DIR/data/validation"

        test -d "$REPORT_DIR"
        test -d "$VAL_DIR"

        echo "=== Checking component inconsistency reports ==="
        bad=0
        for f in "$REPORT_DIR"/*.md; do
          [ -e "$f" ] || continue
          content="$(tr -d '\\r' < "$f" | sed -e :a -e '/^\\n*$/{{;$d;N;ba' -e '}}')"
          if [ "$content" != "No explanations found." ]; then
            echo "❌ Inconsistency found in: $f"
            echo "-----"
            cat "$f" || true
            echo "-----"
            bad=1
          fi
        done
        if [ "$bad" -eq 1 ]; then
          echo "FAIL: At least one component has inconsistencies."
          exit 1
        fi
        echo "OK: No component inconsistencies found."

        echo "=== Checking SHACL validation outputs ==="
        if grep -RInE "(Conforms:\\s*False|SHACL\\s*Violation|Violations|violation)" "$VAL_DIR"/*.md 2>/dev/null; then
          echo "FAIL: SHACL violations detected."
          exit 1
        fi
        echo "OK: No SHACL violations detected (by report scan)."

        echo "=== Final gate passed ==="
        """,
    )

    publish = BashOperator(
        task_id="publish_run_artifacts",
        bash_command=f"""
        set -eEuo pipefail
        IFS=$'\\n\\t'

        RUN_DIR="{RUN_DIR}"
        RUN_ID_SAFE="{RUN_ID_SAFE}"

        DEST="{OUT_ROOT}/server_spreadsheets_asserted/${{RUN_ID_SAFE}}"
        mkdir -p "$DEST"
        cp -a "$RUN_DIR/." "$DEST/"
        echo "Published run artifacts to: $DEST"
        find "$DEST" -maxdepth 4 -type f | sed -n '1,200p'
        """,
    )

    # Wiring
    init_run_dir >> download_ontology_and_tsvs >> build_components >> merge_and_save_spreadsheets

    merge_and_save_spreadsheets >> verify_sparql
    merge_and_save_spreadsheets >> [shacl_shape2, shacl_shape3, shacl_shape4]
    [verify_sparql, shacl_shape2, shacl_shape3, shacl_shape4] >> final_consistency_gate >> publish
