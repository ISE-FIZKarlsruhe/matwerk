from __future__ import annotations

import os

from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.hitl import HITLOperator

OUT_BASENAME = "spreadsheets_asserted"
IN_TTL = f"{OUT_BASENAME}.ttl"

MERGE_RUNS_ROOT = "{{ var.value.sharedfs }}/runs/merge_validation"
RUN_ID_SAFE = "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
RUN_DIR = "{{ var.value.sharedfs }}/runs/reason_latest_merge_validation/" + RUN_ID_SAFE

REASONER_OPTIONS = ["elk", "hermit", "jfact", "whelk", "emr", "structural"]

AXIOM_GENERATOR_OPTIONS = [
    "SubClass",
    "EquivalentClass",
    "DisjointClasses",
    "DataPropertyCharacteristic",
    "EquivalentDataProperties",
    "SubDataProperty",
    "ClassAssertion",
    "PropertyAssertion",
    "EquivalentObjectProperty",
    "InverseObjectProperties",
    "ObjectPropertyCharacteristic",
    "SubObjectProperty",
    "ObjectPropertyRange",
    "ObjectPropertyDomain",
    "NONE",  # special option to mean "no axiom generators"
]


@dag(
    schedule=None,
    catchup=False,
    tags=["ontology", "reason", "robot", "merge_validation"],
)
def reason_latest_merge_validation():

    @task()
    def assert_inputs():
        sharedfs = Variable.get("sharedfs")
        if not os.path.isdir(sharedfs):
            raise AirflowFailException(f"sharedfs does not exist or is not a directory: {sharedfs}")
        return {"sharedfs": sharedfs}

    init_run_dir = BashOperator(
        task_id="init_run_dir",
        bash_command=rf"""
set -euo pipefail
mkdir -p "{RUN_DIR}"
""",
    )

    # --- HITL: choose reasoner ---
    choose_reasoner = HITLOperator(
        task_id="choose_reasoner",
        subject="Select the reasoner for ROBOT reason",
        body="Pick exactly one reasoner. This value will be passed to `robot reason --reasoner`.",
        options=REASONER_OPTIONS,
        defaults=["elk"],
    )

    # --- HITL: choose axiom generators (0..n) ---
    choose_axiom_generators = HITLOperator(
        task_id="choose_axiom_generators",
        subject="Select axiom generators (optional)",
        body=(
            "Select zero or more axiom generators.\n\n"
            "If you select `NONE`, no `--axiom-generators` flag will be passed."
        ),
        options=AXIOM_GENERATOR_OPTIONS,
        multiple=True,
        defaults=["NONE"],
    )

    reason_latest = BashOperator(
        task_id="reason_latest",
        # Inject HITL choices into environment variables (templated)
        env={
            # single-choice returns list with one element
            "REASONER": "{{ ti.xcom_pull(task_ids='choose_reasoner')['chosen_options'][0] }}",
            # multi-choice: space-separated list, unless NONE selected => empty
            "AXIOM_GENERATORS": (
                "{% set opts = ti.xcom_pull(task_ids='choose_axiom_generators')['chosen_options'] %}"
                "{{ '' if 'NONE' in opts else (opts | join(' ')) }}"
            ),
        },
        bash_command=rf"""
set -eEuo pipefail

ROOT="{MERGE_RUNS_ROOT}"
OUT="{RUN_DIR}"

# ---- Human-in-the-loop selections (injected via env=) ----
REASONER="${{REASONER}}"
AXIOM_GENERATORS="${{AXIOM_GENERATORS:-}}"

# ---- Other options (Trigger DAG -> Config JSON) ----
INCLUDE_INDIRECT="{{{{ dag_run.conf.get('include_indirect', 'false') }}}}"
EXCLUDE_TAUTOLOGIES="{{{{ dag_run.conf.get('exclude_tautologies', 'false') }}}}"
EQUIV_ALLOWED="{{{{ dag_run.conf.get('equivalent_classes_allowed', 'all') }}}}"
ANNOTATE_INFERRED="{{{{ dag_run.conf.get('annotate_inferred_axioms', 'false') }}}}"
CREATE_NEW="{{{{ dag_run.conf.get('create_new_ontology', 'false') }}}}"
CREATE_NEW_WITH_ANN="{{{{ dag_run.conf.get('create_new_ontology_with_annotations', 'false') }}}}"
EXCLUDE_DUP="{{{{ dag_run.conf.get('exclude_duplicate_axioms', 'false') }}}}"
REMOVE_REDUNDANT="{{{{ dag_run.conf.get('remove_redundant_subclass_axioms', 'true') }}}}"
RUN_REDUCE="{{{{ dag_run.conf.get('run_reduce', 'true') }}}}"

# ---- Validate reasoner ----
case "$REASONER" in
  elk|hermit|jfact|whelk|emr|structural) ;;
  *)
    echo "[ERROR] Invalid reasoner: $REASONER" >&2
    echo "Allowed: elk hermit jfact whelk emr structural" >&2
    exit 2
    ;;
esac

# ---- Validate axiom generators ----
if [ -n "$AXIOM_GENERATORS" ]; then
  for g in $AXIOM_GENERATORS; do
    case "$g" in
      SubClass|EquivalentClass|DisjointClasses|DataPropertyCharacteristic|EquivalentDataProperties|SubDataProperty|ClassAssertion|PropertyAssertion|EquivalentObjectProperty|InverseObjectProperties|ObjectPropertyCharacteristic|SubObjectProperty|ObjectPropertyRange|ObjectPropertyDomain)
        ;;
      *)
        echo "[ERROR] Invalid axiom generator: $g" >&2
        exit 2
        ;;
    esac
  done
fi

# ---- Find latest TTL from merge_validation (prefer _SUCCESS, then newest mtime) ----
LATEST_RUN=""
LATEST_TTL=""

CANDIDATES="$(
  for d in "$ROOT"/*; do
    [ -d "$d" ] || continue

    ttl=""
    if [ -s "$d/{IN_TTL}" ]; then
      ttl="$d/{IN_TTL}"
    else
      ttl="$(ls -1 "$d"/{OUT_BASENAME}*.ttl 2>/dev/null | head -n 1 || true)"
      [ -n "$ttl" ] && [ -s "$ttl" ] || ttl=""
    fi
    [ -n "$ttl" ] || continue

    prio=1
    [ -f "$d/_SUCCESS" ] && prio=0
    mtime="$(stat -c '%Y' "$ttl" 2>/dev/null || echo 0)"
    printf '%s\t%s\t%s\t%s\n' "$prio" "$mtime" "$ttl" "$d"
  done
)"

if [ -n "$CANDIDATES" ]; then
  BEST="$(printf '%s\n' "$CANDIDATES" | sort -k1,1n -k2,2nr | head -n 1)"
  LATEST_TTL="$(printf '%s' "$BEST" | cut -f3)"
  LATEST_RUN="$(printf '%s' "$BEST" | cut -f4)"
fi

if [ -z "$LATEST_TTL" ]; then
  echo "[ERROR] No merge_validation TTL found under: $ROOT" >&2
  ls -lah "$ROOT" >&2 || true
  exit 1
fi

echo "$LATEST_RUN" > "$OUT/source_merge_validation_run_dir.txt"
echo "$LATEST_TTL" > "$OUT/source_ttl_path.txt"

echo "[INFO] Using input TTL: $LATEST_TTL"
echo "[INFO] HITL Options: reasoner=$REASONER axiom_generators='$AXIOM_GENERATORS' include_indirect=$INCLUDE_INDIRECT run_reduce=$RUN_REDUCE"

# ---- ROBOT command (argv-safe, NO global IFS hacks) ----
ROBOTCMD_RAW="{{{{ var.value.robotcmd }}}}"
if [ -z "$ROBOTCMD_RAW" ]; then
  echo "[ERROR] Airflow Variable robotcmd is empty" >&2
  exit 2
fi

# IMPORTANT: do NOT set IFS to newline/tab in this script.
# We need normal whitespace splitting here.
read -r -a ROBOTCMD <<< "$ROBOTCMD_RAW"

echo "[INFO] ROBOTCMD_RAW=$ROBOTCMD_RAW"
echo "[INFO] ROBOTCMD tokens:"
for i in "${{!ROBOTCMD[@]}}"; do
  echo "  ROBOTCMD[$i]=${{ROBOTCMD[$i]}}"
done

# Validate java -jar path if used
if [ "${{ROBOTCMD[0]}}" = "java" ] && [ "${{ROBOTCMD[1]:-}}" = "-jar" ]; then
  JAR="${{ROBOTCMD[2]:-}}"
  if [ -z "$JAR" ] || [ ! -f "$JAR" ]; then
    echo "[ERROR] robot.jar not found at: $JAR" >&2
    exit 2
  fi
fi

OUT_FILE="$OUT/{OUT_BASENAME}_reasoned.ttl"
LOG_FILE="$OUT/reason.log"

CMD=( "${{ROBOTCMD[@]}}" reason
  --input "$LATEST_TTL"
  --reasoner "$REASONER"
  --include-indirect "$INCLUDE_INDIRECT"
  --exclude-tautologies "$EXCLUDE_TAUTOLOGIES"
  --equivalent-classes-allowed "$EQUIV_ALLOWED"
  --annotate-inferred-axioms "$ANNOTATE_INFERRED"
  --exclude-duplicate-axioms "$EXCLUDE_DUP"
  --remove-redundant-subclass-axioms "$REMOVE_REDUNDANT"
  -vvv
)

if [ "$CREATE_NEW" = "true" ] && [ "$CREATE_NEW_WITH_ANN" = "true" ]; then
  echo "[ERROR] create_new_ontology and create_new_ontology_with_annotations cannot both be true" >&2
  exit 2
fi
if [ "$CREATE_NEW" = "true" ]; then
  CMD+=( --create-new-ontology true )
fi
if [ "$CREATE_NEW_WITH_ANN" = "true" ]; then
  CMD+=( --create-new-ontology-with-annotations true )
fi
if [ -n "$AXIOM_GENERATORS" ]; then
  CMD+=( --axiom-generators $AXIOM_GENERATORS )
fi

if [ "$RUN_REDUCE" = "true" ]; then
  echo "[INFO] Running: robot reason ... reduce"
  CMD+=( reduce --output "$OUT_FILE" )
else
  echo "[INFO] Running: robot reason"
  CMD+=( --output "$OUT_FILE" )
fi

set +e
"${{CMD[@]}}" >"$LOG_FILE" 2>&1
RC=$?
set -e

cat > "$OUT/reason_settings.txt" <<EOF
input_ttl=$LATEST_TTL
reasoner=$REASONER
axiom_generators=$AXIOM_GENERATORS
include_indirect=$INCLUDE_INDIRECT
exclude_tautologies=$EXCLUDE_TAUTOLOGIES
equivalent_classes_allowed=$EQUIV_ALLOWED
annotate_inferred_axioms=$ANNOTATE_INFERRED
create_new_ontology=$CREATE_NEW
create_new_ontology_with_annotations=$CREATE_NEW_WITH_ANN
exclude_duplicate_axioms=$EXCLUDE_DUP
remove_redundant_subclass_axioms=$REMOVE_REDUNDANT
run_reduce=$RUN_REDUCE
robotcmd_raw=$ROBOTCMD_RAW
EOF

if [ "$RC" -ne 0 ]; then
  echo "[ERROR] ROBOT reason failed (rc=$RC). See log: $LOG_FILE" >&2
  sed -n '1,200p' "$LOG_FILE" >&2 || true
  exit "$RC"
fi

test -s "$OUT_FILE"
echo "[INFO] Reasoned output: $OUT_FILE"
""",
    )

    inputs_ok = assert_inputs()

    # order:
    # assert -> init -> HITL choices -> reason_latest
    inputs_ok >> init_run_dir >> [choose_reasoner, choose_axiom_generators] >> reason_latest


reason_latest_merge_validation()
