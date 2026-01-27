# pipeline/common.py
from __future__ import annotations

import os
import shlex
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


@dataclass(frozen=True)
class CmdResult:
    returncode: int
    stdout: str
    stderr: str


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def atomic_write_text(path: str | Path, text: str, encoding: str = "utf-8") -> None:
    path = Path(path)
    ensure_dir(path.parent)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent), encoding=encoding) as tmp:
        tmp.write(text)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def run_cmd(
    cmd: Sequence[str],
    *,
    cwd: str | Path | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
    capture: bool = True,
    text: bool = True,
) -> CmdResult:
    """
    Run a subprocess command with strong defaults suitable for pipelines.

    - check=True raises on non-zero exit
    - capture=True captures stdout/stderr for logging
    """
    cmd_str = " ".join(shlex.quote(c) for c in cmd)
    print(f"[pipeline] RUN: {cmd_str}", file=sys.stderr)

    merged_env = os.environ.copy()
    if env:
        merged_env.update({k: str(v) for k, v in env.items()})

    proc = subprocess.run(
        list(cmd),
        cwd=str(cwd) if cwd else None,
        env=merged_env,
        check=False,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        text=text,
    )

    out = proc.stdout if proc.stdout is not None else ""
    err = proc.stderr if proc.stderr is not None else ""
    if proc.returncode != 0 and check:
        raise subprocess.CalledProcessError(proc.returncode, cmd, output=out, stderr=err)
    return CmdResult(proc.returncode, out, err)


def assert_nonempty_file(path: str | Path, label: str | None = None) -> None:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"{label or 'Output'} does not exist: {p}")
    if p.stat().st_size <= 0:
        raise RuntimeError(f"{label or 'Output'} is empty: {p}")


def replace_in_file(path: str | Path, needle: str, repl: str) -> None:
    p = Path(path)
    data = p.read_text(encoding="utf-8", errors="replace")
    if needle not in data:
        return
    p.write_text(data.replace(needle, repl), encoding="utf-8")


def glob_files(dir_path: str | Path, pattern: str) -> list[Path]:
    d = Path(dir_path)
    return sorted(d.glob(pattern))


def robot_query_count_triples(
    *,
    robot_bin: str,
    input_file: str | Path,
    count_query_path: str | Path,
    out_tsv: str | Path,
) -> int:
    """
    Uses ROBOT query with a count-triples.rq query that outputs a TSV with a header row.
    """
    out_tsv = Path(out_tsv)
    ensure_dir(out_tsv.parent)

    run_cmd(
        [
            robot_bin,
            "query",
            "-i",
            str(input_file),
            "--query",
            str(count_query_path),
            str(out_tsv),
        ],
        check=True,
    )

    lines = out_tsv.read_text(encoding="utf-8", errors="replace").splitlines()
    if len(lines) < 2:
        raise RuntimeError(f"Unexpected triple count TSV format in {out_tsv}")
    # Expect tab-separated, take first column of second row
    parts = lines[1].split("\t")
    try:
        return int(parts[0])
    except ValueError as e:
        raise RuntimeError(f"Could not parse triple count from {out_tsv}: {lines[1]}") from e
