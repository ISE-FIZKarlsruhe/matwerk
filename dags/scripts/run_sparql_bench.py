#!/usr/bin/env python3
import argparse, os, time, csv, re
from pathlib import Path
from rdflib import Graph

try:
    from owlrl import DeductiveClosure, RDFS_Semantics, OWLRL_Semantics
    HAS_OWL = True
except Exception:
    HAS_OWL = False

def load_graph(p):
    g = Graph()
    try:
        g.parse(p, format="turtle")
    except Exception:
        g.parse(p)
    return g

def reason(g, mode):
    if mode == "none" or not HAS_OWL:
        return
    if mode == "rdfs":
        DeductiveClosure(RDFS_Semantics).expand(g)
    else:
        try:
            DeductiveClosure(OWLRL_Semantics).expand(g)
        except TypeError:
            DeductiveClosure(OWLRL_Semantics).expand(g)

def first_comment_line(query_text: str) -> str | None:
    for line in query_text.splitlines():
        ls = line.strip()
        if not ls:
            continue
        if ls.startswith("#"):
            return ls.lstrip("#").strip()
    return None

def latex_escape(s: str) -> str:
    return (s.replace("\\", "\\textbackslash{}")
             .replace("&","\\&").replace("%","\\%").replace("$","\\$")
             .replace("#","\\#").replace("_","\\_")
             .replace("{","\\{").replace("}","\\}")
             .replace("~","\\textasciitilde{}").replace("^","\\textasciicircum{}"))

def run(g, q, n):
    t = []
    for _ in range(n):
        t0 = time.perf_counter()
        try:
            _ = list(g.query(q))
            t.append(time.perf_counter()-t0)
        except Exception as e:
            return f"exec-error:{type(e).__name__}", None
    return "ok", sum(t)/len(t)

def gather_queries(dirpath: Path):
    out = []
    for p in sorted(dirpath.glob("*.txt")):
        txt = p.read_text(encoding="utf-8")
        m = re.search(r"(\\d+)", p.stem)
        qid = m.group(1) if m else p.stem
        desc = first_comment_line(txt) or p.stem
        out.append((qid, p, desc, txt))
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kg1", required=True)
    ap.add_argument("--name1", default="BFO_MSE")
    ap.add_argument("--kg2", required=True)
    ap.add_argument("--name2", default="SCHEMA_MSE")
    ap.add_argument("--out", default="out_bench")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--reasoner", choices=["none","rdfs","owlrl"], default="none")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)

    g1 = load_graph(args.kg1); reason(g1, args.reasoner)
    g2 = load_graph(args.kg2); reason(g2, args.reasoner)

    bfo = gather_queries(Path("data")/"compare_kgs"/"sparql"/"BFO_MSE")
    schema = gather_queries(Path("data")/"compare_kgs"/"sparql"/"SCHEMA_MSE")

    bmap = {qid:(path, desc, txt) for qid, path, desc, txt in bfo}
    smap = {qid:(path, desc, txt) for qid, path, desc, txt in schema}
    all_ids = sorted(set(bmap.keys()) | set(smap.keys()), key=lambda x: int(x) if x.isdigit() else x)

    rows = []
    for qid in all_ids:
        p1 = bmap.get(qid)
        p2 = smap.get(qid)
        desc = (p1[1] if p1 else (p2[1] if p2 else qid))
        status1 = "missing"; avg1 = None
        status2 = "missing"; avg2 = None
        if p1:
            status1, avg1 = run(g1, p1[2], args.runs)
        if p2:
            status2, avg2 = run(g2, p2[2], args.runs)
        rows.append({
            "id": qid,
            "description": desc,
            f"{args.name1}_status": status1,
            f"{args.name1}_avg_sec": None if avg1 is None else round(avg1,6),
            f"{args.name2}_status": status2,
            f"{args.name2}_avg_sec": None if avg2 is None else round(avg2,6),
        })
        print(f"Q{qid}: {desc} :: {args.name1}={avg1} ({status1}), {args.name2}={avg2} ({status2})")

    csv_path = os.path.join(args.out, "sparql_benchmark.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        fields = ["id","description", f"{args.name1}_status", f"{args.name1}_avg_sec", f"{args.name2}_status", f"{args.name2}_avg_sec"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(rows)

    tex_path = os.path.join(args.out, "sparql_benchmark.tex")
    with open(tex_path, "w", encoding="utf-8") as f:
        f.write("\\n".join([
            "\\begin{table}[htbp]",
            "\\centering",
            "\\begin{tabular}{p{0.62\\linewidth}rr}",
            "\\hline",
            f"Query & {args.name1} (s) & {args.name2} (s)\\\\\\hline"
        ]))
        for r in rows:
            q = latex_escape(r["description"])
            a1 = r[f"{args.name1}_avg_sec"]; a2 = r[f"{args.name2}_avg_sec"]
            a1s = "" if a1 is None else f"{a1:.3f}"
            a2s = "" if a2 is None else f"{a2:.3f}"
            f.write(f"\n{q} & {a1s} & {a2s}\\\\")
        f.write("\n\\hline\n\\end{tabular}\n")
        f.write("\\caption{Average response times (seconds) for each SPARQL query, run 3 times per KG. The query label is the first comment line starting with \\# in each file.}\n")
        f.write("\\label{tab:sparql-benchmark}\n\\end{table}\n")

if __name__ == "__main__":
    main()
