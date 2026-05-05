import sys
import os
import subprocess
import time
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF, RDFS, OWL
from collections import defaultdict, deque
import xml.etree.ElementTree as ET

# ---------------------------------------------------------
# URI extraction (kept for SPARQL XML fragments if needed)
# ---------------------------------------------------------
def extract_uri(text: str):
    if "<uri>" not in text:
        return None
    try:
        uri = text.split("<uri>")[1].split("</uri>")[0].strip()
        return uri if "://" in uri else None
    except Exception:
        return None


# ---------------------------------------------------------
# SAFE PARSER FOR MULTI-DOCUMENT KONCLUDE XML
# ---------------------------------------------------------
def load_konclude_multixml(path):
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    # Remove all XML declarations
    cleaned = raw.replace("<?xml version=\"1.0\"?>", "")
    cleaned = cleaned.replace("<?xml version=\"1.0\" encoding=\"UTF-8\"?>", "")

    # Wrap in a single root
    wrapped = f"<root>{cleaned}</root>"

    return ET.fromstring(wrapped)


# ---------------------------------------------------------
# SILENT RUN
# ---------------------------------------------------------
def run_silent(cmd):
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)


# ---------------------------------------------------------
# PREPROCESS
# ---------------------------------------------------------
def preprocess(ontology, tmpfolder):
    print("[PRE] preprocessing...")

    g = Graph()
    g.parse(ontology)

    classes = {str(s) for s in g.subjects(RDF.type, OWL.Class) if isinstance(s, URIRef)}
    op_properties = {str(s) for s in g.subjects(RDF.type, OWL.ObjectProperty) if isinstance(s, URIRef)}
    dp_properties = {str(s) for s in g.subjects(RDF.type, OWL.DatatypeProperty) if isinstance(s, URIRef)}

    inverse_map = {}
    for p, _, q in g.triples((None, OWL.inverseOf, None)):
        if isinstance(p, URIRef) and isinstance(q, URIRef):
            inverse_map[str(p)] = str(q)
            inverse_map[str(q)] = str(p)

    prefix = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
"""

    os.makedirs(tmpfolder, exist_ok=True)

    # CLASSES
    with open(os.path.join(tmpfolder, "classes.sparql"), "w") as f:
        f.write(
            prefix + "\n".join(
                f'SELECT (IRI("{c}") as ?class) ?superclass WHERE {{ <{c}> rdfs:subClassOf ?superclass . }}'
                for c in classes
            )
        )

    # OBJECT PROPERTIES
    with open(os.path.join(tmpfolder, "oprops.sparql"), "w") as f:
        f.write(
            prefix + "\n".join(
                f'SELECT ?s (IRI("{op}") as ?op) ?o WHERE {{ ?s <{op}> ?o . }}'
                for op in op_properties
            )
        )

    # DATA PROPERTIES
    with open(os.path.join(tmpfolder, "dprops.sparql"), "w") as f:
        f.write(
            prefix + "\n".join(
                f'SELECT ?s (IRI("{dp}") as ?dp) ?val WHERE {{ ?s <{dp}> ?val . }}'
                for dp in dp_properties
            )
        )

    # OBJECT SUBPROPERTIES
    with open(os.path.join(tmpfolder, "osubprops.sparql"), "w") as f:
        f.write(
            prefix + "\n".join(
                f'SELECT (IRI("{op}") as ?op) ?superop WHERE {{ <{op}> rdfs:subPropertyOf ?superop . }}'
                for op in op_properties
            )
        )

    # CLASS ASSERTIONS (SPARQL)
    with open(os.path.join(tmpfolder, "class_assertions.sparql"), "w") as f:
        f.write(
            prefix + """
SELECT ?s ?type WHERE {
    ?s rdf:type ?type .
    FILTER(isIRI(?type))
}
"""
        )

    print("[PRE] done")
    return classes, op_properties, dp_properties, inverse_map


# ---------------------------------------------------------
# RUN KONCLUDE
# ---------------------------------------------------------
from concurrent.futures import ThreadPoolExecutor, as_completed


def run_one_job(binary, input_file, tmpfolder, job):
    cmd = [
        binary, "sparqlfile",
        "-s", os.path.join(tmpfolder, f"{job}.sparql"),
        "-o", os.path.join(tmpfolder, f"{job}.xml"),
        "-i", input_file
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    return job


def run_jobs(binary, input_file, tmpfolder):
    print("[RUN] executing konclude (parallel)...")

    jobs = ["classes", "oprops", "dprops", "osubprops", "class_assertions"]

    start = time.time()

    with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
        futures = [
            executor.submit(run_one_job, binary, input_file, tmpfolder, job)
            for job in jobs
        ]

        for f in as_completed(futures):
            print(f"[DONE] {f.result()}")

    print(f"[RUN] finished in {time.time() - start:.2f}s\n")


# ---------------------------------------------------------
# HIERARCHY
# ---------------------------------------------------------
def parse_hierarchy(file, sub_name, sup_name):
    pairs = set()
    if not os.path.exists(file):
        return pairs

    sub = sup = None

    with open(file) as f:
        for line in f:
            if f'binding name="{sub_name}"' in line:
                sub = extract_uri(line)
            elif f'binding name="{sup_name}"' in line:
                sup = extract_uri(line)
                if sub and sup and sub != sup:
                    pairs.add((sub, sup))

    return pairs


def compute_closure(pairs):
    g = defaultdict(set)
    nodes = set()

    for a, b in pairs:
        g[a].add(b)
        nodes.add(a)
        nodes.add(b)

    out = set()

    for n in nodes:
        vis = set()
        q = deque([n])

        while q:
            x = q.popleft()
            for y in g[x]:
                if y not in vis:
                    vis.add(y)
                    q.append(y)
                    out.add((n, y))

    return out


# ---------------------------------------------------------
# REALISATION PARSER (OWL/XML)
# ---------------------------------------------------------
def parse_realisation_owlxml(file, graph):
    if not os.path.exists(file):
        return

    print("[POST] parsing realisation (OWL/XML)...")

    tree = ET.parse(file)
    root = tree.getroot()

    for ca in root.findall(".//{http://www.w3.org/2002/07/owl#}ClassAssertion"):

        cls = ca.find(".//{*}Class")
        ind = ca.find(".//{*}NamedIndividual")

        if cls is None or ind is None:
            continue

        cls_iri = cls.attrib.get("IRI")
        ind_iri = ind.attrib.get("IRI")

        if cls_iri and ind_iri:
            graph.add((URIRef(ind_iri), RDF.type, URIRef(cls_iri)))


# ---------------------------------------------------------
# POSTPROCESS (FIXED DATA PROPERTY PARSER)
# ---------------------------------------------------------
def postprocess(outfile, tmp, classes, op_properties, dp_properties, inverse_map):
    print("[POST] building graph...")

    g = Graph()

    for op in op_properties:
        g.add((URIRef(op), RDF.type, OWL.ObjectProperty))
    for dp in dp_properties:
        g.add((URIRef(dp), RDF.type, OWL.DatatypeProperty))
    for p, q in inverse_map.items():
        g.add((URIRef(p), OWL.inverseOf, URIRef(q)))

    # OBJECT PROPERTIES
    oprops_file = os.path.join(tmp, "oprops.xml")
    if os.path.exists(oprops_file):
        with open(oprops_file, encoding='utf-8') as f:
            s = op = None
            for line in f:
                if 'binding name="s"' in line:
                    s = extract_uri(line)
                elif 'binding name="op"' in line:
                    op = extract_uri(line)
                elif 'binding name="o"' in line:
                    o = extract_uri(line)
                    if s and op and o:
                        g.add((URIRef(s), URIRef(op), URIRef(o)))

    # ---------------------------------------------------------
    # FIXED DATA PROPERTY PARSER (NAMESPACE-AWARE + MULTI-DOC SAFE)
    # ---------------------------------------------------------
    dprops_file = os.path.join(tmp, "dprops.xml")
    if os.path.exists(dprops_file):
        print("[POST] parsing data properties...")

        root = load_konclude_multixml(dprops_file)
        ns = {"sr": "http://www.w3.org/2005/sparql-results#"}

        for result in root.findall(".//sr:result", ns):

            s_val = dp_val = val_text = val_dtype = None

            for b in result.findall("sr:binding", ns):
                name = b.attrib.get("name")

                uri_node = b.find("sr:uri", ns)
                lit_node = b.find("sr:literal", ns)

                if uri_node is not None:
                    if name == "s":
                        s_val = uri_node.text.strip()
                    elif name == "dp":
                        dp_val = uri_node.text.strip()

                if lit_node is not None and name == "val":
                    val_text = lit_node.text.strip() if lit_node.text else ""
                    val_dtype = lit_node.attrib.get("datatype")

            if s_val and dp_val and val_text is not None:
                if val_dtype:
                    g.add((URIRef(s_val), URIRef(dp_val), Literal(val_text, datatype=URIRef(val_dtype))))
                else:
                    g.add((URIRef(s_val), URIRef(dp_val), Literal(val_text)))

    # CLASS ASSERTIONS (SPARQL)
    ca_file = os.path.join(tmp, "class_assertions.xml")
    if os.path.exists(ca_file):
        print("[POST] parsing class assertions...")
        s = t = None
        with open(ca_file, encoding='utf-8') as f:
            for line in f:
                if 'binding name="s"' in line:
                    s = extract_uri(line)
                elif 'binding name="type"' in line:
                    t = extract_uri(line)
                    if s and t:
                        g.add((URIRef(s), RDF.type, URIRef(t)))

    # REALISATION
    real_file = os.path.join(tmp, "realisation.owl")
    parse_realisation_owlxml(real_file, g)

    # HIERARCHY
    subclass_pairs = parse_hierarchy(os.path.join(tmp, "classes.xml"), "class", "superclass")
    for s, t in compute_closure(subclass_pairs):
        g.add((URIRef(s), RDFS.subClassOf, URIRef(t)))

    osub_pairs = parse_hierarchy(os.path.join(tmp, "osubprops.xml"), "op", "superop")
    for s, t in compute_closure(osub_pairs):
        g.add((URIRef(s), RDFS.subPropertyOf, URIRef(t)))

    print("[POST] writing output...")
    g.serialize(outfile, format="turtle")


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def koncludix(binary, input_file, output_file, work_dir):
    start = time.time()

    classes, op_properties, dp_properties, inverse_map = preprocess(
        input_file, work_dir
    )

    run_jobs(binary, input_file, work_dir)

    postprocess(
        output_file,
        work_dir,
        classes,
        op_properties,
        dp_properties,
        inverse_map
    )

    print(f"\nTOTAL TIME: {time.time() - start:.2f}s")


# ---------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python koncludix.py <konclude_binary> <input.owl> <output.ttl>")
    else:
        koncludix(sys.argv[1], sys.argv[2], sys.argv[3], "tmp")

