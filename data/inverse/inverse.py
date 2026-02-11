import subprocess
from rdflib import Graph, Namespace, URIRef
from pathlib import Path

# Define paths relative to the script location
base_dir = Path(__name__).resolve().parent.parent

all_ttl_path = base_dir / "all.ttl"
inverse_ttl_path = base_dir / "inverse" / "inverse_all.ttl"

# File paths
inverse_file = "inverse-pairs.txt"
output_file = "inverse_all.ttl"
all_file = "../all.ttl"

# Namespaces
OWL = Namespace("http://www.w3.org/2002/07/owl#")

# Load the graph
g = Graph()
g.parse(all_ttl_path, format="turtle")

# Extract owl:inverseOf triples
inverse_triples = [
    (str(p), str(inv))
    for p, _, inv in g.triples((None, OWL.inverseOf, None))
    if isinstance(p, URIRef) and isinstance(inv, URIRef)
]

# Write to inverse-pairs.txt as TSV
with open(inverse_file, "w", encoding="utf-8") as f:
    f.write("p\tinv\n")
    for p, inv in inverse_triples:
        f.write(f"<{p}>\t<{inv}>\n")

print(f"✓ Found {len(inverse_triples)} inverse pairs in all.ttl")

# Initialize or clear output file
Path(output_file).write_text("")

# Loop through inverse pairs and materialize inverses
inverse_graph = Graph()
for i, (p1, p2) in enumerate(inverse_triples, 1):
    p1_uri = URIRef(p1)
    p2_uri = URIRef(p2)

    for subj, _, obj in g.triples((None, p1_uri, None)):
        inverse_graph.add((obj, p2_uri, subj))  # r1 p1 r2 → r2 p2 r1

    for subj, _, obj in g.triples((None, p2_uri, None)):
        inverse_graph.add((obj, p1_uri, subj))  # r2 p2 r1 → r1 p1 r2

    print(f"✓ Processed inverse pair {i}: {p1} ⇄ {p2}")

# Write the inferred triples to output TTL file
inverse_graph.serialize(destination=inverse_ttl_path, format="turtle")
print(f"✓ Inferred inverse triples written to {inverse_ttl_path}")

# Merge using ROBOT
subprocess.run([
    "docker", "run", "--rm",
    "-v", f"{base_dir}:/work",
    "-w", "/work",
    "-e", "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8",
    "obolibrary/robot",  # assuming %ROBOT_IMAGE% = "robotframework/robot"
    "robot", "merge",
    "--input", "all.ttl",
    "--input", "inverse/inverse_all.ttl",
    "--output", "all.ttl"
])
