import subprocess
from pathlib import Path

# Define paths relative to the script location
base_dir = Path(__name__).resolve().parent.parent

all_ttl_path = base_dir / "all.ttl"
inverse_ttl_path = base_dir / "inverse" / "inverse_all.ttl"

# File paths
inverse_file = "inverse-pairs.txt"
output_file = "inverse_all.ttl"
all_file = "../all.ttl"

# Read inverse pairs
with open(inverse_file, "r") as f:
    pairs = [line.strip().strip('<>').split('><') for line in f if line.strip()]

# Initialize or clear output file
Path(output_file).write_text("")

# Loop through pairs and query both directions
for i, (p1, p2) in enumerate(pairs, 1):
    for subj, pred in [(p1, p2), (p2, p1)]:
        # SPARQL query
        sparql = f"""
        CONSTRUCT {{
            ?s <{subj}> ?o .
        }}
        WHERE {{
            ?s <{pred}> ?o .
        }}
        """

        # URL encode query
        encoded_query = subprocess.run(
            ["python3", "-c", f"import urllib.parse; print(urllib.parse.quote('''{sparql}'''))"],
            capture_output=True, text=True).stdout.strip()

        # cURL command
        curl_cmd = [
            "curl", "-s",
            "https://nfdi.fiz-karlsruhe.de/matwerk/sparql",
            "--data", f"query={encoded_query}",
            "-H", "accept: text/turtle"
        ]

        # Append results to output file
        with open(output_file, "ab") as out:
            subprocess.run(curl_cmd, stdout=out)
    print(i)
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
