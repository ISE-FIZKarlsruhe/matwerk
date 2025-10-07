import subprocess
from pathlib import Path

# Define paths
all_ttl_path = "data/all.ttl"
inverse_ttl_path = "data/inverse/inverse_all.ttl"
output_file = "data/inverse/inverse_new.ttl"
Path(output_file).write_text("")


# SPARQL query
sparql = """
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

CONSTRUCT {
  ?r2 ?p2 ?r1 .
}
WHERE {
  # Get all (sub/super) property paths of inverse relationships
  {
    ?inv1 owl:inverseOf ?inv2 .
  }
  UNION
  {
    ?inv2 owl:inverseOf ?inv1 .
  }

  # Resolve all sub-properties of inverse relationships
  ?p1 rdfs:subPropertyOf* ?inv1 .
  ?p2 rdfs:subPropertyOf* ?inv2 .

  # Find triples in data
  ?r1 ?p1 ?r2 .

  # Exclude existing inverse triples
  FILTER NOT EXISTS {
    ?r2 ?p2 ?r1 .
  }
}

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
'
