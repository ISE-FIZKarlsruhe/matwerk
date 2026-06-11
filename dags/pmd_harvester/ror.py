import requests
import json
import re
import unicodedata

   


ROR_API = "https://api.ror.org/v2/organizations"


def get_ror_ID(query, country="Germany"):
    response = requests.get(
        ROR_API,
        params={"query": query},
        timeout=20
    )

    response.raise_for_status()

    data = response.json()

    candidates = []

    for item in data.get("items", []):
        org = item.get("organization", item)

        # Country filter
        countries = [
            loc.get("geonames_details", {}).get("country_name", "")
            for loc in org.get("locations", [])
        ]

        if country not in countries:
            continue

        names = []

        if org.get("name"):
            names.append(org["name"])

        names.extend(org.get("aliases", []))
        names.extend(org.get("acronyms", []))

        # Exact / acronym matching first
        if query.lower() in [n.lower() for n in names]:
            return {
                "query": query,
                "name": org["name"],
                "ror": org["id"]
            }

        candidates.append({
            "query": query,
            "name": org.get("name"),
            "ror": org["id"]
        })

    # fallback to first German candidate
    if candidates:
        return candidates[0]

    return None



# =========================
# EXAMPLES
# =========================
''' 
examples = [
    "The Clausthal University of Technology	Clausthal-Zellerfeld", 	
    "Friedrich Schiller University Jena	Jena",
    "Fraunhofer Institute for Silicate Research	Würzburg",	
    "Technical University of Berlin - Faculty V of Mechanical Engineering and Transport Systems - Institute for Machine Tools and Factory Management IWF	Berlin",	
    "Ernst-Abbe-Hochschule Jena University of Applied Sciences"
]

for e in examples:
    print("\n", "=" * 60)
    print(e)
    entity = get_ror_ID(e)
    print(entity["name"], entity["ror"])

    '''