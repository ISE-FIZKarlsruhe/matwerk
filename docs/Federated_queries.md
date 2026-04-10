## Federated queries

### atomRDF KG <-> MSE KG: Which atomistic samples in AtomRDF correspond to a specific dataset entry in MSE-KG, and what are their segregation energies for Fe–Au Σ5 grain boundaries?

```sparql
PREFIX nfdicore_dataset: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009>
PREFIX denoted_by: <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX has_url: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>

PREFIX cmso: <http://purls.helmholtz-metadaten.de/cmso/>
PREFIX asmo: <http://purls.helmholtz-metadaten.de/asmo/>
PREFIX cdco: <http://purls.helmholtz-metadaten.de/cdos/cdco/>
PREFIX pldo: <http://purls.helmholtz-metadaten.de/cdos/pldo/>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT DISTINCT ?dataset ?link ?doi ?sample ?E_seg ?unit
WHERE {

  # --- Step 1: Select dataset from MSE-KG ---
  ?dataset a nfdicore_dataset: .
  ?dataset denoted_by: ?linkNode .
  ?linkNode has_url: ?link .

  # --- Step 2: Query AtomRDF for scientific data ---
  SERVICE <https://atomrdf.fair-workflows.org/sparql> {

    # Atomic sample
    ?sample a cmso:AtomicScaleSample .

    # Grain boundary with Σ = 5
    ?sample cmso:hasMaterial ?mat .
    ?mat cdco:hasCrystallographicDefect ?gb .
    ?gb pldo:hasSigmaValue 5 .

    # Material contains Fe
    ?sample cmso:hasSpecies ?sp1 .
    ?sp1 cmso:hasElement ?el1 .
    ?el1 cmso:hasChemicalSymbol "Fe" .

    # Material contains Au
    ?sample cmso:hasSpecies ?sp2 .
    ?sp2 cmso:hasElement ?el2 .
    ?el2 cmso:hasChemicalSymbol "Au" .

    # Segregation energy
    ?sample asmo:hasCalculatedProperty ?prop .
    ?prop a asmo:SegregationEnergy ;
          asmo:hasValue ?E_seg ;
          asmo:hasUnit ?unit .

    # DOI of associated publication
    ?sample dcterms:isPartOf ?ds .
    ?ds dcterms:isReferencedBy ?pub .
    ?pub dcterms:identifier ?doi .
  }
  FILTER(STR(?link) = STR(?doi))
}
ORDER BY ?E_seg
```

---

### atomRDF KG <-> MSE KG: Who created datasets that are linked to atomistic segregation energy calculations?

```sparql
PREFIX nfdicore_dataset: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009>
PREFIX denoted_by: <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX has_part: <http://purl.obolibrary.org/obo/BFO_0000051>
PREFIX nfdicore_creator_role: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001032>
PREFIX nfdicore_institution_list: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001033>
PREFIX has_url: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

PREFIX cmso: <http://purls.helmholtz-metadaten.de/cmso/>
PREFIX asmo: <http://purls.helmholtz-metadaten.de/asmo/>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT DISTINCT ?dataset ?creatorLabel ?affiliation ?E_seg
WHERE {

  # --- MSE-KG ---
  ?dataset denoted_by: ?linkNode .
  ?linkNode has_url: ?link .

  OPTIONAL {
    ?dataset has_part: ?creator .
    ?creator a nfdicore_creator_role: ;
             rdfs:label ?creatorLabel .
  }

  OPTIONAL {
    ?dataset has_part: ?aff .
    ?aff a nfdicore_institution_list: ;
         rdfs:label ?affiliation .
  }

  # --- AtomRDF ---
  SERVICE <https://atomrdf.fair-workflows.org/sparql> {

    # 🔗 JOIN
    ?sample dcterms:isPartOf ?ds .
    ?ds dcterms:isReferencedBy ?pub .
    ?pub dcterms:identifier ?doi .

    ?sample asmo:hasCalculatedProperty ?prop .
    ?prop a asmo:SegregationEnergy ;
          asmo:hasValue ?E_seg .
  }
  FILTER(STR(?link) = STR(?doi))
}
```

---

### atomRDF KG <-> MSE KG: Which chemical elements are studied in datasets linked to AtomRDF simulations?

```sparql
PREFIX nfdicore_dataset: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009>
PREFIX denoted_by: <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX has_url: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>

PREFIX cmso: <http://purls.helmholtz-metadaten.de/cmso/>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT DISTINCT ?dataset ?element
WHERE {

  # --- MSE-KG ---
  ?dataset denoted_by: ?linkNode .
  ?linkNode has_url: ?link .

  # --- AtomRDF ---
  SERVICE <https://atomrdf.fair-workflows.org/sparql> {

    # 🔗 JOIN
    ?sample dcterms:isPartOf ?ds .
    ?ds dcterms:isReferencedBy ?pub .
    ?pub dcterms:identifier ?doi .

    # Elements
    ?sample cmso:hasSpecies ?sp .
    ?sp cmso:hasElement ?el .
    ?el cmso:hasChemicalSymbol ?element .
  }
  FILTER(STR(?link) = STR(?doi))
}
```

---

### WikiData <-> MSE KG: Which organizations in the MSE-KG can be linked to Wikidata via their ROR ID, and what are their city, acronym, and country?

```sparql
PREFIX organization: <http://purl.obolibrary.org/obo/OBI_0000245>
PREFIX located_in:   <http://purl.obolibrary.org/obo/RO_0001025>
PREFIX nfdicore_city:<https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000106>
PREFIX has_external_identifier: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006>
PREFIX has_url:      <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX has_acronym:  <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0010015>
PREFIX rdfs:         <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wdt:          <http://www.wikidata.org/prop/direct/>

SELECT DISTINCT
  ?org
  ?orgLabel
  ?acronym
  ?city
  ?rorURL
  ?wikidataOrg
  ?wikidataOrgLabel
  ?countryLabel
WHERE {
  ?org a organization: .
  OPTIONAL { ?org rdfs:label ?orgLabel . }
  OPTIONAL { ?org has_acronym: ?acronym . }

  OPTIONAL {
    ?org located_in: ?cityNode .
    ?cityNode a nfdicore_city: ;
              rdfs:label ?city .
  }

  ?org has_external_identifier: ?idNode .
  ?idNode has_url: ?rorURL .

  BIND(REPLACE(STR(?rorURL), "^.*/", "") AS ?rorId)

  SERVICE <https://query.wikidata.org/sparql> {
    ?wikidataOrg wdt:P6782 ?rorId .
    OPTIONAL {
      ?wikidataOrg rdfs:label ?wikidataOrgLabel .
      FILTER(LANG(?wikidataOrgLabel) = "en")
    }
    OPTIONAL {
      ?wikidataOrg wdt:P17 ?country .
      ?country rdfs:label ?countryLabel .
      FILTER(LANG(?countryLabel) = "en")
    }
  }
}
LIMIT 100
```

---

### WikiData <-> MSE KG: Which people in the MSE-KG are linked to Wikidata through ORCID, and what are their employers and countries?

```sparql
PREFIX nfdicore_person:  <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000004>
PREFIX has_external_identifier: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006>
PREFIX has_url: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT DISTINCT
  ?person
  ?personLabel
  ?orcidURL
  ?orcidId
  ?wikidataPerson
  ?wikidataPersonLabel
  ?employerLabel
  ?countryLabel
WHERE {
  ?person a nfdicore_person: .
  OPTIONAL { ?person rdfs:label ?personLabel . }

  ?person has_external_identifier: ?orcidURL .

  FILTER(CONTAINS(LCASE(STR(?orcidURL)), "orcid.org"))

  BIND(REPLACE(REPLACE(STR(?orcidURL), "^https?://orcid.org/", ""), "/$", "") AS ?orcidId)

  SERVICE <https://query.wikidata.org/sparql> {
    ?wikidataPerson wdt:P496 ?orcidId .

    OPTIONAL {
      ?wikidataPerson rdfs:label ?wikidataPersonLabel .
      FILTER(LANG(?wikidataPersonLabel) = "en")
    }

    OPTIONAL {
      ?wikidataPerson wdt:P108 ?employer .
      ?employer rdfs:label ?employerLabel .
      FILTER(LANG(?employerLabel) = "en")
    }

    OPTIONAL {
      ?wikidataPerson wdt:P27 ?country .
      ?country rdfs:label ?countryLabel .
      FILTER(LANG(?countryLabel) = "en")
    }
  }
}
LIMIT 100
```

---