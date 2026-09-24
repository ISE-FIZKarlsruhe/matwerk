# Creep Literature Knowledge Graph (CreepLitKG)

**CreepLitKG** is an LLM + ontology-driven pipeline and knowledge graph that converts creep test metadata **extracted from scientific literature** into ontology-grounded RDF. It makes experimental creep data reported in publications — material identity, chemical composition, heat treatment history, microstructural features, test conditions and creep results — findable, machine-readable and queryable through a public SPARQL endpoint, and federates it into MSE-KG.

Every entity is typed with classes from the **[Creep Testing Ontology (CTO)](https://github.com/HosseinBeygiNasrabadi/creep-testing-ontology)**, derived from *ISO 204:2018 — Metallic materials — Uniaxial creep testing in tension*, together with the ontologies CTO reuses (BFO, RO, IAO, OBI, PMDco, NFDIcore, MWO) and measurement units from QUDT.

## At a glance

| | |
|---|---|
| **IRI in MSE-KG** | [`msekg:17858428366781`](https://nfdi.fiz-karlsruhe.de/matwerk/msekg/17858428366781) |
| **`rdfs:label`** | Creep Literature Knowledge Graph (CreepLitKG) |
| **Type** | `nfdi:NFDI_0000009` — dataset |
| **SPARQL endpoint** | `https://dataportal.material-digital.de/dataset/a5b4edc4-43ef-44ff-a386-5d1f6fbbc439/fuseki/$/sparql` |
| **Endpoint node** | [`msekg:17858430496111`](https://nfdi.fiz-karlsruhe.de/matwerk/msekg/17858430496111) (`nfdi:NFDI_0001095`) |
| **Licence** | CC0 1.0 |
| **Creator** | [Dr. Hossein Beygi Nasrabadi](https://orcid.org/0000-0002-3092-0532) |
| **Affiliation** | FIZ Karlsruhe – Leibniz Institute for Information Infrastructure |
| **Version** | 1.0.0 (released 2026-07-20) |
| **Documentation** | <https://hosseinbeyginasrabadi.github.io/Creep_Literature_Knowledge_Graph/> |
| **Repository** | [HosseinBeygiNasrabadi/Creep_Literature_Knowledge_Graph](https://github.com/HosseinBeygiNasrabadi/Creep_Literature_Knowledge_Graph) |
| **RDF dataset** | [MaterialDigital Dataportal](https://dataportal.material-digital.de/dataset/creep_literature_knowledge_graph) |

!!! note "Two URLs, two purposes"
    The endpoint node registered in MSE-KG points at the **documentation site** (`https://hosseinbeyginasrabadi.github.io/Creep_Literature_Knowledge_Graph/`). The **queryable** SPARQL endpoint is the Fuseki URL in the table above, as published in the project's own [SPARQL access page](https://github.com/HosseinBeygiNasrabadi/Creep_Literature_Knowledge_Graph/blob/main/docs/sparql.md). Use the Fuseki URL in a `SERVICE` clause.

## How the graph is built

```
new creep paper
   → LLM4CreepLitKG              (LLM-assisted metadata extraction)
   → human check, into creep_literature_spreadsheet.xlsm
   → ./map.sh                    (xlsm → JSON → YARRRML/RML → RDF → pySHACL)
   → creep_literature_rdf.ttl
   → MaterialDigital Dataportal → public SPARQL endpoint
   → harvested into MSE-KG
```

The mapping lives entirely in the YARRRML file, IRIs are derived deterministically from record IDs (so re-runs are idempotent), and the pipeline exits non-zero on any SHACL violation.

## Query interfaces

| Interface | Notes |
|---|---|
| [**Sparklis** (guided query builder)](https://dataportal.material-digital.de/sparklis/?title=creep_literature_knowledge_graph&endpoint=https%3A//dataportal.material-digital.de/dataset/a5b4edc4-43ef-44ff-a386-5d1f6fbbc439/fuseki/%24/sparql&entity_lexicon_select=http%3A//www.w3.org/2000/01/rdf-schema%23label&concept_lexicons_select=http%3A//www.w3.org/2000/01/rdf-schema%23label) | Build queries in natural language, pre-configured with `rdfs:label` lexicons |
| PMD Dataportal Query UI | Built-in editor on the dataset page |
| Programmatic | HTTP `POST`, form-urlencoded, `query=` key — `curl`, Python `SPARQLWrapper`, … |

```bash
curl -X POST \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode 'query=SELECT * WHERE { ?s ?p ?o } LIMIT 10' \
  'https://dataportal.material-digital.de/dataset/a5b4edc4-43ef-44ff-a386-5d1f6fbbc439/fuseki/$/sparql'
```

!!! tip "CORS"
    Browser-based editors hosted elsewhere (e.g. a self-hosted YASGUI) may fail against the endpoint because of CORS. The portal's own Query UI, Sparklis, and any server-side `POST` all work.

## Locate it in the MatWerk KG

```sparql
PREFIX nfdicore_dataset:         <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009>
PREFIX nfdicore_sparql_endpoint: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001095>
PREFIX textual_entity:           <http://purl.obolibrary.org/obo/IAO_0000300>
PREFIX denoted_by:               <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX has_url:                  <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX has_license:              <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000142>
PREFIX has_part:                 <http://purl.obolibrary.org/obo/BFO_0000051>
PREFIX rdfs:                     <http://www.w3.org/2000/01/rdf-schema#>

SELECT
  (SAMPLE(?label) AS ?kgLabel)
  (SAMPLE(?url)   AS ?sparqlEndpoint)
  (SAMPLE(?lic)   AS ?licence)
  (GROUP_CONCAT(DISTINCT ?part; SEPARATOR=" · ") AS ?creditedParts)
  (SAMPLE(?desc)  AS ?shortDescription)
WHERE {
  BIND(<https://nfdi.fiz-karlsruhe.de/matwerk/msekg/17858428366781> AS ?kg)

  ?kg a nfdicore_dataset: ;
      rdfs:label ?label .

  OPTIONAL { ?kg denoted_by:  ?e . ?e a nfdicore_sparql_endpoint: ; has_url: ?url }
  OPTIONAL { ?kg denoted_by:  ?d . ?d a textual_entity: ; rdfs:label ?desc }
  OPTIONAL { ?kg has_license: ?l . ?l rdfs:label ?lic }
  OPTIONAL { ?kg has_part:    ?p . ?p rdfs:label ?part }
}
```

## Competency questions

The eight competency questions below are taken **verbatim** from the project's own [`docs/sparql.md`](https://github.com/HosseinBeygiNasrabadi/Creep_Literature_Knowledge_Graph/blob/main/docs/sparql.md). Run them against the Fuseki endpoint, or wrap them in a `SERVICE` clause to combine them with MSE-KG.

Prefixes used throughout:

```turtle
PREFIX co:   <https://w3id.org/pmd/co/>
PREFIX cto:  <https://w3id.org/pmd/cto/>
PREFIX mwo:  <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX nfdi: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX obo:  <http://purl.obolibrary.org/obo/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
```

### CQ1 — From which literature sources (publications) does the creep data originate?

```turtle
SELECT DISTINCT ?doi WHERE {
  ?d a nfdi:NFDI_0001037 ;   # digital object identifier
     co:PMD_0000006 ?doi .   # has value
}
```

### CQ2 — Which materials have been creep tested?

```turtle
SELECT DISTINCT ?material WHERE {
  ?id a mwo:MWO_0001099 ;        # material identifier
      co:PMD_0000006 ?material .
}
```

### CQ3 — Which testing standards have been used for creep testing Inconel materials?

```turtle
SELECT DISTINCT ?material ?standard WHERE {
  ?mid a mwo:MWO_0001099 ;
       obo:IAO_0000219 ?piece ;        # denotes creep test piece
       co:PMD_0000006 ?material .
  ?piece obo:RO_0000056 ?process .     # participates in
  ?process obo:COB_0000081 ?plan .     # intended to realize
  ?std a nfdi:NFDI_0000206 ;           # standard
       obo:RO_0000058 ?plan ;          # is concretized as
       co:PMD_0000006 ?standard .
  FILTER(CONTAINS(?material, "Inconel"))
}
```

### CQ4 — At what temperature and applied stress was each creep test performed?

```turtle
SELECT ?process ?temperature ?tempUnit ?stress ?stressUnit WHERE {
  ?process a co:PMD_0000589 ;          # creep testing process
           co:PMD_0025013 ?tq .        # changes quality (temperature)
  ?tSpec obo:RO_0000058 ?tq ;
         obo:OBI_0001937 ?temperature ;
         obo:IAO_0000039 ?tempUnit .
  ?sq a cto:CTO_1000304 ;              # mechanical stress
      obo:BFO_0000054 ?process .       # realized in
  ?sSpec obo:RO_0000058 ?sq ;
         obo:OBI_0001937 ?stress ;
         obo:IAO_0000039 ?stressUnit .
}
```

### CQ5 — What heat treatment steps (solutionizing, aging) were applied to a test piece before creep testing?

```turtle
SELECT ?process ?step ?condition WHERE {
  ?ht obo:BFO_0000062 ?process .       # heat treatment preceded by creep test
  ?desc obo:IAO_0000219 ?ht ;          # description denotes the treatment
        co:PMD_0000006 ?condition .
  OPTIONAL { ?desc rdfs:label ?step }  # "Solutionizing" / "Aging"
}
ORDER BY ?process ?step
```

### CQ6 — What is the grain size of the material for each heat treatment condition?

```turtle
SELECT ?piece ?grainSize ?unit ?agingCondition WHERE {
  ?gs a co:PMD_0020243 ;               # grain size
      obo:RO_0000080 ?cryst .          # quality of crystallite
  ?spec obo:OBI_0001927 ?gs ;
        obo:OBI_0001937 ?grainSize ;
        obo:IAO_0000039 ?unit .
  ?cryst obo:RO_0002350 ?micro .       # member of microstructure
  ?micro obo:BFO_0000050 ?piece .      # part of test piece
  ?piece obo:RO_0000056 ?process .
  ?aging obo:BFO_0000062 ?process ;
         rdfs:label "aging process" .
  ?desc obo:IAO_0000219 ?aging ;
        co:PMD_0000006 ?agingCondition .
}
```

### CQ7 — What percentage elongation after creep fracture was observed for each test?

```turtle
SELECT ?process ?elongation WHERE {
  ?q a cto:CTO_0000005 ;               # % elongation after creep fracture
     obo:BFO_0000054 ?process .
  ?spec obo:OBI_0001927 ?q ;
        obo:OBI_0002135 ?elongation .  # text value incl. uncertainty
}
```

### CQ8 — What are the stress rupture time and steady-state creep rate measured for Inconel 718?

```turtle
SELECT ?sample ?ruptureTime ?creepRate WHERE {
  ?mid a mwo:MWO_0001099 ;
       obo:IAO_0000219 ?piece ;
       co:PMD_0000006 "Inconel 718" .
  ?oid a mwo:MWO_0001015 ;             # sample identifier
       obo:IAO_0000219 ?piece ;
       co:PMD_0000006 ?sample .
  ?piece obo:RO_0000056 ?process .

  ?load a cto:CTO_0000011 ;            # loading process
        obo:BFO_0000050 ?process ;
        obo:BFO_0000199 ?rt .          # occupies temporal region
  ?rtSpec obo:OBI_0001927 ?rt ;
          obo:OBI_0002135 ?ruptureTime .

  ?cr a cto:CTO_1000035 ;              # creep rate
      co:PMD_0025006 ?process .        # process attribute of
  ?crSpec obo:OBI_0001927 ?cr ;
          obo:OBI_0002135 ?creepRate .
}
```

## How to cite

```bibtex
@software{CreepLitKG,
  author  = {Beygi Nasrabadi, Hossein and Molaei, Soheil and Bayani, AmirHossein
             and Norouzi, Ebrahim and Waitelonis, J{\"o}rg and Sack, Harald},
  title   = {Creep Literature Knowledge Graph (CreepLitKG)},
  url     = {https://github.com/HosseinBeygiNasrabadi/Creep_Literature_Knowledge_Graph},
  version = {1.0.0},
  date    = {2026-07-20},
}
```

## Sources

- Competency questions and endpoint — [`docs/sparql.md`](https://github.com/HosseinBeygiNasrabadi/Creep_Literature_Knowledge_Graph/blob/main/docs/sparql.md), CreepLitKG repository (CC0 1.0)
- Pipeline description and citation — [repository README](https://github.com/HosseinBeygiNasrabadi/Creep_Literature_Knowledge_Graph) and [`CITATION.cff`](https://github.com/HosseinBeygiNasrabadi/Creep_Literature_Knowledge_Graph/blob/main/CITATION.cff)
- Creep Testing Ontology (CTO) — <https://github.com/HosseinBeygiNasrabadi/creep-testing-ontology>, derived from ISO 204:2018
- RDF dataset — [MaterialDigital Dataportal](https://dataportal.material-digital.de/dataset/creep_literature_knowledge_graph)
