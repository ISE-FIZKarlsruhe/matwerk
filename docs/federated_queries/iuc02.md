# Creep Reference Datasets Knowledge Graph (NFDI MatWerk IUC02)

An RDF repository for **BAM Reference Data: Creep of Single-Crystal Ni-Based Superalloy CMSX-6**. The RDF-converted sources semantically describe the creep testing process, test pieces, materials and chemical composition, test machines and extensometers, input specifications (stress, temperature), and primary/secondary test results (rupture time, gauge lengths, durations, elongation/extension percentages).

The graph is produced by the **[RML4MSE-KG](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG)** pipeline: a YARRRML mapping describes how the source JSON maps to RDF grounded in the [Creep Testing Ontology (CTO)](https://github.com/HosseinBeygiNasrabadi/creep-testing-ontology); a driver script compiles that mapping to RML and runs it over every JSON file, producing one combined, idempotent `.ttl`.

## At a glance

| | |
|---|---|
| **IRI in MSE-KG** | [`msekg:17902496761671`](https://nfdi.fiz-karlsruhe.de/matwerk/msekg/17902496761671) |
| **`rdfs:label`** | Knowledge Graph for Creep Reference Datasets (NFDI MatWerk IUC02) |
| **Type** | `nfdi:NFDI_0000009` — dataset |
| **SPARQL endpoint** | `https://dataportal.material-digital.de/dataset/bb5b86d3-ade4-4b63-9e84-783de85a4abd/fuseki/$/sparql` |
| **Endpoint node** | [`msekg:17902496761673`](https://nfdi.fiz-karlsruhe.de/matwerk/msekg/17902496761673) (`nfdi:NFDI_0001095`) |
| **Licence** | CC0 1.0 |
| **Creator** | [Dr. Hossein Beygi Nasrabadi](https://orcid.org/0000-0002-3092-0532) |
| **Affiliation** | FIZ Karlsruhe – Leibniz Institute for Information Infrastructure |
| **NFDI-MatWerk IUC** | IUC02 |
| **Source data** | [BAM Reference Data: Creep of Single-Crystal Ni-Based Superalloy CMSX-6](https://zenodo.org/records/20132712) (Zenodo) |
| **Pipeline** | [`Creep reference dataset (IUC02)`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/tree/main/Creep%20reference%20dataset%20(IUC02)) in RML4MSE-KG |
| **RDF dataset** | [MaterialDigital Dataportal](https://dataportal.material-digital.de/dataset/knowledge-graph-for-creep-reference-datasets-nfdi-matwerk-iuc02) |
| **Guided query UI** | [Sparklis](https://dataportal.material-digital.de/sparklis/?title=knowledge-graph-for-creep-reference-datasets-nfdi-matwerk-iuc02&endpoint=https%3A//dataportal.material-digital.de/dataset/bb5b86d3-ade4-4b63-9e84-783de85a4abd/fuseki/%24/sparql&entity_lexicon_select=http%3A//www.w3.org/2000/01/rdf-schema%23label&concept_lexicons_select=http%3A//www.w3.org/2000/01/rdf-schema%23label) |

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
  BIND(<https://nfdi.fiz-karlsruhe.de/matwerk/msekg/17902496761671> AS ?kg)

  ?kg a nfdicore_dataset: ;
      rdfs:label ?label .

  OPTIONAL { ?kg denoted_by:  ?e . ?e a nfdicore_sparql_endpoint: ; has_url: ?url }
  OPTIONAL { ?kg denoted_by:  ?d . ?d a textual_entity: ; rdfs:label ?desc }
  OPTIONAL { ?kg has_license: ?l . ?l rdfs:label ?lic }
  OPTIONAL { ?kg has_part:    ?p . ?p rdfs:label ?part }
}
```

## Competency questions

The eight competency questions below are taken **verbatim** from [`Creep reference dataset (IUC02)/queries/`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/tree/main/Creep%20reference%20dataset%20(IUC02)/queries) in the RML4MSE-KG repository.

### CQ1 — List all creep datasets together with their test piece IDs and material identifiers

[`01_datasets_test_piece_and_material.rq`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/blob/main/Creep%20reference%20dataset%20(IUC02)/queries/01_datasets_test_piece_and_material.rq)

```turtle
PREFIX cto:   <https://w3id.org/pmd/cto/>
PREFIX pmdco: <https://w3id.org/pmd/co/>
PREFIX obo:   <http://purl.obolibrary.org/obo/>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?dataset ?testPieceID ?materialID
WHERE {
  ?dataset a cto:CTO_0000009 ;                 # creep_reference_dataset
           obo:OBI_0000312 ?process .           # is_specified_output_of

  ?testPiece a cto:CTO_0000008 ;                # creep_test_piece
             obo:RO_0000056 ?process ;           # participates_in
             rdfs:label ?testPieceID .

  ?material a pmdco:PMD_0000000 ;               # Material
            obo:RO_0000056 ?process .           # participates_in

  ?matId a cto:CTO_0000021 ;                    # creep_material_identifier
         obo:IAO_0000219 ?material ;             # denotes
         pmdco:PMD_0000006 ?materialID .        # has_value
}
ORDER BY ?dataset
```

### CQ2 — Retrieve the initial stress and temperature used for each creep test

[`02_initial_stress_and_temperature.rq`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/blob/main/Creep%20reference%20dataset%20(IUC02)/queries/02_initial_stress_and_temperature.rq)

```turtle
PREFIX cto:   <https://w3id.org/pmd/cto/>
PREFIX pmdco: <https://w3id.org/pmd/co/>
PREFIX obo:   <http://purl.obolibrary.org/obo/>

SELECT ?dataset ?stressValue ?stressUnit ?tempValue ?tempUnit
WHERE {
  ?dataset a cto:CTO_0000009 ;
           obo:OBI_0000312 ?process .

  # initial stress: input_specification -is_concretized_as-> quality (mechanical_stress) -realized_in-> process
  ?qstress a cto:CTO_1000304 ;
           obo:BFO_0000054 ?process .
  ?stressSpec obo:RO_0000058 ?qstress ;
              pmdco:PMD_0000006 ?stressValue ;
              obo:IAO_0000039 ?stressUnit .

  # specified temperature: quality (temperature) is a quality_of the machine that participates_in the process
  ?machine a pmdco:PMD_0000588 ;
           obo:RO_0000056 ?process .
  ?qtemp a pmdco:PMD_0000967 ;
         obo:RO_0000080 ?machine .
  ?tempSpec obo:RO_0000058 ?qtemp ;
            pmdco:PMD_0000006 ?tempValue ;
            obo:IAO_0000039 ?tempUnit .
}
ORDER BY ?dataset
```

### CQ3 — Rank datasets by creep rupture time, longest to shortest

[`03_rank_by_creep_rupture_time.rq`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/blob/main/Creep%20reference%20dataset%20(IUC02)/queries/03_rank_by_creep_rupture_time.rq)

```turtle
PREFIX cto:   <https://w3id.org/pmd/cto/>
PREFIX pmdco: <https://w3id.org/pmd/co/>
PREFIX obo:   <http://purl.obolibrary.org/obo/>
PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>

SELECT ?dataset ?ruptureTime_h
WHERE {
  ?dataset a cto:CTO_0000009 ;
           obo:OBI_0000312 ?process .

  ?process obo:BFO_0000199 ?srt .              # occupies_temporal_region
  ?srt a cto:CTO_0000013 .                      # stress_rupture_time

  ?spec obo:OBI_0001927 ?srt ;                  # specifies_value_of
        pmdco:PMD_0000006 ?ruptureStr .         # has_value

  BIND(xsd:double(?ruptureStr) AS ?ruptureTime_h)
}
ORDER BY DESC(?ruptureTime_h)
```

### CQ4 — Find all datasets tested at the temperature range 900–1000 °C

[`04_datasets_in_temperature_range.rq`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/blob/main/Creep%20reference%20dataset%20(IUC02)/queries/04_datasets_in_temperature_range.rq)

```turtle
PREFIX cto:   <https://w3id.org/pmd/cto/>
PREFIX pmdco: <https://w3id.org/pmd/co/>
PREFIX obo:   <http://purl.obolibrary.org/obo/>
PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>

SELECT ?dataset ?tempValue
WHERE {
  ?dataset a cto:CTO_0000009 ;
           obo:OBI_0000312 ?process .

  ?machine a pmdco:PMD_0000588 ;
           obo:RO_0000056 ?process .
  ?qtemp a pmdco:PMD_0000967 ;
         obo:RO_0000080 ?machine .
  ?tempSpec obo:RO_0000058 ?qtemp ;
            pmdco:PMD_0000006 ?tempStr .

  BIND(xsd:double(?tempStr) AS ?tempValue)
  FILTER(?tempValue >= 900 && ?tempValue <= 1000)
}
ORDER BY ?dataset
```

### CQ5 — Retrieve the full chemical composition (all elements, wt.% and ppm) for the `Vh5205_C-78` test piece

[`05_chemical_composition_for_test_piece.rq`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/blob/main/Creep%20reference%20dataset%20(IUC02)/queries/05_chemical_composition_for_test_piece.rq)

```turtle
PREFIX cto:   <https://w3id.org/pmd/cto/>
PREFIX pmdco: <https://w3id.org/pmd/co/>
PREFIX obo:   <http://purl.obolibrary.org/obo/>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?element ?value ?unitLabel
WHERE {
  ?testPiece a cto:CTO_0000008 ;
             rdfs:label "Vh5205_C-78" .

  ?qcomp a pmdco:PMD_0000551 ;                  # chemical_composition
         obo:RO_0000080 ?testPiece .            # quality_of

  ?item obo:IAO_0000418 ?qcomp ;                 # is_quality_specification_of
        rdfs:label ?element ;
        pmdco:PMD_0000006 ?value ;              # has_value
        obo:IAO_0000039 ?unitIRI .              # has_measurement_unit_label

  BIND(STRAFTER(STR(?unitIRI), "vocab/unit/") AS ?unitLabel)
}
ORDER BY ?element
```

### CQ6 — Compare percentage elongation after creep fracture across all datasets

[`06_compare_percentage_elongation.rq`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/blob/main/Creep%20reference%20dataset%20(IUC02)/queries/06_compare_percentage_elongation.rq)

```turtle
PREFIX cto:   <https://w3id.org/pmd/cto/>
PREFIX pmdco: <https://w3id.org/pmd/co/>
PREFIX obo:   <http://purl.obolibrary.org/obo/>
PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>

SELECT ?dataset ?elongation_pct
WHERE {
  ?dataset a cto:CTO_0000009 ;
           obo:OBI_0000312 ?process .

  ?q a cto:CTO_0000005 ;                        # percentage_elongation_after_creep_fracture
     obo:BFO_0000054 ?process .                  # realized_in

  ?spec obo:OBI_0001927 ?q ;                     # specifies_value_of
        pmdco:PMD_0000006 ?elongStr .            # has_value

  BIND(xsd:double(?elongStr) AS ?elongation_pct)
}
ORDER BY DESC(?elongation_pct)
```

### CQ7 — Retrieve test duration, soak time, and heating time together for each creep testing process

[`07_test_duration_soak_time_heating_time.rq`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/blob/main/Creep%20reference%20dataset%20(IUC02)/queries/07_test_duration_soak_time_heating_time.rq)

```turtle
PREFIX cto:   <https://w3id.org/pmd/cto/>
PREFIX pmdco: <https://w3id.org/pmd/co/>
PREFIX obo:   <http://purl.obolibrary.org/obo/>

SELECT ?dataset ?testDuration ?soakTime ?heatingTime
WHERE {
  ?dataset a cto:CTO_0000009 ;
           obo:OBI_0000312 ?process .

  OPTIONAL {
    ?process obo:BFO_0000199 ?td .
    ?td a cto:CTO_1000062 .                      # test_duration
    ?tdSpec obo:OBI_0001927 ?td ;
            pmdco:PMD_0000006 ?testDuration .
  }
  OPTIONAL {
    ?process obo:BFO_0000199 ?st .
    ?st a cto:CTO_1000064 .                      # soaking_time
    ?stSpec obo:OBI_0001927 ?st ;
            pmdco:PMD_0000006 ?soakTime .
  }
  OPTIONAL {
    ?process obo:BFO_0000199 ?ht .
    ?ht a obo:BFO_0000202 .                      # temporal_interval (heating time)
    ?htSpec obo:OBI_0001927 ?ht ;
            pmdco:PMD_0000006 ?heatingTime .
  }
}
ORDER BY ?dataset
```

### CQ8 — List all creep testing machines and extensometers together with the datasets that used them

[`08_machines_and_extensometers_per_dataset.rq`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/blob/main/Creep%20reference%20dataset%20(IUC02)/queries/08_machines_and_extensometers_per_dataset.rq)

```turtle
PREFIX cto:   <https://w3id.org/pmd/cto/>
PREFIX pmdco: <https://w3id.org/pmd/co/>
PREFIX obo:   <http://purl.obolibrary.org/obo/>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?dataset ?machineLabel ?extensometerLabel
WHERE {
  ?dataset a cto:CTO_0000009 ;
           obo:OBI_0000312 ?process .

  ?machine a pmdco:PMD_0000588 ;                # creep_testing_machine
           obo:RO_0000056 ?process ;             # participates_in
           rdfs:label ?machineLabel .

  ?extensometer a pmdco:PMD_0000636 ;            # extensometer
                obo:RO_0000056 ?process ;         # participates_in
                rdfs:label ?extensometerLabel .
}
ORDER BY ?dataset
```

## How to cite

```bibtex
@software{RML4MSE-KG,
  author  = {Beygi Nasrabadi, Hossein and Norouzi, Ebrahim and Waitelonis, J{\"o}rg and Sack, Harald},
  title   = {RML pipelines for integrating NFDI MatWerk community datasets to MSE-KG (RML4MSE-KG)},
  url     = {https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG},
  version = {1.0.0},
  date    = {2026-07-22},
}
```

## Sources

- Competency questions — [`Creep reference dataset (IUC02)/queries/`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/tree/main/Creep%20reference%20dataset%20(IUC02)/queries), RML4MSE-KG repository
- Pipeline, endpoint and citation — [RML4MSE-KG README](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG)
- Source data — [BAM Reference Data: Creep of Single-Crystal Ni-Based Superalloy CMSX-6](https://zenodo.org/records/20132712)
- Creep Testing Ontology (CTO) — <https://github.com/HosseinBeygiNasrabadi/creep-testing-ontology>
