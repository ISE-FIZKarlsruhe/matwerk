# Microstructure-Sensitive Mechanical Data Knowledge Graph (MiMeDat-KG, NFDI MatWerk IUC07)

An RDF repository for **MiMeDat: Metadata Datasets for Microstructure-Sensitive Mechanical Data**. The RDF-converted sources semantically describe simulation datasets and their provenance (dataset identifier, title, creators and contributors with ORCID, affiliation, institute and research group; funding, publisher, licence, rights holder), RVE size, discretization, mechanical parameters, phase (constitutive model, orientation), stress, total strain, plastic strain, and related quantities.

!!! info "Mapping status"
    Currently mapped: **dataset and provenance metadata**. The simulation parameters themselves — RVE geometry, mechanical boundary conditions, constitutive model, crystallographic orientation, stress–strain results — are planned next, pending their ontology mapping. The competency questions below reflect what is queryable today.

The graph is produced by the **[RML4MSE-KG](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG)** pipeline, grounded in the [MatWerk Ontology (MWO)](https://github.com/ISE-FIZKarlsruhe/mwo) and NFDIcore.

## At a glance

| | |
|---|---|
| **IRI in MSE-KG** | [`msekg:17902496761672`](https://nfdi.fiz-karlsruhe.de/matwerk/msekg/17902496761672) |
| **`rdfs:label`** | Microstructure-Sensitive Mechanical Data Knowledge Graph (MiMeDat-KG, NFDI MatWerk IUC07) |
| **Type** | `nfdi:NFDI_0000009` — dataset |
| **SPARQL endpoint** | `https://dataportal.material-digital.de/dataset/9dc27507-bf47-4425-877d-1e5249e9db05/fuseki/$/sparql` |
| **Endpoint node** | [`msekg:17902496761674`](https://nfdi.fiz-karlsruhe.de/matwerk/msekg/17902496761674) (`nfdi:NFDI_0001095`) |
| **Licence** | CC0 1.0 |
| **Creator** | [Dr. Hossein Beygi Nasrabadi](https://orcid.org/0000-0002-3092-0532) |
| **Affiliation** | FIZ Karlsruhe – Leibniz Institute for Information Infrastructure |
| **NFDI-MatWerk IUC** | IUC07 |
| **Source data** | [MiMeDat source repository & JSON schema](https://github.com/Ronakshoghi/MiMeDat) |
| **Pipeline** | [`MiMeDat (IUC07)`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/tree/main/MiMeDat%20(IUC07)) in RML4MSE-KG |
| **RDF dataset** | [MaterialDigital Dataportal](https://dataportal.material-digital.de/dataset/microstructure-sensitive-mechanical-data-knowledge-graph-mimedat-kg-nfdi-matwerk-iuc07) |
| **Guided query UI** | [Sparklis](https://dataportal.material-digital.de/sparklis/?title=microstructure-sensitive-mechanical-data-knowledge-graph-mimedat-kg-nfdi-matwerk-iuc07&endpoint=https%3A//dataportal.material-digital.de/dataset/9dc27507-bf47-4425-877d-1e5249e9db05/fuseki/%24/sparql&entity_lexicon_select=http%3A//www.w3.org/2000/01/rdf-schema%23label&concept_lexicons_select=http%3A//www.w3.org/2000/01/rdf-schema%23label) |

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
  BIND(<https://nfdi.fiz-karlsruhe.de/matwerk/msekg/17902496761672> AS ?kg)

  ?kg a nfdicore_dataset: ;
      rdfs:label ?label .

  OPTIONAL { ?kg denoted_by:  ?e . ?e a nfdicore_sparql_endpoint: ; has_url: ?url }
  OPTIONAL { ?kg denoted_by:  ?d . ?d a textual_entity: ; rdfs:label ?desc }
  OPTIONAL { ?kg has_license: ?l . ?l rdfs:label ?lic }
  OPTIONAL { ?kg has_part:    ?p . ?p rdfs:label ?part }
}
```

## Competency questions

The three competency questions below are taken **verbatim** from [`MiMeDat (IUC07)/queries/`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/tree/main/MiMeDat%20(IUC07)/queries) in the RML4MSE-KG repository. CQ2 and CQ3 are written against the example dataset `msekg:a46fde6c` — change that `BIND` to target another dataset.

### CQ1 — List all simulation datasets together with their title and identifier

[`01_datasets_title_and_identifier.rq`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/blob/main/MiMeDat%20(IUC07)/queries/01_datasets_title_and_identifier.rq)

```turtle
PREFIX nfdi: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX obo:  <http://purl.obolibrary.org/obo/>

SELECT ?dataset ?title ?identifier
WHERE {
  ?dataset a nfdi:NFDI_0001205 .                 # simulation dataset

  ?titleEntity a nfdi:NFDI_0001019 ;             # title
               obo:IAO_0000219 ?dataset ;         # denotes
               nfdi:NFDI_0001007 ?title .         # has_value

  ?identifierEntity a obo:IAO_0020000 ;          # identifier
                     obo:IAO_0000219 ?dataset ;   # denotes
                     nfdi:NFDI_0001007 ?identifier . # has_value
}
ORDER BY ?dataset
```

### CQ2 — Retrieve the full creator and contributor list (name, ORCID, affiliation, institute, research group) for a dataset

[`02_creators_and_contributors_for_dataset.rq`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/blob/main/MiMeDat%20(IUC07)/queries/02_creators_and_contributors_for_dataset.rq)

```turtle
PREFIX msekg: <https://nfdi.fiz-karlsruhe.de/matwerk/msekg/>
PREFIX nfdi:  <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX mwo:   <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX obo:   <http://purl.obolibrary.org/obo/>

SELECT ?role ?person ?name ?orcid ?affiliation ?institute ?researchGroup
WHERE {
  BIND(msekg:a46fde6c AS ?dataset)
  ?dataset obo:OBI_0000312 ?process .            # is_specified_output_of

  ?person a nfdi:NFDI_0000004 ;                  # person
          obo:RO_0000056 ?process ;               # participates_in
          obo:RO_0000087 ?roleInst .              # has_role
  ?roleInst a ?roleClass .
  FILTER(?roleClass IN (nfdi:NFDI_0001026, nfdi:NFDI_0000118))   # creator role / contributor role
  BIND(IF(?roleClass = nfdi:NFDI_0001026, "creator", "contributor") AS ?role)

  OPTIONAL {
    ?nameEnt a obo:IAO_0020015 ;                 # personal name
             obo:IAO_0000219 ?roleInst ;          # denotes
             nfdi:NFDI_0001007 ?name .            # has_value
  }
  OPTIONAL {
    ?orcidEnt a obo:IAO_0000708 ;                # ORCID identifier
              obo:IAO_0000219 ?roleInst ;
              nfdi:NFDI_0001007 ?orcid .
  }
  OPTIONAL {
    ?affEnt a nfdi:NFDI_0001102 ;                # affiliation
            obo:IAO_0000219 ?roleInst ;
            nfdi:NFDI_0001007 ?affiliation .
  }
  OPTIONAL {
    ?instRole a mwo:MWO_0001063 ;                # institute role
              obo:RO_0000081 ?instOrg .           # role_of
    ?instOrg obo:RO_0002351 ?person .             # has_member
    ?instIdent a obo:IAO_0020000 ;
               obo:IAO_0000219 ?instRole ;
               nfdi:NFDI_0001007 ?institute .
  }
  OPTIONAL {
    ?groupRole a mwo:MWO_0001055 ;               # research group role
               obo:RO_0000081 ?groupOrg .
    ?groupOrg obo:RO_0002351 ?person .
    ?groupIdent a obo:IAO_0020000 ;
                obo:IAO_0000219 ?groupRole ;
                nfdi:NFDI_0001007 ?researchGroup .
  }
}
ORDER BY ?role ?person
```

### CQ3 — Retrieve the funding organization, publisher, licence, and rights holder for a dataset

[`03_funding_publisher_licence_rightsholder.rq`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/blob/main/MiMeDat%20(IUC07)/queries/03_funding_publisher_licence_rightsholder.rq)

```turtle
PREFIX msekg: <https://nfdi.fiz-karlsruhe.de/matwerk/msekg/>
PREFIX nfdi:  <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX mwo:   <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX obo:   <http://purl.obolibrary.org/obo/>
PREFIX swo:   <http://www.ebi.ac.uk/swo/>

SELECT ?funder ?fundIdentifier ?publisher ?licence ?rightsHolder
WHERE {
  BIND(msekg:a46fde6c AS ?dataset)
  ?dataset obo:OBI_0000312 ?process .            # is_specified_output_of

  OPTIONAL {
    ?funderIdent a obo:IAO_0020000 ;
                 obo:IAO_0000219 ?funderRole ;     # denotes
                 nfdi:NFDI_0001007 ?funder .       # has_value
    ?funderRole a nfdi:NFDI_0000139 ;              # funding organization role
                obo:RO_0000081 ?funderOrg .        # role_of
    ?funderOrg obo:RO_0000056 ?fundingProcess .    # participates_in
    ?fundingProcess obo:BFO_0000063 ?process .     # precedes
  }
  OPTIONAL {
    ?fundIdEnt a mwo:MWO_0001068 ;                 # funding identifier
               obo:IAO_0000219 ?fundingProcess2 ;
               nfdi:NFDI_0001007 ?fundIdentifier .
    ?fundingProcess2 obo:BFO_0000063 ?process .
  }
  OPTIONAL {
    ?publisherIdent a obo:IAO_0020000 ;
                    obo:IAO_0000219 ?publisherRole ;
                    nfdi:NFDI_0001007 ?publisher .
    ?publisherRole a nfdi:NFDI_0000193 ;           # publisher role
                   obo:RO_0000081 ?publisherOrg .
    ?publisherOrg obo:RO_0000056 ?publishingProcess .
    ?publishingProcess obo:BFO_0000063 ?process .
  }
  OPTIONAL {
    ?licenceEnt a swo:SWO_0000002 ;                # licence
                obo:IAO_0000219 ?dataset ;
                nfdi:NFDI_0001007 ?licence .
  }
  OPTIONAL {
    ?rightsHolderIdent a obo:IAO_0020000 ;
                       obo:IAO_0000219 ?rightsHolderRole ;
                       nfdi:NFDI_0001007 ?rightsHolder .
    ?rightsHolderRole a mwo:MWO_0001065 ;          # license holder role
                      obo:RO_0000081 ?rightsHolderOrg .
    ?rightsHolderOrg obo:RO_0000056 ?licensingProcess .
    ?licensingProcess obo:BFO_0000063 ?process .
  }
}
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

- Competency questions — [`MiMeDat (IUC07)/queries/`](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG/tree/main/MiMeDat%20(IUC07)/queries), RML4MSE-KG repository
- Pipeline, endpoint and citation — [RML4MSE-KG README](https://github.com/HosseinBeygiNasrabadi/RML4MSE-KG)
- Source data — [MiMeDat: Metadata Datasets for Microstructure-Sensitive Mechanical Data](https://github.com/Ronakshoghi/MiMeDat)
- MatWerk Ontology (MWO) — <https://github.com/ISE-FIZKarlsruhe/mwo>
