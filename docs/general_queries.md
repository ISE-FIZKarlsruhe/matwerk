## Competency Questions

## General questions with the corresponding SPARQL queries

### What are the submitted records in NFDI-MatWerk Zenodo community present in the MSE-KG?

```sparql
PREFIX nfdicore_dataset: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009>
PREFIX has_part:         <http://purl.obolibrary.org/obo/BFO_0000051>
PREFIX rdfs:             <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?records ?recordsLabel_ ?filesInRecordLabel_ ?doi WHERE {

  # Records are datasets (upper class)
  ?records a nfdicore_dataset: ;
           rdfs:label ?recordsLabel_ ;
           has_part: ?containsInRecord .

  ?containsInRecord rdfs:label ?filesInRecordLabel_ ;
                    rdfs:seeAlso ?doi .
}
LIMIT 999
```

---

### What are the Fair Digital Objects present in the MSE-KG?

```sparql
PREFIX nfdicore_fdo:     <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001037>
PREFIX part_of:          <http://purl.obolibrary.org/obo/BFO_0000050>
PREFIX has_url:        <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX rdfs:             <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?FDOs ?FDOsLabel_ ?FDOsParentDataset ?FDOsURL WHERE {

  ?FDOs a nfdicore_fdo: ;
        rdfs:label ?FDOsLabel_ ;
        part_of: ?FDOsParentDataset ;
        has_url: ?FDOsURL .
}
LIMIT 999
```

---

### What are the softwares present in the MSE-KG? What are the license, programming language, repository URL and publication of these softwares?

```sparql
PREFIX nfdicore_software:        <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000121>
PREFIX has_license:             <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000142>
PREFIX has_programming_language:<https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000187>
PREFIX nfdicore_publication:    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000190>
PREFIX denoted_by:            <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX has_value:           <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007>
PREFIX has_url:               <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX mwo_repository:          <http://purls.helmholtz-metadaten.de/mwo/MWO_0001113>
PREFIX rdfs:                    <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT
  ?software
  ?softwareLabel_
  ?licenseLabel
  ?languageLabel
  ?repositoryURL
  ?publicationLabel
WHERE {

  ?software a nfdicore_software: .

  # Software label: via a metadata node with rdfs:label
  OPTIONAL {
    ?software denoted_by: ?softwareLabelNode .
    ?softwareLabelNode rdfs:label ?softwareLabel_ .
  }

  OPTIONAL {
    ?software has_license: ?license .
    OPTIONAL { ?license rdfs:label ?licenseLabel . }
  }

  OPTIONAL {
    ?software has_programming_language: ?language .
    OPTIONAL { ?language rdfs:label ?languageLabel . }
  }

  OPTIONAL {
    ?software denoted_by: ?publication .
    ?publication a nfdicore_publication: ;
                 rdfs:label ?publicationLabel .
  }

  OPTIONAL {
    ?software denoted_by: ?repositoryNode .
    ?repositoryNode a mwo_repository: ;
                    has_url: ?repositoryURL .
  }
}
LIMIT 999
```

---

### What are the services present in the MSE-KG? What are the service URL, documentation URL and code URL of these services?

```sparql
PREFIX nfdicore_service:  <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000232>
PREFIX has_specification:          <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000204>
PREFIX denoted_by:      <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX written_name:     <http://purl.obolibrary.org/obo/IAO_0000590>
PREFIX has_url:         <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX rdfs:              <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?service ?serviceLabel_ ?serviceURL ?docURL ?codeURL WHERE {

  ?service a nfdicore_service: .

  OPTIONAL {
    ?service denoted_by: ?serviceLabelNode .
    ?serviceLabelNode a written_name: ;
                      rdfs:label ?serviceLabel_ .
  }

  OPTIONAL {
    ?service has_specification: ?urlNode .
    ?urlNode has_url: ?serviceURL .
  }

  OPTIONAL {
    ?service has_specification: ?docNode .
    ?docNode has_url: ?docURL .
  }

  OPTIONAL {
    ?service has_specification: ?codeNode .
    ?codeNode has_url: ?codeURL .
  }
}
LIMIT 999
```

---

### What are the organizations present in the MSE-KG? What are the acronym, city, rorID of these organizations?

```sparql
PREFIX organization: <http://purl.obolibrary.org/obo/OBI_0000245>
PREFIX located_in:            <http://purl.obolibrary.org/obo/BFO_0000171>
PREFIX nfdicore_city:         <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000106>
PREFIX denoted_by:          <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX abbreviation_textual_entity:         <http://purl.obolibrary.org/obo/IAO_0000605>
PREFIX has_external_identifier:        <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006>
PREFIX has_url:             <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX rdfs:                  <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?org ?label_en ?acronym ?city ?rorID WHERE {

  ?org a organization: .
  OPTIONAL { ?org rdfs:label ?label_en . }

  OPTIONAL {
    ?org denoted_by: ?acrNode .
    ?acrNode a abbreviation_textual_entity: ;
             rdfs:label ?acronym .
  }

  OPTIONAL {
    ?org located_in: ?cityNode .
    ?cityNode a nfdicore_city: ;
              rdfs:label ?city .
  }

  OPTIONAL {
    ?org has_external_identifier: ?idNode .
    ?idNode has_url: ?rorID .
  }
}
LIMIT 999
```

---

### What are the people present in the MSE-KG? What are the email addresses and ORCID IDs of these people?

```sparql
PREFIX nfdicore_person:  <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000004>
PREFIX denoted_by:     <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX email_address:      <http://purl.obolibrary.org/obo/IAO_0000429>
PREFIX has_external_identifier:   <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006>
PREFIX has_url:        <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX rdfs:             <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?person ?label ?email ?orcid WHERE {

  ?person a nfdicore_person: .
  OPTIONAL { ?person rdfs:label ?label . }

  OPTIONAL {
    ?person denoted_by: ?emailNode .
    ?emailNode a email_address: ;
               rdfs:label ?email .
  }

  OPTIONAL {
    ?person has_external_identifier: ?idNode .
    ?idNode has_url: ?orcid .
  }
}
LIMIT 999
```

---

### What are the events present in the MSE-KG? What are the URL, associated organization and participating consortia of these events?

```sparql
PREFIX nfdicore_event:        <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000018>
PREFIX denoted_by:          <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX nfdicore_title:        <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001019>
PREFIX has_url:             <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX participant:           <http://purl.obolibrary.org/obo/RO_0000057>
PREFIX organization: <http://purl.obolibrary.org/obo/OBI_0000245>
PREFIX nfdicore_consortium:   <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000006>
PREFIX rdfs:                  <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?event ?contributionLabel ?eventURL ?orgLabel ?consortiumLabel WHERE {

  ?event a nfdicore_event: .

  # Event contribution label
  OPTIONAL {
    ?event denoted_by: ?contributionLabelNode .
    ?contributionLabelNode a nfdicore_title: ;
                           rdfs:label ?contributionLabel .
  }

  # Link (URL / PID)
  OPTIONAL {
    ?event denoted_by: ?URL .
    ?URL has_url: ?eventURL .
  }

  # Associated organization
  OPTIONAL {
    ?event participant: ?org .
    ?org a organization: ;
         rdfs:label ?orgLabel .
  }

  # Participating consortia
  OPTIONAL {
    ?event participant: ?consortium .
    ?consortium a nfdicore_consortium: ;
                rdfs:label ?consortiumLabel .
  }
}
LIMIT 999
```

---

### What are the people present in the MSE-KG? What are the email addresses and ORCID IDs of these people?

```sparql
PREFIX nfdicore_person: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000004>
PREFIX denoted_by:    <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX email_address:   <http://purl.obolibrary.org/obo/IAO_0000429>
PREFIX has_external_identifier:  <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006>
PREFIX has_url:       <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX rdfs:            <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?person ?label ?email ?orcid WHERE {

  ?person a nfdicore_person: .

  OPTIONAL { ?person rdfs:label ?label . }

  # Title (kept from original; guarded to literals to avoid LANG() on IRIs)
  OPTIONAL {
    ?person denoted_by: ?title .
    FILTER(isLiteral(?title))
    FILTER(LANG(?title) = "" || LANG(?title) = "en")
  }

  # E-mail (via email node)
  OPTIONAL {
    ?person denoted_by: ?emailNode .
    ?emailNode a email_address: ;
               rdfs:label ?email .
  }

  # ORCID ID
  OPTIONAL {
    ?person has_external_identifier: ?id .
    ?id has_url: ?orcid .
  }
}
LIMIT 999
```

---

### What are the SPARQL endpoints in the MSE-KG that are about "process"?

```sparql
PREFIX nfdicore_dataset: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009>
PREFIX denoted_by:     <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX has_url:        <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX bfo_process:      <http://purl.obolibrary.org/obo/BFO_0000015>
PREFIX rdfs:             <http://www.w3.org/2000/01/rdf-schema#>
PREFIX void:             <http://rdfs.org/ns/void#>
PREFIX xsd:              <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?dataset_name ?dataset ?sparql_endpoint ?term_class (xsd:integer(?n) AS ?instances)
WHERE {
  ?dataset a nfdicore_dataset: ;
           denoted_by: ?epInd ;
           void:classPartition ?cp ;
           denoted_by: ?sparql_endpoint_node .

  ?sparql_endpoint_node has_url: ?sparql_endpoint .

  OPTIONAL {
    ?dataset rdfs:label ?dataset_name .
  }

  ?cp void:class ?term_class ;
      void:entities ?n .

  VALUES ?term_class {
    bfo_process:      # BFO process
    # add more IRIs
  }
}
ORDER BY ?dataset ?term_class
```

---

### What are the datasets present in the MSE-KG? What are creators, creator's affiliations and link of these datasets?

```sparql
PREFIX nfdicore_dataset:            <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009>
PREFIX denoted_by:                <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX nfdicore_title:              <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001019>
PREFIX has_contributor:             <http://purl.obolibrary.org/obo/BFO_0000178>
PREFIX nfdicore_creator_role:       <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001032>
PREFIX nfdicore_institution_list:   <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001033>
PREFIX has_url:                   <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX rdfs:                        <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?dataset ?title ?creatorLabel ?creatorAffiliationLabel ?link
WHERE {
  ?dataset a nfdicore_dataset: .

  # Dataset title
  OPTIONAL {
    ?dataset denoted_by: ?titleNode .
    ?titleNode a nfdicore_title: ;
               rdfs:label ?title .
  }

  # Creator
  OPTIONAL {
    ?dataset has_contributor: ?creator .
    ?creator a nfdicore_creator_role: ;
             rdfs:label ?creatorLabel .
  }

  # Creator affiliation
  OPTIONAL {
    ?dataset has_contributor: ?creatorAffiliation .
    ?creatorAffiliation a nfdicore_institution_list: ;
                        rdfs:label ?creatorAffiliationLabel .
  }

  # Link (URL or PID)
  OPTIONAL {
    ?dataset denoted_by: ?linkNode .
    ?linkNode has_url: ?link .
  }
}
LIMIT 999
```

---

### What are the data portals present in the MSE-KG? What are the links, repositories and contactpoint names or email addresses of these data portals?

```sparql
PREFIX nfdicore_dataportal:  <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000123>
PREFIX denoted_by:         <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX nfdicore_title:       <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001019>
PREFIX has_value:        <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007>
PREFIX has_url:            <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX participates_in:      <http://purl.obolibrary.org/obo/BFO_0000056>
PREFIX participant:          <http://purl.obolibrary.org/obo/RO_0000057>
PREFIX has_role:             <http://purl.obolibrary.org/obo/BFO_0000055>
PREFIX written_name:        <http://purl.obolibrary.org/obo/IAO_0000590>
PREFIX email_address:        <http://purl.obolibrary.org/obo/IAO_0000429>
PREFIX nfdicore_website:         <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000223>
PREFIX rdfs:                 <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?portal ?name ?link ?repository
WHERE {
  ?portal a nfdicore_dataportal: .

  ?portal denoted_by: ?nameNode .
  ?nameNode a nfdicore_title: ;
              rdfs:label ?name .
  
  ?portal denoted_by: ?linkNode .
  ?linkNode has_url: ?link .
  
  ?portal denoted_by: ?repositoryNode .
  ?repositoryNode has_url: ?repository .
  
  # Participates in contacting process
  OPTIONAL {
    ?portal participates_in: ?contactingProcess .
    ?contactingProcess participant: ?contactpoint ;
                      has_role: ?contactpointRole .
    
    ?contactpoint denoted_by: ?contactpointNode .
    OPTIONAL {
      ?contactpointNode a written_name: ;
                        has_value: ?contactpointName .
    }

    ?contactpointRole denoted_by: ?Email .
    OPTIONAL {
      ?Email a email_address: ;
               has_value: ?Email .
    }
  }
}
LIMIT 999
```

---

### What are the instruments present in the MSE-KG? What are the contactpoint names or email addresses or websites of these instruments?

```sparql
PREFIX pmd_instrument:    <https://w3id.org/pmd/co/PMD_0000602>
PREFIX denoted_by:      <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX has_value:     <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007>
PREFIX participates_in:   <http://purl.obolibrary.org/obo/BFO_0000056>
PREFIX participant:       <http://purl.obolibrary.org/obo/RO_0000057>
PREFIX has_role:          <http://purl.obolibrary.org/obo/BFO_0000055>
PREFIX written_name:     <http://purl.obolibrary.org/obo/IAO_0000590>
PREFIX email_address:     <http://purl.obolibrary.org/obo/IAO_0000429>
PREFIX nfdicore_website:      <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000223>
PREFIX has_url:         <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX rdfs:              <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?instrument ?name ?contactpointName ?Email ?Website
WHERE {
  ?instrument a pmd_instrument: .

  OPTIONAL {
    ?instrument denoted_by: ?instrumentNode .
    ?instrumentNode rdfs:label ?name . 
  }

  OPTIONAL {
    ?instrument participates_in: ?contactingProcess .
    ?contactingProcess participant: ?contactpoint ;
                      has_role: ?contactpointRole .

    OPTIONAL {
      ?contactpoint denoted_by: ?contactpointNode .
      ?contactpointNode a written_name: ;
                        has_value: ?contactpointName .
    }

    OPTIONAL {
      ?contactpointRole denoted_by: ?EmailWebsite .
      ?EmailWebsite a email_address: ;
                    has_value: ?Email .
    }

    OPTIONAL {
      ?contactpointRole denoted_by: ?EmailWebsite2 .
      ?EmailWebsite2 a nfdicore_website: ;
                     has_url: ?Website .
    }
  }
}
LIMIT 999
```

---

### What are the large scale facilities present in the MSE-KG? What are the acronyms, organization or email or website of providers of these large scale facilities?

```sparql
PREFIX nfdicore_large_scale_facility:          <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001206>
PREFIX denoted_by:          <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX written_name:        <http://purl.obolibrary.org/obo/IAO_0000590>
PREFIX abbreviation_textual_entity:         <http://purl.obolibrary.org/obo/IAO_0000605>
PREFIX is_output_of:    <http://purl.obolibrary.org/obo/OBI_0000312>
PREFIX participant:           <http://purl.obolibrary.org/obo/RO_0000057>
PREFIX has_role:              <http://purl.obolibrary.org/obo/BFO_0000055>
PREFIX organization: <http://purl.obolibrary.org/obo/OBI_0000245>
PREFIX nfdicore_providerrole: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000230>
PREFIX email_address:         <http://purl.obolibrary.org/obo/IAO_0000429>
PREFIX has_value:         <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007>
PREFIX nfdicore_website:          <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000223>
PREFIX has_url:             <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX rdfs:                  <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?facility ?name ?acronym ?orgLabel ?Email ?Website
WHERE {
  ?facility a nfdicore_large_scale_facility: .

  OPTIONAL {
    ?facility denoted_by: ?facilityNode .
    ?facilityNode a written_name: ;
                  rdfs:label ?name .
  }

  OPTIONAL {
    ?facility denoted_by: ?acr .
    ?acr a abbreviation_textual_entity: ;
         rdfs:label ?acronym .
  }

  OPTIONAL {
    ?facility is_output_of: ?outputProcess .
    ?outputProcess participant: ?org ;
                   has_role: ?Role .

    OPTIONAL {
      ?org a organization: ;
           rdfs:label ?orgLabel .
    }

    ?Role a nfdicore_providerrole: ;
          denoted_by: ?EmailWebsite .

    OPTIONAL {
      ?EmailWebsite a email_address: ;
                    has_value: ?Email .
    }
    OPTIONAL {
      ?EmailWebsite a nfdicore_website: ;
                    has_url: ?Website .
    }
  }
}
LIMIT 999
```

---

### What are the present metadata in the MSE-KG? What are the names and repository links of these metadata?

```sparql
PREFIX nfdicore_metadata_specification: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001054>
PREFIX denoted_by:      <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX written_name:        <http://purl.obolibrary.org/obo/IAO_0000590>
PREFIX has_url:         <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX rdfs:              <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?metadata ?name ?repoLink
WHERE {
  ?metadata a nfdicore_metadata_specification: .

  OPTIONAL {
    ?metadata denoted_by: ?metadataNode .
    ?metadataNode a written_name: ;
                  rdfs:label ?name .
  }

  OPTIONAL {
    ?metadata denoted_by: ?Link .
    ?Link has_url: ?repoLink .
  }

  OPTIONAL {
    ?metadata denoted_by: ?Link2 .
    ?Link2 has_url: ?docLink .
  }
}
```

---

### What are the ontologies present in the MSE-KG? What are the name and links of these ontologies?

```sparql
PREFIX nfdicore_ontology: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000023>
PREFIX is_subject_of:       <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000226>
PREFIX nfdicore_title:    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001019>
PREFIX has_value:     <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007>
PREFIX nfdicore_source_code_repository:     <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000030>
PREFIX has_url:         <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>

SELECT DISTINCT ?ontology ?ontoname ?repoLink
WHERE {
  ?ontology a nfdicore_ontology: .

  OPTIONAL {
    ?ontology is_subject_of: ?ontologyTitle .
    ?ontologyTitle a nfdicore_title: ;
                   has_value: ?ontoname .
  }

  OPTIONAL {
    ?ontology is_subject_of: ?ontologyRepo .
    ?ontologyRepo a nfdicore_source_code_repository: ;
                  has_url: ?repoLink .
  }
}
LIMIT 999
```

---

### What are the international collaborations present in the MSE-KG? What are the names of these international collaborations?

```sparql
PREFIX mwo_collaboration: <http://purls.helmholtz-metadaten.de/mwo/MWO_0001003>
PREFIX denoted_by:                   <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX nfdicore_title:                 <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001019>
PREFIX rdfs:                           <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?int_collaboration ?name
WHERE {
  ?int_collaboration a mwo_collaboration: .

  OPTIONAL {
    ?int_collaboration denoted_by: ?nameNode .
    ?nameNode a nfdicore_title: ;
              rdfs:label ?name .
  }
}
LIMIT 999
```

---

### What are the publications present in the MSE-KG? What are the DOI, authors and authors affiliations of these publications?

```sparql
PREFIX nfdicore_publication:       <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000190>
PREFIX has_external_identifier:             <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006>
PREFIX has_url:                  <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX has_contributor:            <http://purl.obolibrary.org/obo/BFO_0000178>
PREFIX nfdicore_author_list:       <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001030>
PREFIX nfdicore_institution_list:  <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001033>
PREFIX rdfs:                       <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?publications ?name ?DOI ?authorsLabel ?authorsAffiliationLabel
WHERE {
  ?publications a nfdicore_publication: .

  OPTIONAL { ?publications rdfs:label ?name . }

  OPTIONAL {
    ?publications has_external_identifier: ?DOINode .
    ?DOINode has_url: ?DOI .
  }

  OPTIONAL {
    ?publications has_contributor: ?authors .
    ?authors a nfdicore_author_list: ;
             rdfs:label ?authorsLabel .
  }

  OPTIONAL {
    ?publications has_contributor: ?authorsAffiliation .
    ?authorsAffiliation a nfdicore_institution_list: ;
                        rdfs:label ?authorsAffiliationLabel .
  }
}
LIMIT 999
```

---

### What are the Task Areas in MatWerk which are present in the MSE-KG?

```sparql
PREFIX mwo_nfdi_matwerk_consortium:     <http://purls.helmholtz-metadaten.de/mwo/MWO_0001022>
PREFIX participates_in:      <http://purl.obolibrary.org/obo/BFO_0000056>
PREFIX concretizes:         <http://purl.obolibrary.org/obo/RO_0000059>
PREFIX objective_specification:        <http://purl.obolibrary.org/obo/IAO_0000005>
PREFIX denoted_by:         <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX nfdicore_description: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001018>
PREFIX rdfs:                 <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?ta_organization ?ta_process ?ta ?label ?description
WHERE {
  ?ta_organization a mwo_nfdi_matwerk_consortium: ;
                   participates_in: ?ta_process .

  ?ta_process concretizes: ?ta .
  ?ta a objective_specification: ;
      rdfs:label ?label .

  OPTIONAL {
    ?ta denoted_by: ?descriptionNode .
    ?descriptionNode a nfdicore_description: ;
                     rdfs:label ?description .
  }
}
LIMIT 999
```

---

### What are the Infrastructure Use Cases in MatWerk which are present in the MSE-KG?

```sparql
PREFIX mwo_iuc:       <http://purls.helmholtz-metadaten.de/mwo/MWO_0001026>
PREFIX denoted_by:  <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX written_name:    <http://purl.obolibrary.org/obo/IAO_0000590>
PREFIX rdfs:          <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?iuc ?label ?name
WHERE {
  ?iuc a mwo_iuc: .
  OPTIONAL { ?iuc rdfs:label ?label . }
  OPTIONAL {
    ?iuc denoted_by: ?nameNode .
    ?nameNode a written_name: ;
              rdfs:label ?name .
  }
}
LIMIT 999
```

---

### What are the Participant Projects in MatWerk which are present in the MSE-KG?

```sparql
PREFIX mwo_pp:        <http://purls.helmholtz-metadaten.de/mwo/MWO_0001029>
PREFIX denoted_by:  <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX written_name:    <http://purl.obolibrary.org/obo/IAO_0000590>
PREFIX rdfs:          <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?pp ?label ?name 
WHERE {
  ?pp a mwo_pp: .

  OPTIONAL { ?pp rdfs:label ?label . }
  OPTIONAL {
    ?pp denoted_by: ?nameNode .
    ?nameNode a written_name: ;
              rdfs:label ?name .
  }
}
```

---

### What are the SPARQL endpoints in the MSE-KG?

```sparql
PREFIX nfdicore_sparql_endpoint:     <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001095>
PREFIX has_url:        <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX nfdicore_dataset: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009>
PREFIX denoted_by:     <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX rdfs:             <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?dataset ?datasetLabel ?endpoint ?endpointLabel ?sparqlURL
WHERE {
  ?endpoint a nfdicore_sparql_endpoint: ;
            rdfs:label ?endpointLabel ;
            has_url: ?sparqlURL .

  OPTIONAL {
    ?dataset a nfdicore_dataset: ;
             denoted_by: ?endpoint .
    OPTIONAL { ?dataset rdfs:label ?datasetLabel }
  }
}
ORDER BY ?dataset ?endpoint
```
---

