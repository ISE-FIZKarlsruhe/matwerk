## Competency Questions

## General questions with the corresponding SPARQL queries

### What are the entitiy types (concepts) present in the MSE-KG?

```sparql
SELECT DISTINCT ?Concept ?label
WHERE {
  [] a ?Concept
  optional {
    ?Concept rdfs:label ?label .    
  }
}
LIMIT 999
```

---
### How many entities exist for each concept in the MSE-KG?

```sparql
SELECT ?Concept ?label (COUNT(?entity) AS ?count)
WHERE {
  ?entity a ?Concept .
  optional {
    ?Concept rdfs:label ?label .    
  }
}
GROUP BY ?Concept ?label
ORDER BY DESC(?count)
LIMIT 999
```

---
### What are the submitted records in NFDI-MatWerk Zenodo community present in the MSE-KG?

```sparql
# This query retrieves NFDI-MatWerk Zenodo community.

SELECT ?records ?recordsLabel_ ?filesInRecordLabel_ ?doi WHERE {

  # Match any subject (?records) that is an instance of one of the Dataset classes
  ?records a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009> .
  ?records rdfs:label ?recordsLabel_ .
  ?records <http://purl.obolibrary.org/obo/BFO_0000051> ?containsInRecord .
  ?containsInRecord rdfs:label ?filesInRecordLabel_ .
  ?containsInRecord rdfs:seeAlso ?doi .

}

# Limit results to 999 rows
LIMIT 999
```


---
### What are the Fair Digital Objects present in the MSE-KG?

```sparql
# This query retrieves Fair Digital Objects.

SELECT ?FDOs ?FDOsLabel_ ?FDOsParentDataset ?FDOsURL WHERE {

  # Match any subject (?FDOs) that is an instance of one of the digital object identifier classes
  ?FDOs a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001037> .
  ?FDOs rdfs:label ?FDOsLabel_ .
  ?FDOs <http://purl.obolibrary.org/obo/BFO_0000051> ?FDOsParentDataset .
  ?FDOs <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?FDOsURL .

}

# Limit results to 999 rows
LIMIT 999
```


---
### What are the softwares present in the MSE-KG? What are the license, programming language, repository URL and publication of these softwares?

```sparql
# This query retrieves software resources.

SELECT ?software ?softwareLabel_ ?licenseLabel ?languageLabel ?repositoryURL ?publicationLabel WHERE {

  # Match any subject (?software) that is an instance of one of the software-related classes
  VALUES ?class {
    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000121>  # Software
    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001045>
    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001046>
    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001048>
    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000218>
    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000140>
    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0010039>
    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0010040>
    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0010041>
    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000222>
    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001049>
    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001044>
  }
  ?software a ?class .
  
  # Try to extract a human-readable label for the software
  OPTIONAL { 
    ?software <http://purl.obolibrary.org/obo/IAO_0000235> ?softwareLabel .
    ?softwareLabel a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001000> .
    ?softwareLabel rdfs:label ?softwareLabel_ . }

  # Optionally, retrieve the associated license and its label
  OPTIONAL {
    ?software <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000142> ?license .
    ?license rdfs:label ?licenseLabel .
  }
  # Optionally, retrieve the Programming Language and its label
  OPTIONAL {
    ?software <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000187> ?language .
    ?language rdfs:label ?languageLabel .
  }
  # Optionally, retrieve the related publication and its label
  OPTIONAL {
    ?software <http://purl.obolibrary.org/obo/IAO_0000235> ?publication .
    ?publication a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000190> .
    ?publication rdfs:label ?publicationLabel .
  }
  # Optionally, retrieve the repository link
  OPTIONAL {
    ?software <http://purl.obolibrary.org/obo/IAO_0000235> ?repository .
    ?repository a <http://purls.helmholtz-metadaten.de/mwo/MWO_0001113> .
    ?repository <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?repositoryURL .
  }
}

# Limit results to 999 rows
LIMIT 999
```

---
### What are the services present in the MSE-KG? What are the service URL, documentation URL and code URL of these services?

```sparql
# This query retrieves service resources and related metadata from the MSE-KG.

SELECT DISTINCT ?service ?serviceLabel_ ?serviceURL ?docURL ?codeURL WHERE {

  # ?service is a resource of type Service Product
  ?service a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000232> .

  # The label resource for the service
  OPTIONAL {
    ?service <http://purl.obolibrary.org/obo/IAO_0000235> ?serviceLabel .
    ?serviceLabel a <http://purl.obolibrary.org/obo/IAO_0000590> .
    ?serviceLabel rdfs:label ?serviceLabel_ . 
  }


  # Service link (URL)
  OPTIONAL {
    ?service <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000204> ?URL .
    ?URL <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?serviceURL .
  }

  # Documentation link
  OPTIONAL {
    ?service <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000204> ?URL .
    ?URL <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?docURL .
  }

  # Source code link
  OPTIONAL {
    ?service <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000204> ?URL .
    ?URL <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?codeURL .
  }

}
LIMIT 999

```

---
### What are the organizations present in the MSE-KG? What are the acronym, city, rorID of these organizations?

```sparql
# This query retrieves organization entities and their metadata from the MSE-KG.

SELECT ?org ?label_en ?acronym ?city ?rorID WHERE {

  # Restrict to entities of type 'organization'
  ?org a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000003> .

  # English label
  OPTIONAL {
    ?org rdfs:label ?label_en .
  }


  # Acronym
  OPTIONAL {
    ?org <http://purl.obolibrary.org/obo/IAO_0000235> ?acr .
    ?acr a <http://purl.obolibrary.org/obo/IAO_0000605> .
    ?acr rdfs:label ?acronym .
  }

  # City
  OPTIONAL {
    ?org <http://purl.obolibrary.org/obo/BFO_0000171> ?c .
    ?c a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000106> .
    ?c rdfs:label ?city .
  }


  # ROR ID
  OPTIONAL {
    ?org <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006> ?ID .
    ?ID <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?rorID .
  }
}
LIMIT 999

```

---
### What are the events present in the MSE-KG? What are the URL, associated organization and participating consortia of these events?

```sparql
SELECT ?event ?contributionLabel ?eventURL ?orgLabel ?consortiumLabel WHERE {

  # Find resources typed as one of the event types
  ?event a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000018> .

  # Retrieve event contribution label (main label)
  OPTIONAL {
    ?event <http://purl.obolibrary.org/obo/IAO_0000235> ?contributionLabelNode .
    ?contributionLabelNode a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001019> .
    ?contributionLabelNode rdfs:label ?contributionLabel .
  }


  # Link (URL / PID)
  OPTIONAL {
    ?event <http://purl.obolibrary.org/obo/IAO_0000235> ?URL .
    ?URL <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?eventURL .
  }

  # Associated organization
  OPTIONAL {
    ?event <http://purl.obolibrary.org/obo/BFO_0000057> ?org .
    ?org a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000003> .
    ?org rdfs:label ?orgLabel .
  }

  # Participating consortia (multiple possible)
  OPTIONAL {
    ?event <http://purl.obolibrary.org/obo/BFO_0000057> ?consortium .
    ?consortium a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000006> .
    ?consortium rdfs:label ?consortiumLabel .
  }

}
LIMIT 999

```

---
### What are the people present in the MSE-KG? What are the email addresses and ORCID IDs of these people?

```sparql
SELECT ?person ?label ?email ?orcid WHERE {

  # Filter to only retrieve individuals (persons)
  ?person a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000004> .
  # NFDI_0000004 represents the class 'Person' in the MSE ontology

  # Retrieve the main label of the person
  OPTIONAL {
    ?person rdfs:label ?label .
  }

  # Retrieve the title (e.g., Dr., Prof.)
  OPTIONAL {
    ?person <http://purl.obolibrary.org/obo/IAO_0000235> ?title .
    FILTER (LANG(?title) = "" || LANG(?title) = "en")
  }

  # E-mail
  OPTIONAL {
    ?person <http://purl.obolibrary.org/obo/IAO_0000235> ?emailNode .
    ?emailNode a <http://purl.obolibrary.org/obo/IAO_0000429> .
    ?emailNode rdfs:label ?email .
  }


  # ORCID ID
  OPTIONAL {
    ?person <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006> ?id .
    ?id <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?orcid .
  }

}
LIMIT 999

```

---
### What are the datasets present in the MSE-KG? What are creators, creator's affiliations and link of these datasets?

```sparql
SELECT ?dataset ?title ?creatorLabel ?creatorAffiliationLabel ?link
WHERE {
  # Dataset type
  VALUES ?class {
    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009>  # Dataset
    <http://purls.helmholtz-metadaten.de/mwo/MWO_0001058>
    <http://purls.helmholtz-metadaten.de/mwo/MWO_0001056>
    <http://purls.helmholtz-metadaten.de/mwo/MWO_0001057>
  }
  ?dataset a ?class .

  # Dataset title
  OPTIONAL {
    ?dataset <http://purl.obolibrary.org/obo/IAO_0000235> ?titleNode .
    ?titleNode a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001019> .
    ?titleNode rdfs:label ?title .
  }

  # Creator(s)
  OPTIONAL {
    ?dataset <http://purl.obolibrary.org/obo/BFO_0000178> ?creator .
    ?creator a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001032> .
    ?creator rdfs:label ?creatorLabel .
  }

  # Creator affiliation(s)
  OPTIONAL {
    ?dataset <http://purl.obolibrary.org/obo/BFO_0000178> ?creatorAffiliation .
    ?creatorAffiliation a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001033> .
    ?creatorAffiliation rdfs:label ?creatorAffiliationLabel .
  }

  # Link (URL or PID)
  OPTIONAL {
    ?dataset <http://purl.obolibrary.org/obo/IAO_0000235> ?linkNode .
    ?linkNode <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?link .
  }

}
LIMIT 999

```

---
### What are the data portals present in the MSE-KG? What are the links, repositories and contactpoint names or email addresses or websites of these data portals?

```sparql
SELECT ?portal ?name ?link ?repository ?contactpointName ?Email ?Website
WHERE {
  # Identify resources of type Data Portal
  ?portal a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000123> .

  # Portal name
  OPTIONAL {
    ?portal <http://purl.obolibrary.org/obo/IAO_0000235> ?nameNode .
    ?nameNode a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001019> .
    ?nameNode rdfs:label ?title .
  }

  # Link (URL or PID)
  OPTIONAL {
    ?portal <http://purl.obolibrary.org/obo/IAO_0000235> ?linkNode .
    ?linkNode <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?link .
  }

  # Link to repository
  OPTIONAL {
    ?portal <http://purl.obolibrary.org/obo/IAO_0000235> ?repositoryNode .
    ?repositoryNode <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?repository .
  }

  # Participates in contacting process
  OPTIONAL {
    ?portal <http://purl.obolibrary.org/obo/BFO_0000056> ?contactingProcess .
    ?contactingProcess <http://purl.obolibrary.org/obo/BFO_0000057> ?contactpoint .
    ?contactingProcess <http://purl.obolibrary.org/obo/BFO_0000055> ?contactpointRole .
    
    ?contactpoint <http://purl.obolibrary.org/obo/IAO_0000235> ?contactpointNode .
    OPTIONAL {?contactpointNode a <http://purl.obolibrary.org/obo/IAO_0000590> .
              ?contactpointNode <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007> ?contactpointName
             }
    
    ?contactpointRole <http://purl.obolibrary.org/obo/IAO_0000235> ?EmailWebsite .
    OPTIONAL {?EmailWebsite a <http://purl.obolibrary.org/obo/IAO_0000429> .
              ?EmailWebsite <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007> ?Email
             }
    OPTIONAL {?EmailWebsite a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000223> .
              ?EmailWebsite <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?Website
             }
  }

}
LIMIT 999

```

---
### What are the instruments present in the MSE-KG? What are the contactpoint names or email addresses or websites of these instruments?

```sparql
SELECT DISTINCT ?instrument ?name ?contactpointName ?Email ?Website
WHERE {
  ?instrument a <https://w3id.org/pmd/co/PMD_0000602> .

  OPTIONAL {
    ?instrument <http://purl.obolibrary.org/obo/IAO_0000235> ?instrumentNode .
    ?instrumentNode a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001019> .
    ?instrumentNode rdfs:label ?name .
  }


  # Contacting process
  OPTIONAL {
    ?instrument <http://purl.obolibrary.org/obo/BFO_0000056> ?contactingProcess .
    ?contactingProcess <http://purl.obolibrary.org/obo/BFO_0000057> ?contactpoint .
    ?contactingProcess <http://purl.obolibrary.org/obo/BFO_0000055> ?contactpointRole .
    
    ?contactpoint <http://purl.obolibrary.org/obo/IAO_0000235> ?contactpointNode .
    OPTIONAL {?contactpointNode a <http://purl.obolibrary.org/obo/IAO_0000590> .
              ?contactpointNode <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007> ?contactpointName
             }
    
    ?contactpointRole <http://purl.obolibrary.org/obo/IAO_0000235> ?EmailWebsite .
    OPTIONAL {?EmailWebsite a <http://purl.obolibrary.org/obo/IAO_0000429> .
              ?EmailWebsite <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007> ?Email
             }
    OPTIONAL {?EmailWebsite a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000223> .
              ?EmailWebsite <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?Website
             }
  }

}

```

---
### What are the larg scale facilities present in the MSE-KG? What are the of acronyms, organization or emial or website of providersof these larg scale facilities?

```sparql
SELECT ?facility ?name ?acronym ?orgLabel ?Email ?Website
WHERE {
  ?facility a <http://purls.helmholtz-metadaten.de/mwo/MWO_0001027> .

  OPTIONAL {
    ?facility <http://purl.obolibrary.org/obo/IAO_0000235> ?facilityNode .
    ?facilityNode a <http://purl.obolibrary.org/obo/IAO_0000590> .
    ?facilityNode rdfs:label ?name .
  }
  # Acronym
  OPTIONAL {
    ?facility <http://purl.obolibrary.org/obo/IAO_0000235> ?acr .
    ?acr a <http://purl.obolibrary.org/obo/IAO_0000605> .
    ?acr rdfs:label ?acronym .
  }

  # Output Process
  OPTIONAL {
    ?facility <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001023> ?outputProcess .
    ?outputProcess <http://purl.obolibrary.org/obo/BFO_0000057> ?org .
    ?outputProcess <http://purl.obolibrary.org/obo/BFO_0000055> ?Role .
    
    OPTIONAL {?org a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000003> .
              ?org rdfs:label ?orgLabel .
             }
    
    ?Role a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000230> .
    ?Role <http://purl.obolibrary.org/obo/IAO_0000235> ?EmailWebsite .
    OPTIONAL {?EmailWebsite a <http://purl.obolibrary.org/obo/IAO_0000429> .
              ?EmailWebsite <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007> ?Email
             }
    OPTIONAL {?EmailWebsite a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000223> .
              ?EmailWebsite <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?Website
             }
  }
}
LIMIT 999

```

---
### What are the present metadata in the MSE-KG? What are the names and repository links of these metadata?

```sparql
SELECT ?metadata ?name ?repoLink
WHERE {
  ?metadata a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001054> .

  OPTIONAL {
    ?metadata <http://purl.obolibrary.org/obo/IAO_0000235> ?metadataNode .
    ?metadataNode a <http://purl.obolibrary.org/obo/IAO_0000590> .
    ?metadataNode rdfs:label ?name .
  }

  OPTIONAL {
    ?metadata <http://purl.obolibrary.org/obo/IAO_0000235> ?Link .
    ?Link <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?repoLink
  			}
  OPTIONAL {
    ?metadata <http://purl.obolibrary.org/obo/IAO_0000235> ?Link .
    ?Link <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?docLink
  			}
 
}

```

---
### What are the ontologies present in the MSE-KG? What are the name and links of these ontologies?

```sparql
SELECT ?ontology ?ontoname ?repoLink
WHERE {
  ?ontology a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000023> .
  OPTIONAL {
    ?ontology <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000226> ?ontologyTitle .
    ?ontologyTitle a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001019> .
    ?ontologyTitle <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007> ?ontoname.
  }
   OPTIONAL {
    ?ontology <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000226> ?ontologyRepo .
    ?ontologyRepo a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000030> .
    ?ontologyRepo <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?repoLink.
  }
}
LIMIT 999

```

---
### What are the international collabrations present in the MSE-KG? What are the of these international collabrations?

```sparql
SELECT ?int_colaborations ?name 
WHERE {

  ?int_colaborations a <http://purls.helmholtz-metadaten.de/mwo/MWO_0001005> .

  OPTIONAL { ?int_colaborations <http://purl.obolibrary.org/obo/IAO_0000235> ?nameNode . 
           ?nameNode a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001019> .
           ?nameNode rdfs:label ?name .
           }
}
LIMIT 999

```

---
### What are the ontologies present in the MSE-KG? What are the name and links of these ontologies?

```sparql
SELECT ?ontology ?ontoname ?repoLink
WHERE {
  ?ontology a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000023> .
  OPTIONAL {
    ?ontology <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000226> ?ontologyTitle .
    ?ontologyTitle a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001019> .
    ?ontologyTitle <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007> ?ontoname.
  }
   OPTIONAL {
    ?ontology <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000226> ?ontologyRepo .
    ?ontologyRepo a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000030> .
    ?ontologyRepo <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?repoLink.
  }
}
LIMIT 999

```

---
### List the ontologies can be downloaded in turtle serialization format, along with the links

```sparql
SELECT ?ontology ?ontoname ?filelink
WHERE {
  ?ontology a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000023> .
    ?ontology <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000226> ?ontologyTitle .
    ?ontologyTitle a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001019> .
    ?ontologyTitle <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007> ?ontoname.
    ?ontology <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000226> ?ontologyRepo .
    ?ontologyRepo a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000030> .
  
    ?file a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027> .
    ?file <http://purl.obolibrary.org/obo/BFO_0000176> ?ontologyRepo .
    
    ?extens <http://purl.obolibrary.org/obo/IAO_0000136> ?file .
    ?extens a <http://edamontology.org/format_3255> .
    ?file <http://www.w3.org/ns/dcat#downloadURL> ?downlURL .
    ?downlURL <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?filelink .
}
LIMIT 999

```

---
### What are the publications present in the MSE-KG? What are the DOI, authors and authors affiliations of these publications?

```sparql
SELECT ?publications ?name ?DOI ?authorsLabel ?authorsAffiliationLabel
WHERE {
  ?publications a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000190> .

  OPTIONAL { ?publications rdfs:label ?name .
           }
  
  OPTIONAL {
    ?publications <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006> ?DOINode .
    ?DOINode <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?DOI
  			}
  
  # Authors
  OPTIONAL {
    ?publications <http://purl.obolibrary.org/obo/BFO_0000178> ?authors .
    ?authors a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001030> .
    ?authors rdfs:label ?authorsLabel .
  }

  # Authors Affiliation
  OPTIONAL {
    ?publications <http://purl.obolibrary.org/obo/BFO_0000178> ?authorsAffiliation .
    ?authorsAffiliation a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001033> .
    ?authorsAffiliation rdfs:label ?authorsAffiliationLabel .
  }
}
LIMIT 999
```

---
### What are the Task Areas in MatWerk which are present in the MSE-KG?

```sparql
SELECT *
WHERE {
  ?ta_organization a <http://purls.helmholtz-metadaten.de/mwo/MWO_0001022> .
  ?ta_organization <http://purl.obolibrary.org/obo/BFO_0000056> ?ta_process .
    ?ta_organization <http://purl.obolibrary.org/obo/BFO_0000056> ?ta_process .
  	?ta_process <http://purl.obolibrary.org/obo/BFO_0000059> ?ta .
    ?ta a <http://purl.obolibrary.org/obo/IAO_0000005> .
    ?ta rdfs:label ?label .
  OPTIONAL { ?ta <http://purl.obolibrary.org/obo/IAO_0000235> ?descriptionNode . 
           ?descriptionNode a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001018> .
           ?descriptionNode rdfs:label ?description .
           }
}
LIMIT 999
```

---
### What are the Infrastructure Use Cases in MatWerk which are present in the MSE-KG?

```sparql
SELECT ?iuc ?label ?name
WHERE {
  ?iuc a <http://purls.helmholtz-metadaten.de/mwo/MWO_0001026> .

  OPTIONAL { ?iuc rdfs:label ?label . }
  OPTIONAL { ?iuc <http://purl.obolibrary.org/obo/IAO_0000235> ?nameNode . 
           ?nameNode a <http://purl.obolibrary.org/obo/IAO_0000590> .
           ?nameNode rdfs:label ?name .
           }
  #OPTIONAL { ?iuc <http://purl.obolibrary.org/obo/BFO_0000178> ?mainTask . }
  #OPTIONAL { ?iuc <http://purl.obolibrary.org/obo/BFO_0000178> ?relatedTA . }
  #OPTIONAL { ?iuc <http://purl.obolibrary.org/obo/BFO_0000056> ?project . }
}
LIMIT 999

```

---
### What are the Participant Projects in MatWerk which are present in the MSE-KG?

```sparql
SELECT ?pp ?label ?name 
WHERE {
  ?pp a <http://purls.helmholtz-metadaten.de/mwo/MWO_0001029> .

  OPTIONAL { ?pp rdfs:label ?label . }
  OPTIONAL { ?pp <http://purl.obolibrary.org/obo/IAO_0000235> ?nameNode . 
           ?nameNode a <http://purl.obolibrary.org/obo/IAO_0000590> .
           ?nameNode rdfs:label ?name .
           }
}

```

---
### What are the SPARQL endpoints in the MSE-KG?

```sparql
PREFIX mwo:    <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ontology: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX obo:    <http://purl.obolibrary.org/obo/>

SELECT ?dataset ?datasetLabel ?endpoint ?endpointLabel ?sparqlURL
WHERE {
  ?endpoint a mwo:MWO_0001060 ;
            rdfs:label ?endpointLabel ;
            ontology:NFDI_0001008 ?sparqlURL .

  OPTIONAL {
    ?dataset a ontology:NFDI_0000009 ;
             obo:IAO_0000235 ?endpoint .
    OPTIONAL { ?dataset rdfs:label ?datasetLabel }
  }
}
ORDER BY ?dataset ?endpoint

```

---
