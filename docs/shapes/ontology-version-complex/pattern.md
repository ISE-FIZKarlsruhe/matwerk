# Ontology Version (Complex)

Every ontology release version must be the subject of a version number or version IRI, and may additionally reference file data items. This shape uses `sh:or` to handle two cases: when no file data item exists (exactly one `NFDI_0000226` link to a version number/IRI) and when file data items are present (at least two `NFDI_0000226` links covering version metadata and files). This ensures complete provenance tracking for published ontology releases.

## Visualization

```ontoink
source: shapes/ontology-version-complex/shape-data.ttl
shape: shapes/ontology-version-complex/shape.ttl
```

## SHACL Constraint

| Property | Value |
|----------|-------|
| **Target Class** | `nfdi:NFDI_0000026` (Ontology Version) |
| **Constraint** | Must have `nfdi:NFDI_0000226` (is subject of) linking to a version number/IRI (`NFDI_0001053` or `IAO_0000129`), optionally also to file data items (`NFDI_0000027`) |
| **Severity** | Violation |

## Shape Definition

```turtle
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix sh:    <http://www.w3.org/ns/shacl#> .
@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix ex:    <http://www.example.org/#> .
@prefix owl:   <http://www.w3.org/2002/07/owl#> .

#
# Every ontology release version is subject of ontology version number/IRI. 
# The latter should take into account, that ontology version can also be subject of file data items.
# 

#shape in case we have no file which "is about" ontology

ex:filesIfNotExist
    sh:path <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000226> ; 
    sh:maxCount 1 ;
    sh:qualifiedValueShape [
          sh:path <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000226> ; 
       sh:class (<https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001053> <http://purl.obolibrary.org/obo/IAO_0000129>) ; #version IRI/version NUMBER
        sh:message "an 'ontology release version' (NFDI_0000026) should always 'is subject of' (NFDI_0000226) ontology version number/IRI"
    ] ;
    sh:qualifiedMinCount 1 ;
.

#shape in case we have at least one file which "is about" ontology

ex:filesIfExist
   sh:path <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000226> ; 
    sh:minCount 2 ;
    sh:qualifiedValueShape [
         sh:path <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000226> ; 
       sh:class (<https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001053> <http://purl.obolibrary.org/obo/IAO_0000129> <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027>) ; #version IRI/version NUMBER/file data item
        sh:message "an 'ontology release version' (NFDI_0000026) should always 'is subject of' (NFDI_0000226) ontology version number/IRI, additionally can be subject of file data item"
    ] ;
    sh:qualifiedMinCount 1 ;
.


#main shape

  ex:OntoVersionShape
    a sh:NodeShape ;
    sh:targetClass <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000026> ;    # nfdi:ontology version

 sh:or (
        [ sh:property ex:filesIfExist]
        [ sh:property ex:filesIfNotExist]
    )
.  
```

## Example Data

```turtle
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl:   <http://www.w3.org/2002/07/owl#> .
@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
@prefix obo:   <http://purl.obolibrary.org/obo/> .
@prefix nfdi:  <https://nfdi.fiz-karlsruhe.de/ontology/> .
@prefix mse:   <https://nfdi.fiz-karlsruhe.de/matwerk/> .

# Ontology version with version number and a file data item
mse:nfdicore_v2_1_0
    a nfdi:NFDI_0000026 ;
    rdfs:label "NFDIcore v2.1.0" ;
    nfdi:NFDI_0000226 mse:nfdicore_v2_1_0_version_number ;
    nfdi:NFDI_0000226 mse:nfdicore_v2_1_0_owl_file .

mse:nfdicore_v2_1_0_version_number
    a obo:IAO_0000129 ;
    rdfs:label "2.1.0" .

mse:nfdicore_v2_1_0_owl_file
    a nfdi:NFDI_0000027 ;
    rdfs:label "nfdicore-v2.1.0.owl" .
```
