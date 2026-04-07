# URL Instance

Every URL instance must carry an actual URL value. This shape enforces that each instance of `NFDI_0000223` (URL Instance) has at least one `NFDI_0001008` (has url) property. This prevents placeholder URL records from entering the knowledge graph without a resolvable address.

## Visualization

```ontoink
source: shapes/url-instance/shape-data.ttl
shape: shapes/url-instance/shape.ttl
```

## SHACL Constraint

| Property | Value |
|----------|-------|
| **Target Class** | `nfdi:NFDI_0000223` (URL Instance) |
| **Constraint** | Must have at least one `nfdi:NFDI_0001008` (has url) property |
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
#  a URL instance (NFDI_0000223) always needs to have a url (NFDI_0001008)
#

ex:UrlInstanceShape
    a sh:NodeShape ;
    sh:targetClass <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000223> ;    # URL instance
    sh:property [
        sh:path <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ;       # has url
        sh:minCount 1 ;
        sh:message "a URL instance (NFDI_0000223) must always have a url (NFDI_0001008)"
    ] ;
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

mse:nfdicore_homepage_url
    a nfdi:NFDI_0000223 ;
    rdfs:label "NFDIcore ontology homepage" ;
    nfdi:NFDI_0001008 "https://nfdi.fiz-karlsruhe.de/ontology/nfdicore"^^xsd:anyURI .

mse:matwerk_sparql_endpoint_url
    a nfdi:NFDI_0000223 ;
    rdfs:label "MatWerk SPARQL endpoint" ;
    nfdi:NFDI_0001008 "https://matwerk.2e7.2f.2d.2e.2f.2d.2e.2f/sparql"^^xsd:anyURI .
```
