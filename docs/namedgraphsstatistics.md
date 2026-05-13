## Named graphs statistics

### List all named graphs

```sparql
SELECT DISTINCT ?g
WHERE {
  GRAPH ?g { ?s ?p ?o }
}
ORDER BY ?g
```

---

### Number of triples per named graph

```sparql
SELECT ?g (COUNT(*) AS ?tripleCount)
WHERE {
  GRAPH ?g { ?s ?p ?o }
}
GROUP BY ?g
ORDER BY DESC(?tripleCount)
```

---

### Number of subjects / predicates / objects per named graph

```sparql
SELECT ?g
       (COUNT(DISTINCT ?s) AS ?subjects)
       (COUNT(DISTINCT ?p) AS ?predicates)
       (COUNT(DISTINCT ?o) AS ?objects)
WHERE {
  GRAPH ?g { ?s ?p ?o }
}
GROUP BY ?g
ORDER BY DESC(?subjects)
```

---

### Get the provenance description text per Named graph

```sparql
PREFIX obo:      <http://purl.obolibrary.org/obo/>
PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>

SELECT ?graph ?descriptionText
WHERE {
  ?graph a nfdicore:NFDI_0000009 ;
         obo:IAO_0000235 ?descNode .

  ?descNode a nfdicore:NFDI_0000018 ;
            nfdicore:NFDI_0001007 ?descriptionText .
}
ORDER BY ?graph
```

---

### List the begin/end timestamps per Named Graph

```sparql
PREFIX obo:      <http://purl.obolibrary.org/obo/>
PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX time:     <http://www.w3.org/2006/time#>

SELECT ?graph ?beginTS ?endTS
WHERE {
  ?graph a nfdicore:NFDI_0000009 ;
         obo:RO_0002353 ?process .

  ?process obo:BFO_0000199 ?temporalRegion .
  ?temporalRegion obo:BFO_0000222 ?beginNode ;
                  obo:BFO_0000224 ?endNode .

  ?beginNode time:inXSDDateTimeStamp ?beginTS .
  ?endNode   time:inXSDDateTimeStamp ?endTS .
}
ORDER BY DESC(?beginTS)
```

---

### What are the Zenodo file graphs imported into the MSE-KG?

```sparql
PREFIX zenodo_metadata: <https://nfdi.fiz-karlsruhe.de/matwerk/zenodo_metadata>
PREFIX nfdicore_file:   <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027>
PREFIX part_of:         <http://purl.obolibrary.org/obo/BFO_0000050>
PREFIX rdfs:            <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?fileGraph ?fileGraphLabel_ ?record WHERE {

  GRAPH zenodo_metadata: {
    ?fileGraph a nfdicore_file: ;
               part_of: ?record .
    OPTIONAL { ?fileGraph rdfs:label ?fileGraphLabel_ . }
  }
}
ORDER BY ?record ?fileGraph
LIMIT 999
```

---

### How many triples does each Zenodo file graph carry?

```sparql
PREFIX zenodo_metadata: <https://nfdi.fiz-karlsruhe.de/matwerk/zenodo_metadata>
PREFIX nfdicore_file:   <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027>
PREFIX part_of:         <http://purl.obolibrary.org/obo/BFO_0000050>

SELECT ?fileGraph (COUNT(*) AS ?triples) WHERE {

  GRAPH zenodo_metadata: {
    ?fileGraph a nfdicore_file: ;
               part_of: ?record .
  }

  GRAPH ?fileGraph { ?s ?p ?o }
}
GROUP BY ?fileGraph
ORDER BY DESC(?triples)
LIMIT 999
```

---

### Which Zenodo records contributed RDF, and how many files each?

```sparql
PREFIX zenodo_metadata: <https://nfdi.fiz-karlsruhe.de/matwerk/zenodo_metadata>
PREFIX nfdicore_file:   <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027>
PREFIX part_of:         <http://purl.obolibrary.org/obo/BFO_0000050>

SELECT ?record (COUNT(DISTINCT ?fileGraph) AS ?fileCount) WHERE {

  GRAPH zenodo_metadata: {
    ?fileGraph a nfdicore_file: ;
               part_of: ?record .
  }
}
GROUP BY ?record
ORDER BY DESC(?fileCount)
LIMIT 999
```

---

### What is the download URL of each Zenodo file graph?

```sparql
PREFIX zenodo_metadata: <https://nfdi.fiz-karlsruhe.de/matwerk/zenodo_metadata>
PREFIX nfdicore_file:   <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027>
PREFIX part_of:         <http://purl.obolibrary.org/obo/BFO_0000050>
PREFIX has_url:         <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>

SELECT ?record ?fileGraph ?downloadURL WHERE {

  GRAPH zenodo_metadata: {
    ?fileGraph a nfdicore_file: ;
               part_of: ?record ;
               has_url: ?downloadURL .
  }
}
ORDER BY ?record ?fileGraph
LIMIT 999
```

---

### Which files per Zenodo record were imported (with description text)?

```sparql
PREFIX zenodo_metadata:      <https://nfdi.fiz-karlsruhe.de/matwerk/zenodo_metadata>
PREFIX nfdicore_file:        <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027>
PREFIX nfdicore_description: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000018>
PREFIX has_value:            <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007>
PREFIX part_of:              <http://purl.obolibrary.org/obo/BFO_0000050>
PREFIX denoted_by:           <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX rdfs:                 <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?record ?fileGraph ?fileGraphLabel_ ?descriptionText WHERE {

  GRAPH zenodo_metadata: {
    ?fileGraph a nfdicore_file: ;
               part_of: ?record ;
               denoted_by: ?descNode .

    ?descNode a nfdicore_description: ;
              rdfs:label "description" ;
              has_value: ?descriptionText .

    OPTIONAL { ?fileGraph rdfs:label ?fileGraphLabel_ . }
  }
}
ORDER BY ?record ?fileGraph
LIMIT 999
```

---

### When was each Zenodo file graph harvested?

```sparql
PREFIX zenodo_metadata:  <https://nfdi.fiz-karlsruhe.de/matwerk/zenodo_metadata>
PREFIX nfdicore_file:    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027>
PREFIX output_of:        <http://purl.obolibrary.org/obo/RO_0002353>
PREFIX occupies_temporal_region: <http://purl.obolibrary.org/obo/BFO_0000199>
PREFIX has_first_instant: <http://purl.obolibrary.org/obo/BFO_0000222>
PREFIX has_last_instant:  <http://purl.obolibrary.org/obo/BFO_0000224>
PREFIX time:             <http://www.w3.org/2006/time#>

SELECT ?fileGraph ?harvestedAt WHERE {

  GRAPH zenodo_metadata: {
    ?fileGraph a nfdicore_file: ;
               output_of: ?process .

    ?process occupies_temporal_region: ?temporalRegion .

    ?temporalRegion has_first_instant: ?instant .

    ?instant time:inXSDDateTimeStamp ?harvestedAt .
  }
}
ORDER BY DESC(?harvestedAt)
LIMIT 999
```

---