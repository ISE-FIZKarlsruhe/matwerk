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
         nfdicore:NFDI_0001023 ?process .

  ?process obo:BFO_0000199 ?temporalRegion .
  ?temporalRegion obo:BFO_0000222 ?beginNode ;
                  obo:BFO_0000224 ?endNode .

  ?beginNode time:inXSDDateTimeStamp ?beginTS .
  ?endNode   time:inXSDDateTimeStamp ?endTS .
}
ORDER BY DESC(?beginTS)
```

---