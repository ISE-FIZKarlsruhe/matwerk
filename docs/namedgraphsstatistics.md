## Named graphs statistics

Every query below returns one row per thing counted — no duplicates.

Provenance triples are matched inside `GRAPH ?provGraph` rather than against a
hard-coded graph IRI, so a query keeps working when a harvester changes where it
writes its registry.

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

SELECT DISTINCT ?graph ?descriptionText
WHERE {
  GRAPH ?provGraph {
    ?graph a nfdicore:NFDI_0000009 ;
           obo:IAO_0000235 ?descNode .

    ?descNode a nfdicore:NFDI_0001018 ;
              nfdicore:NFDI_0001007 ?descriptionText .
  }
}
ORDER BY ?graph
```

---

### List the begin/end timestamps per Named Graph

```sparql
PREFIX obo:      <http://purl.obolibrary.org/obo/>
PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX time:     <http://www.w3.org/2006/time#>

SELECT DISTINCT ?graph ?beginTS ?endTS
WHERE {
  GRAPH ?provGraph {
    ?graph a nfdicore:NFDI_0000009 ;
           obo:RO_0002353 ?process .

    ?process obo:BFO_0000199 ?temporalRegion .
    ?temporalRegion obo:BFO_0000222 ?beginNode ;
                    obo:BFO_0000224 ?endNode .

    ?beginNode time:inXSDDateTimeStamp ?beginTS .
    ?endNode   time:inXSDDateTimeStamp ?endTS .
  }
}
ORDER BY DESC(?beginTS)
```

---

### What are the Zenodo file graphs imported into the MatWerk KG?

```sparql
PREFIX nfdicore_file: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027>
PREFIX part_of:       <http://purl.obolibrary.org/obo/BFO_0000050>
PREFIX rdfs:          <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?record ?fileGraph ?fileGraphLabel_
WHERE {
  GRAPH ?provGraph {
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
PREFIX nfdicore_file: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027>
PREFIX part_of:       <http://purl.obolibrary.org/obo/BFO_0000050>
PREFIX rdfs:          <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?fileGraph ?fileGraphLabel_ (COUNT(*) AS ?triples)
WHERE {
  {
    SELECT DISTINCT ?fileGraph ?fileGraphLabel_
    WHERE {
      GRAPH ?provGraph {
        ?fileGraph a nfdicore_file: ;
                   part_of: ?record ;
                   rdfs:label ?fileGraphLabel_ .
      }
    }
  }

  GRAPH ?fileGraph { ?s ?p ?o }
}
GROUP BY ?fileGraph ?fileGraphLabel_
ORDER BY DESC(?triples)
LIMIT 999
```

The inner `SELECT DISTINCT` matters: without it a file graph that is `part of`
more than one record would have its triples counted once per record.

---

### Which Zenodo records contributed RDF, and how many files each?

```sparql
PREFIX nfdicore_file: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027>
PREFIX part_of:       <http://purl.obolibrary.org/obo/BFO_0000050>

SELECT ?record (COUNT(DISTINCT ?fileGraph) AS ?fileCount)
WHERE {
  GRAPH ?provGraph {
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
PREFIX nfdicore_file: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027>
PREFIX part_of:       <http://purl.obolibrary.org/obo/BFO_0000050>
PREFIX has_url:       <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>

SELECT DISTINCT ?record ?fileGraph ?downloadURL
WHERE {
  GRAPH ?provGraph {
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
PREFIX nfdicore_file:        <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027>
PREFIX nfdicore_description: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001018>
PREFIX has_value:            <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007>
PREFIX part_of:              <http://purl.obolibrary.org/obo/BFO_0000050>
PREFIX denoted_by:           <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX rdfs:                 <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?record ?fileGraph ?fileGraphLabel_ ?descriptionText
WHERE {
  GRAPH ?provGraph {
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

A file graph is `denoted by` both a *description* node and a *validation* node —
both typed `nfdicore:NFDI_0001018`. The `rdfs:label "description"` filter is what
keeps the two apart; drop it and every file comes back twice.

---

### What is the reasoner validation status of each Zenodo file graph?

```sparql
PREFIX nfdicore_file:        <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027>
PREFIX nfdicore_description: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001018>
PREFIX has_value:            <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007>
PREFIX denoted_by:           <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX rdfs:                 <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?fileGraph ?fileGraphLabel_ ?validationText
WHERE {
  GRAPH ?provGraph {
    ?fileGraph a nfdicore_file: ;
               denoted_by: ?valNode .

    ?valNode a nfdicore_description: ;
             rdfs:label "validation" ;
             has_value: ?validationText .

    OPTIONAL { ?fileGraph rdfs:label ?fileGraphLabel_ . }
  }
}
ORDER BY ?fileGraph
LIMIT 999
```

---

### When was each Zenodo file graph harvested?

```sparql
PREFIX nfdicore_file:    <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027>
PREFIX output_of:        <http://purl.obolibrary.org/obo/RO_0002353>
PREFIX occupies_temporal_region: <http://purl.obolibrary.org/obo/BFO_0000199>
PREFIX has_first_instant: <http://purl.obolibrary.org/obo/BFO_0000222>
PREFIX time:             <http://www.w3.org/2006/time#>
PREFIX rdfs:             <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?fileGraph ?fileGraphLabel_ ?harvestedAt
WHERE {
  GRAPH ?provGraph {
    ?fileGraph a nfdicore_file: ;
               output_of: ?process .

    ?process occupies_temporal_region: ?temporalRegion .
    ?temporalRegion has_first_instant: ?instant .
    ?instant time:inXSDDateTimeStamp ?harvestedAt .

    OPTIONAL { ?fileGraph rdfs:label ?fileGraphLabel_ . }
  }
}
ORDER BY DESC(?harvestedAt)
LIMIT 999
```

---
