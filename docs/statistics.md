## General questions related to the statistics of the MSE KG with the corresponding SPARQL queries

### What are the entity types (concepts) present in the MSE-KG?

```sparql
SELECT DISTINCT ?Concept ?label
WHERE {
  [] a ?Concept .
  OPTIONAL { ?Concept rdfs:label ?label . }
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
### How many entities (distinct subjects) are in the KG?

```sparql
SELECT (COUNT(DISTINCT ?s) AS ?entityCount)
WHERE {
  ?s ?p ?o .
}
```

---

### How many triples are stored in the KG?

```sparql
SELECT (COUNT(*) AS ?tripleCount)
WHERE {
  ?s ?p ?o .
}
```

---

### How many distinct classes (types) are used?

```sparql
SELECT (COUNT(DISTINCT ?type) AS ?classCount)
WHERE {
  ?s a ?type .
}
```

---

### How many distinct properties are used?

```sparql
SELECT (COUNT(DISTINCT ?p) AS ?propertyCount)
WHERE {
  ?s ?p ?o .
}
```

---

### What are the top 10 most frequently used properties?

```sparql
SELECT ?p ?plabel (COUNT(*) AS ?usageCount)
WHERE {
  ?s ?p ?o .
  OPTIONAL { ?p rdfs:label ?plabel . }
}
GROUP BY ?p ?plabel
ORDER BY DESC(?usageCount)
LIMIT 10
```

---

### What is the average number of triples per entity?

```sparql
SELECT (AVG(?triplesPerEntity) AS ?avgTriplesPerEntity)
WHERE {
  {
    SELECT ?s (COUNT(*) AS ?triplesPerEntity)
    WHERE {
      ?s ?p ?o .
    }
    GROUP BY ?s
  }
}
```

---

### How many literal values (e.g., strings, numbers) are used as objects?

```sparql
SELECT (COUNT(?o) AS ?literalCount)
WHERE {
  ?s ?p ?o .
  FILTER(isLiteral(?o))
}
```

---

### What are the most connected entities (by outgoing triples)?

```sparql
SELECT ?s (COUNT(?p) AS ?outDegree)
WHERE {
  ?s ?p ?o .
}
GROUP BY ?s
ORDER BY DESC(?outDegree)
LIMIT 10
```

---

### How many entities have no rdf:type?

```sparql
SELECT (COUNT(DISTINCT ?s) AS ?noTypeCount)
WHERE {
  ?s ?p ?o .
  FILTER NOT EXISTS { ?s a ?type }
}
```

---

### How many triples contain blank nodes?

```sparql
SELECT (COUNT(*) AS ?blankNodeTripleCount)
WHERE {
  ?s ?p ?o .
  FILTER(isBlank(?s) || isBlank(?o))
}
```

---

### What is the distribution of language tags in literals?

```sparql
SELECT ?lang (COUNT(*) AS ?count)
WHERE {
  ?s ?p ?o .
  FILTER(isLiteral(?o) && lang(?o) != "")
  BIND(lang(?o) AS ?lang)
}
GROUP BY ?lang
ORDER BY DESC(?count)
```

---

### What datatypes are used in literals and how often?

```sparql
SELECT ?datatype (COUNT(*) AS ?count)
WHERE {
  ?s ?p ?o .
  FILTER(isLiteral(?o))
  BIND(datatype(?o) AS ?datatype)
}
GROUP BY ?datatype
ORDER BY DESC(?count)
```

---

### Which classes are defined but never used (no instances)?

```sparql
SELECT ?class ?label
WHERE {
  ?class a owl:Class .
  FILTER NOT EXISTS { ?instance a ?class . }
  OPTIONAL { ?class rdfs:label ?label . }
}
```

---

### How many entities have no rdfs:label?

```sparql
SELECT (COUNT(DISTINCT ?s) AS ?noLabelCount)
WHERE {
  ?s ?p ?o .
  FILTER NOT EXISTS { ?s rdfs:label ?label }
}
```

---
### Entities linked to external vocabularies

```sparql
SELECT (COUNT(DISTINCT ?o) AS ?externalLinks)
WHERE {
  ?s ?p ?o .
  FILTER(
    isIRI(?o) &&
    STRSTARTS(STR(?o), "http") &&
    !STRSTARTS(STR(?o), "https://nfdi.fiz-karlsruhe.de/")
  )
}
```

---
### Number of owl:sameAs links

```sparql
SELECT (COUNT(*) AS ?sameAsCount)
WHERE {
  ?s owl:sameAs ?o .
}
```

---