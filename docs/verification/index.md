# SPARQL Verification Queries

## Introduction

The MSE-KG validation pipeline includes a set of SPARQL verification queries that
act as automated quality gates. Each query is designed to detect a specific class of
data-quality or ontology-conformance issue **before** new triples are published to the
production knowledge graph.

The principle is simple: every verification query is written so that it returns
**zero rows** when the graph is valid. Any row returned by a query signals a
concrete violation that must be fixed before the data can proceed through the
pipeline.

!!! info "How verification fits into the pipeline"
    These queries run after RDF generation and SHACL shape validation. They
    complement shape-based validation by catching cross-graph consistency issues
    (e.g., undeclared classes or properties) and domain-specific constraints
    (e.g., ORCID representation rules) that are difficult to express in SHACL
    alone.

---

## Summary

| # | File | What it checks |
|---|------|----------------|
| 1 | `verify1.sparql` | ORCIDs must be IRIs, never plain literals |
| 2 | `verify2.sparql` | Forbidden BFO temporal predicates must not appear |
| 3 | `verify3.sparql` | Every `rdf:type` class must be declared in the ontology |
| 4 | `verify4.sparql` | Every predicate must be a declared property in the ontology |
| 5 | `verify5.sparql` | No two textual entities may share the same value |
| 6 | `reasoner_disjoint_violation.sparql` | No individual may instantiate two disjoint classes |

---

## Query Details

### 1 -- ORCID Must Be an IRI, Not a Literal

**What it checks:**
Detects triples where an ORCID identifier (`https://orcid.org/...`) is used as a
plain literal value instead of as an IRI resource, but only for subjects within the
MSE-KG namespace (`https://purls.helmholtz-metadaten.de/msekg`).

**Why it matters:**
ORCIDs are globally unique researcher identifiers and must be represented as IRIs so
that they can be dereferenced, linked across datasets, and matched by standard
federated queries. Storing them as string literals breaks interoperability and
prevents proper entity resolution.

!!! warning "Expected result"
    **0 rows.** Every row returned represents a triple where an ORCID that should be
    an IRI is incorrectly stored as a literal.

```sparql
##### OCRID must be IRI, not literal
#  
#  we don't what ORCIDs as literals used together with our resources
#
#  OCDIS always should be IRIs 
# 
#  Remark: some other ontologies or KGs are using ORCIDS as literals, we do not cover theses cases. 
#  That's why we filter ?s to begin with our namespace.
#

SELECT ?s ?p ?o "Literals should not contain sole ORCID (OCRID must be IRI, not literal)" WHERE {
  ?s ?p ?o .
  FILTER regex(str(?s),"^https://purls.helmholtz-metadaten.de/msekg") .
  FILTER isLiteral(?o) .
  FILTER regex(str(?o), "^https://orcid.org/\\d{4}-\\d{4}-\\d{4}-\\d{3}[0-9X]{1}$")
}
```

---

### 2 -- Forbidden BFO Temporal Predicates

**What it checks:**
Finds any BFO (Basic Formal Ontology) predicates used in the graph that are **not**
declared as `owl:ObjectProperty` in the approved BFO profile
(`bfo-2020-without-some-all-times`). The well-known `part_of` (`BFO_0000050`) and
`has_part` (`BFO_0000051`) relations are explicitly excluded from this check because
they are permitted.

**Why it matters:**
The MSE-KG adopts BFO 2020 in a temporally-neutral profile that deliberately omits
time-indexed predicates. Using a forbidden temporal predicate would introduce
inconsistencies with the chosen ontological commitment and could break reasoning.

!!! warning "Expected result"
    **0 rows.** Every row returned identifies a BFO predicate that is not sanctioned
    by the adopted BFO profile.

```sparql
##### Forbidden temporal predicate must not be used (e.g. BFO_0000176)

PREFIX owl: <http://www.w3.org/2002/07/owl#>

SELECT DISTINCT ?p WHERE {
  ?s ?p ?o .
  FILTER(STRSTARTS(STR(?p), "http://purl.obolibrary.org/obo/BFO_"))

  # skip part_of and has_part, see here: https://oborel.github.io/obo-relations/ro-and-bfo/
  FILTER(?p NOT IN (
    <http://purl.obolibrary.org/obo/BFO_0000050>,
    <http://purl.obolibrary.org/obo/BFO_0000051>
  ))
  
  FILTER NOT EXISTS {
    SERVICE <http://matwerk-virtuoso:8890/sparql> {
      GRAPH <https://nfdi.fiz-karlsruhe.de/matwerk/bfo-2020-without-some-all-times> {
        ?p a owl:ObjectProperty .
      }
    }
  }
}
```

---

### 3 -- Classes Must Be Declared in the Ontology

**What it checks:**
Identifies any individual that is typed with a class (`rdf:type`) which is **not**
declared as `owl:Class` or `rdfs:Class` in the reference ontology graph
(`mwo/3.0.1`), unless that class has been explicitly marked as deprecated
(`owl:deprecated true` or `owl:DeprecatedClass`). Built-in OWL classes are excluded
from the check.

**Why it matters:**
Using an undeclared or removed class means the data no longer conforms to the
governing ontology. This can happen after ontology upgrades where classes are renamed
or retired. Catching these mismatches early prevents silent data loss and reasoning
errors.

!!! warning "Expected result"
    **0 rows.** Every row returned identifies an instance typed with a class that
    does not exist (or is deprecated) in the ontology.

```sparql
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>

SELECT DISTINCT ?s ?c WHERE {
  ?s rdf:type ?c .
  FILTER(isIRI(?c))
  FILTER(!STRSTARTS(STR(?c), STR(owl:)))

  VALUES ?ctype { owl:Class rdfs:Class }
  ?c a ?ctype .

  # Class must be declared in the ontology graph as owl:Class or rdfs:Class
  # Class must not be deprecated in the ontology graph
  FILTER NOT EXISTS {
    SERVICE <http://matwerk-virtuoso:8890/sparql> {
      GRAPH <https://nfdi.fiz-karlsruhe.de/matwerk/mwo/3.0.1> {
        { ?c a ?ctype }
        UNION
        { ?c owl:deprecated true }
        UNION
        { ?c a owl:DeprecatedClass }
      }
    }
  }
}
```

---

### 4 -- Predicates Must Be Declared Properties in the Ontology

**What it checks:**
Ensures every predicate used in the graph that is typed as `owl:DatatypeProperty` or
`owl:ObjectProperty` is actually declared with that type in either the main ontology
graph (`mwo/3.0.1`) or the NFDIcore extension graph (`nfdicore-extension/3.0.4`).

**Why it matters:**
Undeclared properties indicate that the data uses vocabulary not sanctioned by the
ontology. This can result from typos, outdated mappings, or incomplete ontology
imports. Detecting these mismatches protects the integrity of downstream queries and
reasoning.

!!! warning "Expected result"
    **0 rows.** Every row returned identifies a property that is used in the data but
    not declared in any of the reference ontology graphs.

```sparql
##### Predicates must be declared properties in the ontology
#
# Ensures every predicate we use is declared as a property in the ontology graph.

PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?p ?ptype WHERE {
  ?s ?p ?o .

  VALUES ?ptype { owl:DatatypeProperty owl:ObjectProperty }
  ?p a ?ptype .

  FILTER NOT EXISTS {
    SERVICE <http://matwerk-virtuoso:8890/sparql> {
      GRAPH <https://nfdi.fiz-karlsruhe.de/matwerk/mwo/3.0.1> {
        ?p a ?ptype .
      }
    }
  }

  FILTER NOT EXISTS {
    SERVICE <http://matwerk-virtuoso:8890/sparql> {
      GRAPH <https://nfdi.fiz-karlsruhe.de/matwerk/nfdicore-extension/3.0.4> {
        ?p a ?ptype .
      }
    }
  }

}
```

---

### 5 -- Unique Textual Entity Values

**What it checks:**
Detects pairs of distinct resources that share the exact same value for the
`nfdi:NFDI_0001007` (textual entity) property. The query returns one row per
duplicate pair, ordered to avoid reporting the same pair twice.

**Why it matters:**
Textual entities in the MSE-KG are expected to be unique identifiers or labels that
distinguish one resource from another. Duplicate values suggest either a data-entry
error (the same label assigned to two different things) or a merging problem (two
IRIs that should have been reconciled into one). Enforcing uniqueness keeps the graph
clean and prevents ambiguous lookups.

!!! warning "Expected result"
    **0 rows.** Every row returned identifies a pair of resources that incorrectly
    share the same textual entity value.

```sparql
#  no two "textual entities" may share the same value (NFDI_0001007)

PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX nfdi: <https://nfdi.fiz-karlsruhe.de/ontology/>

SELECT $this ?value ?other
WHERE {
    $this  nfdi:NFDI_0001007 ?value .
    ?other nfdi:NFDI_0001007 ?value .
    FILTER ( $this != ?other && str($this) < str(?other) )
}
```

---

### 6 -- Disjoint Class Violation

**What it checks:**
Finds any individual `?x` that is simultaneously an instance of two classes `?C` and
`?D` where those classes have been declared `owl:disjointWith` each other. This is a
classic OWL consistency check that would normally be caught by a DL reasoner.

**Why it matters:**
Disjoint-class axioms encode fundamental domain constraints -- for example, a
*process* cannot simultaneously be a *material entity*. Violating these axioms makes
the graph logically inconsistent, which causes reasoners to derive arbitrary (and
therefore useless) conclusions. Running this check without a full reasoner provides a
lightweight yet effective consistency safeguard.

!!! warning "Expected result"
    **0 rows.** Every row returned identifies an individual that belongs to two
    mutually exclusive classes, indicating a logical inconsistency.

```sparql
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?x ?C ?D
WHERE {
  ?C owl:disjointWith ?D .
  ?x rdf:type ?C .
  ?x rdf:type ?D .
}
```
