# process

The `process` tab of the MatWerk workbook currently holds **140 rows**. Every row follows the same shape, and that shape is the pattern below: a **entity** instance carrying the row's identity, denoted by **value node** nodes that hold its values, and object properties pointing at entities in other tabs.

*Generated from the live sheet and MWO 3.0.2 by `scripts/gen_patterns.py` — regenerate it rather than editing it by hand.*

## The pattern

```ontoink
source: patterns/process/pattern.ttl
height: 560px
reasoning: false
```

> Drag the nodes, change the layout, or use **Group** to collapse a namespace. The picture is the example only; the declarations and annotations behind it are in [`module.ttl`](module.ttl).

## Columns

| Column | ROBOT | Property | Meaning |
|---|---|---|---|
| label | `A` | `rdfs:label` | label |
| has participant | `I` | `obo:RO_0000057` | has participant |
| realizes role | `I` | `obo:BFO_0000055` | realizes |
| occupies temporal region | `I` | `obo:BFO_0000199` | occupies temporal region |
| Standard | `I` | `nfdi:NFDI_0000207` |  |
| concretizes | `I` | `obo:RO_0000059` | concretizes |

## How to read it

* The **entity** is typed `obo:BFO_0000015` (process) and carries only its identity, its label, and links.
* A column marked `I` does **not** contain a value — it contains the *label of another instance*, which ROBOT resolves at build time. That is why every value node needs a unique label, and why a typo produces a silently missing relation rather than an error.
* Columns marked `A` are literals on the entity itself.

## Components (value nodes)

These value-node types live in the workbook's shared `req_2` tab but belong to this pattern — they exist to describe it, and are counted by how often this tab references them.

| Component | Class | References |
|---|---|---|
| contact point role | `nfdi:NFDI_0000114` | 78 |
| temporal region | `obo:BFO_0000008` | 19 |
| manufacturer role | `nfdi:NFDI_0001060` | 10 |
| service provider role | `nfdi:NFDI_0000230` | 8 |
| co spokesperson role | `nfdi:NFDI_0001058` | 5 |

## Query it

Retrieve instances of this pattern from the MatWerk Knowledge Graph ([SPARQL endpoint](https://nfdi.fiz-karlsruhe.de/matwerk/sparql)):

```sparql
SELECT ?x ?label WHERE { ?x a <http://purl.obolibrary.org/obo/BFO_0000015> ; <http://www.w3.org/2000/01/rdf-schema#label> ?label } LIMIT 10
```

The rows behind it are curated in the [`process` tab of the workbook](https://docs.google.com/spreadsheets/d/1OyoWwcX4zUtrJilwXdtTooavELw278nQSNW2oniBBsk/edit#gid=26830533).

## Competency questions

The questions this pattern exists to answer. Each is answerable against the published graphs.

1. Which processes are recorded in the knowledge graph, and who participates in them?
2. Which role is realised in a given process, and over which period?

## Pattern metadata

The [Ontology Design Patterns](http://ontologydesignpatterns.org) content-ODP annotation schema, in the community's own field order, so a reader who knows ODP pages can read this one without translation.

| Field | Value |
|---|---|
| **Name** | process |
| **Submitted by** | the NFDI-MatWerk consortium |
| **Also Known As** | event; event |
| **Intent** | To represent a process in the MatWerk Knowledge Graph: its identity, the value nodes that describe it, and its links to the other entities the NFDI-MatWerk consortium records. |
| **Domains** | research data management; NFDI consortium structure |
| **Competency Questions** | Which processes are recorded in the knowledge graph, and who participates in them?; Which role is realised in a given process, and over which period? |
| **Solution description** | An entity individual typed `obo:BFO_0000015` carries identity and label; each descriptive value is a separate *value node* individual reached by `obo:IAO_0000235` (denoted by); links to other patterns are object properties onto those entities. Identity is a minted IRI `msekg:<epoch_ms><counter>`, so re-running the pipeline is idempotent. |
| **Reusable OWL Building Block** | [`module.ttl`](module.ttl) — the example together with the axioms of every term it uses, extracted from the ontology, so the file stands alone |
| **Consequences** | Values become reusable and independently addressable, and a value may be shared by many entities. The cost: every `I` reference resolves **by label**, so labels must be unique and a typo yields a silently missing relation rather than an error. |
| **Scenarios** | “work of TA-SD_organization” is a process, with has participant “TACoSpokesperson 1”; realizes role “co-spokesperson role for work of TA-SD_organization”; concretizes “TA-SD”. |
| **Known Uses** | the MatWerk Knowledge Graph (`https://nfdi.fiz-karlsruhe.de/matwerk`), published as Virtuoso named graphs |
| **Web References** | <https://nfdi-matwerk.de> · <https://ise-fizkarlsruhe.github.io/mwo/> |
| **Other References** | https://doi.org/10.1002/adem.202502331 |
| **Examples (OWL files)** | [`pattern.ttl`](pattern.ttl) — one real row of the tab, with its value nodes resolved |
| **Extracted From** | the `process` tab of the NFDI-MatWerk curation workbook (140 rows at generation time) |
| **Reengineered From** | the ROBOT template directives in row 2 of that tab |
| **Has Components** | contact point role, temporal region, manufacturer role, service provider role, co spokesperson role |
| **Specialization Of** | the entity-and-value-node pattern shared by every tab ([overview](../index.md)) |
| **Related CPs** | [agent](../agent/pattern.md), [matwerkta](../matwerkta/pattern.md) |

### Provenance and versioning

| Field | Value |
|---|---|
| **Pattern version** | 1.0.0 |
| **Authors** | [Harald Sack](https://orcid.org/0000-0001-7069-9804) · [Jörg Waitelonis](https://orcid.org/0000-0001-7192-7143) · [Ebrahim Norouzi](https://orcid.org/0000-0002-2691-6995) · [Hossein Beygi Nasrabadi](https://orcid.org/0000-0002-3092-0532) — the authors of the ontology these patterns are derived from (`dcterms:creator` of MWO 3.0.2) |
| **Derived from ontology** | MatWerk Ontology (MWO) **3.0.2** — `http://purls.helmholtz-metadaten.de/mwo/mwo.owl/3.0.2` |
| **Reused ontologies** | BFO 2020 (`http://purl.obolibrary.org/obo/bfo.owl`), NFDIcore (`https://nfdi.fiz-karlsruhe.de/ontology/`), IAO (`http://purl.obolibrary.org/obo/iao.owl`), RO (`http://purl.obolibrary.org/obo/ro.owl`) |
| **Ontology licence** | CC BY 4.0 (repository); CC0 1.0 declared in the ontology header |
| **Cite the ontology as** | Hossein Beygi Nasrabadi, Jörg Waitelonis, Ebrahim Norouzi, Kostiantyn Hubaiev, Harald Sack. NFDI MatWerk Ontology (mwo). Revision: v3.0.2. Retrieved from: http://purls.helmholtz-metadaten.de/mwo/3.0.2 |
| **Generated** | `scripts/gen_patterns.py`, 2026-10-21 |

### Formal characterisation

| Measure | Value |
|---|---|
| **Consistency** | **consistent** — ROBOT 1.9.10, reasoner HermiT, merged with MWO 3.0.2 |
| **DL expressivity** | `RRESTRCUCINTUNIVRESTREROIF` — measured on the pattern together with a STAR module of exactly the terms it uses, so the figure describes this pattern rather than the whole ontology |
| **Axioms / logical axioms** | 11 / 4 |
| **Classes / individuals** | 4 / 4 |

## Consistency

`pattern.ttl` is checked against MWO 3.0.2 with ROBOT (`--reasoner hermit`) — the same reasoner `process_spreadsheets` runs over every generated module. A pattern that contradicts the ontology fails the documentation build.
