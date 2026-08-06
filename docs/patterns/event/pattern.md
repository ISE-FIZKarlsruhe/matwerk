# event

The `event` tab of the MatWerk workbook currently holds **148 rows**. Every row follows the same shape, and that shape is the pattern below: a **entity** instance carrying the row's identity, denoted by **value node** nodes that hold its values, and object properties pointing at entities in other tabs.

*Generated from the live sheet and MWO 3.0.1 by `scripts/gen_patterns.py` — regenerate it rather than editing it by hand.*

## The pattern

```ontoink
source: patterns/event/pattern.ttl
height: 560px
reasoning: false
```

> Drag the nodes, change the layout, or use **Group** to collapse a namespace. The picture is the example only; the declarations and annotations behind it are in [`module.ttl`](module.ttl).

## Columns

| Column | ROBOT | Property | Meaning |
|---|---|---|---|
| Contribution name* | `I` | `obo:IAO_0000235` | denoted by |
| Short description* | `I` | `obo:IAO_0000235` | denoted by |
| Link (URL / PID) | `I` | `obo:IAO_0000235` | denoted by |
| occupies temporal region | `I` | `obo:BFO_0000199` | occupies temporal region |
| Teilnehmendenzahl | `I` | `obo:RO_0002233` | has input |
| Frequency | `I` | `obo:IAO_0000235` | denoted by |
| Associated organization | `I` | `obo:RO_0000057` | has participant |
| weitere beteiligte Konsortien | `I` | `obo:RO_0000057` | has participant |
| Zielgruppe | `I` | `obo:RO_0000057` | has participant |

## How to read it

* The **entity** is typed `nfdi:NFDI_0010020` (conference) and carries only its identity, its label, and links.
* A column marked `I` does **not** contain a value — it contains the *label of another instance*, which ROBOT resolves at build time. That is why every value node needs a unique label, and why a typo produces a silently missing relation rather than an error.
* Columns marked `A` are literals on the entity itself.

## Components (value nodes)

These value-node types live in the workbook's shared `req_2` tab but belong to this pattern — they exist to describe it, and are counted by how often this tab references them.

| Component | Class | References |
|---|---|---|
| title | `nfdi:NFDI_0001019` | 141 |
| event frequency datum | `mwo:MWO_0001001` | 100 |
| collection | `nfdi:NFDI_0000107` | 92 |
| quality | `obo:BFO_0000019` | 84 |

## Query it

Retrieve instances of this pattern from the MatWerk Knowledge Graph ([SPARQL endpoint](https://nfdi.fiz-karlsruhe.de/matwerk/sparql)):

```sparql
SELECT ?x ?label WHERE { ?x a <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0010020> ; <http://purl.obolibrary.org/obo/IAO_0000235> ?d . ?d <http://www.w3.org/2000/01/rdf-schema#label> ?label } LIMIT 10
```

The rows behind it are curated in the [`event` tab of the workbook](https://docs.google.com/spreadsheets/d/1OyoWwcX4zUtrJilwXdtTooavELw278nQSNW2oniBBsk/edit#gid=638946284).

## Competency questions

The questions this pattern exists to answer. Each is answerable against the published graphs.

1. Which academic events has NFDI-MatWerk organised, and when?
2. Which organisation or task area organised a given event, and how often does it recur?
3. Which publications or educational materials came out of a given event?

## Pattern metadata

The [Ontology Design Patterns](http://ontologydesignpatterns.org) content-ODP annotation schema, in the community's own field order, so a reader who knows ODP pages can read this one without translation.

| Field | Value |
|---|---|
| **Name** | event |
| **Submitted by** | the NFDI-MatWerk consortium |
| **Also Known As** | the `event` template tab of the NFDI-MatWerk workbook |
| **Intent** | To represent a conference in the MatWerk Knowledge Graph: its identity, the value nodes that describe it, and its links to the other entities the NFDI-MatWerk consortium records. |
| **Domains** | research data management; NFDI consortium structure |
| **Competency Questions** | Which academic events has NFDI-MatWerk organised, and when?; Which organisation or task area organised a given event, and how often does it recur?; Which publications or educational materials came out of a given event? |
| **Solution description** | An entity individual typed `nfdi:NFDI_0010020` carries identity and label; each descriptive value is a separate *value node* individual reached by `obo:IAO_0000235` (denoted by); links to other patterns are object properties onto those entities. Identity is a minted IRI `msekg:<epoch_ms><counter>`, so re-running the pipeline is idempotent. |
| **Reusable OWL Building Block** | [`module.ttl`](module.ttl) — the example together with the axioms of every term it uses, extracted from the ontology, so the file stands alone |
| **Consequences** | Values become reusable and independently addressable, and a value may be shared by many entities. The cost: every `I` reference resolves **by label**, so labels must be unique and a typo yields a silently missing relation rather than an error. |
| **Scenarios** | “2nd Materials Science Colloquium 2023 (Universität Leoben)” is a conference, with contribution name “2nd Materials Science Colloquium 2023 (Universität Leoben)”; occupies temporal region “temporal region 46”; teilnehmendenzahl “Gruppen (11-50)”. |
| **Known Uses** | the MatWerk Knowledge Graph (`https://nfdi.fiz-karlsruhe.de/matwerk`), published as Virtuoso named graphs |
| **Web References** | <https://nfdi-matwerk.de> · <https://ise-fizkarlsruhe.github.io/mwo/> |
| **Other References** | https://doi.org/10.1002/adem.202502331 |
| **Examples (OWL files)** | [`pattern.ttl`](pattern.ttl) — one real row of the tab, with its value nodes resolved |
| **Extracted From** | the `event` tab of the NFDI-MatWerk curation workbook (148 rows at generation time) |
| **Reengineered From** | the ROBOT template directives in row 2 of that tab |
| **Has Components** | title, event frequency datum, collection, quality |
| **Specialization Of** | the entity-and-value-node pattern shared by every tab ([overview](../index.md)) |
| **Related CPs** | [temporal](../temporal/pattern.md) |

### Provenance and versioning

| Field | Value |
|---|---|
| **Pattern version** | 1.0.0 |
| **Authors** | [Harald Sack](https://orcid.org/0000-0001-7069-9804) · [Jörg Waitelonis](https://orcid.org/0000-0001-7192-7143) · [Ebrahim Norouzi](https://orcid.org/0000-0002-2691-6995) · [Hossein Beygi Nasrabadi](https://orcid.org/0000-0002-3092-0532) — the authors of the ontology these patterns are derived from (`dcterms:creator` of MWO 3.0.1) |
| **Derived from ontology** | MatWerk Ontology (MWO) **3.0.1** — `http://purls.helmholtz-metadaten.de/mwo/mwo.owl/3.0.1` |
| **Reused ontologies** | BFO 2020 (`http://purl.obolibrary.org/obo/bfo.owl`), NFDIcore (`https://nfdi.fiz-karlsruhe.de/ontology/`), IAO (`http://purl.obolibrary.org/obo/iao.owl`), RO (`http://purl.obolibrary.org/obo/ro.owl`) |
| **Ontology licence** | CC BY 4.0 (repository); CC0 1.0 declared in the ontology header |
| **Cite the ontology as** | Hossein Beygi Nasrabadi, Jörg Waitelonis, Ebrahim Norouzi, Kostiantyn Hubaiev, Harald Sack. NFDI MatWerk Ontology (mwo). Revision: v3.0.1. Retrieved from: http://purls.helmholtz-metadaten.de/mwo/3.0.1 |
| **Generated** | `scripts/gen_patterns.py`, 2026-08-04 |

### Formal characterisation

| Measure | Value |
|---|---|
| **Consistency** | **consistent** — ROBOT 1.9.10, reasoner HermiT, merged with MWO 3.0.1 |
| **DL expressivity** | `RRESTRCUCINTUNIVRESTREROIF` — measured on the pattern together with a STAR module of exactly the terms it uses, so the figure describes this pattern rather than the whole ontology |
| **Axioms / logical axioms** | 19 / 7 |
| **Classes / individuals** | 7 / 7 |

## Consistency

`pattern.ttl` is checked against MWO 3.0.1 with ROBOT (`--reasoner hermit`) — the same reasoner `process_spreadsheets` runs over every generated module. A pattern that contradicts the ontology fails the documentation build.
