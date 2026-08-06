# matwerkpp

The `matwerkpp` tab of the MatWerk workbook currently holds **41 rows**. Every row follows the same shape, and that shape is the pattern below: a **entity** instance carrying the row's identity, denoted by **value node** nodes that hold its values, and object properties pointing at entities in other tabs.

*Generated from the live sheet and MWO 3.0.1 by `scripts/gen_patterns.py` — regenerate it rather than editing it by hand.*

## The pattern

```ontoink
source: patterns/matwerkpp/pattern.ttl
height: 560px
reasoning: false
```

> Drag the nodes, change the layout, or use **Group** to collapse a namespace. The picture is the example only; the declarations and annotations behind it are in [`module.ttl`](module.ttl).

## Columns

| Column | ROBOT | Property | Meaning |
|---|---|---|---|
| Label | `A` | `rdfs:label` | label |
| Participant Project collabration name | `I` | `obo:IAO_0000235` | denoted by |
| participates in contacting process | `I` | `obo:RO_0000056` | participates in |
| Material/Methodology Description | `I` | `obo:IAO_0000235` | denoted by |
| Engagement statistics | `I` | `obo:IAO_0000235` | denoted by |
| has participant | `I` | `obo:RO_0000057` | has participant |
| Related IUCs | `I` | `obo:RO_0000057` | has participant |
| Project collabration objectives | `I` | `obo:IAO_0000235` | denoted by |
| has part PP | `I` | `obo:BFO_0000051` | has part |
| participates in project | `I` | `obo:RO_0000056` | participates in |

## How to read it

* The **entity** is typed `mwo:MWO_0001029` (matwerk participant project) and carries only its identity, its label, and links.
* A column marked `I` does **not** contain a value — it contains the *label of another instance*, which ROBOT resolves at build time. That is why every value node needs a unique label, and why a typo produces a silently missing relation rather than an error.
* Columns marked `A` are literals on the entity itself.

## Query it

Retrieve instances of this pattern from the MatWerk Knowledge Graph ([SPARQL endpoint](https://nfdi.fiz-karlsruhe.de/matwerk/sparql)):

```sparql
SELECT ?x ?label WHERE { ?x a <http://purls.helmholtz-metadaten.de/mwo/MWO_0001029> ; <http://www.w3.org/2000/01/rdf-schema#label> ?label } LIMIT 10
```

The rows behind it are curated in the [`matwerkpp` tab of the workbook](https://docs.google.com/spreadsheets/d/1OyoWwcX4zUtrJilwXdtTooavELw278nQSNW2oniBBsk/edit#gid=606786541).

## Competency questions

The questions this pattern exists to answer. Each is answerable against the published graphs.

1. Which participant projects are part of NFDI-MatWerk?
2. Which organisation runs a given participant project, and who leads it?
3. Which resources has a participant project contributed to the knowledge graph?

## Pattern metadata

The [Ontology Design Patterns](http://ontologydesignpatterns.org) content-ODP annotation schema, in the community's own field order, so a reader who knows ODP pages can read this one without translation.

| Field | Value |
|---|---|
| **Name** | matwerkpp |
| **Submitted by** | the NFDI-MatWerk consortium |
| **Also Known As** | the `matwerkpp` template tab of the NFDI-MatWerk workbook |
| **Intent** | To represent a matwerk participant project in the MatWerk Knowledge Graph: its identity, the value nodes that describe it, and its links to the other entities the NFDI-MatWerk consortium records. |
| **Domains** | research data management; NFDI consortium structure |
| **Competency Questions** | Which participant projects are part of NFDI-MatWerk?; Which organisation runs a given participant project, and who leads it?; Which resources has a participant project contributed to the knowledge graph? |
| **Solution description** | An entity individual typed `mwo:MWO_0001029` carries identity and label; each descriptive value is a separate *value node* individual reached by `obo:IAO_0000235` (denoted by); links to other patterns are object properties onto those entities. Identity is a minted IRI `msekg:<epoch_ms><counter>`, so re-running the pipeline is idempotent. |
| **Reusable OWL Building Block** | [`module.ttl`](module.ttl) — the example together with the axioms of every term it uses, extracted from the ontology, so the file stands alone |
| **Consequences** | Values become reusable and independently addressable, and a value may be shared by many entities. The cost: every `I` reference resolves **by label**, so labels must be unique and a typo yields a silently missing relation rather than an error. |
| **Scenarios** | “PP1” is a matwerk participant project, with participant project collabration name “From atoms to turbine blades”; material/methodology description “Ni-based and Co-based single-crystal superalloys, metallic complex solid solutions / experiments and simulations”; engagement statistics “24 Project collabrations in CRC/TRR103, 10 of them on ‘Machine-Learning and Material Informatics‘”. |
| **Known Uses** | the MatWerk Knowledge Graph (`https://nfdi.fiz-karlsruhe.de/matwerk`), published as Virtuoso named graphs |
| **Web References** | <https://nfdi-matwerk.de> · <https://ise-fizkarlsruhe.github.io/mwo/> |
| **Other References** | https://doi.org/10.1002/adem.202502331 |
| **Examples (OWL files)** | [`pattern.ttl`](pattern.ttl) — one real row of the tab, with its value nodes resolved |
| **Extracted From** | the `matwerkpp` tab of the NFDI-MatWerk curation workbook (41 rows at generation time) |
| **Reengineered From** | the ROBOT template directives in row 2 of that tab |
| **Has Components** | — |
| **Specialization Of** | the entity-and-value-node pattern shared by every tab ([overview](../index.md)) |
| **Related CPs** | [matwerkta](../matwerkta/pattern.md) |

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
| **Axioms / logical axioms** | 14 / 5 |
| **Classes / individuals** | 5 / 5 |

## Consistency

`pattern.ttl` is checked against MWO 3.0.1 with ROBOT (`--reasoner hermit`) — the same reasoner `process_spreadsheets` runs over every generated module. A pattern that contradicts the ontology fails the documentation build.
