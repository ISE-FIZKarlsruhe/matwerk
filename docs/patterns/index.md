# Ontology design patterns

The MatWerk KG is built from a spreadsheet whose tabs are not arbitrary: each one
encodes a **design pattern**. A row is an *entity* — an individual carrying identity and
type — surrounded by *value nodes* that hold its values, and object properties
pointing at entities in other tabs.

That pattern has until now been implicit, spread across a ROBOT directive row and
whatever a curator typed. These pages make it explicit: for every tab, the terms it
uses (**TBox**) and one **real row** from the live sheet (**ABox**), drawn, explained,
and checked by a reasoner.

!!! info "Generated, not written"
    Every page here is produced by `scripts/gen_patterns.py` from the **live
    workbook** and the **live ontology**. Re-run it after a sheet edit or an ontology
    upgrade and the documentation follows; it cannot drift from the data it claims to
    describe. Do not edit these pages by hand.

    ```bash
    python scripts/gen_patterns.py --fetch \
        --mwo mwo-full.ttl --mwo-version 3.0.1 --out docs/patterns
    ```

**Ontology version: MWO 3.0.1** (`http://purls.helmholtz-metadaten.de/mwo/mwo.owl/3.0.1`).
Each `pattern.ttl` records the version it was generated against in its header.

## The shape every tab shares

```
      ┌─────────────── entity ───────────────┐
      │  msekg:<epoch_ms><counter>           │   ← the row's identity
      │  a  <the tab's class>                │   ← the TYPE column
      │  rdfs:label "…"                      │
      └───┬──────────────────────────┬───────┘
   denoted by (IAO_0000235)       other object properties
          │                           │
   ┌──────▼───────┐          ┌────────▼─────────────┐
   │  value node  │          │  an entity in another│
   │  title / url │          │  tab (organisation,  │
   │  description │          │  person, process …)  │
   └──────────────┘          └──────────────────────┘
```

Two consequences are worth stating plainly, because both cause silent damage:

* A column marked `I` holds **the label of another instance**, not a value. ROBOT
  resolves it at build time. A typo therefore produces a *missing relation*, not an
  error — 301 of 7 955 label references in the workbook currently resolve to nothing.
* Every value node needs a **unique label**, or the reference is ambiguous.

## Patterns

### Core building blocks
| Pattern | What a row is |
|---|---|
| [value nodes](#where-the-value-nodes-went) | the shared value nodes — titles, descriptions, URLs, names — that every other tab points at |
| [agent](agent/pattern.md) · [role](role/pattern.md) · [process](process/pattern.md) | agents, the roles they bear, and the processes those roles are realised in |
| [temporal](temporal/pattern.md) · [city](city/pattern.md) | time regions and places |

### Who and where
| Pattern | What a row is |
|---|---|
| [people](people/pattern.md) | a person, with names, e-mail and ORCID |
| [organization](organization/pattern.md) | an institution, with ROR, city and parent |
| [collaboration](collaboration/pattern.md) | a collaboration between agents |

### What is produced
| Pattern | What a row is |
|---|---|
| [dataset](dataset/pattern.md) | a dataset |
| [publication](publication/pattern.md) | a publication |
| [software](software/pattern.md) | a software product |
| [ontologies](ontologies/pattern.md) | an ontology |
| [materials](materials/pattern.md) | a material designation |
| [fdos](fdos/pattern.md) | a FAIR digital object |

### Where it lives
| Pattern | What a row is |
|---|---|
| [dataportal](dataportal/pattern.md) | a data portal / repository |
| [sparql_endpoints](sparql_endpoints/pattern.md) | a queryable endpoint and the dataset behind it |
| [metadata](metadata/pattern.md) | a metadata standard |
| [service](service/pattern.md) | a service offered |
| [instrument](instrument/pattern.md) · [largescalefacility](largescalefacility/pattern.md) | instruments and the facilities housing them |

### NFDI-MatWerk structure
| Pattern | What a row is |
|---|---|
| [matwerkta](matwerkta/pattern.md) | a Task Area |
| [matwerkiuc](matwerkiuc/pattern.md) | an Infrastructure Use Case |
| [matwerkpp](matwerkpp/pattern.md) | a Participating Project |
| [event](event/pattern.md) | an event |


## Where the value nodes went

`req_1` and `req_2` are not patterns. They are the workbook's shared **pool of value
nodes** — titles, names, websites, roles, identifiers — that every other tab's `I`
columns point into. `req_2` alone holds 4 871 rows across 70 distinct types.

Documenting them as two pages would say nothing; documenting them as seventy pages
would divorce each value node from the entity it exists to describe. So they are
**dissolved**: every value type is attributed to the entity tab that actually
references it, and appears there under **Components (value nodes)** — where it is
already present in the example, because value nodes are resolved.

| Value node | Documented in | References |
|---|---|---|
| contact point role | [process](process/pattern.md) | 316 |
| publishing process · document title · author list · DOI | [publication](publication/pattern.md) | 231 · 230 · 150 · 139 |
| software title · software repository · version number | [software](software/pattern.md) | 209 · 124 · 85 |
| written name | [agent](agent/pattern.md) · [ontologies](ontologies/pattern.md) | 105 · 61 |
| event frequency datum · collection | [event](event/pattern.md) | 100 · 92 |
| email address | [people](people/pattern.md) · [role](role/pattern.md) | 76 · 56 |

A value type that **no** entity tab references keeps a page of its own — currently
only [nfdi matwerk consortium](value-nfdi-matwerk-consortium/pattern.md).

## Consistency

Each `pattern.ttl` is merged with MWO and checked with

```bash
robot merge -i mwo-full.ttl -i pattern.ttl \
      explain --reasoner hermit -M inconsistency --explanation report.md
```

— the same reasoner `process_spreadsheets` runs over every generated module, so a
pattern that contradicts the ontology fails here rather than in production.

The check is not cosmetic. It is what catches the class of error that produced the
[Graph Metadata](../shapes/graph-metadata/pattern.md) bug: a description node typed
*academic event* instead of *textual description* — two IRIs one digit apart, both
real, both undeprecated — which made every graph carrying it inconsistent while every
test still passed.

## Related

* [Shapes](../shapes/index.md) — SHACL constraints covering what a reasoner
  structurally cannot (missing values, deprecated terms, which node was at fault).
* [Harvested RDF modelling](../pipeline/rdf-files-modelling.md) — the same treatment
  for graphs harvested from GitHub and Zenodo.
* [Pipeline](../pipeline/index.md) — the DAGs that turn these tabs into the KG.
