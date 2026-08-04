# How harvested RDF is modelled

A curator registers a GitHub repository or a Zenodo record in the spreadsheet, and
the `harvester_rdf_files` DAG turns each RDF file it finds into **its own named
graph**. The file's own triples go in untouched — nothing is remodelled — but two
blocks of metadata are added around them, and those blocks are the subject of this
page.

Every example below is **real output**, copied from a harvest of
[`creep-testing-ontology`](https://github.com/HosseinBeygiNasrabadi/creep-testing-ontology)
and a Zenodo record, not written by hand for the documentation.

---

## 1. The graph describes itself

Each named graph carries the same self-description every pipeline graph carries, so
a harvested graph is indistinguishable from a curated one to the dashboard, to
`/graphs`, and to SPARQL.

```turtle
cto:cto-full.ttl
    a void:Dataset , nfdi:NFDI_0000009 ;
    rdfs:label "HosseinBeygiNasrabadi/creep-testing-ontology — cto-full.ttl" ;
    void:triples          13057 ;
    void:distinctSubjects  3694 ;
    void:classes             21 ;
    void:properties          67 ;
    void:dataDump   "https://raw.githubusercontent.com/…/1.0.0/cto-full.ttl"^^xsd:anyURI ;
    obo:RO_0002353  cto:cto-full.ttl#load ;      # is output of  → the load process
    obo:IAO_0000235 cto:cto-full.ttl#desc .      # denoted by    → the description
```

Four kinds of thing appear, and telling them apart is the whole model:

| Node | Kind | Why it matters |
|---|---|---|
| `…/cto-full.ttl` | the graph — `void:Dataset` | a **continuant**: it persists, it does not happen |
| `#desc` | textual description | also a **continuant** — information, not an event |
| `#load` | the load process | the only **occurrent**, so the only thing that may occupy time |
| `#t0` | temporal instant | zero-dimensional; when the load happened |
| `#source` | the input file | carries the download URL and the `sha256:` of the bytes |

```ontoink
source: pipeline/examples/harvested-graph-metadata.ttl
height: 620px
reasoning: true
```

> Drag the nodes, switch the layout, or use **Group** to collapse each namespace into
> one hexagon. **Reasoning** shows what a reasoner adds on top of what is asserted.

### The two rules that are easy to break

**The description is `NFDI_0001018` *textual description*.** It is emphatically not
`NFDI_0000018` *academic event* — one digit away. A description sits on the
right-hand side of `denoted by`, which requires information rather than an event, so
using the wrong IRI makes **every graph carrying it logically inconsistent**. This
mistake was live in the pipeline and is the reason the
[Graph Metadata shape](../shapes/graph-metadata/pattern.md) exists.

**`occupies temporal region` goes on the load process only** — never on the graph,
never on the description. It has domain *process or process boundary* and is
functional. If you want to say when a *continuant* was valid, that is a different
pattern; see [Role Bearer](../shapes/role-bearer/pattern.md).

### Statistics are part of the graph

The counts are written as [VoID](https://www.w3.org/TR/void/), including per-class
and per-predicate partitions, so "how big is this graph and what is in it" is
answerable by SPARQL rather than only by a dashboard:

```sparql
SELECT ?class ?n WHERE {
  GRAPH ?g {
    ?g void:classPartition [ void:class ?class ; void:entities ?n ]
  }
} ORDER BY DESC(?n)
```

---

## 2. The registry keeps the link to where the data came from

One registration can produce several named graphs — an ontology repository might
ship four RDF files. If those graphs only existed on their own, the connection back
to the repository would be lost. So a second block, written into the TriG's **default
graph** and published to `…/matwerk/rdf_files_metadata`, links every file-graph to
its source with BFO part-of / has-part:

```turtle
mw:github/HosseinBeygiNasrabadi/creep-testing-ontology
    rdfs:label "HosseinBeygiNasrabadi/creep-testing-ontology" ;
    obo:BFO_0000051 cto:cto-full.ttl .           # has part → the file graph

cto:cto-full.ttl
    a nfdi:NFDI_0000027 ;                        # file data item
    obo:BFO_0000050 mw:github/…/creep-testing-ontology ;   # part of → the repository
    nfdi:NFDI_0001008 "https://raw.githubusercontent.com/…"^^xsd:anyURI ;
    stat:registeredFrom "https://github.com/HosseinBeygiNasrabadi/creep-testing-ontology" ;
    stat:sourceFile "cto-full.ttl" ;
    stat:triples 13057 ;
    stat:version "1.0.0" .
```

```ontoink
source: pipeline/examples/rdf-files-registry.ttl
height: 560px
reasoning: false
```

When the repository is **already an entity in the KG**, the harvester attaches the
graphs to *that* entity instead of minting a new node — so harvested data hangs off
what the KG already knows, rather than beside it.

### Why the flat `stat:` predicates exist

`stat:registeredFrom` repeats, as a plain literal, the URL exactly as the curator
typed it in the spreadsheet. That makes the round trip a **one-hop** query, which is
what lets a spreadsheet look up its own rows:

```sparql
PREFIX stat: <https://nfdi.fiz-karlsruhe.de/matwerk/msekg/stat/>
SELECT ?graph ?file ?triples ?version WHERE {
  GRAPH <https://nfdi.fiz-karlsruhe.de/matwerk/rdf_files_metadata> {
    ?graph stat:registeredFrom "https://github.com/OWNER/REPO" ;
           stat:sourceFile ?file ; stat:triples ?triples .
    OPTIONAL { ?graph stat:version ?version }
  }
}
```

---

## 3. Graph IRIs are deterministic

```
https://nfdi.fiz-karlsruhe.de/matwerk/github/<owner>/<repo>/<file>
https://nfdi.fiz-karlsruhe.de/matwerk/zenodo/<record id>/<file>
```

Because the IRI is a pure function of (source, key, filename), a re-run **updates the
same graph in place** instead of minting a second copy. That is what makes the weekly
schedule safe: the latest release wins, and an unchanged file — detected by its
`sha256` — is skipped entirely.

---

## 4. Vocabulary

Only BFO, IAO, nfdicore, VoID and W3C-Time are used. There is deliberately **no PROV
and no Dublin Core** in anything this pipeline writes, matching the rest of the
graphs.

Third-party `dcterms` triples *do* appear inside harvested graphs — the CTO file uses
them internally. That is correct and intentional: harvested content is inserted
verbatim, and rewriting someone else's vocabulary would misrepresent their data.

| Term | IRI | Used for |
|---|---|---|
| textual description | `nfdi:NFDI_0001018` | the description node |
| file data item | `nfdi:NFDI_0000027` | a harvested RDF file |
| research data repository | `nfdi:NFDI_0001201` | a minted GitHub repository node |
| denoted by | `obo:IAO_0000235` | entity → description / URL / version |
| has part / part of | `obo:BFO_0000051` / `obo:BFO_0000050` | repository ↔ file graph |
| is output of | `obo:RO_0002353` | graph → the load process |
| has input | `obo:RO_0002233` | load process → the source file |
| process | `obo:BFO_0000015` | the load |
| occupies temporal region | `obo:BFO_0000199` | **the load only** |
| has value / has url | `nfdi:NFDI_0001007` / `nfdi:NFDI_0001008` | literals |

The dataset class is version-dependent: MWO 3.0.2 deprecates `nfdi:NFDI_0000009` in
favour of `obo:IAO_0001000`. The DAGs read the class from the
`matwerk_mwo_version` Airflow Variable rather than hard-coding it, so the vocabulary
moves with the ontology.
