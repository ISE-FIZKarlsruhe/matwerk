# Graph Metadata

Every named graph the pipeline publishes carries a small block of triples in
which the graph describes *itself* — what it is, what it is about, who loaded it,
when, and how big it turned out to be. It is the same shape in every graph, which
makes it both the easiest thing to get uniformly right and the easiest to get
uniformly wrong.

It was uniformly wrong. The description node was typed **academic event** instead
of **textual description** — two IRIs one digit apart — and because a description
sits on the right-hand side of `denoted by`, which requires information rather
than an event, every graph carrying one was logically inconsistent. Nothing
caught it: the IRI was real, it was not deprecated, and the tests asserted the
same wrong term the code wrote.

## Visualization

```ontoink
source: shapes/graph-metadata/shape-data.ttl
shape: shapes/graph-metadata/shape.ttl
height: 560px
reasoning: true
```

Four kinds of thing appear here, and telling them apart is the whole model:

| Node | Kind | Why it matters |
|------|------|----------------|
| the graph | data collection, `void:Dataset` | a **continuant** — it persists, it does not happen |
| the description | textual description | also a **continuant**: information, not an event |
| the load | process | the only **occurrent**, and so the only thing that may occupy a stretch of time |
| the instants | temporal instant | zero-dimensional; the begin and end of the load |

## The rule that does the work

[`occupies temporal region`](http://purl.obolibrary.org/obo/BFO_0000199) has
domain *process or process boundary*, and is functional. It may not be written on
the graph, or on the description, or on a person — only on the load. If you want
to say when a *continuant* was valid, that is a different property and a
different pattern; see [Role Bearer](../role-bearer/pattern.md).

## What goes wrong

```ontoink
source: shapes/graph-metadata/broken-data.ttl
shape: shapes/graph-metadata/shape.ttl
height: 560px
reasoning: true
```

Diff this against the block above: the description is typed `NFDI_0000018`, and
the instants are typed with the parent class. The first is fatal, the second is
silent, and the pair is a fair summary of what a reasoner can and cannot do for
you.

**Fatal — the reasoner catches it.** Turn reasoning on and the graph is
inconsistent. Every axiom in the chain belongs to BFO, IAO or nfdicore; only the
one asserted type is ours:

| | |
|---|---|
| [`denoted by`](http://purl.obolibrary.org/obo/IAO_0000235) | range [information content entity](http://purl.obolibrary.org/obo/IAO_0000030) |
| [information content entity](http://purl.obolibrary.org/obo/IAO_0000030) | ⊑ [generically dependent continuant](http://purl.obolibrary.org/obo/BFO_0000031) ⊑ [continuant](http://purl.obolibrary.org/obo/BFO_0000002) |
| [academic event](https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000018) | ⊑ event ⊑ [process](http://purl.obolibrary.org/obo/BFO_0000015) ⊑ [occurrent](http://purl.obolibrary.org/obo/BFO_0000003) |
| [continuant](http://purl.obolibrary.org/obo/BFO_0000002) | `owl:disjointWith` [occurrent](http://purl.obolibrary.org/obo/BFO_0000003) |

An inconsistent graph is not a graph with one bad triple in it. Under OWL
semantics *everything* follows from a contradiction, so every query against that
graph is entitled to return anything at all — which is why the check belongs
before publication, not after.

**Silent — only the shape catches it.** The instants are typed
[`BFO_0000148`](http://purl.obolibrary.org/obo/BFO_0000148) *zero-dimensional
temporal region* rather than
[`BFO_0000203`](http://purl.obolibrary.org/obo/BFO_0000203) *temporal instant*.
`BFO_0000203` is a subclass of `BFO_0000148`, so the looser type contradicts
nothing and no reasoner will ever mention it. It simply discards the precision
the range axiom exists to provide. The SHACL shape on this page notices; HermiT
never will.

The same holds for [`NFDI_0000009`](https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009),
which is deprecated in favour of
[`IAO_0001000`](http://purl.obolibrary.org/obo/IAO_0001000) *data collection*.
Deprecation is advice, not an axiom — it cannot make anything inconsistent, so
only a shape will raise it.

## Declare your properties, or none of this happens

Worth stating on its own, because it invalidates the obvious way of checking any
of the above.

An OWL parser must decide what kind of property a statement uses. If the file
being read does not **declare** it, the fallback is to treat the statement as an
*annotation* — a note, carrying no logical meaning. A reasoner does not evaluate
annotations; it does not see them. So a reasoner run over a bare data export
returns *consistent* however broken the data is, and the run looks exactly like a
passing one.

Both `.ttl` files on this page begin with declarations for that reason:

```turtle
obo:IAO_0000235 a owl:ObjectProperty .   # denoted by
obo:BFO_0000199 a owl:ObjectProperty .   # occupies temporal region
```

Delete those two lines from `broken-data.ttl` and the inconsistency disappears —
the data is just as wrong, and the reasoner has been handed nothing to check.
Before trusting any green reasoner result over pipeline output, confirm the check
was live: inject a known-bad triple and make sure it is still caught.

## SHACL Constraints

| Target | Constraint |
|--------|-----------|
| `void:Dataset` | has a non-empty `rdfs:label` |
| `void:Dataset` | `denoted by` a `NFDI_0001018` textual description carrying a non-empty `NFDI_0001007` value |
| `void:Dataset` | `output of` exactly one `BFO_0000015` load process |
| `void:Dataset` | not typed with a deprecated class |
| load process | at least one `RO_0000057` participant |
| load process | exactly one `BFO_0000199` region, a `BFO_0000038` with first and last instants |
| `BFO_0000203` | exactly one `time:inXSDDateTimeStamp` typed `xsd:dateTimeStamp` |

## Terms

| Term | Name |
|------|------|
| [`IAO_0001000`](http://purl.obolibrary.org/obo/IAO_0001000) | data collection — replaces the deprecated `NFDI_0000009` |
| [`IAO_0000235`](http://purl.obolibrary.org/obo/IAO_0000235) | denoted by |
| [`NFDI_0001018`](https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001018) | textual description |
| [`NFDI_0001007`](https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007) | has value |
| [`RO_0002353`](http://purl.obolibrary.org/obo/RO_0002353) | output of |
| [`BFO_0000015`](http://purl.obolibrary.org/obo/BFO_0000015) | process |
| [`RO_0000057`](http://purl.obolibrary.org/obo/RO_0000057) | has participant |
| [`BFO_0000199`](http://purl.obolibrary.org/obo/BFO_0000199) | occupies temporal region — functional, processes only |
| [`BFO_0000038`](http://purl.obolibrary.org/obo/BFO_0000038) | one-dimensional temporal region |
| [`BFO_0000222`](http://purl.obolibrary.org/obo/BFO_0000222) / [`BFO_0000224`](http://purl.obolibrary.org/obo/BFO_0000224) | has first / last instant — range is `BFO_0000203` |
| [`BFO_0000203`](http://purl.obolibrary.org/obo/BFO_0000203) | temporal instant |
| [`NFDI_0000004`](https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000004) | person |
