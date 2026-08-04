# The pipeline, as a graph

The pipeline is usually drawn. Drawing it means the picture and the system drift
apart the moment a DAG changes, and it means questions like *"which DAG produces
`spreadsheets_inferences.ttl`?"* have to be answered by reading code.

So the pipeline is **modelled** instead: every DAG is a
[process](http://purl.obolibrary.org/obo/BFO_0000015), every artefact it reads or
writes is a [file data item](https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000027),
and the edges are `has input`, `has output` and `precedes`. The diagram below is
rendered from that RDF — it is the same data an answer would come from.

*Ontology version: MWO 3.0.1.*

```ontoink
source: pipeline/examples/pipeline.ttl
height: 700px
reasoning: false
```

> Drag a DAG to pull it out of the tangle. **Layout** switches between dagre (the
> left-to-right flow), force, circle, concentric, tree and grid. **Group** collapses
> each namespace into one hexagon; click a hexagon to expand it. **Search** highlights
> a node by name, and **PNG** exports what you see.

## Reading it

Three chains converge on `publish_to_virtuoso`:

| Chain | DAGs |
|---|---|
| the workbook | `process_spreadsheets` → `merge` → `reason_koncludix` → `validation_checks` |
| registered RDF | `harvester_rdf_files` (weekly) and `semantic_dataset_resync` (monthly, full) |
| other harvests | `harvester_zenodo`, `harvester_endpoints` |

and three consumers hang off it: `dashboard` (Superset statistics),
`dump_and_archive` (the RDF dumps deposited to Zenodo) and `cq-test` (competency
questions).

Note that `merge` takes `spreadsheets_asserted.ttl` as both input and output: it
merges the per-tab modules produced by ROBOT into the single asserted graph the rest
of the pipeline reads.

## Because it is data, you can ask it questions

Which DAG produces a given artefact:

```sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?dag WHERE {
  ?d obo:RO_0002234 <https://nfdi.fiz-karlsruhe.de/matwerk/pipeline/spreadsheets_inferences.ttl> ;
     rdfs:label ?dag .
}
```

What breaks if `spreadsheets_asserted.ttl` is wrong — everything downstream of the
DAG that produces it:

```sparql
SELECT ?downstream WHERE {
  ?d obo:RO_0002234 <…/pipeline/spreadsheets_asserted.ttl> .
  ?d obo:BFO_0000063+ ?later .          # precedes, transitively
  ?later rdfs:label ?downstream .
}
```

## Keeping it honest

`pipeline.ttl` is hand-maintained and small — it lists twelve DAGs. It is checked for
consistency against MWO with ROBOT alongside the generated patterns, so a term used
wrongly here fails the build. When a DAG is added, add it here too: the file is short
and the diagram is only as true as the file.
