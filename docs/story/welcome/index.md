---
title: Welcome to the MatWerk Knowledge Graph
description: A short tour of the MatWerk Knowledge Graph and how to ask it questions.
keywords: [NFDI-MatWerk, knowledge graph, SPARQL, materials science]
language: en
license: CC BY 4.0
lodeon:
  htmlMeta:
    description: A short tour of the MatWerk Knowledge Graph.
    keywords: [NFDI-MatWerk, knowledge graph, SPARQL, materials science]
    language: en
    license: CC BY 4.0
  openGraph:
    title: Welcome to the MatWerk Knowledge Graph
    type: article
---

# Welcome to the MatWerk Knowledge Graph

MatWerk KG is a **research-data-management** knowledge graph: it indexes the
datasets, publications, software, instruments, facilities, services, people and
organisations of the NFDI-MatWerk consortium, and the provenance that connects
them. It does not hold the measurements themselves — it tells you what exists,
who is responsible for it and where to fetch it.

Everything below is live. The query runs when you open this page, so the numbers
are the graph as it is now, not as it was when this was written.

## What is in the graph?

The data is split into named graphs, one per source and processing stage. The
three `spreadsheets_*` graphs hold curated content; the others hold metadata
harvested from the NFDI-MatWerk Zenodo community and from registered SPARQL
endpoints.

```shmarql {title="Triples per named graph"}
SELECT ?graph (COUNT(*) AS ?triples) WHERE {
  GRAPH ?graph { ?s ?p ?o }
}
GROUP BY ?graph
ORDER BY DESC(?triples)
```

## The same numbers as a chart

```lodeon-visualization {title="Triples per named graph"}
{
  "version": 2,
  "renderer": "plotly",
  "sourceMode": "sparql",
  "source": "SELECT ?graph (COUNT(*) AS ?triples) WHERE {\n  GRAPH ?graph { ?s ?p ?o }\n}\nGROUP BY ?graph\nORDER BY DESC(?triples)",
  "chartType": "bar",
  "mapping": { "x": "graph", "y": "triples" },
  "figure": {
    "data": [
      {
        "type": "bar",
        "x": [
          "spreadsheets_validated",
          "spreadsheets_inferences",
          "spreadsheets_assertions",
          "endpoints_validated",
          "zenodo_validated"
        ],
        "y": [80203, 65137, 30498, 18329, 15831],
        "marker": { "color": "#0072B2" }
      }
    ],
    "layout": {
      "autosize": true,
      "height": 380,
      "margin": { "l": 72, "r": 24, "t": 16, "b": 120 },
      "showlegend": false,
      "xaxis": { "title": { "text": "Named graph" }, "automargin": true },
      "yaxis": { "title": { "text": "Triples" }, "automargin": true }
    },
    "config": { "responsive": true, "displaylogo": false }
  }
}
```

The bars are stored with the story, so they show release **2.2.1** without
re-running the query. The table above them is live, so the two will drift apart
as the graph grows — that difference is itself worth noticing.

## Asking your own questions

Every query in the [catalogue](/matwerk/general_queries/) has an *execute query*
link that opens it in the editor, ready to run. Or start from a blank one in the
[query editor](/matwerk/query), and open the [story editor](/lodeon/) to build a
page like this one.
