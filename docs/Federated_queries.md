# Federated queries

The MatWerk Knowledge Graph (MSE-KG) does not copy the data of its partner graphs. It **registers** them: every partner graph appears in MSE-KG as a `nfdi:NFDI_0000009` (dataset) individual carrying its label, licence, creators — and the URL of its public SPARQL endpoint. A federated query then reaches that endpoint live with SPARQL 1.1 `SERVICE`, so the answer is always computed against the partner's current data.

```sparql
SELECT ... WHERE {
  # ---- local: the MatWerk KG --------------------------------
  ?dataset a nfdicore_dataset: ; ... .

  # ---- remote: evaluated at the partner endpoint ------------
  SERVICE <https://partner.example.org/sparql> { ... }

  # ---- the join: a value the two graphs share ---------------
  FILTER(STR(?link) = STR(?doi))
}
```

Everything hinges on the last part — the **join key**. Two graphs only federate usefully where they say something about the same thing. Across the partners below, three kinds of join key are in use:

| Join key | Used by | Why it works |
|---|---|---|
| A shared **DOI** | [atomRDF KG](federated_queries/atomrdf.md) | Both graphs reference the same published dataset by its DOI |
| A shared **authority identifier** (ROR, ORCID) | [Wikidata](federated_queries/wikidata.md) | Organisations and people carry the same external ID in both graphs |
| A shared **IRI** in the `msekg:` namespace | [CreepLitKG](federated_queries/creeplitkg.md), [IUC02](federated_queries/iuc02.md), [IUC07](federated_queries/iuc07.md) | The RML pipelines mint their IRIs directly in the MSE-KG namespace, so subject IRIs are literally identical |

## Partner graphs

| Graph | Registered in MSE-KG as | Join key |
|---|---|---|
| [atomRDF KG](federated_queries/atomrdf.md) | [`msekg:176113442890318`](https://nfdi.fiz-karlsruhe.de/matwerk/msekg/176113442890318) | DOI |
| [Wikidata](federated_queries/wikidata.md) | *(not registered — queried directly)* | ROR ID, ORCID |
| [Creep Literature KG (CreepLitKG)](federated_queries/creeplitkg.md) | [`msekg:17858428366781`](https://nfdi.fiz-karlsruhe.de/matwerk/msekg/17858428366781) | shared `msekg:` IRI |
| [Creep Reference Datasets KG (IUC02)](federated_queries/iuc02.md) | [`msekg:17902496761671`](https://nfdi.fiz-karlsruhe.de/matwerk/msekg/17902496761671) | shared `msekg:` IRI |
| [MiMeDat-KG (IUC07)](federated_queries/iuc07.md) | [`msekg:17902496761672`](https://nfdi.fiz-karlsruhe.de/matwerk/msekg/17902496761672) | shared `msekg:` IRI |

Pick a graph from the navigation on the left for its endpoint, its modelling, and its competency questions.

## Discovering endpoints from the graph itself

You do not have to hard-code a partner endpoint. MSE-KG knows all of them — this query returns every registered endpoint together with the dataset it belongs to:

```sparql
PREFIX nfdicore_sparql_endpoint: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001095>
PREFIX nfdicore_dataset:         <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009>
PREFIX denoted_by:               <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX has_url:                  <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX rdfs:                     <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?dataset ?datasetLabel ?sparqlURL
WHERE {
  ?endpoint a nfdicore_sparql_endpoint: ;
            has_url: ?sparqlURL .

  ?dataset a nfdicore_dataset: ;
           denoted_by: ?endpoint ;
           rdfs:label ?datasetLabel .
}
ORDER BY ?datasetLabel
```

The shape behind those rows is documented in the [sparql endpoints pattern](patterns/sparql_endpoints/pattern.md).

!!! warning "Running federated queries in practice"

    - `SERVICE` must be **enabled** on the endpoint you submit the query to. The [MatWerk SPARQL endpoint](/matwerk/shmarql/) evaluates the local part; the remote part is executed by the partner.
    - Remote endpoints **time out**. Keep the remote pattern selective and add `LIMIT` while you develop.
    - Some portals restrict browser-based clients via CORS. Server-side clients (`curl`, Python `SPARQLWrapper`) and the portals' own query UIs are unaffected.
    - The Wikidata Query Service requires a descriptive `User-Agent` header and rejects anonymous default agents.
