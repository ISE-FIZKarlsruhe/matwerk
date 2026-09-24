# Wikidata

[**Wikidata**](https://www.wikidata.org) is the free, collaborative knowledge base of the Wikimedia Foundation. For MSE-KG it is the *reference hub*: it holds authority records for the organisations and people that also appear in the MatWerk graph, together with everything the wider web has said about them — country, parent institution, employer, coordinates, sitelinks.

Unlike the other partners on these pages, Wikidata is **not registered as a dataset in MSE-KG**. It is queried directly at the public Wikidata Query Service (WDQS).

## At a glance

| | |
|---|---|
| **IRI in MSE-KG** | *not registered — Wikidata is an external authority, not a MatWerk dataset* |
| **SPARQL endpoint** | `https://query.wikidata.org/sparql` |
| **Query UI** | <https://query.wikidata.org> |
| **Licence** | CC0 1.0 (Wikidata data) |
| **Maintainer** | Wikimedia Foundation / the Wikidata community |

## Join keys

MSE-KG and Wikidata share no IRIs, so federation runs through **external authority identifiers** that both sides record:

| Identifier | In MSE-KG | In Wikidata | Links |
|---|---|---|---|
| **ROR ID** | `nfdi:NFDI_0001006` → URL node → `nfdi:NFDI_0001008` | `wdt:P6782` | organisations |
| **ORCID iD** | `nfdi:NFDI_0001006` | `wdt:P496` | people |

Both queries below extract the bare identifier from the MSE-KG URL with `REPLACE(...)`, because Wikidata stores the identifier as a plain string rather than a URL.

!!! warning "WDQS requires a User-Agent"
    The Wikidata Query Service rejects requests that do not send a descriptive `User-Agent` header identifying the client and a contact. A default `curl`/library agent will fail. WDQS also enforces a query timeout (60 s) — keep the remote pattern selective and use `LIMIT`.

## Federated queries

### Which organizations in the MatWerk KG can be linked to Wikidata via their ROR ID, and what are their city, acronym, and country?

```sparql
PREFIX organization: <http://purl.obolibrary.org/obo/OBI_0000245>
PREFIX located_in:   <http://purl.obolibrary.org/obo/RO_0001025>
PREFIX nfdicore_city:<https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000106>
PREFIX has_external_identifier: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006>
PREFIX has_url:      <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX has_acronym:  <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0010015>
PREFIX rdfs:         <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wdt:          <http://www.wikidata.org/prop/direct/>

SELECT DISTINCT
  ?org
  ?orgLabel
  ?acronym
  ?city
  ?rorURL
  ?wikidataOrg
  ?wikidataOrgLabel
  ?countryLabel
WHERE {
  ?org a organization: .
  OPTIONAL { ?org rdfs:label ?orgLabel . }
  OPTIONAL { ?org has_acronym: ?acronym . }

  OPTIONAL {
    ?org located_in: ?cityNode .
    ?cityNode a nfdicore_city: ;
              rdfs:label ?city .
  }

  ?org has_external_identifier: ?idNode .
  ?idNode has_url: ?rorURL .

  BIND(REPLACE(STR(?rorURL), "^.*/", "") AS ?rorId)

  SERVICE <https://query.wikidata.org/sparql> {
    ?wikidataOrg wdt:P6782 ?rorId .
    OPTIONAL {
      ?wikidataOrg rdfs:label ?wikidataOrgLabel .
      FILTER(LANG(?wikidataOrgLabel) = "en")
    }
    OPTIONAL {
      ?wikidataOrg wdt:P17 ?country .
      ?country rdfs:label ?countryLabel .
      FILTER(LANG(?countryLabel) = "en")
    }
  }
}
LIMIT 100
```

---

### Which people in the MatWerk KG are linked to Wikidata through ORCID, and what are their employers and countries?

```sparql
PREFIX nfdicore_person:  <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000004>
PREFIX has_external_identifier: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001006>
PREFIX has_url: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT DISTINCT
  ?person
  ?personLabel
  ?orcidURL
  ?orcidId
  ?wikidataPerson
  ?wikidataPersonLabel
  ?employerLabel
  ?countryLabel
WHERE {
  ?person a nfdicore_person: .
  OPTIONAL { ?person rdfs:label ?personLabel . }

  ?person has_external_identifier: ?orcidURL .

  FILTER(CONTAINS(LCASE(STR(?orcidURL)), "orcid.org"))

  BIND(REPLACE(REPLACE(STR(?orcidURL), "^https?://orcid.org/", ""), "/$", "") AS ?orcidId)

  SERVICE <https://query.wikidata.org/sparql> {
    ?wikidataPerson wdt:P496 ?orcidId .

    OPTIONAL {
      ?wikidataPerson rdfs:label ?wikidataPersonLabel .
      FILTER(LANG(?wikidataPersonLabel) = "en")
    }

    OPTIONAL {
      ?wikidataPerson wdt:P108 ?employer .
      ?employer rdfs:label ?employerLabel .
      FILTER(LANG(?employerLabel) = "en")
    }

    OPTIONAL {
      ?wikidataPerson wdt:P27 ?country .
      ?country rdfs:label ?countryLabel .
      FILTER(LANG(?countryLabel) = "en")
    }
  }
}
LIMIT 100
```

## Sources

- Wikidata Query Service — <https://query.wikidata.org/sparql> · [user manual](https://www.mediawiki.org/wiki/Wikidata_Query_Service/User_Manual)
- WDQS [User-Agent policy](https://meta.wikimedia.org/wiki/User-Agent_policy)
- ROR ID (`P6782`) — <https://www.wikidata.org/wiki/Property:P6782> · [ROR](https://ror.org)
- ORCID iD (`P496`) — <https://www.wikidata.org/wiki/Property:P496> · [ORCID](https://orcid.org)
