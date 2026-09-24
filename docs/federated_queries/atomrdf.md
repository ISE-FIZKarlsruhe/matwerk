# atomRDF KG

**atomRDF KG** is a FAIR, RDF-based knowledge graph for **atomistic simulation data in metals**. It integrates seven published datasets covering materials such as Al, Cu, Fe, W and Mg, and includes results from molecular statics, molecular dynamics and DFT calculations. The graph represents grain boundary energies, work of separation, segregation and defect formation energies, elastic constants, and structural data, using established materials science ontologies and provenance standards.

The graph is produced with [**atomRDF**](https://github.com/pyscal/atomRDF), a Python tool for ontology-based creation, manipulation and querying of atomic structures, built on the **Computational Material Sample Ontology (CMSO)**.

## At a glance

| | |
|---|---|
| **IRI in MSE-KG** | [`msekg:176113442890318`](https://nfdi.fiz-karlsruhe.de/matwerk/msekg/176113442890318) |
| **`rdfs:label`** | atomRDF Knowledge Graph v0.0.1 |
| **Type** | `nfdi:NFDI_0000009` — dataset |
| **SPARQL endpoint** | `https://atomrdf.fair-workflows.org/sparql` |
| **Endpoint node** | [`msekg:17611350506671`](https://nfdi.fiz-karlsruhe.de/matwerk/msekg/17611350506671) (`nfdi:NFDI_0001095`) |
| **Licence** | CC BY 4.0 |
| **Creator(s)** | Abril Azocar Guzman, Sarath Menon, Stefan Sandfeld |
| **Affiliation** | IAS-9, Forschungszentrum Jülich GmbH · ICAMS, Ruhr-Universität Bochum |
| **Tooling** | [pyscal/atomRDF](https://github.com/pyscal/atomRDF) (MIT) |

## Vocabularies used on the remote side

| Prefix | Namespace | Covers |
|---|---|---|
| `cmso:` | `http://purls.helmholtz-metadaten.de/cmso/` | atomic-scale samples, species, elements |
| `asmo:` | `http://purls.helmholtz-metadaten.de/asmo/` | calculated properties, values, units |
| `cdco:` | `http://purls.helmholtz-metadaten.de/cdos/cdco/` | crystallographic defects |
| `pldo:` | `http://purls.helmholtz-metadaten.de/cdos/pldo/` | planar defects, Σ values |
| `dcterms:` | `http://purl.org/dc/terms/` | provenance, DOIs |

## Locate it in the MatWerk KG

```sparql
PREFIX nfdicore_dataset:         <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009>
PREFIX nfdicore_sparql_endpoint: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001095>
PREFIX denoted_by:               <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX has_url:                  <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX has_part:                 <http://purl.obolibrary.org/obo/BFO_0000051>
PREFIX has_license:              <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000142>
PREFIX rdfs:                     <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?label ?sparqlURL ?licence ?creator
WHERE {
  BIND(<https://nfdi.fiz-karlsruhe.de/matwerk/msekg/176113442890318> AS ?kg)

  ?kg a nfdicore_dataset: ;
      rdfs:label ?label ;
      denoted_by: ?endpoint .

  ?endpoint a nfdicore_sparql_endpoint: ;
            has_url: ?sparqlURL .

  OPTIONAL { ?kg has_license: ?lic  . ?lic     rdfs:label ?licence }
  OPTIONAL { ?kg has_part:    ?crea . ?crea    rdfs:label ?creator }
}
```

## Federated queries

The join key is the **DOI**: MSE-KG records the DOI of a published dataset via its URL node, and atomRDF records the DOI of the publication a sample belongs to. `FILTER(STR(?link) = STR(?doi))` is what ties the two halves together.

### Which atomistic samples correspond to a dataset entry in MSE-KG, and what are their segregation energies for Fe–Au Σ5 grain boundaries?

```sparql
PREFIX nfdicore_dataset: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009>
PREFIX denoted_by: <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX has_url: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>

PREFIX cmso: <http://purls.helmholtz-metadaten.de/cmso/>
PREFIX asmo: <http://purls.helmholtz-metadaten.de/asmo/>
PREFIX cdco: <http://purls.helmholtz-metadaten.de/cdos/cdco/>
PREFIX pldo: <http://purls.helmholtz-metadaten.de/cdos/pldo/>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT DISTINCT ?dataset ?link ?doi ?sample ?E_seg ?unit
WHERE {

  # --- Step 1: Select dataset from MatWerk KG ---
  ?dataset a nfdicore_dataset: .
  ?dataset denoted_by: ?linkNode .
  ?linkNode has_url: ?link .

  # --- Step 2: Query AtomRDF for scientific data ---
  SERVICE <https://atomrdf.fair-workflows.org/sparql> {

    # Atomic sample
    ?sample a cmso:AtomicScaleSample .

    # Grain boundary with Σ = 5
    ?sample cmso:hasMaterial ?mat .
    ?mat cdco:hasCrystallographicDefect ?gb .
    ?gb pldo:hasSigmaValue 5 .

    # Material contains Fe
    ?sample cmso:hasSpecies ?sp1 .
    ?sp1 cmso:hasElement ?el1 .
    ?el1 cmso:hasChemicalSymbol "Fe" .

    # Material contains Au
    ?sample cmso:hasSpecies ?sp2 .
    ?sp2 cmso:hasElement ?el2 .
    ?el2 cmso:hasChemicalSymbol "Au" .

    # Segregation energy
    ?sample asmo:hasCalculatedProperty ?prop .
    ?prop a asmo:SegregationEnergy ;
          asmo:hasValue ?E_seg ;
          asmo:hasUnit ?unit .

    # DOI of associated publication
    ?sample dcterms:isPartOf ?ds .
    ?ds dcterms:isReferencedBy ?pub .
    ?pub dcterms:identifier ?doi .
  }
  FILTER(STR(?link) = STR(?doi))
}
ORDER BY ?E_seg
```

---

### Who created datasets that are linked to atomistic segregation energy calculations?

```sparql
PREFIX nfdicore_dataset: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009>
PREFIX denoted_by: <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX has_part: <http://purl.obolibrary.org/obo/BFO_0000051>
PREFIX nfdicore_creator_role: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001032>
PREFIX nfdicore_institution_list: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001033>
PREFIX has_url: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

PREFIX cmso: <http://purls.helmholtz-metadaten.de/cmso/>
PREFIX asmo: <http://purls.helmholtz-metadaten.de/asmo/>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT DISTINCT ?dataset ?creatorLabel ?affiliation ?E_seg
WHERE {

  # --- MatWerk KG ---
  ?dataset denoted_by: ?linkNode .
  ?linkNode has_url: ?link .

  OPTIONAL {
    ?dataset has_part: ?creator .
    ?creator a nfdicore_creator_role: ;
             rdfs:label ?creatorLabel .
  }

  OPTIONAL {
    ?dataset has_part: ?aff .
    ?aff a nfdicore_institution_list: ;
         rdfs:label ?affiliation .
  }

  # --- AtomRDF ---
  SERVICE <https://atomrdf.fair-workflows.org/sparql> {

    # 🔗 JOIN
    ?sample dcterms:isPartOf ?ds .
    ?ds dcterms:isReferencedBy ?pub .
    ?pub dcterms:identifier ?doi .

    ?sample asmo:hasCalculatedProperty ?prop .
    ?prop a asmo:SegregationEnergy ;
          asmo:hasValue ?E_seg .
  }
  FILTER(STR(?link) = STR(?doi))
}
```

---

### Which chemical elements are studied in datasets linked to AtomRDF simulations?

```sparql
PREFIX nfdicore_dataset: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000009>
PREFIX denoted_by: <http://purl.obolibrary.org/obo/IAO_0000235>
PREFIX has_url: <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008>

PREFIX cmso: <http://purls.helmholtz-metadaten.de/cmso/>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT DISTINCT ?dataset ?element
WHERE {

  # --- MatWerk KG ---
  ?dataset denoted_by: ?linkNode .
  ?linkNode has_url: ?link .

  # --- AtomRDF ---
  SERVICE <https://atomrdf.fair-workflows.org/sparql> {

    # 🔗 JOIN
    ?sample dcterms:isPartOf ?ds .
    ?ds dcterms:isReferencedBy ?pub .
    ?pub dcterms:identifier ?doi .

    # Elements
    ?sample cmso:hasSpecies ?sp .
    ?sp cmso:hasElement ?el .
    ?el cmso:hasChemicalSymbol ?element .
  }
  FILTER(STR(?link) = STR(?doi))
}
```

## Sources

- atomRDF KG SPARQL endpoint — <https://atomrdf.fair-workflows.org/sparql>
- atomRDF tool — Menon, S., Azócar Guzmán, A., Sandfeld, S. *atomRDF: a python tool for ontology-based creation, manipulation, and querying of atomic structures.* <https://github.com/pyscal/atomRDF> (MIT)
- CMSO / ASMO / CDCO / PLDO — Helmholtz Metadata Collaboration, <http://purls.helmholtz-metadaten.de/>
- Registration in MSE-KG — [`sparql_endpoints` pattern](../patterns/sparql_endpoints/pattern.md)
