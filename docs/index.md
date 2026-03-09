# Materials Science and Engineering Knowledge Graph (MSE KG)

!!! info "Last update"
    09.03.2026

## Semantic integration of distributed materials science resources

The **Materials Science and Engineering Knowledge Graph (MSE KG) v2.1** integrates heterogeneous resources from the **NFDI-MatWerk** community into a semantically explicit knowledge graph. Its purpose is to enable structured discovery, cross-resource linkage, and machine-actionable reuse of information across datasets, publications, software, infrastructures, organizations, and domain actors.  
The graph is aligned with the **Basic Formal Ontology (BFO)** and related domain ontologies to support **semantic interoperability**, **formal integration**, and **consistent representation of entities and relations** across distributed sources.

---

## Access pathways

### Exploratory access

The **MSE KG Dashboard** helps users explore the knowledge graph through **clear visualizations, useful statistics, and interactive views**. It offers an accessible overview of the graph’s content and structure, making it easier to understand what kinds of entities are included, how they are distributed, and how the knowledge graph evolves over time.

<a href="https://superset.ise.fiz-karlsruhe.de/superset/dashboard/mse-kg-dashboard/" target="_blank" rel="noopener noreferrer">Open Dashboard</a>

### Semantic and query access

The graph can be explored and queried through different interfaces, depending on the user’s level of technical experience. Guided tools support users who want to search and browse the graph more easily, while the SPARQL endpoint enables advanced users to perform precise queries, integrate the data into workflows, and run federated queries across multiple sources.

<a href="https://nfdi.fiz-karlsruhe.de/matwerk/shmarql/" target="_blank" rel="noopener noreferrer">Search with SHMARQL</a>

<a href="https://nfdi.fiz-karlsruhe.de/matwerk/sparql" target="_blank" rel="noopener noreferrer">SPARQL endpoint</a>

<a href="http://www.irisa.fr/LIS/ferre/sparklis/osparklis.html?title=Core%20English%20DBpedia&endpoint=https%3A//nfdi.fiz-karlsruhe.de/matwerk/sparql" target="_blank" rel="noopener noreferrer">Sparklis</a>

### Data contribution and ingestion

The MSE KG supports the continuous extension of graph content through community contributions. New resources can be incorporated in multiple forms, including spreadsheet-based metadata, ontology-aligned RDF, externally maintained SPARQL endpoints, and FAIR Digital Objects.

<a href="https://nfdi.fiz-karlsruhe.de/matwerk/upload/" target="_blank" rel="noopener noreferrer">Contribute Data</a>

---

## Ontological foundation and semantic modeling

The semantic architecture of the MSE KG is grounded in explicit ontological modeling. Ontologies provide the formal vocabulary required to define classes, relations, constraints, and shared meanings across data sources. This is essential for integrating heterogeneous materials science information in a way that remains logically interpretable by both humans and machines.

Here is the list of ontologies used:

<a href="https://ise-fizkarlsruhe.github.io/nfdicore/" target="_blank" rel="noopener noreferrer">The NFDI Core Ontology (NFDIcore)</a>

<a href="https://ise-fizkarlsruhe.github.io/mwo/" target="_blank" rel="noopener noreferrer">The MatWerk ontology (MWO)</a>

---

## Community coordination and curation

Knowledge graph development is not only a technical process but also a collaborative curation task. Community meetings and working groups are essential for discussing modeling decisions, onboarding new data sources, refining mappings, and coordinating integration strategies across participating institutions.

<a href="https://iuc12-nfdi-matwerk-ta-oms-7fd4826d9051b0dd93b21aa77d06d1d8c71c4.pages.rwth-aachen.de/" target="_blank" rel="noopener noreferrer">NFDI-MatWerk LOD Working Group</a>

---

## Purpose of the MSE KG

The MSE KG serves as a semantic integration layer for the NFDI-MatWerk ecosystem. Its scientific and infrastructural functions include:

- Providing a structured backend knowledge resource for the NFDI-MatWerk portal
- Integrating distributed research metadata from heterogeneous institutional sources
- Enabling semantic search, graph-based navigation, and formal querying
- Supporting interoperability across consortia, repositories, and technical infrastructures
- Facilitating reuse, linkage, and contextualization of research-relevant entities
- Evolving continuously through community-driven updates and semantic enrichment

![MSE-KG Visualization](img/matwerkBox.png)

---

## Scope of graph content

The MSE Knowledge Graph encompasses diverse categories:

- **Community Structure**: Researchers, projects, universities, institutions.
- **Infrastructure**: Software, workflows, vocabularies, facilities.
- **Data**: Repositories, publications, datasets, reference data.
- **Educational Resources**: Lectures, workshops, lecture notes.

---

## External context

For broader information on the consortium context in which the MSE KG is developed, please visit the <a href="https://nfdi-matwerk.de/" target="_blank" rel="noopener noreferrer">NFDI-MatWerk official website</a>.