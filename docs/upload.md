# 🌐 Add Your Data to the MatWerk Knowledge Graph (MatWerk KG)

The **MatWerk Knowledge Graph (MatWerk KG)** improves **data visibility** and serves as an **indexing and discovery layer** for materials science and engineering resources. It helps people **find, connect, and reuse data** more easily.

Adding more data benefits us all. Depending on **your data type** and **how your data is currently stored**, choose the scenario below that best matches your case.

---

## 🚀 Choose Your Contribution Scenario

| Your situation | Recommended scenario |
|---|---|
| I have a spreadsheet or other tabular data | [Scenario 1](#scenario-1-unstructured-data-eg-spreadsheet-not-represented-with-ontology) or [Scenario 4](#scenario-4-data-type-already-supported-in-matwerk-kg-eg-person-software) |
| I already have RDF | [Scenario 2](#scenario-2-rdf-data-already-represented-with-ontology) |
| I have RDF but **nowhere to host it** | [Scenario 2](#scenario-2-rdf-data-already-represented-with-ontology) — Route A (we harvest it) or Route B (data portal + federation) |
| I run my own SPARQL endpoint | [Scenario 3](#scenario-3-rdf-data-in-a-triple-store-graph-database) |
| I want to add FDOs | [Scenario 5](#scenario-5-fair-digital-objects-fdos) |

> Click a scenario to jump directly to the instructions.

---

## 📚 Supported Data Types

The MatWerk KG currently supports the following resource types:

<div class="datatype-grid">
  <div class="dtype">🏙️ City</div>
  <div class="dtype">🔬 Materials</div>
  <div class="dtype">🏛️ Organization</div>
  <div class="dtype">👤 People</div>
  <div class="dtype">🗃️ Datasets</div>
  <div class="dtype">💻 Software</div>
  <div class="dtype">🌐 Data Portals</div>
  <div class="dtype">🔭 Instruments</div>
  <div class="dtype">🏗️ Large-scale Facilities</div>
  <div class="dtype">🏷️ Metadata</div>
  <div class="dtype">📐 Ontologies</div>
  <div class="dtype">🎓 Educational Resources</div>
  <div class="dtype">📜 Patents</div>
  <div class="dtype">📦 FDOs</div>
  <div class="dtype">⚙️ Workflows</div>
  <div class="dtype">🔧 Services</div>
  <div class="dtype">🌍 International Collaborations</div>
  <div class="dtype">📅 Events</div>
  <div class="dtype">📄 Publications</div>
  <div class="dtype">🛠️ Tools (NFDI resources)</div>
  <div class="dtype">🔩 OMS Tools</div>
  <div class="dtype">🧪 MatWerk-TA</div>
  <div class="dtype">🔗 MatWerk-IUC</div>
  <div class="dtype">📋 MatWerk-PP</div>
  <div class="dtype">📝 DFG Preface</div>
  <div class="dtype">ℹ️ DFG General Information</div>
</div>
---

## 🧭 Contribution Scenarios

### Scenario 1: Unstructured Data (e.g., spreadsheet not represented with ontology)

You have unstructured data such as a spreadsheet, but you do not want — or do not currently have the time — to model it using an ontology.

This is still a valid way to contribute to the MatWerk KG ecosystem.

> ⚠️ **Please note:** We cannot structure the data for you.

**What to do**


1. Upload your spreadsheet to **Zenodo**:  
   <a href="https://zenodo.org/communities/nfdi-matwerk/" target="_blank" rel="noopener noreferrer">NFDI MatWerk Community on Zenodo</a>
2. Provide **ROR IDs** for organizations and **ORCID IDs** for people, where applicable.
3. Your data can then be **automatically harvested** and added to the MatWerk KG.

[⬆ Back to scenario selection](#choose-your-contribution-scenario)

---

### Scenario 2: RDF Data Already Represented with Ontology

You already have **RDF data**, properly represented using an ontology, and you **cannot
or do not want to host it yourself**. There are two routes, and they differ in one
thing: who stores the data.

#### Route A — we harvest it from where it already lives (GitHub or Zenodo)

Best if your RDF is already published in a repository or a Zenodo record. We fetch the
files on a schedule and load each one into **its own named graph** in the MatWerk KG, so it
is queryable alongside everything else and stays linked to the repository it came from.

1. Publish the RDF where we can reach it:
    * a **GitHub release** — <a href="https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository" target="_blank" rel="noopener noreferrer">how to create a release</a>; or
    * a **Zenodo record** — <a href="https://zenodo.org/communities/nfdi-matwerk/" target="_blank" rel="noopener noreferrer">NFDI-MatWerk community on Zenodo</a>.
2. Register it in the **Semantic Dataset Sheet**:
   <a href="https://docs.google.com/spreadsheets/d/1tiB4IZTCsjcw5QxBWk70XpRcwfw5-gs7CW2QTM5ZBiI/edit" target="_blank" rel="noopener noreferrer">Semantic Dataset Sheet</a> — one row, and only `url` is mandatory:

    | column | what to put | needed |
    |---|---|---|
    | `Label` | a name for the dataset | yes |
    | `url` | the repository URL, Zenodo record URL, or a DOI | **yes** |
    | `source` | `github` or `zenodo` | no — inferred from the URL |
    | `files` | which RDF files to load | no — the released artefacts are picked automatically |
    | `Re-sync` | `weekly`, `monthly` or `none` | no |
    | `License`, `Creator(s)` | as in the other sheets | recommended |

3. Leave `files` empty unless you need to override the choice. The harvester takes the
   **release assets** if the release has any, otherwise the **root-level RDF** of the
   repository at the release tag, and collapses OBO-style variants
   (`x.ttl`, `x-base.ttl`, `x-full.ttl`, `x-simple.ttl` + the `.owl` of each) to the
   **full Turtle**. Name files explicitly only to pin a different variant, or to load
   several data files that are not variants of one another.
4. Open a <a href="https://github.com/ISE-FIZKarlsruhe/matwerk/issues" target="_blank" rel="noopener noreferrer">GitHub issue</a> so we can
   discuss federated queries, mappings and schema alignment.

After the next run your row is filled in with the **graph IRI** and a **query link**.
See [How harvested RDF is modelled](pipeline/rdf-files-modelling.md) for what is
created, and [the design patterns](patterns/index.md) for how it is shaped.

#### Route B — publish it yourself on a data portal, and we federate

Best if you want a citable landing page and your own SPARQL endpoint. The
**MaterialDigital Data Portal** hosts the dataset and gives you a public Fuseki
endpoint; we then federate that endpoint rather than copying your data.

1. Register your dataset at
   <a href="https://dataportal.material-digital.de/" target="_blank" rel="noopener noreferrer">dataportal.material-digital.de</a>
   (a single RDF file or a whole repository).
2. Copy the **public SPARQL endpoint** the portal gives you.
3. Add it to the
   <a href="https://docs.google.com/spreadsheets/d/1tiB4IZTCsjcw5QxBWk70XpRcwfw5-gs7CW2QTM5ZBiI/edit?gid=85394968#gid=85394968" target="_blank" rel="noopener noreferrer">SPARQL Endpoint Integration Sheet</a>.
4. Open a <a href="https://github.com/ISE-FIZKarlsruhe/matwerk/issues" target="_blank" rel="noopener noreferrer">GitHub issue</a> to request federation.

!!! example "A real one, end to end"
    **Creep Literature Knowledge Graph (CreepLitKG)** took Route B — see
    <a href="https://github.com/ISE-FIZKarlsruhe/matwerk/issues/17" target="_blank" rel="noopener noreferrer">issue #17</a>:

    * source — <a href="https://github.com/HosseinBeygiNasrabadi/Creep_Literature_Knowledge_Graph" target="_blank" rel="noopener noreferrer">GitHub repository</a>
    * landing page — <a href="https://dataportal.material-digital.de/dataset/creep_literature_knowledge_graph" target="_blank" rel="noopener noreferrer">MaterialDigital Data Portal</a>
    * endpoint — `https://dataportal.material-digital.de/dataset/a5b4edc4-43ef-44ff-a386-5d1f6fbbc439/fuseki/$/sparql`
    * registered in the SPARQL Endpoint Sheet, then federation requested by issue.

#### Which route?

| | Route A (we harvest) | Route B (portal + federation) |
|---|---|---|
| Who stores the data | the MatWerk KG, in its own named graph | you / the data portal |
| You need to run a server | no | no (the portal runs it) |
| Queryable in the MatWerk KG | yes, directly | yes, by federation |
| Stays in step with your source | automatically, on a schedule | whenever you update the portal |
| Citable landing page + DOI | via Zenodo | via the portal |

Both are fine. Choose **A** if the RDF already lives in a repository and you want it
inside the KG; choose **B** if you want your own endpoint and landing page.

[⬆ Back to scenario selection](#choose-your-contribution-scenario)

---

### Scenario 3: RDF Data in a Triple Store (Graph Database)

You already maintain your own **triple store** and provide a **SPARQL endpoint**. In this case, we can connect your graph to the MatWerk KG so users can query it more seamlessly.

**What to do**


1. Add your SPARQL endpoint to the integration sheet:  
   <a href="https://docs.google.com/spreadsheets/d/1tiB4IZTCsjcw5QxBWk70XpRcwfw5-gs7CW2QTM5ZBiI/edit?gid=85394968#gid=85394968" target="_blank" rel="noopener noreferrer">SPARQL Endpoint Integration Sheet</a>
2. Your endpoint can then be **automatically integrated** into the MatWerk KG.
3. Optionally, open a GitHub issue to discuss:
   - mappings  
   - schema alignment  
   - federated SPARQL queries  

   <a href="https://github.com/ISE-FIZKarlsruhe/matwerk/issues" target="_blank" rel="noopener noreferrer">Submit GitHub Issue</a>

[⬆ Back to scenario selection](#choose-your-contribution-scenario)

---

### Scenario 4: Data Type Already Supported in MatWerk KG (e.g., Person, Software)

You want to contribute data about entities that are already supported in the MatWerk KG, such as **people**, **software**, or **organizations**.

**What to do**


1. Use the **data collection spreadsheet template** based on the **MatWerk ontology**:  
   <a href="https://docs.google.com/spreadsheets/d/1tiB4IZTCsjcw5QxBWk70XpRcwfw5-gs7CW2QTM5ZBiI/edit?usp=sharing" target="_blank" rel="noopener noreferrer">Data Collection Spreadsheet</a>
2. Fill in your data according to the provided format.
3. To check whether your data is already available in the knowledge graph, use this spreadsheet:  
   <a href="https://docs.google.com/spreadsheets/d/1OyoWwcX4zUtrJilwXdtTooavELw278nQSNW2oniBBsk/edit?usp=sharing" target="_blank" rel="noopener noreferrer">Availability Check Spreadsheet</a>

> ℹ️ This spreadsheet is connected to a **Apache-Airflow workflow** and is therefore **read-only**, which prevents accidental edits from interfering with synchronization.

[⬆ Back to scenario selection](#choose-your-contribution-scenario)

---

### Scenario 5: FAIR Digital Objects (FDOs)

You want to add your **FAIR Digital Objects (FDOs)** to the MatWerk KG.  
We provide a simple, semi-automated process so your registered FDOs can be harvested and integrated directly into the graph.

**What to do**


1. Add your FDO metadata to the integration spreadsheet:  
   <a href="https://docs.google.com/spreadsheets/d/1tiB4IZTCsjcw5QxBWk70XpRcwfw5-gs7CW2QTM5ZBiI/edit?usp=sharing" target="_blank" rel="noopener noreferrer">FAIR Digital Object Integration Sheet</a>
2. For each entry, include:
   - **FDO identifier**
   - **type**
   - **related dataset or publication**
   - **persistent URL**
3. Your FDOs will then be **automatically harvested** and integrated into the MatWerk KG via the **FAIR Digital Object Harvester** pipeline.
4. Optionally, open a GitHub issue if you would like to discuss:
   - custom FDO mappings  
   - schema alignment  
   - FDO–dataset linking strategies 
  
   <a href="https://github.com/ISE-FIZKarlsruhe/matwerk/issues" target="_blank" rel="noopener noreferrer">Submit GitHub Issue</a>

[⬆ Back to scenario selection](#choose-your-contribution-scenario)

---

### ❓ Not Sure Where Your Data Fits?

No problem — we can help.

- 📩 <a href="https://github.com/ISE-FIZKarlsruhe/matwerk/issues" target="_blank" rel="noopener noreferrer">Create a GitHub Issue</a>
- 📧 Contact us directly: [ebrahim.norouzi@fiz-karlsruhe.de](mailto:ebrahim.norouzi@fiz-karlsruhe.de)
