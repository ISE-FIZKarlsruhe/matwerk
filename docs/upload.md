# 🌐 Add Your Data to the MSE Knowledge Graph (MSE KG)

The **MSE Knowledge Graph (MSE KG)** improves **data visibility** and serves as an **indexing and discovery layer** for materials science and engineering resources. It helps people **find, connect, and reuse data** more easily.

Adding more data benefits us all. Depending on **your data type** and **how your data is currently stored**, choose the scenario below that best matches your case.

---

## 🚀 Choose Your Contribution Scenario

| Your situation | Recommended scenario |
|---|---|
| I have a spreadsheet or other tabular data | [Scenario 1](#scenario-1-unstructured-data-eg-spreadsheet-not-represented-with-ontology) or [Scenario 4](#scenario-4-data-type-already-supported-in-mse-kg-eg-person-software) |
| I already have RDF | [Scenario 2](#scenario-2-rdf-data-already-represented-with-ontology) |
| I run my own SPARQL endpoint | [Scenario 3](#scenario-3-rdf-data-in-a-triple-store-graph-database) |
| I want to add FDOs | [Scenario 5](#scenario-5-fair-digital-objects-fdos) |

> Click a scenario to jump directly to the instructions.

---

## 📚 Supported Data Types

The MSE KG currently supports the following resource types:

- City  
- Materials  
- Organization  
- People  
- Datasets  
- Software  
- Data Portals  
- Instruments  
- Large-scale Facilities  
- Metadata  
- Ontologies  
- Educational Resources  
- Patents  
- FDOs  
- Workflows  
- Services  
- International Collaborations  
- Events  
- Publications  
- Tools (NFDI resources)  
- OMS Tools  
- MatWerk-TA  
- MatWerk-IUC  
- MatWerk-PP  
- DFG Preface  
- DFG General Information (NFDI MatWerk Consortium)

---

## 🧭 Contribution Scenarios

### Scenario 1: Unstructured Data (e.g., spreadsheet not represented with ontology)

You have unstructured data such as a spreadsheet, but you do not want — or do not currently have the time — to model it using an ontology.

This is still a valid way to contribute to the MSE KG ecosystem.

> ⚠️ **Please note:** We cannot structure the data for you.

**What to do**


1. Upload your spreadsheet to **Zenodo**:  
   <a href="https://zenodo.org/communities/nfdi-matwerk/" target="_blank" rel="noopener noreferrer">NFDI MatWerk Community on Zenodo</a>
2. Provide **ROR IDs** for organizations and **ORCID IDs** for people, where applicable.
3. Your data can then be **automatically harvested** and added to the MSE KG.

[⬆ Back to scenario selection](#choose-your-contribution-scenario)

---

### Scenario 2: RDF Data Already Represented with Ontology

You already have **RDF data** and it is properly represented using an ontology. This allows direct integration into the MSE KG.

**What to do**


1. Upload your repository to **Zenodo**:  
   <a href="https://zenodo.org/communities/nfdi-matwerk/" target="_blank" rel="noopener noreferrer">NFDI MatWerk Community on Zenodo</a>
2. Ensure that the **RDF files are included in the repository**.
3. Your RDF will be integrated into the MSE KG in a **separate named graph**.

[⬆ Back to scenario selection](#choose-your-contribution-scenario)

---

### Scenario 3: RDF Data in a Triple Store (Graph Database)

You already maintain your own **triple store** and provide a **SPARQL endpoint**. In this case, we can connect your graph to the MSE KG so users can query it more seamlessly.

**What to do**


1. Add your SPARQL endpoint to the integration sheet:  
   <a href="https://docs.google.com/spreadsheets/d/1tiB4IZTCsjcw5QxBWk70XpRcwfw5-gs7CW2QTM5ZBiI/edit?gid=85394968#gid=85394968" target="_blank" rel="noopener noreferrer">SPARQL Endpoint Integration Sheet</a>
2. Your endpoint can then be **automatically integrated** into the MSE KG.
3. Optionally, open a GitHub issue to discuss:
   - mappings  
   - schema alignment  
   - federated SPARQL queries  

   <a href="https://github.com/ISE-FIZKarlsruhe/matwerk/issues" target="_blank" rel="noopener noreferrer">Submit GitHub Issue</a>

[⬆ Back to scenario selection](#choose-your-contribution-scenario)

---

### Scenario 4: Data Type Already Supported in MSE KG (e.g., Person, Software)

You want to contribute data about entities that are already supported in the MSE KG, such as **people**, **software**, or **organizations**.

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

You want to add your **FAIR Digital Objects (FDOs)** to the MSE KG.  
We provide a simple, semi-automated process so your registered FDOs can be harvested and integrated directly into the graph.

**What to do**


1. Add your FDO metadata to the integration spreadsheet:  
   <a href="https://docs.google.com/spreadsheets/d/1tiB4IZTCsjcw5QxBWk70XpRcwfw5-gs7CW2QTM5ZBiI/edit?usp=sharing" target="_blank" rel="noopener noreferrer">FAIR Digital Object Integration Sheet</a>
2. For each entry, include:
   - **FDO identifier**
   - **type**
   - **related dataset or publication**
   - **persistent URL**
3. Your FDOs will then be **automatically harvested** and integrated into the MSE KG via the **FAIR Digital Object Harvester** pipeline.
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
