# 🌐 How to Add Your Data to the MSE Knowledge Graph (MSE KG)

The **MSE KG** (Materials Science and Engineering Knowledge Graph) enhances **data visibility** and acts as an **indexing system**, allowing everyone to discover, connect, and reuse data.

Adding more data benefits us all. Depending on **your contribution scenario and type of data**, here’s how you can contribute.

---

## 📚 Supported Data Types

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

### 🔹 Scenario 1: Unstructured Data (e.g., spreadsheet not represented with ontology)

You have unstructured data such as a spreadsheet, but you do not want (or do not have the time) to represent the information using an ontology. We will not structure this data for you, but you can still make it available in the MSE KG ecosystem.

⚠️ **Note**: We are not able to structure your data for you.  

✅ **What to do**  
1. Upload your spreadsheet to **Zenodo** [https://zenodo.org/communities/nfdi-matwerk/](https://zenodo.org/communities/nfdi-matwerk/)  
2. Provide **ROR IDs** for organizations and **ORCID IDs** for people (if applicable)  
3. Your data would automatically harvested and added to the MSE KG.

---

### 🔹 Scenario 2: RDF Data Already Represented with Ontology

You already have RDF data and it is properly represented with an ontology. This makes it possible to directly integrate your data into the MSE KG.

✅ **What to do**  
1. Upload your spreadsheet (and RDF data) to **Zenodo** [https://zenodo.org/communities/nfdi-matwerk/](https://zenodo.org/communities/nfdi-matwerk/)  
   - Make sure the **RDF data is included inside the repository**  
2. We will integrate your RDF into the MSE KG in a **separate named graph**

---

### 🔹 Scenario 3: RDF Data in a Triple Store (Graph Database)

**Description**:  
You already maintain your own triple store with a SPARQL endpoint. In this case, we can help connect your graph with the MSE KG so others can query it seamlessly.

✅ **What to do**  
1. If you have a **SPARQL endpoint**, please add it to this spreadsheet [SPARQL Endpoint Integration Sheet](https://docs.google.com/spreadsheets/d/1tiB4IZTCsjcw5QxBWk70XpRcwfw5-gs7CW2QTM5ZBiI/edit?gid=85394968#gid=85394968).  
   - We will automatically integrate it into the MSE KG.   
2. You may also open a GitHub issue 👉 [Submit GitHub Issue](https://github.com/ISE-FIZKarlsruhe/matwerk/issues) and include details about your SPARQL endpoint and dataset to discuss **mappings** and enabling **federated SPARQL queries** .  

---

### 🔹 Scenario 4: Data Type Already Supported in MSE KG (e.g., Person, Software)

You have data about entities that are already supported in the MSE KG (like people, software, or organizations).

✅ **What to do**  
1. Use our **spreadsheet template** based on the **MatWerk ontology** [Data collection spreadsheet](https://docs.google.com/spreadsheets/d/1tiB4IZTCsjcw5QxBWk70XpRcwfw5-gs7CW2QTM5ZBiI/edit?usp=sharing). 
2. Fill in your data following the format. 
3. You can also use this [Spreadsheet](https://docs.google.com/spreadsheets/d/1OyoWwcX4zUtrJilwXdtTooavELw278nQSNW2oniBBsk/edit?usp=sharing) to **check whether your data is already available in the knowledge graph**, without needing to run a SPARQL query yourself.  
   - The spreadsheet is connected to our **GitHub workflow**, which keeps the information synchronized.  
   - For this reason, the file is **read-only**, preventing accidental edits that could interfere with the automated process.

---

### 🔹 Scenario 5: FAIR Digital Objects (FDOs)

**Description:**  
You want to add your **FAIR Digital Objects (FDOs)** to the **MSE-KG**?  
We’ve made the process simple and semi-automated — your registered FDOs can be harvested and integrated directly into the graph.

✅ **What to do**  
1. Please add your FDO metadata to this spreadsheet:  
   [**FAIR Digital Object Integration Sheet**](https://docs.google.com/spreadsheets/d/1tiB4IZTCsjcw5QxBWk70XpRcwfw5-gs7CW2QTM5ZBiI/edit?usp=sharing).  
   - Each entry should include your **FDO identifier**, **type**, **related dataset/publication**, and **persistent URL**.  
2. Once added, your FDOs will be automatically harvested and integrated into the MSE-KG through the **FAIR Digital Object Harvester** pipeline.  
3. Optionally, open a GitHub issue 👉 [**Submit GitHub Issue**](https://github.com/ISE-FIZKarlsruhe/matwerk/issues) if you’d like to discuss **custom FDO mappings**, **schema alignment**, or **FDO–dataset linking** strategies.

---

## ❓ Not Sure Where Your Data Fits?

No worries! You can:  
- 📩 [Create a GitHub issue](https://github.com/ISE-FIZKarlsruhe/matwerk/issues)  
- 📧 Or contact us directly at [ebrahim.norouzi@fiz-karlsruhe.de](mailto:ebrahim.norouzi@fiz-karlsruhe.de)

---
