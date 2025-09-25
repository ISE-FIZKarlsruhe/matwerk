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

### 🔹 Scenario 1: I have a data (e.g., spreadsheet) but I don’t want to represent the information with ontology

✅ **What to do**  
- Upload your spreadsheet to [https://zenodo.org/](https://zenodo.org/)  
- Send the link to 📧 [ebrahim.norouzi@fiz-karlsruhe.de](mailto:ebrahim.norouzi@fiz-karlsruhe.de)

---

### 🔹 Scenario 2: I have a data (e.g., spreadsheet) and want to represent the information with ontology, but my data is not mapped or aligned (and I don’t have time to structure it)

⚠️ **Note**: We cannot map or structure your data for you.  

✅ **What to do**  
- Upload your spreadsheet to [https://zenodo.org/](https://zenodo.org/)  
- Send the link to 📧 [ebrahim.norouzi@fiz-karlsruhe.de](mailto:ebrahim.norouzi@fiz-karlsruhe.de)

---

### 🔹 Scenario 3: I have a data (e.g., spreadsheet) where the RDF data is already represented with ontology

✅ **What to do**  
1. Upload your spreadsheet (and RDF data) to [https://zenodo.org/](https://zenodo.org/) and make sure the **RDF data is included inside the repository** 
2. Create a GitHub issue 👉 [Open GitHub Issues](https://github.com/ISE-FIZKarlsruhe/matwerk/issues and include:  
   - Link to your dataset  
   - The data itself  
   - The ontology used  
   - Your SPARQL endpoint (if available)
3. Contact us (📧 [ebrahim.norouzi@fiz-karlsruhe.de](mailto:ebrahim.norouzi@fiz-karlsruhe.de)) directly to discuss **mappings** and enabling **federated SPARQL queries** and integrating it into the MSE KG in a **separate named graph**

---

### 🔹 Scenario 4: I have RDF data already in a triple store (Graph database)

✅ **What to do**  
1. Create a GitHub issue 👉 [Open GitHub Issues](https://github.com/ISE-FIZKarlsruhe/matwerk/issues)  
2. Include:  
   - Link to your dataset  
   - The data itself  
   - The ontology used  
   - Your SPARQL endpoint (if available)
3. Contact us (📧 [ebrahim.norouzi@fiz-karlsruhe.de](mailto:ebrahim.norouzi@fiz-karlsruhe.de)) directly to discuss **mappings** and enabling **federated SPARQL queries**  

---

### 🔹 Scenario 5: I have unstructured data (e.g., text)

✅ **What to do**  
- First, represent your data using ontologies  
- Need help? Contact our ontology expert:  
📧 [ebrahim.norouzi@fiz-karlsruhe.de](mailto:ebrahim.norouzi@fiz-karlsruhe.de)

---

### 🔹 Scenario 6: I have a data (e.g., person, software) that is already supported in MSE KG

You have two contribution options:

#### A. 🚀 Quick Contribution (No technical knowledge needed)  
1. Use our **spreadsheet template** based on the **MatWerk ontology** 👉 [Download template](https://drive.google.com/file/d/1GS5vKDWDPXeNWJX6UMZk78gq0gaVQ2RW/view)  
2. Fill in your data following the format  
3. Email the filled sheet to 📧 [ebrahim.norouzi@fiz-karlsruhe.de](mailto:ebrahim.norouzi@fiz-karlsruhe.de)

#### B. 🔬 Advanced Contribution (For ontology/workflow enthusiasts)  
1. Open the spreadsheet 👉 [Open Spreadsheet Template](#)  
2. Go to the relevant sheet (e.g., `Person`, `Software`)  
3. Each row = one instance; IDs are auto-generated via:  
   `Custom Actions > Replace with new ID`  
4. Insert your data in the appropriate cells  
5. Submit via:  
   `Custom Actions > Generate & Upload Data`  
6. To **update or delete**, use:  
   `Update Data` or `Delete Data`

📌 After submission:  
- An **error log** and **SHACL validation report** will be generated on GitHub 👉 [GitHub Repo](https://github.com/ISE-FIZKarlsruhe/matwerk)  
- You can fix errors yourself or get help via 📧 [ebrahim.norouzi@fiz-karlsruhe.de](mailto:ebrahim.norouzi@fiz-karlsruhe.de)

---

## ❓ Not Sure Where Your Data Fits?

No worries! You can:  
- 📩 [Create a GitHub issue](https://github.com/ISE-FIZKarlsruhe/matwerk/issues)  
- 📧 Or contact us directly at [ebrahim.norouzi@fiz-karlsruhe.de](mailto:ebrahim.norouzi@fiz-karlsruhe.de)

---
