## Competency Questions

## Infrastructure use cases (IUCs)-related questions with the corresponding SPARQL queries

### (IUC02) What are datasets produced by the BAM organization? List the title, standard, license, hosting repository and which material it is about.

```sparql
#What are datasets produced by the BAM organization? List the title, standard, license, hosting repository and which material it is about.
PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mwo: <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX swo: <http://www.ebi.ac.uk/swo/>

SELECT DISTINCT ?data ?label ?materiallabel ?licenselabel ?standardlabel ?repositoryurl ?affiliation 
WHERE {
    ?data rdf:type nfdicore:Dataset .      # Get the entities of type nfdicore:Dataset
    ?data mwo:hasCreator ?creators .                      # Get the creators of the dataset
    ?data rdfs:label ?label .
    ?creators rdfs:label ?personlabel .                  # Get the label of the creators
    ?creators mwo:hasAffiliation ?affiliation .          # Get the affiliation of the creators
    ?affiliation rdfs:label ?affiliationlabel .          # Get the label of the affiliation
   ?affiliation  mwo:hasAcronym ?affiliationAcc FILTER CONTAINS(?affiliationAcc, "BAM")  # Get the datasets with labels containing "BAM"
  OPTIONAL{  
    ?data mwo:aboutMaterial ?material .                  # Get the material the data is about
    ?material rdfs:label ?materiallabel .                # Get the label of the material
}
  OPTIONAL{  ?data swo:has_license ?license .                     # Get the license of the dataset
    ?license rdfs:label ?licenselabel .                  # Get the label of the license
}
  OPTIONAL{  
    ?data nfdicore:standard ?standard .                  # Get the standard associated with the dataset
    ?standard rdfs:label ?standardlabel .                # Get the label of the standard
}
  OPTIONAL{  
    ?data mwo:hasRepository ?repository .                # Get the repository of the dataset
    ?repository nfdicore:url ?repositoryurl .             # Get the label of the repository
}
}
```

### (IUC04) What are the resources related to the SFB1394 Project?

```sparql
# List the resources related to the SFB1394 Project?

PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mwo: <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>

SELECT  ?resource ?label ?project ?doi2
WHERE {
   ?resource rdf:type ?type .              # The type of the resource
    ?resource rdfs:label ?label .                                        # The label of the resource 
    ?resource nfdicore:relatedProject ?project .            # Get the related projects of the resource
   ?resource mwo:hasDOI ?doi.
   ?project rdfs:label ?projectlabel  FILTER CONTAINS(?projectlabel, "SFB1394")          # Filter projects with the required one
BIND( IRI(?doi) AS ?doi2 ).
}
```

### (IUC09) What are Computational Workflows associated with the Atom Probe Tomography method? List the funding project(s), license and the repository URI.

```sparql
#  What are Computational Workflows associated with the Atom Probe Tomography method? List the funding project(s), license and repository URI

PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mwo: <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX swo: <http://www.ebi.ac.uk/swo/>

SELECT DISTINCT *
WHERE {
    ?data rdf:type mwo:ComputationalWorkflow.
    ?data rdfs:label ?label .
    ?data nfdicore:fundingProject ?fundingProject.
   ?data mwo:hasRepository ?repo.
   ?data swo:has_license ?license. 
  ?data mwo:usesMethod ?method.
   ?license rdfs:label ?LicenseLabel .
?method rdfs:label ?methodlabel  FILTER CONTAINS(?methodlabel, "Atom Probe Tomography") 
}
```

### (IUC17) What are ontologies which describe "crystalline defects"? List the repositories and related project.

```sparql
#   What are ontologies which describe "crystalline defects"? List the repositories and related project.

PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mwo: <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX swo: <http://www.ebi.ac.uk/swo/>

SELECT DISTINCT *
WHERE {
    ?data rdf:type mwo:SemanticResource.
    ?data rdfs:label ?label .
    ?data nfdicore:semanticExpressivity nfdicore:Ontology .
    ?data mwo:description ?description  FILTER CONTAINS(?description, "crystalline defect")
    ?data mwo:hasRepository ?repo.
    ?data nfdicore:relatedProject ?proj.
    ?proj rdfs:label ?projLabel .


}
```

### (Indentation) What are workflows related to keywords: Aluminium and Elastic Constants? List the type, URL, funding project and authors. 

```sparql
#  What are workflows related to keywords: Aluminium and Elastic Constants? List the type, URL and authors. 


PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mwo: <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX swo: <http://www.ebi.ac.uk/swo/>

SELECT DISTINCT *
WHERE {
    ?data rdf:type mwo:ComputationalWorkflow.
    ?data rdfs:label ?label .
    ?data nfdicore:fundingProject ?fundingProject.
   ?data mwo:hasRepository ?repo.
   ?data swo:has_license ?license. 
  ?data mwo:hasContributor ?contributor.
   ?data mwo:keyword  ?keyword . FILTER (?keyword IN( "Aluminium","Elastic Constants")) 
}
```
