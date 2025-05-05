## Competency Questions

## General questions with the corresponding SPARQL queries

### What are all the types of entities (concepts) in the Chemotion-KG?
This query retrieves a distinct list of all entity types (concepts) present in the knowledge graph.

```sparql
SELECT DISTINCT ?Concept
WHERE {
  [] a ?Concept
}
LIMIT 999
```

---

### How many entities exist for each concept in the Chemotion-KG?
This query counts the number of entities associated with each concept and orders them by frequency.

```sparql
SELECT ?Concept (COUNT(?entity) AS ?count)
WHERE {
  ?entity a ?Concept .
}
GROUP BY ?Concept
ORDER BY DESC(?count)
LIMIT 999
```

### Who is working with Researcher "Ebrahim Norouzi" in the same group? Return the ORCID IDs?

```sparql
# Who is working with Researcher "Ebrahim Norouzi" in the same group? Return the ORCID IDs.

PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mwo: <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person ?personlabel ?orcidid
WHERE {
    # Find the researcher with the name "Ebrahim Norouzi"
    ?researcher rdfs:label ?researcherlabel FILTER REGEX (?researcherlabel, "ebrahim norouzi", "i")

    # Get the affiliation of the researcher
    ?researcher mwo:hasAffiliation ?Affiliation .

    # Find persons with the same affiliation but different from the researcher
    ?person mwo:hasAffiliation ?Affiliation FILTER (?person != ?researcher) .

    # Get the label of the person
    ?person rdfs:label ?personlabel .

    # Get the ORCID ID of the person
    ?person mwo:hasORCID ?orcidid .
}
```

### What is the email address of the contact point of "NOMAD" DataPortal?

```sparql
# What is the email address of the contactpoint of "NOMAD" DataPortal?

PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mwo: <http://purls.helmholtz-metadaten.de/mwo/>

SELECT ?resource ?contactpointemail 
WHERE {
    ?resource rdf:type nfdicore:DataPortal .            # Get the entities of type nfdicore:DataPortal
    ?resource rdfs:label ?label FILTER CONTAINS(?label, "NOMAD")  # Filter data portals with labels containing "NOMAD"
    ?resource mwo:hasContactPoint ?contactpoint.        # Get the contact point of the data portal
    ?contactpoint mwo:emailAddress ?contactpointemail . # Get the email address of the contact point
}
```

### What is "Molecular Dynamics" Software? List the programming language, documentation page, repository, and license information.

```sparql
# What are "Molecular Dynamics" Software? List the programming language, documentation page, repository, and license information.

PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mwo: <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX swo: <http://www.ebi.ac.uk/swo/>

SELECT DISTINCT ?resource ?label ?programminglanguagelabel ?documentationurl ?websiteurl ?licenseinfo
WHERE {
    ?resource rdf:type nfdicore:Software .                   # Get the entities of type nfdicore:Software
    ?resource rdfs:label ?label .                            # Get the label of the programming language
    ?resource mwo:usesMethod ?method .                       # Get the methods used by the software
    ?method rdfs:label ?methodlabel  FILTER CONTAINS(?methodlabel , "molecular dynamics")  # Filter label of the methods containing "Molecular Dynamics"
    ?resource nfdicore:programmingLanguage ?programminglanguage .  # Get the programming language used by the software
    ?programminglanguage rdfs:label ?programminglanguagelabel .   # Get the label of the programming language
    ?resource mwo:hasDocumentation ?documentation .          # Get the documentation of the software
    ?documentation nfdicore:url ?documentationurl .          # Get the label of the documentation
    ?resource mwo:hasWebsite ?website .                      # Get the website of the software
    ?website nfdicore:url ?websiteurl .                      # Get the label of the website
    ?resource swo:has_license ?license .                     # Get the license associated with the software
    ?license rdfs:label ?licenseinfo .                       # Get the label of the license
} GROUP BY ?label
```

### What are the ontologies in the nanomaterials domain?

```sparql
# What are the ontologies in nanomaterials domain?

PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mwo: <http://purls.helmholtz-metadaten.de/mwo/>

SELECT ?resource ?resourcelabel ?website
WHERE {
    ?resource rdf:type mwo:SemanticResource .                    # Get the entities of type mwo:SemanticResource
    ?resource mwo:description ?description FILTER CONTAINS(?description, "nanomaterials")  # Filter resources with descriptions containing "nanomaterials"
    ?resource rdfs:label ?resourcelabel .                        # Get the label of the resource
    ?resource mwo:hasRepository ?repositoryentity .              # Get the entity representing the repository of the resource
    ?repositoryentity nfdicore:url ?website .                    # Get the URL of the website/repository
}
```

### What software is used to produce the data in the Materials Cloud repository?

```sparql
# What are the software used to produce the data in the Materials Cloud repository?

PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mwo: <http://purls.helmholtz-metadaten.de/mwo/>

SELECT ?resource ?related_resource ?relatedresourcelabel
WHERE {
    ?resource rdf:type nfdicore:DataPortal .                        # Get the entities of type nfdicore:DataPortal
    ?resource rdfs:label ?label FILTER CONTAINS(?label, "Materials Cloud")  # Filter resources with labels containing "Materials Cloud"
    ?resource mwo:hasRelatedResource ?related_resource .            # Get the related resource of the data portal
    ?related_resource rdfs:label ?relatedresourcelabel .          # Get the label of the related resource
}
```

### What are the organizations in the KG that are categorized as a Public University in Wikidata?

```sparql
# What are the organizations in the KG that are categorized as a Public University in Wikidata?

PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mwo: <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>

SELECT DISTINCT ?organisation ?organisationlabel ?wikidataorganisation
WHERE {
    ?organisation rdf:type nfdicore:Organization .                 # Get the entities of type nfdicore:Organization
    ?organisation rdfs:label ?organisationlabel .                  # Get the label of the organizations
    ?organisation owl:sameAs ?wikidataorganisation .               # Get the equivalent Wikidata entity
    SERVICE <https://query.wikidata.org/sparql> {
        ?wikidataorganisation wdt:P31/wdt:P279* wd:Q875538 .        # Check if the Wikidata entity is categorized as a Public University
    }
}
```

### Give me the contact point of Elemental Multiperspective Material Ontology (EMMO) and the related projects.

```sparql
# Give me the contact point of Elemental Multiperspective Material Ontology (EMMO) and the related projects.

PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mwo: <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>

SELECT ?project ?projectlabel ?contactpointemail
WHERE {
    ?resource rdf:type mwo:SemanticResource .                # Get the entities of type mwo:SemanticResource
    ?resource rdfs:label "Elemental Multiperspective Material Ontology (EMMO)" .   # Filter entities with a specific label
    ?resource nfdicore:relatedProject ?project .              # Get the related projects of the resource
    ?project rdfs:label ?projectlabel .                       # Get the label of the projects
    ?resource mwo:hasContactPoint ?contactpoint .             # Get the contact point of the resource
    ?contactpoint mwo:emailAddress ?contactpointemail .       # Get the email address of the contact point
}
```

### List all ontologies with the Creative Commons Attribution 4.0 license.

```sparql
# List all ontologies with the Creative Commons Attribution 4.0 license?

PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mwo: <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX swo: <http://www.ebi.ac.uk/swo/>

SELECT ?resource ?resourcelabel
WHERE {
    ?resource rdf:type mwo:SemanticResource .                     # Get the entities of type mwo:SemanticResource
    ?resource rdfs:label ?resourcelabel .                          # Get the label of the resources
    ?resource swo:has_license ?license .                           # Get the license associated with the resource
    ?license owl:sameAs <http://www.wikidata.org/entity/Q20007257> .  # Check if the license is Creative Commons Attribution 4.0
}
```

### List people who have expertise in Information Service Engineering and the lecture they give.

```sparql
# List people who have expertise in Information Service Engineering and the lecture they give.

PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mwo: <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person ?personlabel ?resourcewebsite
WHERE {
    ?person mwo:hasExpertiseIn ?expertise .                          # Get people who have expertise in a specific area
    ?expertise rdfs:label ?label FILTER CONTAINS(?label, "Information Service Engineering")  # Filter by expertise label
    ?resource mwo:hasLecturer ?person .                              # Get the resources where the person is a lecturer
    ?person rdfs:label ?personlabel .                                # Get the label of the person
    ?resource rdfs:label ?resourcelabel .                            # Get the label of the resources
    ?resource mwo:hasWebsite ?resourcewebsiteentity .                # Get the website of the resources
    ?resourcewebsiteentity nfdicore:url ?resourcewebsite .           # Get the URL of the website
}
```

### List software that is written in Python with a GNU General Public License.

```sparql
# List software written in Python with a GNU General Public License

PREFIX nfdicore: <https://nfdi.fiz-karlsruhe.de/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mwo: <http://purls.helmholtz-metadaten.de/mwo/>
PREFIX swo: <http://www.ebi.ac.uk/swo/>

SELECT ?resource ?resourcelabel
WHERE {
    ?resource rdf:type nfdicore:Software .                           # Filter entities of type nfdicore:Software
    ?resource rdfs:label ?resourcelabel .                             # Get the label of the resources
    ?resource nfdicore:programmingLanguage ?programminglanguage .    # Get the programming language used by the resource
    ?programminglanguage owl:sameAs <http://www.wikidata.org/entity/Q28865> .  # Check if the programming language is Python
    ?resource swo:has_license ?license .                              # Get the license associated with the resource
    ?license owl:sameAs <http://www.wikidata.org/entity/Q7603> .      # Check if the license is GNU General Public License
}
```
