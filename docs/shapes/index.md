# SHACL Shape Constraints

The MSE-KG enforces structural integrity through SHACL (Shapes Constraint Language) validation. Each shape defines mandatory patterns that instances must satisfy to be included in a published release.

These shapes are executed automatically during every build cycle by the [validation pipeline](../pipeline/validation.md).

## Shape Catalogue

| Shape | Target Class | Constraint |
|-------|-------------|------------|
| [Textual Entity](textual-entity/pattern.md) | `IAO_0000300` | Must be *about* something |
| [No Punning](no-punning/pattern.md) | `owl:NamedIndividual` | Must not use `rdfs:subClassOf` |
| [Role Bearer](role-bearer/pattern.md) | `BFO_0000023` | Must have *role of* and *realized in* |
| [Role Realization](role-realization/pattern.md) | `BFO_0000023` | Must be *realized in* a process |
| [Ontology Variant](ontology-variant/pattern.md) | `NFDI_0000024` | Must be *part of* an ontology |
| [Ontology Version (Complex)](ontology-version-complex/pattern.md) | `NFDI_0000026` | Must be *subject of* version number/IRI, optionally file data items |
| [Email Address Value](email-address/pattern.md) | `IAO_0000300` | Must have a *value* |
| [URL Instance](url-instance/pattern.md) | `NFDI_0000223` | Must have a *url* |
