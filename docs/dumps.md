## RDF Dumps

Versioned RDF dumps of the MSE Knowledge Graph named graphs, archived on Zenodo with persistent DOIs.

Each release includes one Turtle (`.ttl`) file per named graph plus a metadata file describing the dump using the [MWO ontology](https://ise-fizkarlsruhe.github.io/mwo/) (v[[ dumps.releases[0].mwo_version if dumps.releases else "–" ]]).

---

[% if dumps.releases %]

### Latest Release: v[[ dumps.latest_version ]]

| | |
|---|---|
| **Date** | [[ dumps.releases[0].publish_date ]] |
| **MWO Version** | [[ dumps.releases[0].mwo_version ]] |
| **Zenodo DOI** | [% if dumps.releases[0].zenodo_doi %]<a href="https://doi.org/[[ dumps.releases[0].zenodo_doi ]]">[[ dumps.releases[0].zenodo_doi ]]</a>[% else %]–[% endif %] |

#### Named Graphs

| Graph | Triples | Subjects | Types | Dump File |
|-------|--------:|----------:|------:|-----------|
[% for g in dumps.releases[0].graphs %]| [`[[ g.graph_uri ]]`]([[ g.graph_uri ]]) | [[ "{:,}".format(g.stats.triples) ]] | [[ "{:,}".format(g.stats.subjects) ]] | [[ g.stats.distinct_types ]] | [[ g.dump_file ]] |
[% endfor %]

[% if dumps.releases[0].zenodo_url %]
[:material-download: Download from Zenodo]([[ dumps.releases[0].zenodo_url ]]){ .md-button .md-button--primary }
[% endif %]

---

### All Releases

| Version | Date | MWO | Triples (total) | DOI |
|---------|------|-----|----------------:|-----|
[% for r in dumps.releases %]| v[[ r.version ]] | [[ r.publish_date ]] | [[ r.mwo_version ]] | [[ "{:,}".format(r.graphs | sum(attribute='stats.triples')) ]] | [% if r.zenodo_doi %]<a href="https://doi.org/[[ r.zenodo_doi ]]">[[ r.zenodo_doi ]]</a>[% else %]–[% endif %] |
[% endfor %]

[% else %]

!!! info "No releases yet"
    RDF dumps will appear here once the first release is published via the `dump_and_archive` Airflow DAG.

[% endif %]
