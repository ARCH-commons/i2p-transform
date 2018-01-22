# i2p-transform
i2b2 to PCORnet Common Data Model Transformation - requires i2b2 PCORnet ontology

# HERON ETL to i2b2

We (*@@TODO aim to*) transform the data from a number of sources and
load it into an i2b2 data repository, following the
[i2b2 Data Repository (CRC) Cell Design Document][CRC].

[CRC]: https://www.i2b2.org/software/files/PDF/current/CRC_Design.pdf

## Usage

To build flowsheet transformation views,
once installation and configuration (below) is done, invoke the
`FlowsheetViews` task following [luigi][] conventions:

    luigi --module epic_flowsheets FlowsheetViews

In more detail:

    PYTHONPATH=. LUIGI_CONFIG_PATH=heron-test.cfg luigi \
      --local-scheduler \
      --module epic_flowsheets FlowsheetViews

*@@TODO: actual fact loading*

Then use `etl_tasks.MigratePendingUploads` to install the data in the
production i2b2 `observation_fact` table. Use
`@@TODO.PatientDimension` and `@@TODO.VisitDimLoad` to load the
`patient_dimension` and `visit_dimension` tables from
`observation_fact`.

[luigi]: https://github.com/spotify/luigi

Troubleshooting is discussed in [CONTRIBUTING][].

## Installation, Dependencies

See `requirements.txt`.

### luigid (optional)

 *@@TODO elaborate on this once we establish production conventions.*

    docker run --name luigid -p8082:8082 -v/STATE:/luigi/state -v/CONFIG:/etc/luigi -t stockport/luigid


## Configuration

See [client.cfg](client.cfg).

## Design and Development

For design info and project coding style, see [CONTRIBUTING][].

[CONTRIBUTING]: CONTRIBUTING.md

