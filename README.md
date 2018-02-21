# i2p-transform

The Greater Plains Collaborative ([GPC][]) and the Accessible Research
Commons for Health ([ARCH][]) are PCORI CDRNs that i2b2 as a core
technology. The [i2b2 PCORnet Common Data Model Ontology][ont] is a
representation of the PCORnet Common Data Model ([CDM][]) which allows
not only i2b2 style ad-hoc query of clinical data but also this
i2p-transform of data from an [i2b2 datamart][CRC] into a PCORNet CDM
datamart.

[GPC]: http://pcornet.org/clinical-data-research-networks/cdrn4-university-of-kansas-medical-center-great-plains-collaborative/
[ARCH]: http://pcornet.org/clinical-data-research-networks/cdrn1-harvard-university-scihls/
[ont]: https://github.com/ARCH-commons/arch-ontology
[CDM]: http://www.pcornet.org/pcornet-common-data-model/
[CRC]: https://www.i2b2.org/software/files/PDF/current/CRC_Design.pdf


## Copyright and License

Copyright (c) 2014-2017 Univeristy of Kansas Medical Center
License:	MIT

Portions copyright The Brigham and Women’s Hospital, Inc.
License: i2b2 Software License


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

## References

 - Waitman, L.R., Aaronson, L.S., Nadkarni, P.M., Connolly, D.W. &
   Campbell, J.R. [The Greater Plains Collaborative: a PCORnet Clinical
   Research Data Network][1]. J Am Med Inform Assoc 21, 637-641 (2014).

[1]: https://www.ncbi.nlm.nih.gov/pubmed/24778202
