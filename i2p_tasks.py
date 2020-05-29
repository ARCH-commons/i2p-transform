"""i2p_tasks -- Luigi CDM task support.
"""
from typing import cast, List, Type

import luigi
from sqlalchemy.engine import RowProxy
from sqlalchemy.exc import DatabaseError

from csv_load import LoadCSV
from etl_tasks import CDMStatusTask, SqlScriptTask
from param_val import IntParam, StrParam
from script_lib import Script
from sql_syntax import Environment, Params

import csv
import subprocess
import urllib.request


class I2PConfig(luigi.Config):
    datamart_id = StrParam(description='see client.cfg')
    datamart_name = StrParam(description='see client.cfg')
    enrollment_months_back = StrParam(description='see client.cfg')
    i2b2_data_schema = StrParam(description='see client.cfg')
    i2b2_etl_schema = StrParam(description='see client.cfg')
    i2b2_meta_schema = StrParam(description='see client.cfg')
    min_pat_list_date_dd_mon_rrrr = StrParam(description='see client.cfg')
    min_visit_date_dd_mon_rrrr = StrParam(description='see client.cfg')
    network_id = StrParam(description='see client.cfg')
    network_name = StrParam(description='see client.cfg')


class I2PScriptTask(SqlScriptTask):

    @property
    def variables(self) -> Environment:
        return dict(datamart_id=I2PConfig().datamart_id, datamart_name=I2PConfig().datamart_name,
                    i2b2_data_schema=I2PConfig().i2b2_data_schema,
                    min_pat_list_date_dd_mon_rrrr=I2PConfig().min_pat_list_date_dd_mon_rrrr,
                    min_visit_date_dd_mon_rrrr=I2PConfig().min_visit_date_dd_mon_rrrr,
                    i2b2_meta_schema=I2PConfig().i2b2_meta_schema,
                    enrollment_months_back=I2PConfig().enrollment_months_back, network_id=I2PConfig().network_id,
                    network_name=I2PConfig().network_name, i2b2_etl_schema=I2PConfig().i2b2_etl_schema)


class condition(I2PScriptTask):
    script = Script.condition

    def requires(self) -> List[luigi.Task]:
        return [encounter()]


class death(I2PScriptTask):
    script = Script.death

    def requires(self) -> List[luigi.Task]:
        return [demographic()]


class death_cause(I2PScriptTask):
    script = Script.death_cause

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class demographic(I2PScriptTask):
    script = Script.demographic

    def requires(self) -> List[luigi.Task]:
        return [loadLanguage(), pcornet_init()]


class diagnosis(I2PScriptTask):
    script = Script.diagnosis

    def requires(self) -> List[luigi.Task]:
        return [encounter()]


class dispensing(I2PScriptTask):
    script = Script.dispensing

    def requires(self) -> List[luigi.Task]:
        return [encounter(), loadRouteMap(), loadUnitMap()]


class encounter(I2PScriptTask):
    script = Script.encounter

    def requires(self) -> List[luigi.Task]:
        return [demographic(), loadPayerMap()]


class enrollment(I2PScriptTask):
    script = Script.enrollment

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class harvest(I2PScriptTask):
    script = Script.harvest

    def requires(self) -> List[luigi.Task]:
        return [condition(), death(), death_cause(), diagnosis(), dispensing(), enrollment(),
                lab_result_cm(), loadHarvestLocal(), med_admin(), obs_clin(), obs_gen(), pcornet_trial(),
                prescribing(), pro_cm(), procedures(), provider(), vital()]


class Covid19a(I2PScriptTask):
    script = Script.harvest

    def requires(self) -> List[luigi.Task]:
        return [death(), death_cause(), diagnosis(), dispensing(), enrollment(),
                lab_result_cm(), loadHarvestLocal(), med_admin(), pcornet_trial(),
                prescribing(), pro_cm(), procedures(), provider(), vital()]


class lab_result_cm(I2PScriptTask):
    script = Script.lab_result_cm

    def requires(self) -> List[luigi.Task]:
        return [encounter(), loadLabNormal(), loadSpecimenSourceMap()]


class med_admin(I2PScriptTask):
    script = Script.med_admin

    def requires(self) -> List[luigi.Task]:
        return [encounter(), loadRouteMap(), loadUnitMap()]


class obs_clin(I2PScriptTask):
    script = Script.obs_clin

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class obs_gen(I2PScriptTask):
    script = Script.obs_gen

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class I2PPatientGroupTask(I2PScriptTask):
    patient_num_first = IntParam()
    patient_num_last = IntParam()
    patient_num_qty = IntParam(significant=False, default=-1)
    group_num = IntParam(significant=False, default=-1)
    group_qty = IntParam(significant=False, default=-1)

    def run(self) -> None:
        SqlScriptTask.run_bound(self, script_params=dict(
            patient_num_first=self.patient_num_first, patient_num_last=self.patient_num_last))


class _PatientNumGrouped(luigi.WrapperTask):
    group_tasks = cast(List[Type[I2PPatientGroupTask]], [])  # abstract

    def requires(self) -> List[luigi.Task]:
        deps = []  # type: List[luigi.Task]
        for group_task in self.group_tasks:
            survey = patient_chunks_survey()
            deps += [survey]
            results = survey.results()
            if results:
                deps += [
                    group_task(
                        group_num=ntile.chunk_num,
                        group_qty=len(results),
                        patient_num_qty=ntile.patient_num_qty,
                        patient_num_first=ntile.patient_num_first,
                        patient_num_last=ntile.patient_num_last)
                    for ntile in results
                ]
        return deps


class patient_chunks_survey(SqlScriptTask):
    script = Script.patient_chunks_survey
    patient_chunks = IntParam(default=20)
    patient_chunk_max = IntParam(default=None)

    @property
    def variables(self) -> Environment:
        return dict(chunk_qty=str(self.patient_chunks))

    def run(self) -> None:
        SqlScriptTask.run_bound(self, script_params=dict(chunk_qty=str(self.patient_chunks)))

    def results(self) -> List[RowProxy]:
        with self.connection(event='survey results') as lc:
            q = '''
               select patient_num
                 , patient_num_qty
                 , patient_num_first
                 , patient_num_last
               from patient_chunks
               where chunk_qty = :chunk_qty
                 and (:chunk_max is null or
                      chunk_num <= :chunk_max)
               order by chunk_num
             '''
            Params
            params = dict(chunk_max=self.patient_chunk_max, chunk_qty=self.patient_chunks)  # type: Params

            try:
                return lc.execute(q, params=params).fetchall()
            except DatabaseError:
                return []


# TODO: pcornet_init drops and recreates the cdm_status table, forcing all tasks to wait until init is done.
# Moving this operation to a distinct task would allow some other tasks (e.g. mapping tasks) to proceed, while init
# performs other labor intensive SQL operations.
# In the mean time, don't forget to make all tasks that use the status table dependent on pcornet_init.
# TODO: On a related matter, if the cdm_status table is missing (e.g. on a db where CDM has never been run), running
# the full pipeline, starting at pcornet_loader, will fail.  It would be nice to detect this situation and first
# build the status table before attempting other tasks.
class pcornet_init(I2PScriptTask):
    script = Script.pcornet_init

    def requires(self) -> List[luigi.Task]:
        return []


class pcornet_loader(I2PScriptTask):
    script = Script.pcornet_loader

    def requires(self) -> List[luigi.Task]:
        return [harvest()]


class pcornet_trial(I2PScriptTask):
    script = Script.pcornet_trial

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class prescribing(I2PScriptTask):
    script = Script.prescribing

    def requires(self) -> List[luigi.Task]:
        return [encounter(), loadRouteMap(), loadUnitMap()]


class pro_cm(I2PScriptTask):
    script = Script.pro_cm

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class procedures(I2PScriptTask):
    script = Script.procedures

    def requires(self) -> List[luigi.Task]:
        return [encounter()]


class provider(I2PScriptTask):
    script = Script.provider

    def requires(self) -> List[luigi.Task]:
        return [loadSpecialtyCode(), encounter()]


class vital(I2PScriptTask):
    script = Script.vital

    def requires(self) -> List[luigi.Task]:
        return [encounter()]


class loadPayerMap(LoadCSV):
    taskName = 'PAYER_MAP'
    # payer_map.csv is a combination of the CDM spec's payer_type spreadsheet
    # with payer_name and financial_class values from epic's clarity_epm table.
    # payer_name and financial_class are used together to determine the CDM's
    # payer type code.
    # TODO: incorporte IDX.
    csvname = 'curated_data/payer_map.csv'

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class loadLabNormal(LoadCSV):
    taskName = 'LABNORMAL'
    csvname = 'curated_data/labnormal.csv'

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class loadLabRUnit(LoadCSV):
    taskName = 'LABRUNIT'
    csvname = 'curated_data/resultunit_manualcuration.csv'

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class loadHarvestLocal(LoadCSV):
    taskName = 'HARVEST_LOCAL'
    csvname = 'curated_data/harvest_local.csv'

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class loadLanguage(LoadCSV):
    taskName = 'LANGUAGE_CODE'
    # language.csv is a copy of the CDM spec's patient_pref_language_spoken spreadsheet.
    # It maps a language code to descriptive text.  When the spreadsheet mapped more than
    # one text value to a code, duplicate codes where created in language.csv.
    csvname = 'curated_data/language.csv'

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class loadSpecimenSourceMap(LoadCSV):
    taskName = 'SPECIMEN_SOURCE_MAP'
    # specimen_source_map.csv matches values in the CDM spec's specimen_source spreadsheet
    # to specimen type values from Epic's order_proc table.
    csvname = 'curated_data/specimen_source_map.csv'

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class loadRouteMap(LoadCSV):
    taskName = 'ROUTE_MAP'
    # route_map.csv matches values in the CDM spec's _route spreadsheet
    # to route values from Epic's zc_admin_route table.
    csvname = 'curated_data/route_map.csv'

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class loadUnitMap(LoadCSV):
    taskName = 'UNIT_MAP'
    # unit_map.csv matches values in the CDM spec's _unit spreadsheet
    # to unit values from Epic's zc_med_unit table.
    csvname = 'curated_data/unit_map.csv'

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class NPIDownloadConfig(luigi.Config):
    # The configured 'path' and 'npi' variables are used by the downloadNPI method to fetch and
    # store the NPPES zip file.  Changes to these may require changes to the file system.
    # TODO: Update code to discover the npi_csv automatically.
    dl_path = StrParam(description='Path where the NPPES zip file will be stored and unzipped.')
    extract_path = StrParam(description='Path where the extract')
    npi_csv = StrParam(description='CSV file in the NPPES zip that contains NPI data.')
    npi_url = StrParam(description='URL for the NPPES download site.')
    npi_zip = StrParam(description='Name of the NPPES zip file.')

    # The configured 'col' and 'ct' variables reflect the layout of the NPI data file.
    # The extracNPI method uses these values to parse the NPI data file.
    # Changes to these may require code changes.
    # Complete overkill making these configurable.  Consider reverting to hard coded values.
    taxonomy_col = StrParam(description='Header for the taxonomy columns in the NPI data file.')
    switch_col = StrParam(description='Header for the switch columns in the NPI data file.')
    npi_col = StrParam(description='Header for the NPI column in the NPI data file.')
    taxonomy_ct = IntParam(description='Number of taxonomy columns in the NPI data file.')


class loadSpecialtyMap(LoadCSV):
    taskName = 'PROVIDER_SPECIALTY_MAP'
    # provider_specialty_map.csv is created on demand by the extractNPI method.
    # It maps a National Provider Identifier (NPI) to a primary provider specialty code.
    csvname = 'curated_data/provider_specialty_map.csv'

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init(), extractNPI()]


class loadSpecialtyCode(LoadCSV):
    taskName = 'PROVIDER_SPECIALTY_CODE'
    # provider_specialty_code.csv is a copy of the CDM spec's provider_primary_specialty spreadsheet.
    # It maps a provider specialty code to a descriptive text and grouping.
    csvname = 'curated_data/provider_specialty_code.csv'

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class downloadNPI(CDMStatusTask):
    '''
    Download the NPPES zip file and extract the NPI data file.
    '''
    taskName = 'NPI_DOWNLOAD'
    expectedRecords = 0

    dl_path = NPIDownloadConfig().dl_path
    npi_url = NPIDownloadConfig().npi_url
    npi_zip = NPIDownloadConfig().npi_zip

    def run(self) -> None:
        self.setTaskStart()
        self.fetch()
        self.unzip()
        self.setTaskEnd(self.expectedRecords)

    def fetch(self) -> None:
        r = urllib.request.urlopen(self.npi_url + self.npi_zip)

        with open(self.dl_path + self.npi_zip, 'wb') as fout:
            fout.write(r.read())

    def unzip(self) -> None:
        subprocess.call(['unzip', '-o', self.dl_path + self.npi_zip, '-d', self.dl_path])  # ISSUE: ambient

    def requires(self) -> List[luigi.Task]:
        return [pcornet_init()]


class extractNPI(CDMStatusTask):
    '''
    Extract the Nation Provider Identifier (NPI) and primary specialty from the NPPES download file
    and write the data to a separate csv file.
    '''
    taskName = 'NPI_EXTRACT'
    specialty_csv = 'provider_specialty_map.csv'

    dl_path = NPIDownloadConfig().dl_path
    extract_path = NPIDownloadConfig().extract_path
    npi_col = NPIDownloadConfig().npi_col
    npi_csv = NPIDownloadConfig().npi_csv
    switch_col = NPIDownloadConfig().switch_col
    taxonomy_col = NPIDownloadConfig().taxonomy_col

    taxonomy_ct = NPIDownloadConfig().taxonomy_ct

    def run(self) -> None:
        self.setTaskStart()
        self.extract()
        self.setTaskEnd(self.expectedRecords)

    def requires(self) -> List[luigi.Task]:
        return [downloadNPI()]

    def extract(self) -> None:

        self.expectedRecords = 0

        with open(self.dl_path + self.npi_csv, 'r', encoding='utf-8') as fin:
            with open(self.extract_path + self.specialty_csv, 'w', newline='') as fout:
                reader = csv.DictReader(fin)
                writer = csv.writer(fout)
                writer.writerow(['NPI', 'SPECIALTY'])
                for row in reader:
                    self.expectedRecords = self.expectedRecords + 1
                    useDefault = True
                    # Search the taxonomy columns for a primary specialty.
                    for i in range(1, self.taxonomy_ct + 1):
                        # 'Y' in the switch column indicates that the current taxonomy column contains
                        # the primary specialty.
                        if row[self.switch_col + str(i)] == 'Y':
                            useDefault = False
                            writer.writerow([row[self.npi_col], row[self.taxonomy_col + str(i)]])
                            continue

                    # If true, the NPI file did not explicitly identify a primary specialty.  Use the value
                    # in taxonomy column one as the default.
                    if (useDefault):
                        writer.writerow([row[self.npi_col], row[self.taxonomy_col + str(1)]])
