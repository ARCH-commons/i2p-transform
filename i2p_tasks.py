from etl_tasks import SqlScriptTask
import luigi
from script_lib import Script
from sql_syntax import Environment
from typing import List

class Condition(SqlScriptTask):
    script = Script.condition

    def requires(self) -> List[luigi.Task]:
        return [Encounter()]

class Death(SqlScriptTask):
    script = Script.death

    def requires(self) -> List[luigi.Task]:
        return [Demographic()]

class DeathCause(SqlScriptTask):
    script = Script.death_cause

class Demographic(SqlScriptTask):
    script = Script.demographic

    def requires(self) -> List[luigi.Task]:
        return [PcornetInit()]

class Diagnosis(SqlScriptTask):
    script = Script.diagnosis

    def requires(self) -> List[luigi.Task]:
        return [Encounter()]

    @property
    def variables(self) -> Environment:
        return dict(i2b2_meta_schema='BLUEHERONMETADATA')

class Dispensing(SqlScriptTask):
    script = Script.dispensing

    def requires(self) -> List[luigi.Task]:
        return [Encounter()]

    @property
    def variables(self) -> Environment:
        return dict(i2b2_meta_schema='BLUEHERONMETADATA')


class Encounter(SqlScriptTask):
    script = Script.encounter

    def requires(self) -> List[luigi.Task]:
        return [Demographic()]

class Enrollment(SqlScriptTask):
    script = Script.enrollment

    @property
    def variables(self) -> Environment:
        return dict(enrollment_months_back='2')

class Harvest(SqlScriptTask):
    script = Script.harvest

    def requires(self) -> List[luigi.Task]:
        return [Condition(), Death(), Diagnosis(), Dispensing(), LabResultCM(), Prescribing(), Procedures(), Vital()]

    @property
    def variables(self) -> Environment:
        return dict(network_id='C4', network_name='GPC')

class LabResultCM(SqlScriptTask):
    script = Script.lab_result_cm

    def requires(self) -> List[luigi.Task]:
        return [Encounter()]

class MedAdmin(SqlScriptTask):
    script = Script.med_admin

    def requires(self) -> List[luigi.Task]:
        return [MedAdminInit()]

class MedAdminInit(SqlScriptTask):
    script = Script.med_admin_init

class ObsClin(SqlScriptTask):
    script = Script.obs_clin

    def requires(self) -> List[luigi.Task]:
        return []

class ObsGen(SqlScriptTask):
    script = Script.obs_gen

    def requires(self) -> List[luigi.Task]:
        return []

class PcornetInit(SqlScriptTask):
    script = Script.pcornet_init

    @property
    def variables(self) -> Environment:
        return dict(datamart_id='C4UK', datamart_name='University of Kansas', i2b2_data_schema='BLUEHERONDATA', min_pat_list_date_dd_mon_rrrr='01-Jan-2010', min_visit_date_dd_mon_rrrr='01-Jan-2010',
                    i2b2_meta_schema='BLUEHERONMETADATA')

class PcornetLoader(SqlScriptTask):
    script = Script.pcornet_loader

    def requires(self) -> List[luigi.Task]:
        #return [DeathCause(), Enrollment(), Harvest(), MedAdmin(), PcornetTrail(), ProCM()]
        return [DeathCause(), Demographic(), MedAdmin()]

class PcornetTrail(SqlScriptTask):
    script = Script.pcornet_trail

    def requires(self) -> List[luigi.Task]:
        return []

class Prescribing(SqlScriptTask):
    script = Script.prescribing

    def requires(self) -> List[luigi.Task]:
        return [Encounter()]

class ProCM(SqlScriptTask):
    script = Script.pro_cm

    def requires(self) -> List[luigi.Task]:
        return []

class Procedures(SqlScriptTask):
    script = Script.procedures

    def requires(self) -> List[luigi.Task]:
        return [Encounter()]

class Provider(SqlScriptTask):
    script = Script.provider

    def requires(self) -> List[luigi.Task]:
        return []

class Vital(SqlScriptTask):
    script = Script.vital

    def requires(self) -> List[luigi.Task]:
        return [Encounter()]