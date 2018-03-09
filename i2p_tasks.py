"""i2p_tasks -- Luigi CDM task support.
"""

from etl_tasks import SqlScriptTask
from param_val import IntParam
from script_lib import Script
from sql_syntax import Environment
from sqlalchemy.engine import RowProxy
from sqlalchemy.exc import DatabaseError
from typing import List

class CDMScriptTask(SqlScriptTask):

    @property
    def variables(self) -> Environment:
        return dict(datamart_id='C4UK', datamart_name='University of Kansas', i2b2_data_schema='BLUEHERONDATA',
                    min_pat_list_date_dd_mon_rrrr='01-Jan-2010', min_visit_date_dd_mon_rrrr='01-Jan-2010',
                    i2b2_meta_schema='BLUEHERONMETADATA', enrollment_months_back='2', network_id='C4',
                    network_name='GPC')


class condition(CDMScriptTask):
    script = Script.condition


class death(CDMScriptTask):
    script = Script.death


class death_cause(CDMScriptTask):
    script = Script.death_cause


class demographic(CDMScriptTask):
    script = Script.demographic


class diagnosis(CDMScriptTask):
    script = Script.diagnosis


class dispensing(CDMScriptTask):
    script = Script.dispensing


class encounter(CDMScriptTask):
    script = Script.encounter


class enrollment(CDMScriptTask):
    script = Script.enrollment


class harvest(CDMScriptTask):
    script = Script.harvest


class lab_result_cm(CDMScriptTask):
    script = Script.lab_result_cm


class med_admin(CDMScriptTask):
    script = Script.med_admin


class med_admin_init(CDMScriptTask):
    script = Script.med_admin_init


class obs_clin(CDMScriptTask):
    script = Script.obs_clin


class obs_gen(CDMScriptTask):
    script = Script.obs_gen


class patient_chunks_survey(SqlScriptTask):
    script = Script.patient_chunks_survey
    patient_chunks = IntParam(default=200)
    patient_chunk_max = IntParam(default=None)

    #def variables(self) -> Environment:
    #    return dict(chunk_qty=str(self.patient_chunks))

    #def run(self) -> None:
    #    SqlScriptTask.run_bound(self, script_params=dict(chunk_qty=str(self.patient_chunks))

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
            params = dict(chunk_max=self.patient_chunk_max, chunk_qty=self.patient_chunks)

            try:
                return lc.execute(q, params=params).fetchall()
            except DatabaseError:
                return []


class pcornet_init(CDMScriptTask):
    script = Script.pcornet_init


class pcornet_loader(CDMScriptTask):
    script = Script.pcornet_loader


class pcornet_trial(CDMScriptTask):
    script = Script.pcornet_trial


class prescribing(CDMScriptTask):
    script = Script.prescribing


class pro_cm(CDMScriptTask):
    script = Script.pro_cm


class procedures(CDMScriptTask):
    script = Script.procedures


class provider(CDMScriptTask):
    script = Script.provider


class vital(CDMScriptTask):
    script = Script.vital
