from etl_tasks import SqlScriptTask
import luigi
from script_lib import Script
from sql_syntax import Environment
from typing import List


class PCORNetInit(SqlScriptTask):
    script = Script.PCORNetInit

    @property
    def variables(self) -> Environment:
        return dict(datamart_id='todo1', datamart_name='todo2', i2b2_data_schema='BLUEHERONDATA', min_pat_list_date_dd_mon_rrrr='20-JAN-18', min_visit_date_dd_mon_rrrr='20-JAN-18',
                    i2b2_meta_schema='BLUEHERONMETADATA')

class PCORNetLoader_ora(SqlScriptTask):
    script = Script.PCORNetLoader_ora

    @property
    def variables(self) -> Environment:
        return dict(i2b2_meta_schema='BLUEHERONMETADATA', network_id='todo1', network_name='todo2', enrollment_months_back='2')

    def requires(self) -> List[luigi.Task]:
        '''Wrap each of `self.script.deps()` in a SqlScriptTask.
        '''
        return [PCORNetInit()]

class PCORNetMed_Admin(SqlScriptTask):
    script = Script.med_admin

    @property
    def variables(self) -> Environment:
        return dict()
