from etl_tasks import SqlScriptTask
from script_lib import Script
from sql_syntax import Environment


class PCORNetInit(SqlScriptTask):
    script = Script.PCORNetInit

    @property
    def variables(self) -> Environment:
        return dict(datamart_id='todo', datamart_name='todo2', i2b2_data_schema='todo3')
