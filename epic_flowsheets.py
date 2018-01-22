'''epic_flowsheets -- ETL tasks for Epic Flowsheets to i2b2
'''

from typing import List

import luigi
import sqlalchemy as sqla

from etl_tasks import (
    DBAccessTask, I2B2ProjectCreate, I2B2Task, SourceTask,
    SqlScriptTask, UploadTask,
    DBTarget, SchemaTarget, UploadTarget
)
from script_lib import Script
from sql_syntax import Environment, Params
import param_val as pv


class CLARITYExtract(SourceTask, DBAccessTask):
    download_date = pv.TimeStampParam(description='see client.cfg')
    source_cd = pv.StrParam(default="Epic@kumed.com")

    # ISSUE: parameterize CLARITY schema name?
    schema = 'CLARITY'
    table_eg = 'patient'

    def _dbtarget(self) -> DBTarget:
        return SchemaTarget(self._make_url(self.account),
                            schema_name=self.schema,
                            table_eg=self.table_eg,
                            echo=self.echo)


class NightHeronCreate(I2B2ProjectCreate):
    pass


class NightHeronTask(I2B2Task):
    '''Mix in identified star_schema parameter config.
    '''
    @property
    def project(self) -> I2B2ProjectCreate:
        return NightHeronCreate()


class FromEpic(NightHeronTask):
    '''Mix in source and substitution variables for Epic ETL scripts.
    '''
    @property
    def source(self) -> CLARITYExtract:
        return CLARITYExtract()

    @property
    def variables(self) -> Environment:
        return self.vars_for_deps

    @property
    def vars_for_deps(self) -> Environment:
        # TODO: config = []
        # TODO: design = []  # TODO
        return dict()


class FlowsheetViews(SqlScriptTask):
    # TODO: subsume this in a load task
    # TODO: patient survey using ntile() -- persistent worthwhile?
    script = Script.epic_flowsheets_transform


class EpicDimensionsLoad(FromEpic, DBAccessTask):
    '''Placeholder for heron_load/load_epic_dimensions.sql
    '''
    @property
    def transform_name(self) -> str:
        return 'load_epic_dimensions'

    def complete(self) -> bool:
        return self.output().exists()

    def output(self) -> luigi.Target:
        return self._upload_target()

    def _upload_target(self) -> 'UploadTarget':
        return UploadTarget(self._make_url(self.account),
                            self.project.upload_table,
                            self.transform_name, self.source,
                            echo=self.echo)

    def run(self) -> None:
        raise NotImplementedError


class PatIdMapping(luigi.WrapperTask):
    def requires(self) -> List[luigi.Task]:
        # TODO: split pat_id mapping out of load_epic_dimensions.sql
        return [EpicDimensionsLoad()]


class FSDMapping(luigi.WrapperTask):
    def requires(self) -> List[luigi.Task]:
        # TODO: split pat_id mapping out of load_epic_dimensions.sql
        return [EpicDimensionsLoad()]


class EpicFactsLoadGroup(FromEpic, UploadTask):
    epic_fact_view = pv.StrParam()
    pat_id_lo = pv.StrParam()
    pat_id_hi = pv.StrParam()
    pat_group_num = pv.IntParam(significant=False)
    pat_group_qty = pv.IntParam(significant=False)
    pat_source_cd = pv.StrParam(default='Epic@kumed.com')
    enc_source_cd = pv.StrParam()

    script = Script.epic_facts_load

    @property
    def label(self) -> str:
        return '{view} #{group}: {lo} to {hi}'.format(
            view=self.epic_fact_view, group=self.pat_group_num,
            lo=self.pat_id_lo, hi=self.pat_id_hi)

    def requires(self) -> List[luigi.Task]:
        return UploadTask.requires(self) + [
            self.source,
            FSDMapping(),
            PatIdMapping(),
        ]

    @property
    def variables(self) -> Environment:
        return dict(self.vars_for_deps,
                    epic_fact_view=self.epic_fact_view,
                    log_fact_exceptions='')

    def script_params(self) -> Params:
        return dict(UploadTask.script_params(self),
                    pat_source_cd=self.pat_source_cd,
                    enc_source_cd=self.enc_source_cd,
                    pat_id_lo=self.pat_id_lo,
                    pat_id_hi=self.pat_id_hi)


class FlowsheetsLoad(FromEpic, DBAccessTask, luigi.WrapperTask):
    pat_group_qty = pv.IntParam(default=5, significant=False)
    enc_source_cd = pv.StrParam(default='Epic+pat_id_day@kumed.com')

    # issue: parameterize fact views?
    fact_views = [
        'numerictypeflows',
        'numerictypeflows',
        'datemeasureflows',
        'selectflows',
        'idstringtypeflows',
        'deidstringtypeflows',
    ]

    pat_grp_q = '''
        select :group_qty grp_qty, group_num
             , min(pat_id) pat_id_lo
             , max(pat_id) pat_id_hi
        from (
          select pat_id
               , ntile(:group_qty) over (order by pat_id) as group_num
          from (
            select /*+ parallel(20) */ distinct pat_id
            from clarity.patient
            where pat_id is not null  -- help Oracle use the index
          ) ea
        ) w_ntile
        group by group_num, :group_qty
        order by group_num
    '''

    def requires(self) -> List[luigi.Task]:
        groups = self.partition_patients()
        return [
            EpicFactsLoadGroup(epic_fact_view=view,
                               enc_source_cd=self.enc_source_cd,
                               pat_id_lo=lo,
                               pat_id_hi=hi,
                               pat_group_qty=qty,
                               pat_group_num=num)
            for view in self.fact_views
            for (qty, num, lo, hi) in groups]

    def partition_patients(self) -> List[sqla.engine.RowProxy]:
        # ISSUE: persist partition?
        with self.connection('partition patients') as q:
            groups = q.execute(self.pat_grp_q.format(i2b2_star=self.project.star_schema),
                               params=dict(group_qty=self.pat_group_qty)).fetchall()
            q.log.info('groups: %s', groups)
        return groups
