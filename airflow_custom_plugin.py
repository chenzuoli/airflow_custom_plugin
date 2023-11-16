from airflow.plugins_manager import AirflowPlugin
from datetime import datetime, timedelta
from airflow.utils.db import provide_session
from airflow.models import DagRun
import pendulum


@provide_session
def _get_dag_run(ti, session=None):
    """Get DagRun obj of the TaskInstance ti

    Args:
        ti (TYPE): the TaskInstance object
        session (None, optional): Not in use

    Returns:
        DagRun obj: the DagRun obj of the TaskInstance ti
    """
    task = ti.task
    dag_run = None
    if hasattr(task, 'dag'):
        dag_run = (
            session.query(DagRun)
            .filter_by(
                dag_id=task.dag.dag_id,
                execution_date=ti.execution_date)
            .first()
        )
        session.expunge_all()
        session.commit()
    return dag_run


def ds_add_no_dash(ds, days):
    """
    Add or subtract days from a YYYYMMDD
    :param ds: anchor date in ``YYYYMMDD`` format to add to
    :type ds: str
    :param days: number of days to add to the ds, you can use negative values
    :type days: int
    >>> ds_add('20150101', 5)
    '20150106'
    >>> ds_add('20150106', -5)
    '20150101'
    """

    ds = datetime.strptime(ds, '%Y%m%d')
    if days:
        ds = ds + timedelta(days)
    return ds.isoformat()[:10].replace('-', '')


def dagtz_execution_date(ti):
    """get the TaskInstance execution date (in DAG timezone) in pendulum obj

    Args:
        ti (TaskInstance): the TaskInstance object

    Returns:
        pendulum obj: execution_date in pendulum object (in DAG tz)
    """
    execution_date_pdl = pendulum.instance(ti.execution_date)
    dagtz_execution_date_pdl = execution_date_pdl.in_timezone(ti.task.dag.timezone)
    return dagtz_execution_date_pdl


def dagtz_next_execution_date(ti):
    """get the TaskInstance next execution date (in DAG timezone) in pendulum obj

    Args:
        ti (TaskInstance): the TaskInstance object

    Returns:
        pendulum obj: next execution_date in pendulum object (in DAG tz)
    """

    # For manually triggered dagruns that aren't run on a schedule, next/previous
    # schedule dates don't make sense, and should be set to execution date for
    # consistency with how execution_date is set for manually triggered tasks, i.e.
    # triggered_date == execution_date.
    dag_run = _get_dag_run(ti)
    if dag_run and dag_run.external_trigger:
        next_execution_date = ti.execution_date
    else:
        next_execution_date = ti.task.dag.following_schedule(ti.execution_date)

    next_execution_date_pdl = pendulum.instance(next_execution_date)
    dagtz_next_execution_date_pdl = next_execution_date_pdl.in_timezone(ti.task.dag.timezone)
    return dagtz_next_execution_date_pdl


def dagtz_next_ds(ti):
    """get the TaskInstance next execution date (in DAG timezone) in YYYY-MM-DD string
    """
    dagtz_next_execution_date_pdl = dagtz_next_execution_date(ti)
    return dagtz_next_execution_date_pdl.strftime('%Y-%m-%d')


def dagtz_next_ds_nodash(ti):
    """get the TaskInstance next execution date (in DAG timezone) in YYYYMMDD string
    """
    dagtz_next_ds_str = dagtz_next_ds(ti)
    return dagtz_next_ds_str.replace('-', '')


def dagtz_prev_execution_date(ti):
    """get the TaskInstance previous execution date (in DAG timezone) in pendulum obj

    Args:
        ti (TaskInstance): the TaskInstance object

    Returns:
        pendulum obj: previous execution_date in pendulum object (in DAG tz)
    """

    # For manually triggered dagruns that aren't run on a schedule, next/previous
    # schedule dates don't make sense, and should be set to execution date for
    # consistency with how execution_date is set for manually triggered tasks, i.e.
    # triggered_date == execution_date.
    dag_run = _get_dag_run(ti)
    if dag_run and dag_run.external_trigger:
        prev_execution_date = ti.execution_date
    else:
        prev_execution_date = ti.task.dag.previous_schedule(ti.execution_date)

    prev_execution_date_pdl = pendulum.instance(prev_execution_date)
    dagtz_prev_execution_date_pdl = prev_execution_date_pdl.in_timezone(ti.task.dag.timezone)
    return dagtz_prev_execution_date_pdl


def dagtz_prev_ds(ti):
    """get the TaskInstance prev execution date (in DAG timezone) in YYYY-MM-DD string
    """
    dagtz_prev_execution_date_pdl = dagtz_prev_execution_date(ti)
    return dagtz_prev_execution_date_pdl.strftime('%Y-%m-%d')


def dagtz_prev_ds_nodash(ti):
    """get the TaskInstance prev execution date (in DAG timezone) in YYYYMMDD string
    """
    dagtz_prev_ds_str = dagtz_prev_ds(ti)
    return dagtz_prev_ds_str.replace('-', '')


# Defining the plugin class
class AirflowTestPlugin(AirflowPlugin):
    name = "custom_macros"
    macros = [dagtz_execution_date, ds_add_no_dash,
              dagtz_next_execution_date, dagtz_next_ds, dagtz_next_ds_nodash,
              dagtz_prev_execution_date, dagtz_prev_ds, dagtz_prev_ds_nodash]
