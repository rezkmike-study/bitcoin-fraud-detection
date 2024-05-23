# EMR helper functions to create DAGs

import io
import os
import boto3
import gzip

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator, xcom
from airflow.operators.empty import EmptyOperator

from .mon_helper import (
    LogFriendlyOpsGenieOperator,
    scan_for_exceptions,
)

def get_full_script_path(
        path: dict,
        prefix: str='scripts',
) -> str:
    parts = [
        os.environ['SCRIPT_BUCKET'].strip('/'),
        prefix.strip('/'),
        path.strip('/'),
    ]
    return '/'.join(parts)

def get_spark_submit_paramstr(pylibs=None, opts=None, non_conf_params=None) -> str:
    """
    Use this function to prepare spark submit string for your spark job

    :param non_conf_params: Pass in dictionary value to this variable. The fuction will take the
    dictionary key as config key and dictionary value as config value.
        For example:
          -> Pass in dictionary = {'deploy-mode': 'cluster', 'name':'spark_name'}
          -> Function process string to = "--deploy-mode cluster --name spark_name"
    """
    
    # helper method to create submit param strings
    defaults = {
        'spark.executor.instances': 2,
        'spark.executor.memory': '4G',
        'spark.executor.cores': 2,
        'spark.driver.cores': 1,
        'spark.kubernetes.container.image': 'public.ecr.aws/p8x2j9z6/spark/emr:6.13-7',
    }

    s = []

    if not isinstance(pylibs, list):
        pylibs = []

    if len(pylibs) > 0:
        s.append('--py-files %s' % ','.join(pylibs))

    # Only process `non_conf_params` when the variable is dictionary
    if isinstance(non_conf_params, dict):
        for k, v in non_conf_params.items():
            s.append('--%s %s' % (k, v))

    if not isinstance(opts, dict):
        opts = {}

    a = {**defaults, **opts}

    _sep = ' '
    for k, v in a.items():
        s.append('--conf %s=%s' % (k, v))

    return _sep.join(s)

def generate_dynamic_ep_code(task_ids: str, key: str, *keys):
    """
    Use this function to import xcom variable as job argument for emr 
    """

    code = []
    for k in keys:
        prefix = f'--{k.upper()}'
        value = f'{{{{task_instance.xcom_pull(task_ids="{task_ids}", key="{key}")["{k}"]}}}}'
        code.extend([prefix, value])
    print(code)
    return code


def replace_arg_key(original_args, key_to_replace):
    """
    Use this function to rename your job argument key
    """

    staging_args = []
    for item in original_args:
    
        # Trim argument to key
        arg_key = item.replace("--", "")
        if arg_key in key_to_replace:
            staging_args.append(f"--{key_to_replace[arg_key]}")
        else:
            staging_args.append(item)

    return staging_args.copy()

def get_configuration_overrides(dag_id = None) -> dict:
    # ported from Loi's code
    # return a dictionary to be passed as configuration_override
    if isinstance(dag_id, str):
        _loguri = f"{os.environ['SCRIPT_BUCKET']}/sparkHistorylogs/{dag_id}"
    else:
        _loguri = f"{os.environ['SCRIPT_BUCKET']}/sparkHistorylogs"

    conf_override = {
        "applicationConfiguration": [{
            "classification": "spark-defaults",
            "properties": {
                "spark.jars": "local:///usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.defaultCatalog": "glue_catalog",
                "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
                "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                "spark.ui.prometheus.enabled": "true",
                "spark.sql.catalog.glue_catalog.http-client.type": "apache",
                "spark.sql.sources.partitionOverwriteMode": "dynamic",
                "spark.driver.extraJavaOptions": "-Dlog4j.rootCategory=WARN",
                "spark.executor.extraJavaOptions": "-Dlog4j.rootCategory=WARN",
                "spark.executor.processTreeMetrics.enabled": "true",
                "spark.kubernetes.driver.annotation.prometheus.io/scrape": "true",
                "spark.kubernetes.driver.annotation.prometheus.io/path": "/metrics/executors/prometheus/",
                "spark.kubernetes.driver.annotation.prometheus.io/port": "4040",
                "spark.kubernetes.driver.service.annotation.prometheus.io/scrape": "true",
                "spark.kubernetes.driver.service.annotation.prometheus.io/path": "/metrics/driver/prometheus/",
                "spark.kubernetes.driver.service.annotation.prometheus.io/port": "4040",
                "spark.metrics.conf.*.sink.prometheusServlet.class": "org.apache.spark.metrics.sink.PrometheusServlet",
                "spark.metrics.conf.*.sink.prometheusServlet.path": "/metrics/prometheus/",
                "spark.metrics.conf.master.sink.prometheusServlet.path": "/metrics/master/prometheus/",
                "spark.metrics.conf.applications.sink.prometheusServlet.path": "/metrics/applications/prometheus/"
            }
        }],
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": _loguri
            }
        }
    }
    return conf_override


def flatten_arguments_for_entry(job_args: dict) -> list:
    # ported from Loi's code
    # return a flatten list to be fed to the EMR operator
    el = []
    for idx, (key, value) in enumerate(job_args.items()):
        el.append(f'--{key}')
        el.append(f'{value}')
    return el

# def flatten_arguments_selective(job_args: dict, keys_to_include: list) -> list:
#     # return a flatten list to be fed to the EMR operator
#     el = []
#     for key in keys_to_include:
#         if key in job_args:
#             el.append(f'--{key}')
#             el.append(job_args[key])
#     return el


def flatten_arguments_selective(job_args: dict, keys_to_include: list) -> list:
    # return a flattened list to be fed to the EMR operator
    el = []
    for key in keys_to_include:
        if key in job_args:
            el.append(f'--{key}')
            el.append(f'{job_args[key]}')
    return el

def _download_s3_object_as_bytebuf(s3_uri):
    bucket_name, object_key = s3_uri.replace('s3://', '').split('/', 1)
    bs3c = boto3.client('s3')
    buf = io.BytesIO()
    bs3c.download_fileobj(bucket_name, object_key, buf)
    buf.seek(0)
    return buf

_EMR_JOB_ID_XCOM_KEY = 'emr_job_id'

class LogFriendlyEmrContainerOperator(EmrContainerOperator):
    def execute(self, context):
        try:
            return super().execute(context)
        #except AirflowException as e:
        #    # we don't die first if airflowException
        #    #self.log.error(str(e))
        #    # push the job_id
        #    BaseOperator.xcom_push(
        #        context,
        #        key=xcom.XCOM_RETURN_KEY,
        #        value=self.job_id
        #    )
        #    # re-raise exception
        #    raise e
        finally:
            BaseOperator.xcom_push(
                context,
                key=_EMR_JOB_ID_XCOM_KEY,
                value=self.job_id
            )

class LogFriendlyEmrMonitorOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        *args,
        alert_payload_generator=None,
        **kwargs
    ):
        kwargs['trigger_rule'] = 'all_done' # run regardless succ/fail
        self.alert_payload_generator = alert_payload_generator
        super(LogFriendlyEmrMonitorOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        rels = context.get('task').get_direct_relatives(upstream=True)
        assert(len(rels) == 1, "LogFriendlyEmrMonitorOperator must only depend on 1 upstream task")
        prev_task = rels[0]
        assert(prev_task is LogFriendlyEmrContainerOperator, "LogFriendlyEmrMonitorOperator must depend on a LogFriendlyEmrContainerOperator task")

        prev_ti = None
        for ti in context.get('dag_run').get_task_instances():
            if ti.task_id == prev_task.task_id: # rels[0] is the emr task we depend on
                prev_ti = ti
                break

        assert(prev_ti is not None, "Unable to get previous task instance")
        base_logpath = prev_task\
                .configuration_overrides \
                ["monitoringConfiguration"]\
                ["s3MonitoringConfiguration"]\
                ["logUri"]
        emr_job_id = context\
                .get('task_instance')\
                .xcom_pull(key=_EMR_JOB_ID_XCOM_KEY)
        if emr_job_id is None:
            self.log.error('Unable to get emr_job_id from xcom_pull')
            raise Exception('error getting emr_job_id to fetch logs')

        self.log.info(f'prev_ti: {prev_ti}')
        self.log.info(f'xcom_pull emr_job_id: {emr_job_id}')

        driver_logpath = f'{base_logpath}/{prev_task.virtual_cluster_id}/jobs/{emr_job_id}/containers/spark-{emr_job_id}/spark-{emr_job_id}-driver'
        driver_stdout = f'{driver_logpath}/stdout.gz'

        self.log.info(f'fetching logs from: {driver_stdout}')
        buf = _download_s3_object_as_bytebuf(driver_stdout)

        sbuf = io.StringIO()
        with gzip.GzipFile(fileobj=buf, mode='rb') as gzfd:
            for line in gzfd:
                line = line.decode('utf-8')
                print(line, file=sbuf)
                self.log.info(line)
        sbuf.seek(0)
        raise_alert = scan_for_exceptions(
            context,
            sbuf,
            self.alert_payload_generator
        )

        return raise_alert

def monitor_logs(
    emr_task,
    alert_on_exception_detect=True,
    alert_payload_generator=None,
):
    """
    refer to 'nonprod/apps/test/dag_test_mon_emr_opsgenie.py' for example use
    """

    assert(isinstance(emr_task, LogFriendlyEmrContainerOperator),
        "monitor_logs must be preceded by a LogFriendlyEmrContainerOperator"
    )

    logmon = LogFriendlyEmrMonitorOperator(
        task_id=f'{emr_task.task_id}-monitor',
        alert_payload_generator=alert_payload_generator,
        dag = emr_task.dag,
    )

    alerter_tid = f'{emr_task.task_id}-alert'
    if alert_on_exception_detect:
        alerter = LogFriendlyOpsGenieOperator(
            opsgenie_conn_id='opsgenie_default',
            task_id=alerter_tid,
            trigger_rule="all_done",
            dag = emr_task.dag,
        )

    else:
        # debugging branch
        def _print_all_args(**context):
            curr_ti = context.get('task_instance')
            for k in [
                xcom.XCOM_RETURN_KEY,
                'payload',
            ]:
                print(k, curr_ti.xcom_pull(key=k))

        alerter = PythonOperator(
            task_id=alerter_tid,
            python_callable=_print_all_args,
            provide_context=True,
            trigger_rule="all_done",
            dag = emr_task.dag,
        )

    emr_task >> logmon >> alerter
    return emr_task

