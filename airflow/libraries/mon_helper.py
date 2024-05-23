# TODO: logscan stuff here

import re
from airflow.providers.opsgenie.hooks.opsgenie import OpsgenieAlertHook
from airflow.models import BaseOperator, xcom
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException, AirflowSkipException

class LogFriendlyOpsGenieOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        *,
        opsgenie_conn_id: str = 'opsgenie_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.opsgenie_conn_id = opsgenie_conn_id

    def execute(self, context) -> None:
        ti = context.get('task_instance')
        raise_alert = ti.xcom_pull()
        payload = ti.xcom_pull(key="payload")
        self.log.info(f'raise_alert: {raise_alert}')
        self.log.info(f'payload: {payload}')

        if raise_alert:
            self.hook = OpsgenieAlertHook(self.opsgenie_conn_id)
            assert(isinstance(payload, dict), "No suitable payload for OpsGenieHook execution")
            self.hook.create_alert(payload)
        else:
            raise AirflowSkipException("raise_alert None/False, skipping")

def default_payload_generator(exception_name, exception_msg):
    # TODO: do a KT on how to utilize a logging standard payload_generator
    _maxlen = 300
    if len(exception_msg) > _maxlen:
        exception_msg = exception_msg[:_maxlen] + '... truncated ...'

    payload = {
        'alias': exception_name,
        'message': exception_msg,
        'priority': 'P3',
        'tags': ['airflow.data']
    }

    return payload

def scan_for_exceptions(context, buf, payload_generator = None):
    ti = context.get('task_instance')

    if payload_generator is None:
        payload_generator = default_payload_generator

    st_start_pattern = re.compile(r'Traceback \(most recent call last\):')
    st_final_pattern = re.compile(r'(\w+):(.*)')
    st_extra_pattern = re.compile(r'[^\t ].*')

    ignlist = [
        re.compile(r'.*?py4j\.clientserver.*?Closing down clientserver connection')
    ]

    pattern = st_start_pattern
    name = None
    message = None
    for line in buf:
        mt = pattern.match(line)
        if mt is not None:
            if pattern == st_start_pattern:
                pattern = st_final_pattern
            elif pattern == st_final_pattern:
                # we found the exception
                #full = mt.group(0)
                name = mt.group(1).strip()
                message = mt.group(2).strip()
                pattern = st_extra_pattern

            else:
                _add_to_msg = True
                for ign in ignlist:
                    if ign.match(line):
                        _add_to_msg = False

                if _add_to_msg:
                    message += ' ' + mt.group(0).strip()

    if isinstance(message, str) and isinstance(name, str):
        payload = payload_generator(name, message.strip())
        ti.xcom_push('payload', payload)
        return True

    return False
