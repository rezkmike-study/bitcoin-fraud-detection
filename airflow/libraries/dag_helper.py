#!/usr/bin/python

from airflow.operators.empty import EmptyOperator
from collections import namedtuple

SubDag = namedtuple('SubDag', ['entering', 'leaving'])

def create_and_run_tasks_using_timemap(
    timemap: dict,
    subdag_generator,
    serial_run: bool = True,
):
    assert(callable(subdag_generator), "subdag generator arg error")
    assert(serial_run, "parallel run not implemented yet")
    # jason.chia: creates a task list to have dynamic dags with varying tasks
    # this is useful for backfilling
    # it uses dependency injection method to create the operators
    # refer to dag_duitnow_od_bspalias.py in dag/duitnow on usage examples
    subdags = []
    for k, v in timemap.items():
        subdag = subdag_generator(v)
        assert(isinstance(subdag, tuple) and len(subdag) > 1, "must returned an entering and leaving task from subdag")
        entering, leaving = subdag
        #_t = grouped_tasks_generator(v)
        #_t.task_id = v # forcefully set task_id as the mapped value

        subdags.append(SubDag(entering, leaving))

    if serial_run:
        # run the tasks by chain
        for i in range(len(timemap) - 1):
            subdags[i].leaving >> subdags[i + 1].entering
    else:
        # TODO: fix this
        for i in range(len(timemap)):
            pass

    return subdags
