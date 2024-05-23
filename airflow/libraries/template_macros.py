from dateutil import tz
from datetime import datetime, timedelta

def utc2local(dt, tzname: str ='Asia/Kuala_Lumpur'):
    #return dt.replace(tzinfo=tz.gettz('UTC')).astimezone(tz.gettz(tzname))
    #return dt.in_tz(tzname)
    return dt.replace(tzinfo='UTC').in_tz(tzname)

def shifthour(dt, shift: int):
    return dt + timedelta(hours=shift)

def tid_as_path(tid):
    return tid.replace('-', '=').replace('_', '/')

def encode_path_tid(tid):
    return tid.replace('=', '-').replace('/', '_')

def make_indexed_timestrmap(
    start_timestr: str,
    end_timestr: str,
    output_timefmt: str,
    interval_hours: int = 1,
    input_timefmt: str = '%Y%m%d/%H',
    auto_encode: bool = True,
):
    start_time = datetime.strptime(start_timestr, input_timefmt)
    end_time = datetime.strptime(end_timestr, input_timefmt)
    _map_delta = int(
        (end_time - start_time).total_seconds() / (interval_hours * 60 * 60)
    ) + 1

    if auto_encode:
        output_timefmt = encode_path_tid(output_timefmt)

    _map = {}
    for i in range(_map_delta):
        _map[str(i)] = start_time.strftime(output_timefmt)
        start_time = start_time + timedelta(hours=interval_hours)

    return _map

def get_common_macros():
    # return common macros for user to then edit and pass into DAG
    return {
        'shifthour': shifthour,
        'utc2local': utc2local,
    }

def get_common_filters():
    return {
        'tid_as_path': tid_as_path,
    }
