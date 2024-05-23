import yaml
from datetime import timedelta, datetime
from dateutil import parser, tz
from typing import List, Dict, Any

class Config:
    def __init__(self, exec_date):
        self.exec_date = exec_date

    def get_date(self, previous=None, next=None, delta=None, timezone="UTC"):
        d = parser.isoparse(self.exec_date).astimezone(tz.gettz(timezone))  # .split(' ')[0], "%Y-%m-%d").replace(tzinfo=timezone("Asia/Kuala_Lumpur")).date()
        if delta is not None:
            if previous:
                d = d - timedelta(days=delta)
            if next:
                d = d + timedelta(days=next)
        return d


    def get_exec_date(self, previous=None, next=None, delta=None, timezone="UTC"):
        d = parser.isoparse(self.exec_date).astimezone(tz.gettz(timezone))  # .split(' ')[0], "%Y-%m-%d").replace(tzinfo=timezone("Asia/Kuala_Lumpur")).date()
        if delta is not None:
            if previous:
                d = d - timedelta(days=previous)
            if next:
                d = d + timedelta(days=next)
        m_str = d.strftime("%b").lower()
        m_int = d.strftime("%m")
        day = d.strftime("%d")
        current = d.strftime("%Y-%m-%d")
        minute = d.strftime("%M")
        hour = d.strftime("%H")
        return current, m_str, m_int, day, hour, minute

    def get_grouped_minutes(self):
        minutes = datetime.strptime(self.exec_date, "%Y-%m-%dT%H:%M:%S.%f%z").strftime("%M")
        minute_range = int(int(minutes) / 15)
        return f"{minute_range:02d}"
    

def read_config(
        yam_file: str
) ->  List:
    
    with open(yam_file, 'r') as yamlfile:
        configs = yaml.load(yamlfile, Loader=yaml.FullLoader)

    return configs

def parse_config(
    configs: List, cfg: str, cfg_type: str, cfg_id: str=""
    ) -> Dict[Any, Any]:

    for config in configs:
        if config:
            if config[cfg][cfg_type]:
                map_config = config[cfg][cfg_type]

    if cfg_id:
        return map_config[cfg_id]
    else:
        return map_config