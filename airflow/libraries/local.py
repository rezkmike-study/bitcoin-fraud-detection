import os
import datetime


class Local:
    def __init__(self, local_path):
        self.local_path = local_path

    def check_path(self, create: bool):
        if not (os.path.exists(self.local_path) and os.path.isdir(self.local_path)):
            if create:
                os.makedirs(self.local_path, exist_ok=True)
            else:
                return False

    def list_files(self):
        file_list = [f for f in os.listdir(self.local_path) if os.path.isfile(os.path.join(self.local_path, f))]
        return file_list

    def clean_dir(self, dir=None, exclude=None, days: int = 30):
        if dir is None:
            dir = self.local_path
        removed_files = []
        for path, subdirs, files in os.walk(dir, topdown=True):
            if exclude is not None:
                subdirs[:] = [d for d in subdirs if d not in exclude]
            for file in files:
                f_path = os.path.join(path, file)
                timestamp = os.stat(f_path).st_ctime
                createtime = datetime.datetime.fromtimestamp(timestamp)
                now = datetime.datetime.now()
                delta = now - createtime
                if delta.days > days:
                    removed_files.append(f_path)
                    os.remove(f_path)
        return removed_files
