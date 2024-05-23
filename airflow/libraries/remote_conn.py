import stat
import posixpath
from airflow.providers.ssh.hooks.ssh import SSHHook


class Config:
    def __init__(self, local_path, remote_path):
        self.local_path = local_path
        self.remote_path = remote_path


class SFTP(Config):
    def __init__(self, local_path, remote_path):
        super().__init__(local_path, remote_path)


class SSH(Config):
    def __init__(self, id, local_path, remote_path):  # , remote_host, username, key_file, port):
        super().__init__(local_path, remote_path)
        self.id = id

    def list_remote_files(self, pattern=None, get_symlink=False):
        ssh_hook = SSHHook(ssh_conn_id=self.id)  # remote_host=self.remote_host, username=self.username, key_file=self.key_file, port=self.port)
        # exec_date = kwargs['ds_nodash']
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            if get_symlink:
                r_files = sftp_client.listdir_attr(self.remote_path)
                remote_files = []
                for f in r_files:
                    if stat.S_ISLNK(f.st_mode):
                        remote_files.append(f.filename)
            else:
                files = files = sftp_client.listdir(self.remote_path)
                remote_files = []
                for f in files:
                    if not f.startswith('.'):
                        if pattern is not None:
                            if f.startswith(pattern):
                                remote_files.append(f)
                        else:
                            remote_files.append(f)
            return remote_files

    def get_remote_files(self, files: list, to_path=None, pattern=None):
        ssh_hook = SSHHook(ssh_conn_id=self.id)  # remote_host=self.remote_host, username=self.username, key_file=self.key_file, port=self.port)
        # exec_date = kwargs['ds_nodash']

        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            list_files = []
            for file in files:
                if not file.startswith('.'):
                    if pattern is not None:
                        if file.startswith(pattern):
                            print(f'Downloading to {self.local_path}/{to_path}/{file}')
                            sftp_client.get(f'{self.remote_path}/{file}', f'{self.local_path}/{to_path}/{file}')
                            list_files.append(file)
                    else:
                        sftp_client.get(f'{self.remote_path}/{file}', f'{self.local_path}/{to_path}/{file}')
                        list_files.append(file)
            return list_files
            sftp_client.close()

    def list_symlink_files(self):
        ssh_hook = SSHHook(ssh_conn_id=self.id)

        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            remote_files = sftp_client.listdir_attr(self.remote_path)
            r_files = []
            for f in remote_files:
                if stat.S_ISLNK(f.st_mode):
                    r_files.append(f.filename)
            return r_files


class SSHConn:
    def __init__(self, id, keepalive=None):
        self.id = id
        self.keepalive = keepalive

    def ssh_conn(self):
        ssh_hook = SSHHook(ssh_conn_id=self.id, keepalive_interval=self.keepalive)
        return ssh_hook
        # with ssh_hook.get_conn() as ssh_client:
        #     return ssh_hook, ssh_client.open_sftp()
