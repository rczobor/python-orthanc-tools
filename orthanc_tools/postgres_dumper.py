import sys
import datetime
import paramiko
import time, os
import argparse
import logging
import schedule
import subprocess
import tempfile
import uuid

from orthanc_tools.helpers.logging_config import configure_logging

logger = logging.getLogger(__name__)

class PostgresDumper:
    """
    Runs every day to create a gzip compressed dump of the postgres DB and write it to the destination (currently, only sftp)

    Warning:
    `postgresql-client` has to be installed before the execution of this script
    The SFTP host key must be present in the user's known_hosts or in the file
    supplied as sftp_known_hosts.  The server must support the OpenSSH
    posix-rename extension to atomically replace a previous backup.

    To restore:
    pg_restore --clean -U postgres -h 172.21.0.3 -p 5432 -d postgres Friday
    """
    def __init__(self, pg_host: str, pg_port: str, pg_db_name: str, pg_user_name: str, pg_password,
                 execution_time: str,
                 sftp_host: str, sftp_port: int, sftp_user_name: str, sftp_password: str, sftp_folder_path: str,
                 sftp_known_hosts: str = None
                 ):

        if not sftp_folder_path:
            raise ValueError("Please provide an SFTP destination folder")
        self.sftp_folder_path = sftp_folder_path.rstrip('/')
        self.sftp_known_hosts = sftp_known_hosts
        self.sftp_password = sftp_password
        self.sftp_user_name = sftp_user_name
        self.sftp_port = sftp_port
        self.sftp_host = sftp_host
        self.execution_time = execution_time
        self.pg_password = pg_password
        self.pg_user_name = pg_user_name
        self.pg_db_name = pg_db_name
        self.pg_host = pg_host
        self.pg_port = pg_port

    def stream_pg_dump_to_sftp(self):
        try:
            with paramiko.SSHClient() as ssh:
                ssh.load_system_host_keys()
                if self.sftp_known_hosts is not None:
                    ssh.load_host_keys(self.sftp_known_hosts)
                ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
                ssh.connect(
                    hostname=self.sftp_host, port=int(self.sftp_port),
                    username=self.sftp_user_name, password=self.sftp_password,
                    timeout=30, banner_timeout=30, auth_timeout=30
                )
                with ssh.open_sftp() as sftp:
                    sftp.get_channel().settimeout(60)
                    # Keep the existing daily backup intact until both subprocesses
                    # and the upload succeed.  An unsupported atomic rename fails safely.
                    destination = f"{self.sftp_folder_path}/{datetime.date.today().strftime('%A')}.gz"
                    temporary_path = f"{destination}.{uuid.uuid4().hex}.tmp"
                    try:
                        self._upload_dump(sftp, temporary_path)
                        sftp.posix_rename(temporary_path, destination)
                        temporary_path = None
                    finally:
                        if temporary_path is not None:
                            try:
                                sftp.remove(temporary_path)
                            except OSError as error:
                                logger.warning("Could not remove temporary backup %s: %s", temporary_path, error)

            logger.info("Backup successfully uploaded to %s", destination)
        except Exception as error:
            logger.error("Postgres backup failed: %s", error)
            raise

    def _upload_dump(self, sftp, temporary_path):
        dump_process = None
        gzip_process = None
        # Files drain stderr without blocking a child on a full pipe while we
        # stream stdout.  Their size does not increase Python's memory usage.
        with tempfile.TemporaryFile() as dump_stderr, tempfile.TemporaryFile() as gzip_stderr:
            try:
                with sftp.open(temporary_path, "wx") as remote_file:
                    dump_process = subprocess.Popen(
                        ["pg_dump", "-U", self.pg_user_name, "-h", self.pg_host,
                         "-p", str(self.pg_port), "-Fc", self.pg_db_name],
                        stdout=subprocess.PIPE, stderr=dump_stderr,
                        env={**os.environ, "PGPASSWORD": self.pg_password, "PGCONNECT_TIMEOUT": "30"}
                    )
                    gzip_process = subprocess.Popen(
                        ["gzip"], stdin=dump_process.stdout,
                        stdout=subprocess.PIPE, stderr=gzip_stderr
                    )
                    # Only gzip keeps the pipe open, so its failure also releases
                    # pg_dump instead of leaving it blocked writing to this process.
                    dump_process.stdout.close()
                    for chunk in iter(lambda: gzip_process.stdout.read(4096), b""):
                        remote_file.write(chunk)

                    gzip_returncode = gzip_process.wait(timeout=60)
                    dump_returncode = dump_process.wait(timeout=60)
                    for command, returncode, stderr in (
                        ("pg_dump", dump_returncode, dump_stderr),
                        ("gzip", gzip_returncode, gzip_stderr)
                    ):
                        if returncode != 0:
                            stderr.seek(0)
                            error_message = stderr.read(65536).decode(errors="replace").strip()
                            raise RuntimeError(f"{command} failed (exit status {returncode}): {error_message}")
            finally:
                for process in (gzip_process, dump_process):
                    if process is not None:
                        if process.stdout is not None:
                            process.stdout.close()
                        if process.poll() is None:
                            process.terminate()
                            try:
                                process.wait(timeout=10)
                            except subprocess.TimeoutExpired:
                                process.kill()
                                process.wait()


    def execute(self):
        logger.info("----- Initializing Postgres Dumper...")

        # Check if postgresql-client is installed
        try:
            result = subprocess.run(["pg_dump", "--version"], capture_output=True, text=True, check=True)
            logger.info(f"pg_dump version: {result.stdout.strip()}")
        except FileNotFoundError:
            logger.error("it seems that pg_dump is NOT installed, plase install it before running this script (apt install postgresql-client)")
            sys.exit(-1)

        if self.execution_time is None:
            # unit test case
            self.stream_pg_dump_to_sftp()
        else:
            # regular (prod) case
            schedule.every().day.at(self.execution_time).do(self.stream_pg_dump_to_sftp)
            while True:
                schedule.run_pending()
                time.sleep(1)


if __name__ == '__main__':
    level = logging.INFO
    if os.environ.get('VERBOSE_ENABLED'):
        level = logging.DEBUG
    configure_logging(level=level)

    parser = argparse.ArgumentParser(description='Periodically dumps Postgres DB to an SFTP server.')
    parser.add_argument('--pg_host', type=str, default='orthanc-db', help='Postgres hostname')
    parser.add_argument('--pg_port', type=str, default='5432', help='Postgres port number')
    parser.add_argument('--pg_db_name', type=str, default='postgres', help='Postgres database name')
    parser.add_argument('--pg_user_name', type=str, default='postgres', help='Postgres username')
    parser.add_argument('--pg_password', type=str, default='', help='Postgres password')
    parser.add_argument('--execution_time', type=str, default='01:30', help='Time for script execution (format: 23:30 or 23:30:14)')
    parser.add_argument('--sftp_host', type=str, default=None, help='sFTP server hostname')
    parser.add_argument('--sftp_port', type=str, default='22', help='sFTP server port number')
    parser.add_argument('--sftp_user_name', type=str, default=None, help='sFTP server user name')
    parser.add_argument('--sftp_password', type=str, default=None, help='sFTP server password')
    parser.add_argument('--sftp_folder_path', type=str, default=None, help='sFTP server folder path')
    parser.add_argument('--sftp_known_hosts', type=str, default=None, help='Known-hosts file containing the verified SFTP server key')
    args = parser.parse_args()

    pg_host = os.environ.get("PG_HOST", args.pg_host)
    pg_port = os.environ.get("PG_PORT", args.pg_port)
    pg_db_name = os.environ.get("PG_DB_NAME", args.pg_db_name)
    pg_user_name = os.environ.get("PG_USER_NAME", args.pg_user_name)
    pg_password = os.environ.get("PG_PASSWORD", args.pg_password)
    execution_time = os.environ.get("EXECUTION_TIME", args.execution_time)
    if execution_time == '':
        execution_time = None
    sftp_host = os.environ.get("SFTP_HOST", args.sftp_host)
    sftp_port = int(os.environ.get("SFTP_PORT", args.sftp_port))
    sftp_user_name = os.environ.get("SFTP_USER_NAME", args.sftp_user_name)
    sftp_password = os.environ.get("SFTP_PASSWORD", args.sftp_password)
    sftp_folder_path = os.environ.get("SFTP_FOLDER_PATH", args.sftp_folder_path)
    sftp_known_hosts = os.environ.get("SFTP_KNOWN_HOSTS", args.sftp_known_hosts)

    dumper = PostgresDumper(pg_host, pg_port, pg_db_name, pg_user_name, pg_password, execution_time,
                            sftp_host, sftp_port, sftp_user_name, sftp_password, sftp_folder_path,
                            sftp_known_hosts=sftp_known_hosts)

    dumper.execute()
