import datetime
import gzip
import io
import os
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch

import paramiko

from orthanc_tools.postgres_dumper import PostgresDumper


class RemoteFile(io.BytesIO):
    def __init__(self, sftp, path):
        super().__init__()
        self.sftp = sftp
        self.path = path

    def write(self, data):
        if self.sftp.write_error is not None:
            raise self.sftp.write_error
        return super().write(data)

    def close(self):
        if not self.closed:
            self.sftp.files[self.path] = self.getvalue()
        super().close()


class InMemorySftp:
    def __init__(self):
        self.files = {}
        self.write_error = None
        self.rename_error = None
        self.closed = False
        self.channel = Mock()
        self.opened = []
        self.removed = []
        self.renamed = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def get_channel(self):
        return self.channel

    def open(self, path, mode):
        assert mode == "wx"
        assert path not in self.files
        self.files[path] = b""
        remote_file = RemoteFile(self, path)
        self.opened.append(remote_file)
        return remote_file

    def posix_rename(self, source, destination):
        assert all(remote.closed for remote in self.opened)
        self.renamed.append((source, destination))
        if self.rename_error is not None:
            raise self.rename_error
        self.files[destination] = self.files.pop(source)

    def remove(self, path):
        self.removed.append(path)
        del self.files[path]


class TestPostgresDumper(unittest.TestCase):
    def setUp(self):
        self.destination = "/backups/" + datetime.date.today().strftime("%A") + ".gz"
        self.sftp = InMemorySftp()
        self.sftp.files[self.destination] = b"existing good backup"
        self.ssh = Mock()
        self.ssh.__enter__ = Mock(return_value=self.ssh)
        self.ssh.__exit__ = Mock(return_value=False)
        self.ssh.open_sftp.return_value = self.sftp
        self.dumper = PostgresDumper(
            "postgres", "5432", "orthanc", "postgres", "test-password", None,
            "backup-host", 22, "backup-user", "test-password", "/backups/",
            sftp_known_hosts="/verified/known_hosts"
        )
        ssh_patch = patch("orthanc_tools.postgres_dumper.paramiko.SSHClient", return_value=self.ssh)
        ssh_patch.start()
        self.addCleanup(ssh_patch.stop)
        self.actual_popen = subprocess.Popen
        self.children = []
        self.invocations = []
        self.addCleanup(self.cleanup_children)

    def cleanup_children(self):
        for process in self.children:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)

    def launch(self, dump_script, gzip_script=None):
        def popen(command, **kwargs):
            self.invocations.append((command, kwargs))
            if command[0] == "pg_dump":
                command = [sys.executable, "-c", dump_script]
            elif gzip_script is not None:
                command = [sys.executable, "-c", gzip_script]
            process = self.actual_popen(command, **kwargs)
            self.children.append(process)
            return process

        return patch("orthanc_tools.postgres_dumper.subprocess.Popen", side_effect=popen)

    def assert_failed_backup_is_preserved(self, logs):
        self.assertEqual({self.destination: b"existing good backup"}, self.sftp.files)
        self.assertNotIn(self.destination, self.sftp.removed)
        self.assertTrue(self.sftp.closed)
        self.assertNotIn("Backup successfully uploaded", "\n".join(logs.output))

    def test_successful_dump_is_published_after_subprocesses_and_upload_finish(self):
        with self.assertLogs("orthanc_tools.postgres_dumper", level="INFO") as logs:
            with self.launch("import sys; sys.stdout.buffer.write(b'database dump')"):
                self.dumper.stream_pg_dump_to_sftp()

        self.assertEqual(b"database dump", gzip.decompress(self.sftp.files[self.destination]))
        self.assertEqual(1, len(self.sftp.files))
        self.assertEqual(1, len(self.sftp.renamed))
        self.assertFalse(self.sftp.removed)
        self.assertEqual(2, len(self.children))
        self.assertTrue(all(process.returncode == 0 and process.stdout.closed for process in self.children))
        self.assertEqual(os.environ["PATH"], self.invocations[0][1]["env"]["PATH"])
        self.assertEqual("test-password", self.invocations[0][1]["env"]["PGPASSWORD"])
        self.ssh.load_system_host_keys.assert_called_once_with()
        self.ssh.load_host_keys.assert_called_once_with("/verified/known_hosts")
        self.assertIsInstance(self.ssh.set_missing_host_key_policy.call_args.args[0], paramiko.RejectPolicy)
        self.ssh.__exit__.assert_called_once()
        self.assertIn("Backup successfully uploaded", "\n".join(logs.output))

    def test_pg_dump_failure_and_large_stderr_preserve_existing_backup(self):
        script = "import sys; sys.stdout.buffer.write(b'partial dump'); sys.stderr.write('dump failed\\n' * 20000); sys.exit(1)"
        with self.assertLogs("orthanc_tools.postgres_dumper", level="INFO") as logs:
            with self.launch(script), self.assertRaisesRegex(RuntimeError, "pg_dump failed.*exit status 1"):
                self.dumper.stream_pg_dump_to_sftp()

        self.assert_failed_backup_is_preserved(logs)
        self.assertFalse(self.sftp.renamed)
        self.assertEqual(2, len(self.children))
        self.assertTrue(all(process.poll() is not None for process in self.children))
        self.ssh.__exit__.assert_called_once()

    def test_gzip_failure_preserves_existing_backup(self):
        with self.assertLogs("orthanc_tools.postgres_dumper", level="INFO") as logs:
            with self.launch(
                "import sys; sys.stdout.buffer.write(b'dump')",
                "import sys; sys.stdin.buffer.read(); sys.stdout.buffer.write(b'bad compressed data'); sys.stderr.write('gzip failed'); sys.exit(2)"
            ), self.assertRaisesRegex(RuntimeError, "gzip failed.*exit status 2"):
                self.dumper.stream_pg_dump_to_sftp()

        self.assert_failed_backup_is_preserved(logs)
        self.assertFalse(self.sftp.renamed)
        self.assertEqual(2, len(self.children))
        self.assertTrue(all(process.poll() is not None for process in self.children))

    def test_upload_failure_stops_children_and_preserves_existing_backup(self):
        self.sftp.write_error = OSError("connection lost")
        script = "import os\nwhile True:\n os.write(1, os.urandom(65536))"
        with self.assertLogs("orthanc_tools.postgres_dumper", level="INFO") as logs:
            with self.launch(script), self.assertRaisesRegex(OSError, "connection lost"):
                self.dumper.stream_pg_dump_to_sftp()

        self.assert_failed_backup_is_preserved(logs)
        self.assertEqual(2, len(self.children))
        self.assertTrue(all(process.poll() is not None and process.stdout.closed for process in self.children))

    def test_unsupported_atomic_rename_preserves_existing_backup(self):
        self.sftp.rename_error = OSError("Operation unsupported")
        with self.assertLogs("orthanc_tools.postgres_dumper", level="INFO") as logs:
            with self.launch("import sys; sys.stdout.buffer.write(b'dump')"), self.assertRaisesRegex(OSError, "unsupported"):
                self.dumper.stream_pg_dump_to_sftp()

        self.assert_failed_backup_is_preserved(logs)
        self.assertEqual(1, len(self.sftp.renamed))
        self.assertEqual(1, len(self.sftp.removed))

    def test_host_key_rejection_closes_ssh_without_opening_backup(self):
        self.ssh.connect.side_effect = paramiko.SSHException("unknown host key")
        with self.assertLogs("orthanc_tools.postgres_dumper", level="INFO") as logs:
            with patch("orthanc_tools.postgres_dumper.subprocess.Popen") as popen:
                with self.assertRaisesRegex(paramiko.SSHException, "unknown host key"):
                    self.dumper.stream_pg_dump_to_sftp()

        self.assertEqual({self.destination: b"existing good backup"}, self.sftp.files)
        self.assertFalse(self.sftp.opened)
        self.ssh.open_sftp.assert_not_called()
        self.ssh.__exit__.assert_called_once()
        popen.assert_not_called()
        self.assertNotIn("Backup successfully uploaded", "\n".join(logs.output))

    def test_missing_gzip_stops_started_dump(self):
        def popen(command, **kwargs):
            if command[0] == "gzip":
                raise FileNotFoundError("gzip missing")
            process = self.actual_popen([sys.executable, "-c", "import time; time.sleep(120)"], **kwargs)
            self.children.append(process)
            return process

        with self.assertLogs("orthanc_tools.postgres_dumper", level="INFO") as logs:
            with patch("orthanc_tools.postgres_dumper.subprocess.Popen", side_effect=popen):
                with self.assertRaisesRegex(FileNotFoundError, "gzip missing"):
                    self.dumper.stream_pg_dump_to_sftp()

        self.assert_failed_backup_is_preserved(logs)
        self.assertEqual(1, len(self.children))
        self.assertIsNotNone(self.children[0].poll())
