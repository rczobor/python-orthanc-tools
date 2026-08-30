import argparse
import hashlib
import logging
import shutil
from orthanc_api_client import OrthancApiClient, exceptions
from typing import List
import zipfile
import tempfile
import os, time, sys
import multiprocessing
import queue
import threading

from orthanc_tools.helpers.environment import get_env_bool

# examples:
# python orthanc_tools/orthanc_folder_importer.py --folder=./tests/stimuli --url=http://192.168.0.10:8042 --user=user --password=pwd --skip=.txt,.ini

# on a Windows system:
# python -m orthanc_tools.orthanc_folder_importer --url=https://pacs.orthanc.team/orthanc/ --api_key=**************** --folder_path=C:\\Orthanc --state_path=C:\\orthanc-migration\\status.txt --errors_path=C:\\orthanc-migration\\errors.txt --max_retries=2

logger = logging.getLogger(__name__)
DEFAULT_ERRORS_LOG_FILENAME = "errors.txt"
ORTHANC_READY_RECHECK_DELAY_SECONDS = 5
ORTHANC_READY_MAX_CHECKS = 12


class _UnsafePdfImport(RuntimeError):
    pass


def resolve_errors_path(errors_path: str = None, error_folder_path: str = None):
    if errors_path:
        return errors_path

    if error_folder_path:
        return os.path.join(error_folder_path, DEFAULT_ERRORS_LOG_FILENAME)

    return None

class OrthancFolderImporter:
    '''
    Upload all the DICOM files contained in a folder (and its sub folders).
    It is a little bit smart:
    - There is a retry for every file and when it fails anyway, the file path is logged, but the script keeps working
    - Regular imports checkpoint processed subfolders. PDF imports checkpoint the complete import unit only after every
    DICOM upload and PDF attachment succeeds.
    - Zip files are unziped before upload
    '''
    def __init__(self,
                 api_client: OrthancApiClient,
                 folder_path: str,
                 errors_path: str,
                 state_path: str,
                 labels_list: List[str] = None,
                 max_retries: int = 8,
                 worker_threads_count: int = multiprocessing.cpu_count() - 1,  # by default, use all CPUs but one for compression
                 skip_extensions: List[str] = None,
                 dicomize_pdf: bool = False
                 ):
        self._api_client = api_client
        self._folder_path = folder_path
        self._labels_list = labels_list
        self._errors_path = errors_path # will contain the list of all the files path not correctly uploaded
        self._state_path = state_path # will contain the list of all the folders correctly uploaded
        self._skip_extensions = [ext.lower() for ext in skip_extensions] if skip_extensions else []

        self._worker_threads_count = worker_threads_count
        self._worker_threads = []
        self._worker_errors = []
        self._messages = queue.Queue(maxsize=2*worker_threads_count)  # this is thread safe https://docs.python.org/3.5/library/queue.html#module-queue

        self._folders_uploaded = []

        if max_retries > 8:
            self._max_retries = 8
        else:
            self._max_retries = max_retries

        self._dicomize_pdf = dicomize_pdf
        self._lock = threading.Lock()
        self._orthanc_lock = threading.Lock()
        self._next_orthanc_reconnect_attempt = 0

    def _wait_until_orthanc_is_ready(self) -> bool:
        """Pause briefly for transient outages and return whether Orthanc recovered."""
        with self._orthanc_lock:
            if self._api_client.is_alive():
                self._next_orthanc_reconnect_attempt = 0
                return True

            if time.monotonic() < self._next_orthanc_reconnect_attempt:
                return False

            logger.warning("Orthanc is unreachable. Pausing all worker threads...")
            for _ in range(ORTHANC_READY_MAX_CHECKS):
                time.sleep(ORTHANC_READY_RECHECK_DELAY_SECONDS)
                if self._api_client.is_alive():
                    self._next_orthanc_reconnect_attempt = 0
                    logger.info("Orthanc is back up! Resuming workers.")
                    return True

            self._next_orthanc_reconnect_attempt = (
                time.monotonic() + ORTHANC_READY_RECHECK_DELAY_SECONDS
            )
            logger.error("Orthanc is still unreachable after waiting; treating this as a failed upload attempt.")
            return False

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def add_file_name_in_errors_log(self, file_path):
        if not self._errors_path:
            logger.warning(f"No errors log path configured, skipping error logging for {file_path}")
            return
        errors_dir = os.path.dirname(self._errors_path)
        if errors_dir:
            os.makedirs(errors_dir, exist_ok=True)
        with open(self._errors_path, "at") as f:
            f.write(file_path + "\n")

    def add_folder_path_in_state_file(self, folder_path):
        if self._state_path:
            with self._lock:
                self._truncate_incomplete_state_record()
                original_size = (
                    os.path.getsize(self._state_path)
                    if os.path.exists(self._state_path)
                    else 0
                )
                try:
                    with open(self._state_path, "at") as f:
                        f.write(str(folder_path) + "\n")
                        f.flush()
                        os.fsync(f.fileno())
                except Exception:
                    if os.path.exists(self._state_path):
                        with open(self._state_path, "r+b") as state_file:
                            state_file.truncate(original_size)
                            state_file.flush()
                            os.fsync(state_file.fileno())
                    raise

    def _truncate_incomplete_state_record(self):
        if not self._state_path or not os.path.isfile(self._state_path):
            return

        with open(self._state_path, "r+b") as state_file:
            state_file.seek(0, os.SEEK_END)
            if state_file.tell() == 0:
                return
            state_file.seek(-1, os.SEEK_END)
            if state_file.read(1) == b"\n":
                return

            logger.warning("Discarding incomplete final importer state record")
            state_file.seek(0)
            state = state_file.read()
            last_newline = state.rfind(b"\n")
            complete_state = state[:last_newline + 1] if last_newline >= 0 else b""
            state_file.seek(0)
            state_file.write(complete_state)
            state_file.truncate()
            state_file.flush()
            os.fsync(state_file.fileno())

    def _file_version(self, path):
        stat = os.stat(path, follow_symlinks=False)
        return (
            stat.st_dev,
            stat.st_ino,
            stat.st_size,
            stat.st_mtime_ns,
            stat.st_ctime_ns,
        )

    def _attach_pdf_idempotently(self, study_id, pdf_path):
        source_version = self._file_version(pdf_path)
        with tempfile.TemporaryDirectory() as temp_dir:
            pdf_snapshot_path = os.path.join(temp_dir, "report.pdf")
            shutil.copyfile(pdf_path, pdf_snapshot_path)
            if self._file_version(pdf_path) != source_version:
                raise _UnsafePdfImport(f"PDF changed while it was being imported: {pdf_path}")

            with open(pdf_snapshot_path, "rb") as pdf_file:
                pdf_digest = hashlib.file_digest(pdf_file, "sha256").digest()

            already_attached = False
            with self._lock:
                existing_pdf_ids = self._api_client.studies.get_pdf_instances(
                    study_id,
                    max_instance_count_in_series_to_analyze=sys.maxsize,
                )
                for instance_id in existing_pdf_ids:
                    existing_pdf_path = os.path.join(temp_dir, f"{instance_id}.pdf")
                    self._api_client.instances.download_pdf(instance_id, existing_pdf_path)
                    with open(existing_pdf_path, "rb") as existing_pdf_file:
                        existing_digest = hashlib.file_digest(existing_pdf_file, "sha256").digest()
                    if existing_digest == pdf_digest:
                        logger.info(f"PDF report already attached to study {study_id}, skipping")
                        already_attached = True
                        break

                if not already_attached:
                    self._api_client.studies.attach_pdf(
                        study_id=study_id,
                        pdf_path=pdf_snapshot_path,
                        series_description="PDF report",
                    )

            if self._file_version(pdf_path) != source_version:
                raise _UnsafePdfImport(f"PDF changed while it was being imported: {pdf_path}")

    def upload_and_label(self, path_to_upload, study_orthanc_id=None):
        """
        Upload the file if path_to_upload is a file path
        Recursively upload the content of the folder is path_to_upload is a folder path
        Then apply the labels on the study
        """

        is_file = os.path.isfile(path_to_upload)
        if is_file and self._is_skipped_file(path_to_upload):
            _, ext = os.path.splitext(path_to_upload)
            logger.info(f"Skipping file with extension {ext}: {path_to_upload}")
            return study_orthanc_id

        if self._dicomize_pdf and os.path.islink(path_to_upload):
            raise _UnsafePdfImport(
                f"PDF import units cannot contain a symbolic link: {path_to_upload}"
            )

        # file path case
        if is_file:
            # zip file case
            if self._is_zip_archive(path_to_upload):
                with tempfile.TemporaryDirectory() as tempDir:
                    with zipfile.ZipFile(path_to_upload, 'r') as z:
                        z.extractall(tempDir)
                    study_id = study_orthanc_id
                    if self._dicomize_pdf:
                        return self.upload_and_label(
                            path_to_upload=tempDir,
                            study_orthanc_id=study_id,
                        )
                    for path in self._list_and_sort_dir(tempDir):
                        full_path = os.path.join(tempDir, path)
                        study_id = self.upload_and_label(path_to_upload=full_path, study_orthanc_id=study_id)
                    return study_id

            is_pdf = self._dicomize_pdf and path_to_upload.lower().endswith(".pdf")
            if is_pdf and study_orthanc_id is None:
                self.add_file_name_in_errors_log(file_path=path_to_upload)
                raise _UnsafePdfImport(f"PDF report has no study in its import unit: {path_to_upload}")

            retry_count = 0
            retry_delays = [5, 20, 60, 300, 900, 1800, 3600, 7200]

            while retry_count <= self._max_retries:
                if retry_count >= 1:
                    delay = retry_delays[retry_count - 1]
                    logger.info(f"waiting {delay} seconds before retrying the upload of {path_to_upload}")
                    time.sleep(delay)
                try:
                    if is_pdf:
                        self._attach_pdf_idempotently(
                            study_id=study_orthanc_id,
                            pdf_path=path_to_upload,
                        )
                        return study_orthanc_id

                    # here, we should have only files (and no zip file)

                    # let's modify/filter the file if needed
                    with open(path_to_upload, 'rb') as f:
                        buffer = f.read()
                        buffer = self.process_dicom_file(buffer)

                    # filtering out case
                    if buffer is None:
                        logger.debug(f"File {path_to_upload} has been filtered out.")
                        return study_orthanc_id

                    # modification case: let's upload the file
                    logger.info(f"uploading {path_to_upload}")
                    instance_orthanc_ids = self._api_client.upload(buffer, ignore_errors=True)

                    if not instance_orthanc_ids:
                        # If we got nothing back, it might be a file error OR Orthanc is actually down
                        # and the ignore_errors=True swallowed a connection error.
                        if not self._api_client.is_alive():
                            if self._wait_until_orthanc_is_ready():
                                continue # retry this same file
                            raise exceptions.ConnectionError(f"Orthanc remained unreachable while uploading {path_to_upload}")

                        logger.error(f"File not uploaded (likely invalid DICOM): {path_to_upload}.")
                        self.add_file_name_in_errors_log(file_path=path_to_upload)
                        if self._dicomize_pdf:
                            raise _UnsafePdfImport(f"File not uploaded: {path_to_upload}")
                        return study_orthanc_id

                    study_id = study_orthanc_id
                    if self._dicomize_pdf or self._labels_list is not None:
                        study_id = self._api_client.instances.get_parent_study_id(instance_orthanc_ids[0])
                    if (
                        self._dicomize_pdf
                        and study_orthanc_id is not None
                        and study_id != study_orthanc_id
                    ):
                        raise _UnsafePdfImport(
                            f"PDF import unit contains multiple studies: {study_orthanc_id}, {study_id}"
                        )

                    # we label for each instance, not at the end of the study, so that there is never an unlabeled image in Orthanc
                    if self._labels_list is not None:
                        self._api_client.studies.add_labels(orthanc_id=study_id, labels=self._labels_list)

                    return study_id

                except _UnsafePdfImport:
                    raise
                except (exceptions.ConnectionError, exceptions.OrthancApiException) as e:
                    # Handle connection issues without consuming retry count
                    if not self._api_client.is_alive():
                        logger.warning(f"Connection error: {str(e)}. Waiting for Orthanc...")
                        if self._wait_until_orthanc_is_ready():
                            continue # Try the same file again

                    # If it's a different Orthanc error (e.g. 400 Bad Request), treat as a normal retry/fail
                    if retry_count == self._max_retries:
                        logger.error(f"Error while uploading this file: {path_to_upload}. Exception: {str(e)}")
                        logger.error(f"too many attempts, logging the file name...")
                        self.add_file_name_in_errors_log(file_path=path_to_upload)
                        if self._dicomize_pdf:
                            raise
                        break
                    else:
                        retry_count += 1
                        logger.warning(f"Error while uploading this file, retrying...: {path_to_upload}. Exception: {str(e)}")
                except Exception as e:
                    if retry_count == self._max_retries:
                        logger.error(f"Error while uploading this file: {path_to_upload}. Exception: {str(e)}")
                        logger.error(f"too many attempts, logging the file name...")
                        self.add_file_name_in_errors_log(file_path=path_to_upload)
                        if self._dicomize_pdf:
                            raise
                        break
                    else:
                        retry_count += 1
                        logger.warning(f"Error while uploading this file, retrying...: {path_to_upload}. Exception: {str(e)}")

            return study_orthanc_id

        # folder case
        elif os.path.isdir(path_to_upload):
            # this folder could have been processed in a previous run of the script
            if not self._dicomize_pdf and path_to_upload in self._folders_uploaded:
                logger.info(f"Folder {path_to_upload} already processed, skipping...")
                return study_orthanc_id

            ## list dir and check if there is folders or files in this path
            ## if files only:
            ##  sort them (pdf at the end)
            ## process them

            study_id = study_orthanc_id
            if self._dicomize_pdf:
                paths_to_import = self._list_pdf_import_files(path_to_upload)
            else:
                paths_to_import = [
                    os.path.join(path_to_upload, path)
                    for path in self._list_and_sort_dir(path_to_upload)
                ]

            for full_path in paths_to_import:
                study_id = self.upload_and_label(path_to_upload=full_path, study_orthanc_id=study_id)

            # let's process this folder
            # for path in os.listdir(path_to_upload):
            #     full_path = os.path.join(path_to_upload, path)
            #     ## manage id (get and repush)
            #     self.upload_and_label(path_to_upload=full_path)

            # let's add this folder path in the processed ones:
            if not self._dicomize_pdf:
                self.add_folder_path_in_state_file(path_to_upload)
            return study_id

        elif self._dicomize_pdf:
            raise _UnsafePdfImport(
                f"PDF import path disappeared before it could be processed: {path_to_upload}"
            )

    def process_dicom_file(self, file_content: bytes) -> bytes:
        '''
        This method is called just before the upload of the file to Orthanc
        By default, nothing is done, but one could want to apply some modifications on the data before upload
        or to filter out some files.
        To do so, this method should be overridden in a derived class.
        If the goal is to filter out the file, 'None' should be returned.

        file_content: content of the DICOM file, as a buffer of bytes
        output: a buffer of bytes (None to filter out the file)
        '''
        return file_content

    def _process_path(self, worker_id):
        logger.debug(f"Starting Processing thread {worker_id}")

        while True:
            path = self._messages.get()  # block until a message is available

            if path is None:  # sent by stop() to stop all worker threads
                self._messages.task_done()
                break

            try:
                if self._dicomize_pdf and str(path) in self._folders_uploaded:
                    logger.info(f"Folder {path} already processed, skipping...")
                else:
                    is_folder_unit = self._dicomize_pdf and os.path.isdir(path)
                    folder_snapshot = (
                        self._snapshot_pdf_import_folder(path)
                        if is_folder_unit
                        else None
                    )
                    is_zip_unit = self._dicomize_pdf and self._is_zip_archive(path)
                    zip_version = self._file_version(path) if is_zip_unit else None
                    self.upload_and_label(path_to_upload=path)
                    if self._dicomize_pdf:
                        if is_folder_unit:
                            if (
                                not os.path.isdir(path)
                                or self._snapshot_pdf_import_folder(path) != folder_snapshot
                            ):
                                raise _UnsafePdfImport(
                                    f"PDF import folder changed while it was being imported: {path}"
                                )
                            self.add_folder_path_in_state_file(path)
                        elif is_zip_unit:
                            if (
                                not self._is_zip_archive(path)
                                or self._file_version(path) != zip_version
                            ):
                                raise _UnsafePdfImport(
                                    f"ZIP import unit changed while it was being imported: {path}"
                                )
                            self.add_folder_path_in_state_file(path)
            except Exception as error:
                logger.error(f"Importer worker failed while processing {path}: {error}", exc_info=True)
                with self._lock:
                    self._worker_errors.append((path, error))
            finally:
                self._messages.task_done()  # tell the queue the item has been processed

        logger.debug("Processing thread stopped")

    def _list_and_sort_dir(self, folder_path):
        path_entries = sorted(
            self._list_input_entries(folder_path),
            key=lambda name: (
                2 if name.lower().endswith(".pdf")
                else 1 if name.lower().endswith(".dcm")
                else 0,
                name.lower()
            )
        )


        return path_entries

    def _is_importer_file(self, path):
        normalized_path = os.path.normcase(os.path.realpath(path))
        return any(
            configured_path
            and normalized_path == os.path.normcase(os.path.realpath(configured_path))
            for configured_path in (self._state_path, self._errors_path)
        )

    def _is_skipped_file(self, path):
        return os.path.splitext(path)[1].lower() in self._skip_extensions

    def _is_zip_archive(self, path):
        return os.fspath(path).lower().endswith("zip") and zipfile.is_zipfile(path)

    def _list_input_entries(self, folder_path):
        return [
            path
            for path in os.listdir(path=folder_path)
            if not self._is_importer_file(os.path.join(folder_path, path))
        ]

    def _list_pdf_import_files(self, folder_path):
        def raise_walk_error(error):
            raise error

        paths = []
        for current_path, directory_names, file_names in os.walk(
            folder_path,
            onerror=raise_walk_error,
        ):
            directory_names.sort(key=str.lower)
            for directory_name in directory_names:
                full_path = os.path.join(current_path, directory_name)
                if os.path.islink(full_path):
                    raise _UnsafePdfImport(
                        f"PDF import units cannot contain a symbolic link: {full_path}"
                    )
            for file_name in sorted(file_names, key=str.lower):
                full_path = os.path.join(current_path, file_name)
                if self._is_importer_file(full_path):
                    continue
                if self._is_skipped_file(full_path):
                    continue
                if os.path.islink(full_path):
                    raise _UnsafePdfImport(
                        f"PDF import units cannot contain a symbolic link: {full_path}"
                    )
                if self._is_zip_archive(full_path):
                    raise _UnsafePdfImport(
                        f"Nested ZIP archives are not supported in a PDF import folder: {full_path}"
                    )
                paths.append(full_path)

        pdf_paths = [
            path for path in paths
            if path.lower().endswith(".pdf") and ".pdf" not in self._skip_extensions
        ]
        if len(pdf_paths) > 1:
            raise _UnsafePdfImport(
                f"PDF import unit must contain at most one PDF report; found {len(pdf_paths)}"
            )

        return sorted(
            paths,
            key=lambda path: (path.lower().endswith(".pdf"), path.lower())
        )

    def _snapshot_pdf_import_folder(self, folder_path):
        return tuple(
            (path, self._file_version(path))
            for path in self._list_pdf_import_files(folder_path)
        )

    def _has_direct_non_archive_files(self, folder_path):
        return any(
            os.path.isfile(os.path.join(folder_path, path))
            and not self._is_skipped_file(path)
            and not self._is_zip_archive(os.path.join(folder_path, path))
            for path in self._list_input_entries(folder_path)
        )


    def execute(self):
        # read state
        if self._state_path and os.path.isfile(self._state_path):
            self._truncate_incomplete_state_record()
            with open(self._state_path, 'r') as file:
                lines = file.readlines()
                self._folders_uploaded = [line.strip() for line in lines]

        # create worker threads
        for thread_id in range(0, self._worker_threads_count):
            self._worker_threads.append(threading.Thread(
                target=self._process_path,
                name=f"Worker Thread {thread_id}",
                args=(thread_id, )
            ))

        # start threads
        for wt in self._worker_threads:
            wt.start()

        # let's browse the main folder to feed the message queue

        # Direct non-archive files make the root one unit. Otherwise each child is
        # an independent unit that can be processed and resumed separately.
        if self._dicomize_pdf and self._has_direct_non_archive_files(self._folder_path):
            self._messages.put(self._folder_path)
        else:
            for path in sorted(self._list_input_entries(self._folder_path), key=str.lower):
                full_path = os.path.join(self._folder_path, path)
                self._messages.put(full_path) # if the queue is full, this will block until there's a free slot

        # let's wait for the completion of all threads
        self.stop()

        if self._worker_errors:
            path, error = self._worker_errors[0]
            raise RuntimeError(f"Importer worker failed while processing {path}") from error

        logger.info("End of upload!")

    def stop(self):
        logger.info("Waiting for Orthanc Folder Importer to complete each upload...")

        # post one 'empty' exit message per thread to unlock the threads from waiting on the process queue
        for i in range(0, self._worker_threads_count):
            self._messages.put(None)

        for t in self._worker_threads:
            t.join()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    parser = argparse.ArgumentParser(description='Import the content of a folder in Orthanc')
    parser.add_argument('--url', type=str, default='http://localhost:8042', help='Orthanc url')
    parser.add_argument('--user', type=str, default=None, help='Orthanc user name')
    parser.add_argument('--password', type=str, default=None, help='Orthanc password')
    parser.add_argument('--api_key', type=str, default=None, help='Orthanc api-key')
    parser.add_argument('--folder_path', type=str, help='Folder to import, the one containing the DICOM files.')
    parser.add_argument('--labels_list', type=str, default=None, help='List of labels to apply to the uploaded studies, separated by a comma.')
    parser.add_argument('--errors_path', type=str, help='Path of the file which will contain the list of problematic files (not uploaded).')
    parser.add_argument('--state_path', type=str, help='Path of the file which will contain the list of all the folder correctly uploaded.')
    parser.add_argument('--max_retries', type=int, default=8, help='Maximum number of attempts for a file upload.')
    parser.add_argument('--worker_threads_count', type=int, default=1, help='Worker threads count')
    parser.add_argument('--skip_extensions', type=str, default=None, help='List of extensions to skip, separated by a comma.')
    parser.add_argument('--dicomize_pdf', default=False, action='store_true', help='If true, pdf files found will be dicomized and uploaded.')

    args = parser.parse_args()

    url = os.environ.get("ORTHANC_URL", args.url)
    user = os.environ.get("ORTHANC_USER", args.user)
    password = os.environ.get("ORTHANC_PWD", args.password)
    api_key = os.environ.get("ORTHANC_API_KEY", args.api_key)
    folder_path = os.environ.get("FOLDER_PATH", args.folder_path)
    labels_list = os.environ.get("LABELS_LIST", args.labels_list)
    errors_path = resolve_errors_path(
        errors_path=os.environ.get("ERRORS_PATH", args.errors_path),
        error_folder_path=os.environ.get("ERROR_FOLDER_PATH")
    )
    state_path = os.environ.get("STATE_PATH", os.environ.get("PERSIST_STATE_PATH", args.state_path))
    max_retries = int(os.environ.get("MAX_RETRIES", str(args.max_retries)))
    worker_threads_count = int(os.environ.get("WORKER_THREADS_COUNT", str(args.worker_threads_count)))
    skip_extensions = os.environ.get("SKIP_EXTENSIONS", args.skip_extensions)

    if skip_extensions:
        skip_extensions = [ext.strip() for ext in skip_extensions.split(",") if ext.strip()]
    else:
        skip_extensions = []

    dicomize_pdf = get_env_bool("DICOMIZE_PDF", args.dicomize_pdf)

    o = None
    if api_key is not None:
        o=OrthancApiClient(url, headers={"api-key": api_key}, pool_maxsize=max(10, worker_threads_count), pool_block=True)
    else:
        o=OrthancApiClient(url, user=user, pwd=password, pool_maxsize=max(10, worker_threads_count), pool_block=True)

    importer = OrthancFolderImporter(
        api_client=o,
        folder_path=folder_path,
        labels_list=labels_list,
        errors_path=errors_path,
        state_path=state_path,
        max_retries=max_retries,
        worker_threads_count=worker_threads_count,
        skip_extensions=skip_extensions,
        dicomize_pdf=dicomize_pdf
    )

    importer.execute()
