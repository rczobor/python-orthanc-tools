import argparse
import logging
from orthanc_api_client import OrthancApiClient, exceptions
from typing import List
import zipfile
import tempfile
import os, time, sys
import multiprocessing
import queue
import threading
from pathlib import Path

from orthanc_tools.helpers.environment import get_env_bool

# examples:
# python orthanc_tools/orthanc_folder_importer.py --folder=./tests/stimuli --url=http://192.168.0.10:8042 --user=user --password=pwd --skip=.txt,.ini

# on a Windows system:
# python -m orthanc_tools.orthanc_folder_importer --url=https://pacs.orthanc.team/orthanc/ --api_key=**************** --folder_path=C:\\Orthanc --state_path=C:\\orthanc-migration\\status.txt --errors_path=C:\\orthanc-migration\\errors.txt --max_retries=2

logger = logging.getLogger(__name__)
DEFAULT_ERRORS_LOG_FILENAME = "errors.txt"
ORTHANC_READY_RECHECK_DELAY_SECONDS = 5
ORTHANC_READY_MAX_CHECKS = 12


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
    - Every sub folder uploaded (even with errors for some files) is logged, so that if the script is interrupted and
    restarted, it will restart from the last succeeded folder.
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
        self._folder_path = os.fspath(folder_path)
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
                with open(self._state_path, "at") as f:
                    f.write(str(folder_path) + "\n")

    def upload_and_label(self, path_to_upload, study_orthanc_id=None):
        """
        Upload the file if path_to_upload is a file path
        Recursively upload the content of the folder is path_to_upload is a folder path
        Then apply the labels on the study
        """

        path_to_upload = os.fspath(path_to_upload)

        # file path case
        if os.path.isfile(path_to_upload):
            # check skip extensions
            _, ext = os.path.splitext(path_to_upload)
            if ext.lower() in self._skip_extensions:
                logger.info(f"Skipping file with extension {ext}: {path_to_upload}")
                return study_orthanc_id

            # zip file case
            if path_to_upload.lower().endswith("zip") and zipfile.is_zipfile(path_to_upload):
                with tempfile.TemporaryDirectory() as tempDir:
                    with zipfile.ZipFile(path_to_upload, 'r') as z:
                        z.extractall(tempDir)
                    return self.upload_and_label(
                        path_to_upload=tempDir,
                        study_orthanc_id=study_orthanc_id,
                    )

            else:
                is_pdf = self._dicomize_pdf and path_to_upload.lower().endswith(".pdf")
                retry_count = 0
                retry_delays = [5, 20, 60, 300, 900, 1800, 3600, 7200]

                while retry_count <= self._max_retries:
                    if retry_count >= 1:
                        delay = retry_delays[retry_count - 1]
                        logger.info(f"waiting {delay} seconds before retrying the upload of {path_to_upload}")
                        time.sleep(delay)
                    try:
                        if is_pdf:
                            if study_orthanc_id is None:
                                logger.error(f"No study available for PDF report: {path_to_upload}")
                                self.add_file_name_in_errors_log(file_path=path_to_upload)
                                return None
                            self._api_client.studies.attach_pdf(
                                study_id=study_orthanc_id,
                                pdf_path=path_to_upload,
                                series_description="PDF report",
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
                            return None

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
                            return None

                        study_id = study_orthanc_id
                        if self._dicomize_pdf or self._labels_list is not None:
                            study_id = self._api_client.instances.get_parent_study_id(instance_orthanc_ids[0])

                        # we label for each instance, not at the end of the study, so that there is never an unlabeled image in Orthanc
                        if self._labels_list is not None:
                            self._api_client.studies.add_labels(orthanc_id=study_id, labels=self._labels_list)

                        return study_id

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
                            break
                        else:
                            retry_count += 1
                            logger.warning(f"Error while uploading this file, retrying...: {path_to_upload}. Exception: {str(e)}")
                    except Exception as e:
                        if retry_count == self._max_retries:
                            logger.error(f"Error while uploading this file: {path_to_upload}. Exception: {str(e)}")
                            logger.error(f"too many attempts, logging the file name...")
                            self.add_file_name_in_errors_log(file_path=path_to_upload)
                            break
                        else:
                            retry_count += 1
                            logger.warning(f"Error while uploading this file, retrying...: {path_to_upload}. Exception: {str(e)}")

                return study_orthanc_id if is_pdf else None

        # folder case
        elif os.path.isdir(path_to_upload):
            # this folder could have been processed in a previous run of the script
            is_import_root = os.path.abspath(path_to_upload) == os.path.abspath(self._folder_path)
            if (
                path_to_upload in self._folders_uploaded
                and (not self._dicomize_pdf or is_import_root)
            ):
                logger.info(f"Folder {path_to_upload} already processed, skipping...")
                return study_orthanc_id

            ## list dir and check if there is folders or files in this path
            ## if files only:
            ##  sort them (pdf at the end)
            ## process them

            study_id = study_orthanc_id
            path_entries = (
                self._list_and_sort_dir(path_to_upload)
                if self._dicomize_pdf
                else os.listdir(path_to_upload)
            )
            folder_errors = []
            paired_archive_groups = (
                self._paired_archive_groups(path_to_upload, path_entries)
                if self._dicomize_pdf
                else {}
            )
            processed_archive_groups = set()
            for path in path_entries:
                full_path = os.path.join(path_to_upload, path)
                try:
                    _, extension = os.path.splitext(full_path)
                    clear_study_context = (
                        os.path.isfile(full_path)
                        and extension.lower() in self._skip_extensions
                        and self._has_paired_report(path, path_entries)
                    )
                    archive_group = paired_archive_groups.get(path)
                    if archive_group:
                        if archive_group in processed_archive_groups:
                            continue
                        processed_archive_groups.add(archive_group)
                        with tempfile.TemporaryDirectory() as temp_dir:
                            for archive_name, priority in archive_group:
                                role_dir = os.path.join(
                                    temp_dir,
                                    "images" if priority == 1 else "reports",
                                )
                                os.makedirs(role_dir, exist_ok=True)
                                archive_path = os.path.join(path_to_upload, archive_name)
                                with zipfile.ZipFile(archive_path, "r") as archive:
                                    archive.extractall(role_dir)
                            study_id = self.upload_and_label(
                                path_to_upload=temp_dir,
                                study_orthanc_id=study_id,
                            )
                    else:
                        study_id = self.upload_and_label(
                            path_to_upload=full_path,
                            study_orthanc_id=study_id,
                        )
                        if clear_study_context:
                            study_id = None
                except Exception as e:
                    logger.exception(f"Error while processing folder entry: {full_path}")
                    study_id = None
                    folder_errors.append(e)

            if folder_errors:
                raise RuntimeError(
                    f"Failed to process {len(folder_errors)} folder entries in {path_to_upload}"
                ) from folder_errors[0]

            # let's process this folder
            # for path in os.listdir(path_to_upload):
            #     full_path = os.path.join(path_to_upload, path)
            #     ## manage id (get and repush)
            #     self.upload_and_label(path_to_upload=full_path)

            # let's add this folder path in the processed ones:
            if not self._dicomize_pdf or is_import_root:
                self.add_folder_path_in_state_file(path_to_upload)
            return study_id

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
                # path is the full path of a file or a folder
                self.upload_and_label(path_to_upload=path)
            except Exception as e:
                logger.exception(f"Importer worker failed while processing {path}")
                with self._lock:
                    self._worker_errors.append((path, e))
            finally:
                self._messages.task_done()  # tell the queue the item has been processed

        logger.debug("Processing thread stopped")

    @staticmethod
    def _strip_role_suffix(stem):
        for role in ("images", "reports", "image", "report", "dicom", "pdfs", "pdf"):
            for separator in ("-", "_", ".", " "):
                suffix = f"{separator}{role}"
                if stem.endswith(suffix):
                    return stem[:-len(suffix)]
        return stem

    def _has_paired_report(self, name, path_entries):
        stem = self._strip_role_suffix(
            os.path.splitext(os.path.basename(name.lower()))[0]
        )
        return any(
            candidate.lower().endswith(".pdf")
            and self._strip_role_suffix(
                os.path.splitext(os.path.basename(candidate.lower()))[0]
            ) == stem
            for candidate in path_entries
        )

    def _paired_archive_groups(self, folder_path, path_entries):

        groups = {}
        for name in path_entries:
            path = os.path.join(folder_path, name)
            if not os.path.isfile(path) or not zipfile.is_zipfile(path):
                continue
            with zipfile.ZipFile(path, "r") as archive:
                member_names = [
                    member.filename.lower()
                    for member in archive.infolist()
                    if not member.is_dir()
                ]
            has_pdf = any(member.endswith(".pdf") for member in member_names)
            has_dicom = any(
                not member.endswith(".pdf")
                and os.path.splitext(member)[1] not in self._skip_extensions
                for member in member_names
            )
            if not has_pdf and not has_dicom:
                continue
            priority = 1 if has_dicom else 2
            stem = os.path.splitext(os.path.basename(name.lower()))[0]
            parent_parts = Path(name.lower()).parts[:-1]
            role_names = {"image", "images", "dicom", "report", "reports", "pdf", "pdfs"}
            if parent_parts and parent_parts[0] in role_names:
                parent_parts = parent_parts[1:]
            group = (parent_parts, self._strip_role_suffix(stem))
            groups.setdefault(group, []).append((name, priority))

        paired = {}
        for entries in groups.values():
            if {priority for _, priority in entries} != {1, 2}:
                continue
            archive_group = tuple(sorted(entries, key=lambda entry: (entry[1], entry[0])))
            for name, _ in entries:
                paired[name] = archive_group
        return paired

    def _list_and_sort_dir(self, folder_path):
        def sort_priority(path):
            if os.path.isdir(path):
                try:
                    child_names = os.listdir(path)
                except OSError as e:
                    logger.warning(f"Unable to inspect directory {path}: {e}")
                    return 0
                priorities = [
                    sort_priority(os.path.join(path, name))
                    for name in child_names
                ]
                if 1 in priorities:
                    return 1
                return 2 if 2 in priorities else 0
            if zipfile.is_zipfile(path):
                with zipfile.ZipFile(path, "r") as archive:
                    lower_names = [name.lower() for name in archive.namelist()]
                if any(name.endswith(".dcm") for name in lower_names):
                    return 1
                if any(name.endswith(".pdf") for name in lower_names):
                    return 2
            if path.lower().endswith(".dcm"):
                return 1
            if path.lower().endswith(".pdf"):
                return 2
            _, extension = os.path.splitext(path)
            return 0 if extension.lower() in self._skip_extensions else 1

        path_entries = os.listdir(path=folder_path)
        image_roles = {"image", "images", "dicom"}
        report_roles = {"report", "reports", "pdf", "pdfs"}
        centralized_image_dirs = {
            name
            for name in path_entries
            if name.lower() in image_roles
            and os.path.isdir(os.path.join(folder_path, name))
        }
        centralized_report_dirs = {
            name
            for name in path_entries
            if name.lower() in report_roles
            and os.path.isdir(os.path.join(folder_path, name))
        }
        centralized_dirs = centralized_image_dirs | centralized_report_dirs
        if centralized_image_dirs and centralized_report_dirs:
            expanded_entries = []

            def append_files(path, relative_path):
                for child_name in os.listdir(path):
                    child_path = os.path.join(path, child_name)
                    child_relative_path = os.path.join(relative_path, child_name)
                    if os.path.isdir(child_path):
                        append_files(child_path, child_relative_path)
                    else:
                        expanded_entries.append(child_relative_path)

            for name in path_entries:
                if name not in centralized_dirs:
                    expanded_entries.append(name)
                    continue
                path = os.path.join(folder_path, name)
                try:
                    append_files(path, name)
                except OSError:
                    expanded_entries.append(name)
            path_entries = expanded_entries

        def strip_role_suffix(stem):
            return "" if stem in image_roles | report_roles else self._strip_role_suffix(stem)

        centralized_stems = {}
        for name in path_entries:
            parts = Path(name).parts
            if len(parts) <= 2 or parts[0] not in centralized_dirs:
                continue
            role = "image" if parts[0] in centralized_image_dirs else "report"
            parent = tuple(part.lower() for part in parts[1:-1])
            stem = os.path.splitext(os.path.basename(name.lower()))[0]
            centralized_stems.setdefault((parent, role), set()).add(stem)

        named_centralized_parents = set()
        centralized_parents = {parent for parent, _ in centralized_stems}
        for parent in centralized_parents:
            image_stems = centralized_stems.get((parent, "image"), set())
            report_stems = centralized_stems.get((parent, "report"), set())
            if any(
                image_stem == report_stem
                or strip_role_suffix(report_stem) == image_stem
                or any(
                    image_stem.startswith(f"{report_stem}{separator}")
                    for separator in ("-", "_", ".", " ")
                )
                for image_stem in image_stems
                for report_stem in report_stems
            ):
                named_centralized_parents.add(parent)

        def pairing_stem(name):
            path = os.path.join(folder_path, name)
            base_name = os.path.basename(name.lower())
            if os.path.isfile(path):
                parts = Path(name).parts
                if len(parts) > 2 and parts[0] in centralized_dirs:
                    parent = tuple(part.lower() for part in parts[1:-1])
                    if parent in named_centralized_parents:
                        stem = os.path.splitext(base_name)[0]
                        return os.path.join(*parent, stem)
                    return os.path.join(*parent)
                return os.path.splitext(base_name)[0]

            return strip_role_suffix(base_name)

        direct_priorities = {}
        entry_stems = {}
        for name in path_entries:
            path = os.path.join(folder_path, name)
            priority = sort_priority(path)
            direct_priorities[name] = priority
            stem = pairing_stem(name)
            entry_stems[name] = stem

        archive_groups = {
            name: strip_role_suffix(stem)
            for name, stem in entry_stems.items()
            if os.path.splitext(os.path.basename(name.lower()))[1] == ".zip"
        }
        archive_dicom_groups = {
            archive_groups[name]
            for name in archive_groups
            if direct_priorities[name] == 1
        }
        archive_report_groups = {
            archive_groups[name]
            for name in archive_groups
            if direct_priorities[name] == 2
        }
        paired_archive_groups = archive_dicom_groups & archive_report_groups

        dicom_stems = {
            stem
            for name, stem in entry_stems.items()
            if direct_priorities[name] == 1
        }
        entry_groups = {}
        for name, stem in entry_stems.items():
            if archive_groups.get(name) in paired_archive_groups:
                entry_groups[name] = archive_groups[name]
            elif direct_priorities[name] == 2:
                normalized_stem = strip_role_suffix(stem)
                if stem in dicom_stems:
                    entry_groups[name] = stem
                elif any(
                    dicom_stem == normalized_stem
                    or any(
                        dicom_stem.startswith(f"{normalized_stem}{separator}")
                        for separator in ("-", "_", ".", " ")
                    )
                    for dicom_stem in dicom_stems
                ):
                    entry_groups[name] = normalized_stem
                else:
                    entry_groups[name] = stem
            else:
                entry_groups[name] = stem
        report_groups = {
            entry_groups[name]
            for name in entry_groups
            if direct_priorities[name] == 2
        }
        dicom_groups = set()
        for name, stem in entry_groups.items():
            priority = direct_priorities[name]
            group = stem
            if priority == 1 and stem not in report_groups:
                candidates = [
                    report_group
                    for report_group in report_groups
                    if strip_role_suffix(report_group) == stem
                    or any(
                        stem.startswith(f"{report_group}{separator}")
                        for separator in ("-", "_", ".", " ")
                    )
                ]
                if candidates:
                    group = max(candidates, key=len)
            entry_groups[name] = group
            if priority == 1:
                dicom_groups.add(group)
        paired_stems = dicom_groups & report_groups

        def sort_key(name):
            stem = entry_groups[name]
            priority = direct_priorities[name]
            if stem in paired_stems and priority in (1, 2):
                return (0, 0, stem, 0 if priority == 1 else 1, name.lower())
            return (1, priority, "", 0, name.lower())

        path_entries = sorted(
            path_entries,
            key=sort_key,
        )
        return path_entries

    def execute(self):
        # read state
        if self._state_path and os.path.isfile(self._state_path):
            with open(self._state_path, 'r') as file:
                lines = file.readlines()
                self._folders_uploaded = [line.strip() for line in lines]

        if self._folder_path in self._folders_uploaded:
            logger.info(f"Skipping folder already uploaded: {self._folder_path}")
            return

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

        checkpoint_root = False
        if not self._dicomize_pdf:
            paths = os.listdir(path=self._folder_path)
            checkpoint_root = any(
                os.path.isfile(os.path.join(self._folder_path, path))
                and os.path.splitext(path)[1].lower() not in self._skip_extensions
                for path in paths
            )
            for path in paths:
                full_path = os.path.join(self._folder_path, path)
                self._messages.put(full_path) # if the queue is full, this will block until there's a free slot

        # PDF reports may live in a sibling folder and need the study ID produced
        # by an earlier DICOM upload, so keep the root traversal on one worker.
        else:
            self._messages.put(self._folder_path)

        # let's wait for the completion of all threads
        self.stop()

        if self._worker_errors:
            path, error = self._worker_errors[0]
            raise RuntimeError(f"Importer worker failed while processing {path}") from error

        if checkpoint_root:
            self.add_folder_path_in_state_file(self._folder_path)

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
