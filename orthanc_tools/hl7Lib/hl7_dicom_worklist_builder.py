import errno
import os
import secrets
import stat
import typing
from pathlib import Path
import pydicom
from enum import Enum
from typing import List, Union
from pprint import pprint
from urllib.parse import quote

from orthanc_api_client import OrthancApiClient

_IS_WINDOWS = os.name == "nt"


def _write_in_place(ds, output_path, expected_stat):
    open_flags = os.O_WRONLY
    if hasattr(os, "O_BINARY"):
        open_flags |= os.O_BINARY
    output_fd = os.open(output_path, open_flags)
    try:
        opened_stat = os.fstat(output_fd)
        if (
            opened_stat.st_dev != expected_stat.st_dev
            or opened_stat.st_ino != expected_stat.st_ino
        ):
            raise ValueError("Worklist destination changed before it could be written")
        with os.fdopen(output_fd, "wb") as output_file:
            output_fd = None
            output_file.truncate(0)
            ds.save_as(output_file, enforce_file_format=True)
    finally:
        if output_fd is not None:
            os.close(output_fd)


def _unlink_if_same_file(path, expected_stat):
    try:
        current_stat = os.lstat(path)
    except FileNotFoundError:
        return
    if (
        current_stat.st_dev == expected_stat.st_dev
        and current_stat.st_ino == expected_stat.st_ino
    ):
        os.unlink(path)


class DicomElementType(Enum):
    MANDATORY = 1  # for dicom tags that must be there (type 1 or 1c) -> throw an exception if not present
    REQUIRED = 2  # for dicom tags that are mandatory but accepts null value (type 2 or 2c)
    OPTIONAL = 3  # for dicom tags that are not mandatory (type 3)

base_elements = [
    ('AccessionNumber', DicomElementType.REQUIRED),
    ('InstitutionName', DicomElementType.OPTIONAL),
    ('InstitutionAddress', DicomElementType.OPTIONAL),
    ('PatientID', DicomElementType.MANDATORY),
    ('OtherPatientIDs', DicomElementType.OPTIONAL),
    ('IssuerOfPatientID', DicomElementType.OPTIONAL),
    ('PatientName', DicomElementType.MANDATORY),
    ('PatientMotherBirthName', DicomElementType.OPTIONAL),
    ('PatientAddress', DicomElementType.OPTIONAL),
    ('PatientBirthDate', DicomElementType.MANDATORY),
    ('PatientSex', DicomElementType.MANDATORY),
    ('SOPInstanceUID', DicomElementType.MANDATORY),
    ('StudyInstanceUID', DicomElementType.MANDATORY),
    ('RequestingPhysician', DicomElementType.REQUIRED),
    ('ReferringPhysicianName', DicomElementType.REQUIRED),
    ('RequestedProcedureDescription', DicomElementType.REQUIRED),
    ('RequestedProcedureID', DicomElementType.MANDATORY),
    ('SpecificCharacterSet', DicomElementType.MANDATORY),
    ('ConfidentialityConstraintOnPatientDataDescription', DicomElementType.OPTIONAL),
    ('PatientWeight', DicomElementType.OPTIONAL),
    ('PatientSpeciesDescription', DicomElementType.OPTIONAL),
    ('PatientBreedDescription', DicomElementType.OPTIONAL),
    ('ResponsiblePerson', DicomElementType.OPTIONAL),
    ('PatientSexNeutered', DicomElementType.OPTIONAL),
    ('BreedRegistrationNumber', DicomElementType.OPTIONAL)
]

step_elements = [
    ('Modality', DicomElementType.REQUIRED),
    ('ScheduledProcedureStepStartDate', DicomElementType.OPTIONAL),
    ('ScheduledProcedureStepStartTime', DicomElementType.OPTIONAL),
    ('ReasonForTheRequestedProcedure', DicomElementType.OPTIONAL),
    ('ReferringPhysicianName', DicomElementType.REQUIRED),
    ('ScheduledStationAETitle', DicomElementType.MANDATORY),
    ('ScheduledPerformingPhysicianName', DicomElementType.REQUIRED),
    ('ScheduledProcedureStepID', DicomElementType.MANDATORY),
    ('ScheduledStationName', DicomElementType.REQUIRED),
]

class DicomWorklistBuilder:

    def __init__(self, folder: str = None, orthanc_client: OrthancApiClient = None):
        self._folder = folder
        self._orthanc_client = orthanc_client

    def get_folder(self):
        return self._folder

    # to override in derived class to customize the worklist before it is saved to disk
    def customize(self, values: Union[pydicom.dataset.FileDataset, typing.Dict[str, str]]):
        if isinstance(values, pydicom.dataset.FileDataset):
            return values
        return values

    def generate(self, values: typing.Dict[str, str], file_name: str = None, entropy_srcs: List[str] = None) -> str:
        """
        :param values: a Dictionary object created from an HL7 message.  Keys of the dico shall match pydicom tag names (i.e: AccessionNumber, PatientID, ...)
        :param filename:
        :entropy_srcs: a SHA512 hash of the supplied list will be used for the UIDs which means the result is deterministic
        :return: the filename created or the Orthanc ID if WL are stored into Orthanc
        """
        assert self._folder is not None or file_name is not None or self._orthanc_client is not None, "Please always provide a folder (or an Orthanc client) when creating the builder or provide a filename each time you generate a worklist"

        # clip patient address at 64 chars (one of the Avignon CT does not handle them)
        patient_address = values.get('PatientAddress')
        if patient_address and len(patient_address) > 64:
            patient_address = patient_address[:60] + "..."
            values['PatientAddress'] = patient_address

        if self._orthanc_client is  not None:
            r = self._generate_wl_through_api(values=values, entropy_srcs=entropy_srcs)
            return r

        elif self._folder is not None or file_name is not None:
            r = self._generate_file(values=values, file_name=file_name, entropy_srcs=entropy_srcs)
            return r

        else:
            raise Exception("Please provide a folder (or an Orthanc client) when creating the builder or provide a filename each time you generate a worklist")


    def _generate_file(self, values: typing.Dict[str, str], file_name: str = None, entropy_srcs: List[str] = None):
        # now, let's try to build a DWL out of this
        file_meta = pydicom.dataset.Dataset()
        file_meta.MediaStorageSOPClassUID = '1.2.276.0.7230010.3.1.0.1'  # shall we use 1.2.840.10008.5.1.4.31 ?
        file_meta.MediaStorageSOPInstanceUID = pydicom.uid.generate_uid(entropy_srcs=entropy_srcs)
        file_meta.ImplementationClassUID = '1.2.826.0.1.3680043.9.6676.1.0.0.1'  # 1.2.826.0.1.3680043.9.6676. is Osimis prefix
        file_meta.ImplementationVersionName = 'OSIMISHL7DWL'

        ds = pydicom.dataset.FileDataset(file_name, {}, file_meta = file_meta, preamble = b'\0' * 128)
        if not "SOPInstanceUID" in values:
            values["SOPInstanceUID"] = file_meta.MediaStorageSOPInstanceUID  # same values for these 2 DICOM tags is very common
        if not "StudyInstanceUID" in values:
            values["StudyInstanceUID"] = pydicom.uid.generate_uid(entropy_srcs=[file_meta.MediaStorageSOPInstanceUID])  # set a default StudyInstanceUID.  It might be overriden from the dwl object

        for field_name, element_type in base_elements:
            self._add_field(ds, values, field_name, element_type)

        ds.ReferencedStudySequence = pydicom.sequence.Sequence()
        ds.ReferencedPatientSequence = pydicom.sequence.Sequence()

        step = pydicom.dataset.Dataset()
        step.ScheduledProcedureStepDescription = values.get('RequestedProcedureDescription')
        for field_name, element_type in step_elements:
            self._add_field(step, values, field_name, element_type)

        ds.ScheduledProcedureStepSequence = pydicom.sequence.Sequence([step])

        ds = self.customize(ds)

        automatic_worklist_folder = None
        if file_name is None:  # if no filename provided, save in the folder
            filename_source = str(ds.AccessionNumber) or str(ds.SOPInstanceUID)
            safe_accession_number = quote(filename_source, safe="._-")
            if safe_accession_number in {".", ".."}:
                safe_accession_number = safe_accession_number.replace(".", "%2E")
            if not safe_accession_number:
                raise ValueError("AccessionNumber cannot be converted to a safe worklist filename")

            worklist_folder = Path(self._folder).resolve()
            automatic_worklist_folder = worklist_folder
            encoded_output_path = worklist_folder / f"{safe_accession_number}.wl"
            try:
                encoded_output_path.resolve().relative_to(worklist_folder)
            except ValueError:
                raise ValueError("Worklist path must remain inside the configured folder")

            legacy_output_path = worklist_folder / f"{filename_source}.wl"
            uses_encoded_name = legacy_output_path != encoded_output_path
            output_path = encoded_output_path
            if uses_encoded_name and os.path.lexists(legacy_output_path):
                try:
                    legacy_output_path.resolve().relative_to(worklist_folder)
                except ValueError:
                    raise ValueError("Legacy worklist path must remain inside the configured folder")
                try:
                    legacy_accession_number = str(
                        pydicom.dcmread(
                            legacy_output_path,
                            stop_before_pixels=True,
                        ).AccessionNumber
                    )
                except (AttributeError, pydicom.errors.InvalidDicomError) as ex:
                    raise ValueError(
                        "Existing legacy worklist cannot be safely identified"
                    ) from ex
                if legacy_accession_number == filename_source:
                    output_path = legacy_output_path
            if (
                uses_encoded_name
                and output_path == encoded_output_path
                and os.path.lexists(output_path)
            ):
                try:
                    existing_accession_number = str(
                        pydicom.dcmread(output_path, stop_before_pixels=True).AccessionNumber
                    )
                except (AttributeError, pydicom.errors.InvalidDicomError) as ex:
                    raise ValueError(
                        "Existing encoded worklist cannot be safely identified"
                    ) from ex
                if existing_accession_number != filename_source:
                    raise ValueError(
                        "Existing encoded worklist belongs to a different accession"
                    )
            file_name = os.fspath(output_path)

        output_path = Path(file_name)
        if automatic_worklist_folder is not None:
            output_path = output_path.resolve()
            try:
                output_path.relative_to(automatic_worklist_folder)
            except ValueError:
                raise ValueError("Worklist path must remain inside the configured folder")
        elif output_path.is_symlink():
            output_path = output_path.resolve()
        try:
            output_stat = output_path.stat()
        except FileNotFoundError:
            output_stat = None
        nested_automatic_destination = (
            automatic_worklist_folder is not None
            and output_path.parent != automatic_worklist_folder
        )
        # Replacing an inode loses hard links, Windows ACLs, and the validated
        # identity of automatic destinations reached through subdirectories.
        if output_stat is not None and (
            output_stat.st_nlink > 1
            or _IS_WINDOWS
            or nested_automatic_destination
        ):
            _write_in_place(ds, output_path, output_stat)
            return file_name

        temp_file_name = os.fspath(
            output_path.parent
            / f".{secrets.token_hex(16)}.tmp"
        )
        temp_file_stat = None
        try:
            with open(temp_file_name, "xb") as temp_file:
                temp_file_stat = os.fstat(temp_file.fileno())
                ds.save_as(temp_file, enforce_file_format=True)
                if output_stat is not None:
                    if hasattr(os, "fchown"):
                        os.fchown(
                            temp_file.fileno(),
                            output_stat.st_uid,
                            output_stat.st_gid,
                        )
                    elif hasattr(os, "chown"):
                        os.chown(
                            temp_file_name,
                            output_stat.st_uid,
                            output_stat.st_gid,
                        )
                    if hasattr(os, "fchmod"):
                        os.fchmod(
                            temp_file.fileno(),
                            stat.S_IMODE(output_stat.st_mode),
                        )
                    else:
                        os.chmod(temp_file_name, stat.S_IMODE(output_stat.st_mode))
                    if all(
                        hasattr(os, name)
                        for name in ("listxattr", "getxattr", "setxattr")
                    ):
                        try:
                            attribute_names = os.listxattr(output_path)
                        except OSError as ex:
                            if ex.errno != errno.ENOTSUP:
                                raise
                            attribute_names = []
                        for attribute_name in attribute_names:
                            os.setxattr(
                                temp_file.fileno(),
                                attribute_name,
                                os.getxattr(output_path, attribute_name),
                            )
            promoted_stat = os.lstat(temp_file_name)
            if (
                not stat.S_ISREG(promoted_stat.st_mode)
                or promoted_stat.st_dev != temp_file_stat.st_dev
                or promoted_stat.st_ino != temp_file_stat.st_ino
            ):
                raise ValueError("Temporary worklist changed before promotion")
            os.replace(temp_file_name, output_path)
            temp_file_stat = None
        except PermissionError:
            if temp_file_stat is not None:
                _unlink_if_same_file(temp_file_name, temp_file_stat)
            if output_stat is None:
                raise
            _write_in_place(ds, output_path, output_stat)
        except OSError as ex:
            if temp_file_stat is not None:
                _unlink_if_same_file(temp_file_name, temp_file_stat)
            if (
                output_stat is not None
                and ex.errno in {errno.EBUSY, errno.EROFS, errno.ENOTSUP}
            ):
                _write_in_place(ds, output_path, output_stat)
                return file_name
            raise
        except Exception:
            if temp_file_stat is not None:
                _unlink_if_same_file(temp_file_name, temp_file_stat)
            raise

        return file_name

    def _generate_wl_through_api(self, values: typing.Dict[str, str], entropy_srcs: List[str] = None):

        formatted_values = {}

        if not "SOPInstanceUID" in values:
            values["SOPInstanceUID"] = pydicom.uid.generate_uid(entropy_srcs=entropy_srcs)

        for field_name, element_type in base_elements:
            self._add_value(formatted_values, values, field_name, element_type)

        step = {}
        for field_name, element_type in step_elements:
            self._add_value(step, values, field_name, element_type)

        formatted_values["ScheduledProcedureStepSequence"] = [step]

        wl_id = self._orthanc_client.worklists.create(values=formatted_values)
        return wl_id

    def _add_value(self, output: typing.Dict[str, str], values: typing.Dict[str, str], field_name: str, element_type: DicomElementType):
        if field_name in values:
            if values[field_name] is not None:
                output[field_name] = values.get(field_name)
            elif element_type == DicomElementType.REQUIRED:
                output[field_name] = ''
            elif element_type == DicomElementType.MANDATORY:
                raise Exception(f"missing field '{field_name}'")  # TODO: raise a dedicated exception
        elif element_type == DicomElementType.REQUIRED:
            output[field_name] = ''
        elif element_type == DicomElementType.MANDATORY:
            raise Exception(f"missing field '{field_name}'")  # TODO: raise a dedicated exception

    def _add_field(self, ds: pydicom.dataset.Dataset, values: typing.Dict[str, str], field_name: str, element_type: DicomElementType):
        if field_name in values:
            if values[field_name] is not None:
                ds.__setattr__(field_name, values.get(field_name))
            elif element_type == DicomElementType.REQUIRED:
                ds.__setattr__(field_name, '')
            elif element_type == DicomElementType.MANDATORY:
                raise Exception("missing field '{fieldName}'".format(fieldName = field_name))  # TODO: raise a dedicated exception
        elif element_type == DicomElementType.REQUIRED:
            ds.__setattr__(field_name, '')
        elif element_type == DicomElementType.MANDATORY:
            raise Exception("missing field '{fieldName}'".format(fieldName = field_name))  # TODO: raise a dedicated exception

# # Dicom-Meta-Information-Header
#
# # Dicom-Data-Set
# # Used TransferSyntax: Little Endian Explicit
# (0008,0005) CS [ISO_IR 100]                             #  10, 1 SpecificCharacterSet
# (0008,0018) UI [1.2.276.0.7230010.3.1.4.34260742.2908.1486551644.225000] #  56, 1 SOPInstanceUID
# (0008,0050) SH [63]                                     #   2, 1 AccessionNumber
# (0008,0080) LO [REMOVED]              #  26, 1 InstitutionName
# (0008,0081) ST [REMOVED]         #  30, 1 InstitutionAddress
# (0010,0010) PN [SURNAME^NAME]                          #  14, 1 PatientName
# (0010,0020) LO [38]                                     #   2, 1 PatientID
# (0010,0030) DA [19710711]                               #   8, 1 PatientBirthDate
# (0010,0040) CS [F]                                      #   2, 1 PatientSex
# (0020,000d) UI [1.2.276.0.7230010.3.1.2.34260742.2908.1486551644.224998] #  56, 1 StudyInstanceUID
# (0020,000e) UI [1.2.276.0.7230010.3.1.3.34260742.2908.1486551644.224999] #  56, 1 SeriesInstanceUID
# (0032,1032) PN (no value available)                     #   0, 0 RequestingPhysician
# (0032,1060) LO [TC BACINO]                              #  10, 1 RequestedProcedureDescription
# (0038,0010) LO [30]                                     #   2, 1 AdmissionID
# (0040,0100) SQ (Sequence with explicit length #=1)      # 108, 1 ScheduledProcedureStepSequence
#   (fffe,e000) na (Item with explicit length #=8)          # 100, 1 Item
#     (0008,0060) CS [CT]                                     #   2, 1 Modality
#     (0040,0001) AE [CT99]                                   #   4, 1 ScheduledStationAETitle
#     (0040,0002) DA [20170208]                               #   8, 1 ScheduledProcedureStepStartDate
#     (0040,0003) TM [083000]                                 #   6, 1 ScheduledProcedureStepStartTime
#     (0040,0006) PN (no value available)                     #   0, 0 ScheduledPerformingPhysicianName
#     (0040,0007) LO [TC BACINO]                              #  10, 1 ScheduledProcedureStepDescription
#     (0040,0009) SH [134]                                    #   4, 1 ScheduledProcedureStepID
#     (0040,0010) SH [TC]                                     #   2, 1 ScheduledStationName
#   (fffe,e00d) na (ItemDelimitationItem for re-encoding)   #   0, 0 ItemDelimitationItem
# (fffe,e0dd) na (SequenceDelimitationItem for re-encod.) #   0, 0 SequenceDelimitationItem
# (0040,1001) SH [134]                                    #   4, 1 RequestedProcedureID
# (0040,1003) SH [LOW]                                    #   4, 1 RequestedProcedurePriority
