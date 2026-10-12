import datetime
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from orthanc_api_client import LabelsConstraint
from orthanc_tools.orthanc_cleaner import OrthancCleaner


class TestCleanerRules(unittest.TestCase):
    def cleaner(self, content):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        path = Path(temporary.name) / "rules.csv"
        path.write_text(content)
        return OrthancCleaner(Mock(), None, path)

    def test_existing_four_column_rules_and_keep_rules(self):
        cleaner = self.cleaner('OLD,6,CT,acc-nr\nTO_KEEP,52,,,keep\n\n')
        old, keep = cleaner.parse_csv_file()
        self.assertEqual((old.label_name, old.retention_duration, old.modality, old.accession_number, old.keep),
                         ('OLD', 6, 'CT', 'acc-nr', False))
        self.assertTrue(keep.keep)

    def test_invalid_rule_aborts_before_query_or_deletion(self):
        for invalid in ['OLD,6,,,KEEP', 'OLD,6,,,drop', ',52,,,keep', 'OLD,x,,', 'OLD,6', 'OLD,6,,,,']:
            with self.subTest(invalid=invalid):
                cleaner = self.cleaner('VALID,6,,\n' + invalid)
                with self.assertRaisesRegex(ValueError, 'line 2'):
                    cleaner.clean()
                cleaner._api_client.studies.find.assert_not_called()
                cleaner._api_client.studies.delete.assert_not_called()

    def test_signed_retention_values_keep_existing_semantics(self):
        cleaner = self.cleaner('NOW,0,,\nFUTURE,-6,,')
        self.assertEqual([0, -6], [rule.retention_duration for rule in cleaner.parse_csv_file()])

    def test_keep_rule_excludes_label_and_retains_recently_uploaded_studies(self):
        cleaner = self.cleaner('TO_KEEP,6,,,keep')
        old = datetime.datetime.now() - datetime.timedelta(weeks=7)
        cleaner._api_client.studies.find.return_value = [
            SimpleNamespace(orthanc_id='old', last_update=old),
            SimpleNamespace(orthanc_id='recent', last_update=datetime.datetime.now()),
        ]
        self.assertEqual(cleaner.get_studies_to_delete(cleaner.parse_csv_file()), {'old'})
        self.assertEqual(cleaner._api_client.studies.find.call_args.kwargs['labels'], ['TO_KEEP'])
        self.assertEqual(cleaner._api_client.studies.find.call_args.kwargs['labels_constraint'], LabelsConstraint.NONE)


if __name__ == "__main__":
    unittest.main()
