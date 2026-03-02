import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from main import (
    build_output_filename,
    build_doc_id_maps,
    collect_placeholder_doc_ids,
    extract_arxiv_id,
    migrate_placeholders_once,
    normalize_doc_id,
    normalize_title,
    split_rows_for_processing,
    try_migrate_placeholder_file,
)


class NamingRuleTests(unittest.TestCase):
    def _write_valid_pdf(self, path: Path) -> None:
        body = b"%PDF-1.4\n" + (b"0" * 1100) + b"\n%%EOF\n"
        path.write_bytes(body)

    def test_normalize_doc_id_replaces_special_chars(self):
        self.assertEqual(normalize_doc_id("2501.12345v2"), "2501_12345v2")
        self.assertEqual(normalize_doc_id("hep-th/9901001v1"), "hep-th_9901001v1")
        self.assertEqual(normalize_doc_id("id:with/slash.and.dot"), "id_with_slash_and_dot")

    def test_extract_arxiv_id_supports_old_and_new_style(self):
        self.assertEqual(extract_arxiv_id("https://arxiv.org/abs/2501.12345v2"), "2501.12345v2")
        self.assertEqual(extract_arxiv_id("https://arxiv.org/pdf/2501.12345v2.pdf"), "2501.12345v2")
        self.assertEqual(extract_arxiv_id("https://arxiv.org/abs/hep-th/9901001"), "hep-th/9901001")

    def test_normalize_title_is_deterministic_and_safe(self):
        raw = "  A  <Bad> :Title/Name?*  "
        self.assertEqual(normalize_title(raw), "a_bad_title_name")
        self.assertEqual(normalize_title(raw), "a_bad_title_name")

    def test_build_output_filename_follows_required_pattern(self):
        filename = build_output_filename(
            submitted_date="20240203",
            title="A  <Bad> :Title/Name?*",
            doc_id="2501.12345v2",
        )
        self.assertEqual(filename, "20240203--a_bad_title_name--2501_12345v2.pdf")

    def test_migrate_valid_placeholder_file(self):
        with TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            arxiv_id = "2501.12345v2"
            title = "Sample Title"

            old_path = output_dir / build_output_filename("00000000", title, arxiv_id)
            new_path = output_dir / build_output_filename("20240203", title, arxiv_id)
            self._write_valid_pdf(old_path)

            migrated, message = try_migrate_placeholder_file(output_dir, new_path, title, arxiv_id)

            self.assertTrue(migrated)
            self.assertIn("Migrated placeholder", message or "")
            self.assertTrue(new_path.exists())
            self.assertFalse(old_path.exists())

    def test_invalid_placeholder_file_is_deleted_not_migrated(self):
        with TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            arxiv_id = "2501.12345v2"
            title = "Sample Title"

            old_path = output_dir / build_output_filename("00000000", title, arxiv_id)
            new_path = output_dir / build_output_filename("20240203", title, arxiv_id)
            old_path.write_bytes(b"not-a-valid-pdf")

            migrated, message = try_migrate_placeholder_file(output_dir, new_path, title, arxiv_id)

            self.assertFalse(migrated)
            self.assertIsNone(message)
            self.assertFalse(old_path.exists())
            self.assertFalse(new_path.exists())

    def test_split_rows_prioritizes_existing_placeholder_doc_ids(self):
        with TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            placeholder = output_dir / "00000000--x--2501_12345v2.pdf"
            self._write_valid_pdf(placeholder)

            rows = [
                {"Url": "https://arxiv.org/abs/2501.12345v2", "Title": "A", "Key": "K1"},
                {"Url": "https://arxiv.org/abs/2601.99999v1", "Title": "B", "Key": "K2"},
                {"Url": "https://example.com/doc", "Title": "C", "Key": "K3"},
            ]

            ids = collect_placeholder_doc_ids(output_dir)
            self.assertIn("2501_12345v2", ids)

            priority, normal = split_rows_for_processing(rows, output_dir)
            self.assertEqual([idx for idx, _ in priority], [1])
            self.assertEqual([idx for idx, _ in normal], [2, 3])

    def test_migrate_placeholders_once_uses_mapping_and_resolver(self):
        with TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            old_path = output_dir / "00000000--sample_title--2501_12345v2.pdf"
            self._write_valid_pdf(old_path)

            rows = [
                {"Url": "https://arxiv.org/abs/2501.12345v2", "Title": "Sample Title", "Key": "K1"},
            ]
            doc_to_id, doc_to_title = build_doc_id_maps(rows)

            def resolver(_arxiv_id: str) -> str:
                return "20240203"

            migrated = migrate_placeholders_once(
                output_dir, doc_to_id, doc_to_title, date_resolver=resolver
            )
            self.assertEqual(migrated, 1)

            new_path = output_dir / "20240203--sample_title--2501_12345v2.pdf"
            self.assertTrue(new_path.exists())
            self.assertFalse(old_path.exists())


if __name__ == "__main__":
    unittest.main()
