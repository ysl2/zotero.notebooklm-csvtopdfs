# 📚 Zotero arXiv CSV PDF Downloader

This project reads records from a Zotero-exported `main.csv`, downloads arXiv PDFs in batch, and uses a deterministic filename rule to avoid duplicate downloads across runs.

## ✨ Features

- 🔎 Processes arXiv URLs only
- ⏭️ Skips non-arXiv records and prints them in the final summary
- 🧾 Stable and readable filename format:
  - `{submitted_yyyymmdd}--{normalized_title}--{normalized_doc_id}.pdf`
- 📅 Fetches submitted/public date from arXiv metadata on each run (does not trust CSV date)
- 🧩 Uses `.part` temporary files and atomic replace after validation
- ✅ Validates PDF integrity (size, `%PDF-` header, `%%EOF` footer)
- 🔄 Two-phase execution:
  - Phase 1: process records that already have `00000000` placeholder files
  - Phase 2: process all remaining records
- ♻️ Placeholder migration:
  - If a real date is later available, valid `00000000--...` files are renamed to real-date filenames
  - Corrupted placeholder files are deleted, never migrated
- 🧵 Concurrency model:
  - 10 download worker threads
  - 1 independent poller thread for continuous placeholder migration
  - Around 11 active worker threads during Phase 2

## 📦 Requirements

- 🐍 Python `>=3.14`
- ⚡ `uv` is recommended

Install dependencies:

```bash
uv sync
```

## 🧾 Input CSV

Default input file: `main.csv` in project root.

Required fields:

- 🔗 `Url`: paper link (only arXiv links are downloaded)
- 🏷️ `Title`: paper title used in filenames
- 🔑 `Key`: fallback name when `Title` is empty

## 🚀 Usage

```bash
uv run python main.py
```

## 🌐 Proxy (Optional)

If you use a local proxy (for example Clash):

```bash
export HTTP_PROXY=http://127.0.0.1:7890
export HTTPS_PROXY=http://127.0.0.1:7890
uv run python main.py
```

## 🧠 Filename Rule

Final filename:

```text
{submitted_yyyymmdd}--{normalized_title}--{normalized_doc_id}.pdf
```

- 📅 `submitted_yyyymmdd`: fetched from arXiv metadata
- 🧹 `normalized_title`: deterministic title normalization (NFKC, illegal-char cleanup, separator compression)
- 🆔 `normalized_doc_id`: normalized arXiv id (example: `2501.12345v2 -> 2501_12345v2`)

If date lookup fails temporarily, the fallback prefix is:

```text
00000000--...
```

Later runs will keep trying to migrate these placeholders to real-date filenames.

## 📊 Runtime Summary

The script prints these counters at the end:

- ⬇️ `Downloaded`
- ⏭️ `Skipped`
- ❌ `Failed`
- 🚫 `Non-arXiv skipped`
- 🕒 `Date fallback (00000000)`
- 🧮 `Total rows`

If non-arXiv records exist, it also prints up to 20 skipped record entries.

## ⚠️ Notes

- 📄 This tool currently downloads arXiv PDFs only.
- 🌧️ If network/proxy is unavailable, date fetch and PDF download can fail, increasing `00000000` files.
- 🔁 The poller runs continuously (no sleep interval) for faster migration, which may increase CPU and request pressure.

## 🗂️ Project Structure

- 🧠 Main script: `main.py`
- 🧪 Tests: `tests/test_naming.py`

## 📜 License

MIT
