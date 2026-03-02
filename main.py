import csv
import datetime
import re
import threading
import time
import unicodedata
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import requests


lock = threading.Lock()
downloaded = 0
failed = 0
skipped = 0
unsupported = 0
metadata_fallback = 0
unsupported_items: list[tuple[int, str, str]] = []
date_cache: dict[str, str | None] = {}


def extract_arxiv_id(url: str) -> str | None:
    if "arxiv.org" not in url:
        return None

    path = urlparse(url).path
    match = re.match(r"^/(?:abs|pdf)/(.+)$", path)
    if not match:
        return None

    arxiv_id = re.sub(r"\.pdf$", "", match.group(1))
    return arxiv_id or None


def get_pdf_url(arxiv_id: str) -> str:
    return f"https://arxiv.org/pdf/{arxiv_id}.pdf"


def normalize_doc_id(doc_id: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z-]+", "_", doc_id).strip("_").lower()
    return normalized or "unknown_id"


def normalize_title(title: str, max_length: int = 140) -> str:
    normalized = unicodedata.normalize("NFKC", title).lower()
    normalized = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", normalized)
    normalized = re.sub(r"\s+", "_", normalized)
    normalized = re.sub(r"[^0-9A-Za-z_-]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        normalized = "untitled"
    return normalized[:max_length].rstrip("_") or "untitled"


def build_output_filename(submitted_date: str, title: str, doc_id: str) -> str:
    date = submitted_date if re.fullmatch(r"\d{8}", submitted_date) else "00000000"
    normalized_title = normalize_title(title)
    normalized_doc_id = normalize_doc_id(doc_id)
    return f"{date}--{normalized_title}--{normalized_doc_id}.pdf"


def fetch_submitted_date(arxiv_id: str) -> str | None:
    with lock:
        if arxiv_id in date_cache:
            return date_cache[arxiv_id]

    date = None
    endpoints = [
        "https://export.arxiv.org/api/query",
        "http://export.arxiv.org/api/query",
    ]
    headers = {"User-Agent": "zotero-notebooklm-csvtopdfs/1.0"}

    for endpoint in endpoints:
        for attempt in range(3):
            try:
                response = requests.get(
                    endpoint,
                    params={"id_list": arxiv_id},
                    timeout=20,
                    headers=headers,
                )
                response.raise_for_status()

                root = ET.fromstring(response.text)
                ns = {"atom": "http://www.w3.org/2005/Atom"}
                entry = root.find("atom:entry", ns)
                published = ""
                if entry is not None:
                    published = entry.findtext("atom:published", default="", namespaces=ns)

                candidate = published[:10].replace("-", "")
                if re.fullmatch(r"\d{8}", candidate):
                    date = candidate
                    break
            except Exception:
                pass

            time.sleep(0.4 * (attempt + 1))
        if date:
            break

    if not date:
        try:
            abs_resp = requests.get(
                f"https://arxiv.org/abs/{arxiv_id}",
                timeout=20,
                headers=headers,
            )
            abs_resp.raise_for_status()
            html = abs_resp.text

            m = re.search(r'citation_date" content="(\d{4})/(\d{2})/(\d{2})"', html)
            if m:
                date = f"{m.group(1)}{m.group(2)}{m.group(3)}"
            else:
                m = re.search(r"Submitted\s+on\s+(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})", html, re.IGNORECASE)
                if not m:
                    m = re.search(r"\[v1\]\s*(?:[A-Za-z]{3},\s*)?(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})", html)
                if m:
                    parsed = datetime.datetime.strptime(m.group(1), "%d %b %Y")
                    date = parsed.strftime("%Y%m%d")
        except Exception:
            pass

    with lock:
        date_cache[arxiv_id] = date

    return date


def validate_pdf(pdf_path: Path) -> bool:
    try:
        if not pdf_path.exists():
            return False
        if pdf_path.stat().st_size < 1024:
            return False

        with open(pdf_path, "rb") as f:
            if f.read(5) != b"%PDF-":
                return False
            f.seek(-1024, 2)
            if b"%%EOF" not in f.read():
                return False

        return True
    except Exception:
        return False


def record_status(kind: str) -> None:
    global downloaded, failed, skipped, metadata_fallback
    with lock:
        if kind == "downloaded":
            downloaded += 1
        elif kind == "failed":
            failed += 1
        elif kind == "skipped":
            skipped += 1
        elif kind == "metadata_fallback":
            metadata_fallback += 1


def record_unsupported(idx: int, url: str, title: str) -> None:
    global unsupported
    with lock:
        unsupported += 1
        unsupported_items.append((idx, title or "Untitled", url))


def try_migrate_placeholder_file(
    output_dir: Path, output_path: Path, display_title: str, arxiv_id: str
) -> tuple[bool, str | None]:
    """Migrate a valid 00000000 placeholder file to the final dated filename."""
    normalized_doc_id = normalize_doc_id(arxiv_id)
    exact_placeholder = output_dir / build_output_filename("00000000", display_title, arxiv_id)

    candidates: list[Path] = []
    if exact_placeholder.exists():
        candidates.append(exact_placeholder)

    for path in sorted(output_dir.glob(f"00000000--*--{normalized_doc_id}.pdf")):
        if path not in candidates:
            candidates.append(path)

    for old_path in candidates:
        if old_path == output_path:
            continue

        if not validate_pdf(old_path):
            old_path.unlink(missing_ok=True)
            continue

        if output_path.exists():
            if validate_pdf(output_path):
                old_path.unlink(missing_ok=True)
                return True, f"Valid PDF exists: {display_title[:50]}..."
            output_path.unlink(missing_ok=True)

        old_path.replace(output_path)
        return True, f"Migrated placeholder: {display_title[:50]}..."

    return False, None


def collect_placeholder_doc_ids(output_dir: Path) -> set[str]:
    ids: set[str] = set()
    for path in output_dir.glob("00000000--*--*.pdf"):
        if not path.is_file():
            continue
        stem = path.stem
        parts = stem.split("--")
        if len(parts) >= 3 and parts[0] == "00000000":
            ids.add(parts[-1].lower())
    return ids


def split_rows_for_processing(
    rows: list[dict[str, str]], output_dir: Path
) -> tuple[list[tuple[int, dict[str, str]]], list[tuple[int, dict[str, str]]]]:
    placeholder_doc_ids = collect_placeholder_doc_ids(output_dir)
    priority: list[tuple[int, dict[str, str]]] = []
    normal: list[tuple[int, dict[str, str]]] = []

    for idx, row in enumerate(rows, 1):
        url = row.get("Url", "").strip()
        arxiv_id = extract_arxiv_id(url)
        if arxiv_id and normalize_doc_id(arxiv_id) in placeholder_doc_ids:
            priority.append((idx, row))
        else:
            normal.append((idx, row))

    return priority, normal


def build_doc_id_maps(rows: list[dict[str, str]]) -> tuple[dict[str, str], dict[str, str]]:
    doc_id_to_arxiv_id: dict[str, str] = {}
    doc_id_to_title: dict[str, str] = {}
    for row in rows:
        url = row.get("Url", "").strip()
        arxiv_id = extract_arxiv_id(url)
        if not arxiv_id:
            continue
        normalized = normalize_doc_id(arxiv_id)
        if normalized not in doc_id_to_arxiv_id:
            doc_id_to_arxiv_id[normalized] = arxiv_id
        if normalized not in doc_id_to_title:
            title = row.get("Title", "").strip() or row.get("Key", "").strip() or "Untitled"
            doc_id_to_title[normalized] = title
    return doc_id_to_arxiv_id, doc_id_to_title


def migrate_placeholders_once(
    output_dir: Path,
    doc_id_to_arxiv_id: dict[str, str],
    doc_id_to_title: dict[str, str],
    date_resolver=fetch_submitted_date,
) -> int:
    migrated_count = 0
    for old_path in sorted(output_dir.glob("00000000--*--*.pdf")):
        if not old_path.is_file():
            continue
        if not validate_pdf(old_path):
            old_path.unlink(missing_ok=True)
            continue

        parts = old_path.stem.split("--")
        if len(parts) < 3:
            continue
        normalized_doc_id = parts[-1].lower()
        arxiv_id = doc_id_to_arxiv_id.get(normalized_doc_id)
        if not arxiv_id:
            continue

        submitted_date = date_resolver(arxiv_id)
        if not submitted_date:
            continue

        display_title = doc_id_to_title.get(normalized_doc_id, parts[1] if len(parts) > 1 else "Untitled")
        new_path = output_dir / build_output_filename(submitted_date, display_title, arxiv_id)
        if new_path == old_path:
            continue

        if new_path.exists():
            if validate_pdf(new_path):
                old_path.unlink(missing_ok=True)
                migrated_count += 1
                continue
            new_path.unlink(missing_ok=True)

        old_path.replace(new_path)
        migrated_count += 1

    return migrated_count


def placeholder_polling_worker(
    output_dir: Path,
    doc_id_to_arxiv_id: dict[str, str],
    doc_id_to_title: dict[str, str],
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        migrated = migrate_placeholders_once(output_dir, doc_id_to_arxiv_id, doc_id_to_title)
        if migrated:
            print(f"[poller] Migrated {migrated} placeholder file(s)")

    migrated = migrate_placeholders_once(output_dir, doc_id_to_arxiv_id, doc_id_to_title)
    if migrated:
        print(f"[poller] Final migrate pass: {migrated} file(s)")


def process_entries(
    entries: list[tuple[int, dict[str, str]]], output_dir: Path, max_workers: int
) -> None:
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for idx, row in entries:
            url = row.get("Url", "").strip()
            title = row.get("Title", "").strip()
            key = row.get("Key", "").strip()
            futures.append(executor.submit(download_pdf, idx, url, title, key, output_dir))

        for future in as_completed(futures):
            msg, _, _ = future.result()
            print(msg)


def download_pdf(idx: int, url: str, title: str, key: str, output_dir: Path) -> tuple[str, bool, str]:
    if not url:
        record_status("skipped")
        return f"[{idx}] No URL", False, "skipped"

    arxiv_id = extract_arxiv_id(url)
    if not arxiv_id:
        record_unsupported(idx, url, title or key)
        return f"[{idx}] Skipped non-arXiv URL: {url}", True, "unsupported"

    submitted_date = fetch_submitted_date(arxiv_id)
    if not submitted_date:
        submitted_date = "00000000"
        record_status("metadata_fallback")

    display_title = title or key or "Untitled"
    filename = build_output_filename(submitted_date, display_title, arxiv_id)
    output_path = output_dir / filename

    if submitted_date != "00000000":
        migrated, message = try_migrate_placeholder_file(output_dir, output_path, display_title, arxiv_id)
        if migrated:
            record_status("skipped")
            return f"[{idx}] Skipped: {message}", True, "skipped"

    if output_path.exists() and validate_pdf(output_path):
        record_status("skipped")
        return f"[{idx}] Skipped: Valid PDF exists: {display_title[:50]}...", True, "skipped"

    if output_path.exists() and not validate_pdf(output_path):
        output_path.unlink(missing_ok=True)

    temp_path = output_path.with_suffix(output_path.suffix + f".part.{idx}")

    try:
        response = requests.get(
            get_pdf_url(arxiv_id),
            timeout=60,
            stream=True,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        response.raise_for_status()

        with open(temp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        if not validate_pdf(temp_path):
            temp_path.unlink(missing_ok=True)
            record_status("failed")
            return f"[{idx}] Downloaded file is invalid", False, "failed"

        if output_path.exists() and validate_pdf(output_path):
            temp_path.unlink(missing_ok=True)
            record_status("skipped")
            return f"[{idx}] Skipped: Valid PDF exists: {display_title[:50]}...", True, "skipped"

        temp_path.replace(output_path)
        record_status("downloaded")
        return f"[{idx}] Downloaded: {display_title[:50]}...", True, "downloaded"
    except Exception as e:
        temp_path.unlink(missing_ok=True)
        record_status("failed")
        return f"[{idx}] Error: {e}", False, "failed"


def print_summary(total: int) -> None:
    print("\n" + "=" * 60)
    print("Download complete!")
    print(f"  Downloaded: {downloaded}")
    print(f"  Skipped: {skipped}")
    print(f"  Failed: {failed}")
    print(f"  Non-arXiv skipped: {unsupported}")
    print(f"  Date fallback (00000000): {metadata_fallback}")
    print(f"  Total rows: {total}")
    print("=" * 60)

    if unsupported_items:
        print("\nNon-arXiv rows skipped:")
        for idx, title, url in unsupported_items[:20]:
            print(f"  - [{idx}] {title[:60]} | {url}")
        if len(unsupported_items) > 20:
            print(f"  ... and {len(unsupported_items) - 20} more")


def main() -> None:
    csv_path = Path("main.csv")
    output_dir = Path("output")

    if not csv_path.exists():
        print(f"Error: {csv_path} not found!")
        return

    output_dir.mkdir(exist_ok=True)

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    total = len(rows)
    doc_id_to_arxiv_id, doc_id_to_title = build_doc_id_maps(rows)
    print(f"Found {total} rows in CSV")
    priority_entries, normal_entries = split_rows_for_processing(rows, output_dir)
    print(
        f"Starting parallel download... (phase 1 placeholders: {len(priority_entries)}, "
        f"phase 2 others: {len(normal_entries)})\n"
    )

    max_workers = 10
    if priority_entries:
        print("Phase 1: handling rows with existing 00000000 placeholder files...\n")
        process_entries(priority_entries, output_dir, max_workers)
        print("\nPhase 2: handling remaining rows...\n")

    stop_event = threading.Event()
    poller_thread = threading.Thread(
        target=placeholder_polling_worker,
        args=(output_dir, doc_id_to_arxiv_id, doc_id_to_title, stop_event),
        name="placeholder-poller",
        daemon=True,
    )
    poller_thread.start()
    try:
        process_entries(normal_entries, output_dir, max_workers)
    finally:
        stop_event.set()
        poller_thread.join()

    print_summary(total)


if __name__ == "__main__":
    main()
