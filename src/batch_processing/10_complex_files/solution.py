"""Read and parse data from complex files (PDF, XML, XLSX).

Demonstrates extracting structured data from non-trivial file formats:
    1. PDF-1: extract accuracy results (pdfquery — coordinate-based scraping).
    2. PDF-2: extract red-colored text + captions from bottom of page 2 (PyMuPDF).
    3. XML:   read, print document tree, extract <company_description>.
    4. XLSX:  read, print columns, extract row 50, filter age > 40, save to CSV.

Run:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/batch_processing/10_complex_files/solution.py
"""

import logging
import sys
from pathlib import Path
from xml.etree import ElementTree

import fitz  # PyMuPDF
import pandas as pd
import pdfquery

BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%y/%m/%d %H:%M:%S",
)
log = logging.getLogger("Complex Files")
log.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler(str(LOG_DIR / "complex_files.log"))
file_handler.setFormatter(formatter)
log.addHandler(file_handler)


# ── PDF-1: Extract accuracy results ──────────────────────────────────────

def extract_accuracy_from_pdf1(path: str) -> None:
    """Extract accuracy results from example-PDF-1.pdf using pdfquery + PyMuPDF."""
    log.info(f"Reading PDF-1: {path}")

    # Use pdfquery to dump the XML tree (shows document structure).
    pdf = pdfquery.PDFQuery(path)
    pdf.load()

    tree_path = str(OUTPUT_DIR / "pdf1_tree.xml")
    pdf.tree.write(tree_path, pretty_print=True)
    log.info(f"PDF-1 XML tree written to {tree_path}")

    # Use PyMuPDF to extract all text (more reliable for content).
    doc = fitz.open(path)
    page = doc[0]

    print("\n" + "=" * 60)
    print("PDF-1: Full text content")
    print("=" * 60)
    print(page.get_text())

    # Extract accuracy-related lines.
    print("=" * 60)
    print("PDF-1: Accuracy results")
    print("=" * 60)
    for line in page.get_text().splitlines():
        line = line.strip()
        if "%" in line or "Accuracy" in line or "sec" in line:
            print(f"  {line}")


# ── PDF-2: Extract red text + captions from page 2 bottom ────────────────

def extract_red_text_from_pdf2(path: str) -> None:
    """Extract red-colored text from example-PDF-2.pdf using PyMuPDF."""
    log.info(f"Reading PDF-2: {path}")

    doc = fitz.open(path)
    log.info(f"PDF-2 has {len(doc)} pages")

    print("\n" + "=" * 60)
    print("PDF-2: Red-colored text (RGB 255,0,0)")
    print("=" * 60)

    for page_num in range(len(doc)):
        page = doc[page_num]
        blocks = page.get_text("dict")["blocks"]

        for block in blocks:
            if "lines" not in block:
                continue
            for line in block["lines"]:
                for span in line["spans"]:
                    color = span["color"]
                    r = (color >> 16) & 0xFF
                    g = (color >> 8) & 0xFF
                    b = color & 0xFF

                    if r == 255 and g == 0 and b == 0:
                        print(f"  Page {page_num + 1}: {span['text']}")


def extract_captions_from_pdf2_page2(path: str) -> None:
    """Extract captions from the bottom of page 2 of example-PDF-2.pdf."""
    log.info("Extracting captions from bottom of PDF-2 page 2")

    doc = fitz.open(path)
    page = doc[1]  # page 2 (0-indexed)
    height = page.rect.height
    blocks = page.get_text("dict")["blocks"]

    print("\n" + "=" * 60)
    print("PDF-2: Captions from bottom of page 2")
    print("=" * 60)

    for block in blocks:
        if "lines" not in block:
            continue
        for line in block["lines"]:
            y_top = line["bbox"][1]
            # Bottom 20% of the page.
            if y_top > height * 0.8:
                text = " ".join(span["text"] for span in line["spans"]).strip()
                if text:
                    print(f"  y={y_top:.0f}: {text}")


# ── XML: Read, print tree, extract company_description ───────────────────

def process_xml(path: str) -> None:
    """Read example-XML-1.xml, print tree, extract <company_description>."""
    log.info(f"Reading XML: {path}")

    tree = ElementTree.parse(path)
    root = tree.getroot()

    # Print the document tree.
    print("\n" + "=" * 60)
    print("XML: Document tree")
    print("=" * 60)
    print_xml_tree(root, indent=0)

    # Extract <company_description>.
    print("\n" + "=" * 60)
    print("XML: <company_description> content")
    print("=" * 60)

    company_desc = root.find(".//company_description")
    if company_desc is not None and company_desc.text:
        print(f"  {company_desc.text.strip()}")
    else:
        print("  <company_description> not found")


def print_xml_tree(element: ElementTree.Element, indent: int) -> None:
    """Recursively print the XML element tree with indentation."""
    tag = element.tag
    text = (element.text or "").strip()
    prefix = "  " * indent

    if text and len(text) > 80:
        text = text[:80] + "..."

    if text:
        print(f"{prefix}<{tag}> {text}")
    else:
        print(f"{prefix}<{tag}>")

    for child in element:
        print_xml_tree(child, indent + 1)


# ── XLSX: Read, columns, row 50, filter age > 40, save CSV ──────────────

def process_xlsx(path: str) -> None:
    """Read example-XLSX-1.xlsx, print columns, row 50, filter age > 40."""
    log.info(f"Reading XLSX: {path}")

    df = pd.read_excel(path, engine="openpyxl")

    # Print column names.
    print("\n" + "=" * 60)
    print("XLSX: Column names")
    print("=" * 60)
    for i, col_name in enumerate(df.columns):
        print(f"  [{i}] {col_name}")

    # Extract and print row with index 50.
    print("\n" + "=" * 60)
    print("XLSX: Row with index 50")
    print("=" * 60)
    if 50 in df.index:
        row = df.loc[50]
        for col_name, value in row.items():
            print(f"  {col_name}: {value}")
    else:
        print("  Index 50 not found in DataFrame")

    # Filter rows where age > 40.
    print("\n" + "=" * 60)
    print("XLSX: Rows where Age > 40")
    print("=" * 60)
    age_col = "Age"
    filtered = df[df[age_col] > 40]
    log.info(f"Rows with age > 40: {len(filtered)}")
    print(filtered.to_string(index=True))

    # Save filtered results to CSV.
    csv_path = str(OUTPUT_DIR / "age_over_40.csv")
    filtered.to_csv(csv_path, index=False)
    log.info(f"Filtered data saved to {csv_path}")


# ── Main ─────────────────────────────────────────────────────────────────

def main() -> None:
    """Run all complex-file parsing tasks."""
    pdf1 = str(INPUT_DIR / "example-PDF-1.pdf")
    pdf2 = str(INPUT_DIR / "example-PDF-2.pdf")
    xml_file = str(INPUT_DIR / "example-XML-1.xml")
    xlsx_file = str(INPUT_DIR / "example-XLSX-1.xlsx")

    # 1. PDF-1: accuracy results.
    extract_accuracy_from_pdf1(pdf1)

    # 2. PDF-2: red text + captions.
    extract_red_text_from_pdf2(pdf2)
    extract_captions_from_pdf2_page2(pdf2)

    # 3. XML: tree + company_description.
    process_xml(xml_file)

    # 4. XLSX: columns, row 50, age filter, CSV.
    process_xlsx(xlsx_file)

    log.info("All complex-file tasks completed.")


if __name__ == "__main__":
    main()
