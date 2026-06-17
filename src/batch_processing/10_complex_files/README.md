# Complex Files — Read and Parse PDF, XML, XLSX

Demonstrates reading and parsing data from complex file formats: PDF (with text
extraction, color detection, and coordinate-based scraping), XML (tree walking
and node extraction), and Excel (column inspection, row extraction, filtering,
and CSV export).

## Task

Read and parse data from complex files:

1. **PDF-1** — extract accuracy results from `example-PDF-1.pdf`.
2. **PDF-2** — extract red-colored text; extract captions from the bottom of page 2.
3. **XML** — read `example-XML-1.xml`, print the document tree, extract `<company_description>`.
4. **XLSX** — read `example-XLSX-1.xlsx`, print column names, extract row index 50,
   filter rows where `Age > 40`, save results to CSV.

## Results

### PDF-1 — Accuracy results

| Disability | Accuracy | Time to complete |
|---|---|---|
| Blind | 34.5%, n=1 | 1199 sec, n=1 |
| Low Vision | 98.3% n=2 (97.7%, n=3) | 1716 sec, n=3 (1934 sec, n=2) |
| Dexterity | 98.3%, n=4 | 1672.1 sec, n=4 |
| Mobility | 95.4%, n=3 | 1416 sec, n=3 |

### PDF-2 — Red-colored text

Two safety-warning paragraphs (RGB 255,0,0):
- *"Safety precautions in repeating this experiment include using safety goggles
  or safety spectacles and avoiding short circuits."*
- *"Safety precautions in repeating this experiment include hooded ventilation,
  chemical-splash safety goggles, gloves, and apron. Do not use bleach, ammonia,
  or strong acids with children."*

### PDF-2 — Captions from bottom of page 2

`© 2006 WGBH Educational Foundation. All rights reserved.` + data table rows
(distance/time data for Example 5).

### XML — Document tree + company_description

Tree: `<page> → <pageurl>, <record> → <uniq_id>, <job_title>, <company_name>, ...`

`<company_description>`: *"About this company Broadgate Estates"*

### XLSX — Column names, row 50, age filter

Columns: `[0, First Name, Last Name, Gender, Country, Age, Date, Id]`

Row index 50: `Dulce Abril, Female, United States, Age 32`

Rows with Age > 40: **14 rows** → saved to `data/output/age_over_40.csv`.

## Libraries used

| Library | Purpose | File types |
|---|---|---|
| **pdfquery** | PDF → XML tree, coordinate-based text extraction | PDF-1 |
| **PyMuPDF (fitz)** | Text extraction with color/position metadata | PDF-1, PDF-2 |
| **xml.etree.ElementTree** | XML parsing, tree walking, node extraction | XML |
| **pandas + openpyxl** | Excel reading, filtering, CSV export | XLSX |

### Why two PDF libraries?

- **pdfquery** converts PDF to an XML tree — great for structured scraping by
  bounding-box coordinates (the approach from the referenced tutorial). It dumps
  the full tree to `data/output/pdf1_tree.xml`.
- **PyMuPDF (fitz)** extracts text with **color metadata** (RGB per span) —
  required for finding red text in PDF-2. It also gives more reliable raw text
  extraction than pdfquery for PDF-1's accuracy table.

## Layout

```
10_complex_files/
├── solution.py        # all parsing: PDF, XML, XLSX
├── data/              # gitignored
│   ├── input/
│   │   ├── example-PDF-1.pdf
│   │   ├── example-PDF-2.pdf
│   │   ├── example-XML-1.xml
│   │   └── example-XLSX-1.xlsx
│   └── output/
│       ├── pdf1_tree.xml          # pdfquery XML dump of PDF-1
│       └── age_over_40.csv        # filtered XLSX rows (Age > 40)
└── logs/
    └── complex_files.log
```

## Prerequisites

- **Python** managed by `uv` (`uv sync` at the repo root).
- Dependencies added to `pyproject.toml`: `pdfquery`, `pymupdf`, `openpyxl`, `lxml`.
- Input files placed manually in `data/input/` (from task references).

## Run

```bash
uv run python src/batch_processing/10_complex_files/solution.py
```

## Acceptance criteria — status

| # | Criterion | Status |
| --- | --- | --- |
| 1 | Data successfully extracted from PDF files | ✅ accuracy table + red text + captions |
| 2 | Data successfully extracted from XML file | ✅ tree printed + `<company_description>` extracted |
| 3 | Data successfully extracted from XLSX file | ✅ columns, row 50, age filter, CSV saved |

## Implementation notes

- **No Spark in this task.** The task is about parsing complex file formats, not
  distributed processing. Pure Python libraries are the right tool.
- **Input files are gitignored** (`**/data/input/`). They must be placed manually
  from the task references.
- **Color detection** uses PyMuPDF's span-level `color` attribute (integer RGB).
  Red = `0xFF0000` = `(255, 0, 0)`.
