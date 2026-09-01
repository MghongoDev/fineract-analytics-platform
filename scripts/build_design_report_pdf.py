#!/usr/bin/env python3
"""Render docs/DESIGN_REPORT.md to a print-quality PDF.

Markdown -> styled HTML -> headless Chromium print-to-PDF.

Chromium rather than wkhtmltopdf or a Python PDF library: it is the only
option here with real support for `page-break-inside: avoid`, repeating
table headers across pages, and a footer template with page numbers -
all of which a design report with wide tables and long code blocks
actually needs.

    python scripts/build_design_report_pdf.py [--input FILE] [--output FILE]
"""

from __future__ import annotations

import argparse
import asyncio
import re
from datetime import date
from pathlib import Path

import markdown

REPO_ROOT = Path(__file__).resolve().parents[1]

CSS = """
@page {
    size: A4;
    margin: 18mm 16mm 20mm 16mm;
}

:root {
    --ink:        #14161a;
    --muted:      #5b6472;
    --rule:       #d9dee5;
    --accent:     #1f4e79;
    --accent-soft:#eef3f8;
    --code-bg:    #f6f7f9;
}

* { box-sizing: border-box; }

body {
    font-family: "DejaVu Sans", "Helvetica Neue", Helvetica, Arial, sans-serif;
    font-size: 9.6pt;
    line-height: 1.55;
    color: var(--ink);
    margin: 0;
    -webkit-print-color-adjust: exact;
    print-color-adjust: exact;
}

/* ---------------- cover ---------------- */
.cover {
    page-break-after: always;
    padding-top: 55mm;
    text-align: left;
    border-top: 4px solid var(--accent);
}
.cover .eyebrow {
    font-size: 9pt;
    letter-spacing: 0.16em;
    text-transform: uppercase;
    color: var(--muted);
    margin-bottom: 14mm;
}
.cover h1 {
    font-size: 27pt;
    line-height: 1.15;
    margin: 0 0 6mm 0;
    color: var(--accent);
    border: 0;
    padding: 0;
}
.cover .subtitle {
    font-size: 12.5pt;
    color: var(--ink);
    font-weight: 400;
    margin-bottom: 16mm;
    max-width: 135mm;
}
.cover .pipeline {
    font-family: "DejaVu Sans Mono", monospace;
    font-size: 8pt;
    color: var(--muted);
    background: var(--accent-soft);
    border-left: 3px solid var(--accent);
    padding: 5mm 6mm;
    margin-bottom: 16mm;
    white-space: pre;
}
.cover .meta {
    font-size: 9pt;
    color: var(--muted);
    border-top: 1px solid var(--rule);
    padding-top: 4mm;
}
.cover .meta b { color: var(--ink); font-weight: 600; }

/* ---------------- headings ---------------- */
h1, h2, h3, h4 { color: var(--accent); page-break-after: avoid; }

h1 {
    font-size: 16pt;
    margin: 0 0 5mm 0;
    padding-bottom: 2.5mm;
    border-bottom: 2px solid var(--accent);
    page-break-before: always;
}
/* The cover's own title must not inherit the section-break rule, or the
   break fires inside the cover and leaves an almost-empty first page. */
.cover h1 {
    page-break-before: auto;
    border-bottom: 0;
    padding-bottom: 0;
}
.cover + h1 { page-break-before: avoid; }

h2 {
    font-size: 12.5pt;
    margin: 8mm 0 3mm 0;
    padding-bottom: 1.5mm;
    border-bottom: 1px solid var(--rule);
}
h3 { font-size: 10.6pt; margin: 6mm 0 2mm 0; }
h4 { font-size: 9.8pt; margin: 5mm 0 2mm 0; color: var(--ink); }

p { margin: 0 0 3mm 0; orphans: 3; widows: 3; }

/* ---------------- lists ---------------- */
ul, ol { margin: 0 0 3.5mm 0; padding-left: 6mm; }
li { margin-bottom: 1.2mm; }
li > ul, li > ol { margin-top: 1.2mm; }

/* ---------------- tables ---------------- */
table {
    width: 100%;
    border-collapse: collapse;
    margin: 3mm 0 5mm 0;
    font-size: 8.4pt;
    page-break-inside: avoid;
}
thead { display: table-header-group; }   /* repeat headers across pages */
th {
    background: var(--accent-soft);
    color: var(--accent);
    text-align: left;
    font-weight: 600;
    padding: 2mm 2.5mm;
    border-bottom: 1.5px solid var(--accent);
    vertical-align: bottom;
}
td {
    padding: 1.8mm 2.5mm;
    border-bottom: 1px solid var(--rule);
    vertical-align: top;
}
tbody tr:nth-child(even) { background: #fbfcfd; }
table code { font-size: 7.8pt; }

/* ---------------- code ---------------- */
code {
    font-family: "DejaVu Sans Mono", "SFMono-Regular", Consolas, monospace;
    font-size: 8.4pt;
    background: var(--code-bg);
    border: 1px solid var(--rule);
    border-radius: 2px;
    padding: 0.3mm 1mm;
}
pre {
    background: var(--code-bg);
    border: 1px solid var(--rule);
    border-left: 3px solid var(--accent);
    border-radius: 2px;
    padding: 3mm 4mm;
    margin: 3mm 0 4mm 0;
    overflow-x: hidden;
    page-break-inside: avoid;
}
pre code {
    background: none;
    border: 0;
    padding: 0;
    font-size: 7.9pt;
    line-height: 1.4;
    white-space: pre-wrap;
    word-break: break-word;
}

/* ---------------- misc ---------------- */
hr {
    border: 0;
    border-top: 1px solid var(--rule);
    margin: 6mm 0;
}
blockquote {
    margin: 3mm 0;
    padding: 2mm 4mm;
    border-left: 3px solid var(--rule);
    color: var(--muted);
}
a { color: var(--accent); text-decoration: none; }
strong { font-weight: 600; }
em { color: var(--ink); }

/* Keep a heading with the paragraph or table that follows it. */
h2 + p, h2 + table, h3 + p, h3 + table, h3 + ul { page-break-before: avoid; }
"""

COVER = """
<div class="cover">
  <div class="eyebrow">Senior Data Engineer Assessment &middot; August 2026</div>
  <h1>Fineract Analytics Platform</h1>
  <div class="subtitle">Design Report &mdash; an end-to-end, CDC-driven
  analytics engineering pipeline from the Apache&nbsp;Fineract REST API to
  analytics-ready marts and point-in-time-correct ML features.</div>
  <div class="pipeline">Fineract API  &#8594;  PostgreSQL (OLTP)  &#8594;  \
Debezium / Kafka (CDC)  &#8594;  ClickHouse
                                                      &#8595;
       Airflow orchestrates  &#8594;  dbt: staging  &#8594;  intermediate  &#8594;  marts / ml
                                                      &#8595;
                                    BI  &middot;  ML training  &middot;  Grafana</div>
  <div class="meta">
    <b>Stack</b>&nbsp; Python 3.11 &middot; PostgreSQL 16 &middot; Debezium 2.7 \
&middot; Kafka 3.7 (KRaft) &middot; ClickHouse 24.8 &middot; dbt 1.8 &middot;
    Airflow 2.10 &middot; Prometheus &middot; Grafana &middot; Docker Compose \
&middot; GitHub Actions<br/>
    <b>Verified</b>&nbsp; 22 dbt models built on a real ClickHouse engine &middot;
    133 tests passing &middot; 5,510 rows ingested, 0 writes on re-run<br/>
    <b>Generated</b>&nbsp; {generated}
  </div>
</div>
"""

# Chromium's header/footer templates render in a restricted context:
# flexbox and external stylesheets are ignored, so the layout has to be a
# table with inline styles.
FOOTER = """
<table style="width:100%; border:0; border-collapse:collapse;
              font-family:'DejaVu Sans',Helvetica,Arial,sans-serif;
              font-size:7pt; color:#5b6472;">
  <tr>
    <td style="padding:0 0 0 16mm; text-align:left;">
      Fineract Analytics Platform &mdash; Design Report
    </td>
    <td style="padding:0 16mm 0 0; text-align:right;">
      Page <span class="pageNumber"></span> of <span class="totalPages"></span>
    </td>
  </tr>
</table>
"""

HEADER = '<div style="font-size:0; height:0; margin:0;"></div>'


def strip_leading_title(text: str) -> str:
    """Drop the source document's own H1/subtitle block - the PDF has a
    cover page, and repeating the title immediately after it looks like
    a mistake."""
    return re.sub(
        r"\A#\s+Design Report\s*\n+###[^\n]*\n+---\s*\n",
        "", text, count=1)


def build_html(markdown_text: str) -> str:
    body = markdown.markdown(
        strip_leading_title(markdown_text),
        extensions=["tables", "fenced_code", "codehilite", "sane_lists",
                    "attr_list", "md_in_html"],
        extension_configs={"codehilite": {"noclasses": True,
                                          "pygments_style": "friendly",
                                          "guess_lang": False}},
    )
    cover = COVER.format(generated=date.today().strftime("%d %B %Y"))
    return (
        "<!DOCTYPE html><html><head><meta charset='utf-8'>"
        f"<title>Fineract Analytics Platform - Design Report</title>"
        f"<style>{CSS}</style></head><body>{cover}{body}</body></html>"
    )


async def render(html_path: Path, pdf_path: Path) -> None:
    from playwright.async_api import async_playwright

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(args=["--no-sandbox"])
        page = await browser.new_page()
        await page.goto(html_path.as_uri(), wait_until="networkidle")
        await page.pdf(
            path=str(pdf_path),
            format="A4",
            print_background=True,
            display_header_footer=True,
            header_template=HEADER,
            footer_template=FOOTER,
            margin={"top": "16mm", "bottom": "18mm",
                    "left": "16mm", "right": "16mm"},
        )
        await browser.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path,
                        default=REPO_ROOT / "docs" / "DESIGN_REPORT.md")
    parser.add_argument("--output", type=Path,
                        default=REPO_ROOT / "docs" / "DESIGN_REPORT.pdf")
    args = parser.parse_args()

    html = build_html(args.input.read_text())
    html_path = args.output.with_suffix(".html")
    html_path.write_text(html)

    asyncio.run(render(html_path, args.output))
    html_path.unlink(missing_ok=True)

    size_kb = args.output.stat().st_size / 1024
    print(f"wrote {args.output} ({size_kb:.0f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
