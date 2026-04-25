import camelot
import pandas as pd
import sys
import os


def extract_tables_from_pdf(pdf_path: str, pages: str = "all", flavor: str = "stream") -> list[pd.DataFrame]:
    """
    Extract tables from a text-based PDF.

    Args:
        pdf_path: Path to the PDF file.
        pages:    Pages to extract from. E.g. "1", "1,3", "1-5", or "all".
        flavor:   "lattice" for tables with visible borders,
                  "stream"  for tables with whitespace-separated columns.

    Returns:
        A list of pandas DataFrames, one per detected table.
    """
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    print(f"Extracting tables from: {pdf_path}  (pages={pages}, flavor={flavor})")

    tables = camelot.read_pdf(pdf_path, pages=pages, flavor=flavor)

    print(f"Found {len(tables)} table(s).\n")

    dataframes = []
    for i, table in enumerate(tables):
        report = table.parsing_report
        print(f"--- Table {i + 1} | Page {report['page']} | "
              f"Accuracy: {report['accuracy']:.1f}% | "
              f"Whitespace: {report['whitespace']:.1f}% ---")
        df = table.df
        print(df.to_string(index=False))
        print()
        dataframes.append(df)

    return dataframes


def save_tables(dataframes: list[pd.DataFrame], pdf_path: str, output_dir: str = ".") -> None:
    """Save each extracted table as a CSV file, prefixed with the source PDF filename."""
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    for i, df in enumerate(dataframes):
        path = os.path.join(output_dir, f"{base_name}_table_{i + 1}.csv")
        df.to_csv(path, index=False, header=False)
        print(f"Saved: {path}")


if __name__ == "__main__":
    # Usage: python extract_tables.py report.pdf [pages] [lattice|stream]
    if len(sys.argv) < 2:
        print("Usage: python extract_tables.py <pdf_path> [pages] [flavor]")
        print("  pages  — e.g. '1', '1,3', '1-5', 'all'  (default: all)")
        print("  flavor — 'lattice' or 'stream'            (default: stream)")
        sys.exit(1)

    pdf_path = sys.argv[1]
    pages    = sys.argv[2] if len(sys.argv) > 2 else "all"
    flavor   = sys.argv[3] if len(sys.argv) > 3 else "stream"

    try:
        dfs = extract_tables_from_pdf(pdf_path, pages=pages, flavor=flavor)
        save_tables(dfs, pdf_path, output_dir="output")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
