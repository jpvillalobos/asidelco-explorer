"""Thin CLI orchestrator for CFIA‑Explorer ETL.

Usage:
    python -m cfia_explorer.cli.etl --base-dir data --from-step extract
"""
import sys
import json
import argparse
from pathlib import Path
import pandas as pd

# Ensure src is importable when running as module
sys.path.append(str(Path(__file__).parent.parent))

# Import only lightweight modules at top-level
from services.csv_service import CSVService
from services.storage_service import StorageService


def cmd_csv_stats(args: argparse.Namespace) -> int:
    csv = CSVService()
    df = csv.read_csv(args.input_file)
    stats = csv.get_statistics(df)

    print(f"Rows: {stats['rows']}")
    print(f"Columns: {stats['columns']}")
    print(f"Duplicates (excluding first occurrences): {stats['duplicates']}")
    print(f"Memory (MB): {stats['memory_usage']:.2f}")
    print("Column names:")
    for name in stats["column_names"]:
        print(f"  - {name}")

    # Show first 10 duplicate rows (all members of duplicate groups)
    dup_all_df = csv.get_duplicates(df, include_all_members=True)
    dup_all_count = len(dup_all_df)
    if dup_all_count > 0:
        print(f"\nDuplicate rows (all members): {dup_all_count}")
        preview = dup_all_df.head(10)
        # Print a compact preview
        with pd.option_context('display.max_columns', None, 'display.width', 200):
            print("First 10 duplicate rows:")
            print(preview.to_string(index=False))
    else:
        print("\nNo duplicate rows detected.")

    missing = {k: v for k, v in stats["missing_values"].items() if v > 0}
    if missing:
        print("\nMissing values:")
        for col, cnt in missing.items():
            pct = stats["missing_percentage"][col]
            print(f"  - {col}: {cnt} ({pct:.1f}%)")
    return 0


def cmd_normalize_csv(args: argparse.Namespace) -> int:
    csv = CSVService()

    def parse_json(s):
        if not s:
            return None
        try:
            return json.loads(s)
        except json.JSONDecodeError as e:
            raise SystemExit(f"Invalid JSON: {e}")

    col_map = parse_json(args.column_mapping)
    type_map = parse_json(args.type_mapping)
    remove_duplicates = not args.keep_duplicates

    result = csv.normalize_pipeline(
        file_path=args.input_file,
        output_path=args.output_file,
        column_mapping=col_map,
        type_mapping=type_map,
        text_columns=None,
        missing_value_strategy=args.missing_strategy,
        remove_duplicates=remove_duplicates,
        validation_rules=None,
        # NEW: ID options
        id_from_column=args.add_id_from,
        id_column_name=args.id_column_name,
        id_separator=args.id_separator,
        id_start=args.id_start,
    )

    print(f"Normalized CSV saved: {result['output_file']}")
    print(f"Rows: {result['final_rows']} (removed {result['rows_removed']})")
    print(f"Columns: {result['final_columns']}")
    return 0


def cmd_merge_csv(args: argparse.Namespace) -> int:
    csv = CSVService()
    on_cols = [c.strip() for c in args.on.split(",")] if args.on else None
    result_path = csv.merge_csv_files(
        file_paths=args.input_files,
        output_path=args.output_file,
        merge_strategy=args.strategy,
        on=on_cols,
        how=args.how,
    )
    print(f"Merged CSV saved: {result_path}")
    return 0


def cmd_csv_to_json(args: argparse.Namespace) -> int:
    storage = StorageService()
    csv = CSVService()
    df = csv.read_csv(args.csv_file)
    data = df.to_dict(orient="records")
    storage.save_json(data, str(args.json_file))
    print(f"JSON saved: {args.json_file}")
    return 0


def cmd_load_csv_to_opensearch(args: argparse.Namespace) -> int:
    # Lazy import to avoid side-effects unless used
    from services.opensearch_service import OpenSearchService

    csv = CSVService()
    osvc = OpenSearchService(host=args.opensearch_host, port=args.opensearch_port)
    df = csv.read_csv(args.input_file)
    docs = df.to_dict(orient="records")
    result = osvc.bulk_index(index_name=args.index_name, documents=docs, batch_size=args.batch_size)
    print(f"Indexed {result.get('indexed', 0)} documents into {args.index_name}")
    return 0


def cmd_add_embeddings(args: argparse.Namespace) -> int:
    from services.embedding_service import EmbeddingService

    csv = CSVService()
    df = csv.read_csv(args.csv_file)
    if args.text_column not in df.columns:
        raise SystemExit(f"Column not found: {args.text_column}")

    svc = EmbeddingService(provider=args.provider)
    texts = df[args.text_column].astype(str).tolist()
    embs = svc.generate_embeddings(texts, show_progress=True)
    df[args.embedding_column] = embs
    csv.write_csv(df, args.output_file)
    print(f"Saved with embeddings: {args.output_file}")
    return 0


def cmd_add_id(args: argparse.Namespace) -> int:
    csv = CSVService()
    df = csv.read_csv(args.input_file)
    df = csv.add_id_from_column(
        df,
        source_col=args.from_column,
        id_col=args.id_column_name,
        separator=args.id_separator,
        start=args.id_start,
        make_first=True,
        overwrite=True,
    )
    csv.write_csv(df, args.output_file)
    print(f"Added '{args.id_column_name}' from '{args.from_column}' -> {args.output_file}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="etl", description="ASIDELCO Explorer CSV ETL")
    sp = ap.add_subparsers(dest="command", required=True)

    p_stats = sp.add_parser("csv-stats", help="Show CSV statistics")
    p_stats.add_argument("input_file", type=Path)
    p_stats.set_defaults(func=cmd_csv_stats)

    p_norm = sp.add_parser("normalize-csv", help="Normalize a CSV and write a new CSV")
    p_norm.add_argument("input_file", type=Path)
    p_norm.add_argument("output_file", type=Path)
    p_norm.add_argument("--column-mapping", type=str, help="JSON mapping for column renames")
    p_norm.add_argument("--type-mapping", type=str, help="JSON mapping for dtypes (int,float,string,datetime,bool)")
    p_norm.add_argument("--missing-strategy", choices=["drop", "fill", "forward_fill", "backward_fill"], default="drop")
    p_norm.add_argument("--keep-duplicates", action="store_true", help="Keep duplicates instead of removing")
    # NEW: ID creation
    p_norm.add_argument("--add-id-from", dest="add_id_from", type=str, default=None, help="Create ID from this column (e.g., 'proyecto')")
    p_norm.add_argument("--id-column-name", type=str, default="id", help="Name of generated ID column")
    p_norm.add_argument("--id-separator", type=str, default="-", help="Separator between value and occurrence")
    p_norm.add_argument("--id-start", type=int, default=1, help="Starting occurrence number (default 1)")
    p_norm.set_defaults(func=cmd_normalize_csv)

    # NEW: dedicated add-id command
    p_add = sp.add_parser("add-id", help="Create an ID column from a source column")
    p_add.add_argument("input_file", type=Path)
    p_add.add_argument("output_file", type=Path)
    p_add.add_argument("--from-column", type=str, default="proyecto", help="Source column (default: proyecto)")
    p_add.add_argument("--id-column-name", type=str, default="id")
    p_add.add_argument("--id-separator", type=str, default="-")
    p_add.add_argument("--id-start", type=int, default=1)
    p_add.set_defaults(func=cmd_add_id)

    p_merge = sp.add_parser("merge-csv", help="Merge multiple CSV files")
    p_merge.add_argument("output_file", type=Path)
    p_merge.add_argument("input_files", nargs="+", type=Path)
    p_merge.add_argument("--strategy", choices=["concat", "join"], default="concat")
    p_merge.add_argument("--on", type=str, help="Join key(s) for 'join' (comma-separated)")
    p_merge.add_argument("--how", choices=["inner", "outer", "left", "right"], default="inner")
    p_merge.set_defaults(func=cmd_merge_csv)

    p_c2j = sp.add_parser("csv-to-json", help="Convert CSV to JSON (records)")
    p_c2j.add_argument("csv_file", type=Path)
    p_c2j.add_argument("json_file", type=Path)
    p_c2j.set_defaults(func=cmd_csv_to_json)

    p_os = sp.add_parser("load-csv-to-opensearch", help="Index CSV rows into OpenSearch")
    p_os.add_argument("input_file", type=Path)
    p_os.add_argument("--opensearch-host", required=True)
    p_os.add_argument("--opensearch-port", type=int, default=9200)
    p_os.add_argument("--index-name", required=True)
    p_os.add_argument("--batch-size", type=int, default=500)
    p_os.set_defaults(func=cmd_load_csv_to_opensearch)

    p_emb = sp.add_parser("add-embeddings", help="Add embeddings to a CSV column (OpenAI only)")
    p_emb.add_argument("csv_file", type=Path)
    p_emb.add_argument("output_file", type=Path)
    p_emb.add_argument("--text-column", required=True)
    # Only OpenAI now:
    p_emb.add_argument("--provider", choices=["openai"], default="openai")
    p_emb.add_argument("--embedding-column", default="embedding")
    p_emb.set_defaults(func=cmd_add_embeddings)

    return ap


def main():
    ap = build_parser()
    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
