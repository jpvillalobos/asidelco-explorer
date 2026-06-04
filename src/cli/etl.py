"""Thin CLI orchestrator for CFIA‑Explorer ETL.

Usage:
    python -m cfia_explorer.cli.etl --base-dir data --from-step extract
"""
import sys
import json
import argparse
import time
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


def cmd_fix_enrichment(args: argparse.Namespace) -> int:
    from services.validation_enrichment_service import ValidationEnrichmentService

    svc = ValidationEnrichmentService()
    result = svc.validate_and_enrich_streaming(
        input_file=str(args.input_file),
        output_file=str(args.output_file),
        chunk_size=args.chunk_size,
        progress_interval=args.progress_interval,
    )

    stats = result["stats"]
    print(f"Fixed enrichment: {result['output_file']}")
    print(f"Records: {result['count']}")
    print(f"Valid: {stats['records_valid']}")
    print(f"Invalid: {stats['records_invalid']}")
    print(f"Validation errors: {stats['validation_errors']}")
    return 0


def cmd_repair_summary_embeddings(args: argparse.Namespace) -> int:
    from services.enhancement_service import EnhancementService
    from services.embedding_service import EmbeddingService
    from services.validation_enrichment_service import ValidationEnrichmentService

    enhancement = EnhancementService()
    embedding_service = None
    if not args.skip_embeddings:
        embedding_service = EmbeddingService(model=args.embedding_model)

    reader = ValidationEnrichmentService()
    validation_service = ValidationEnrichmentService()
    output_path = Path(args.output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    processed = 0
    embeddings_generated = 0
    first_record = True
    batch = []
    started_at = time.time()
    last_reported_processed = None

    def report_progress(force: bool = False):
        nonlocal last_reported_processed
        if not force and (not args.progress_interval or processed % args.progress_interval != 0):
            return
        if processed == last_reported_processed:
            return

        elapsed = max(time.time() - started_at, 0.001)
        rate = processed / elapsed
        pct = None
        eta_seconds = None
        if args.total_records:
            pct = min((processed / args.total_records) * 100, 100)
            remaining = max(args.total_records - processed, 0)
            eta_seconds = remaining / rate if rate > 0 else None

        status = {
            "processed": processed,
            "embeddings_generated": embeddings_generated,
            "elapsed_seconds": round(elapsed, 1),
            "records_per_second": round(rate, 2),
            "output_file": str(output_path),
        }
        if pct is not None:
            status["percent"] = round(pct, 2)
        if eta_seconds is not None:
            status["eta_seconds"] = round(eta_seconds, 1)

        if args.status_file:
            Path(args.status_file).parent.mkdir(parents=True, exist_ok=True)
            Path(args.status_file).write_text(json.dumps(status, indent=2), encoding="utf-8")

        msg = (
            f"Processed {processed} records; embeddings regenerated: {embeddings_generated}; "
            f"{rate:.2f} records/sec"
        )
        if pct is not None:
            msg += f"; {pct:.2f}%"
        if eta_seconds is not None:
            msg += f"; ETA {eta_seconds / 60:.1f} min"
        print(msg, flush=True)
        last_reported_processed = processed

    def write_record(out, record):
        nonlocal first_record
        if not first_record:
            out.write(",\n")
        json.dump(record, out, ensure_ascii=False)
        first_record = False

    def flush_batch(out):
        nonlocal embeddings_generated
        if not batch:
            return

        if args.skip_embeddings:
            for record in batch:
                record.pop(args.embedding_field, None)
                write_record(out, record)
            batch.clear()
            return

        summaries = [record[args.text_field] for record in batch]
        embeddings = embedding_service.generate_embeddings_batch(
            summaries,
            batch_size=args.embedding_batch_size
        )
        for record, embedding in zip(batch, embeddings):
            record[args.embedding_field] = embedding
            record["embedding_model"] = embedding_service.model
            write_record(out, record)
        embeddings_generated += len(batch)
        batch.clear()

    with open(output_path, "w", encoding="utf-8") as out:
        out.write("[\n")
        for record in reader._iter_json_array(Path(args.input_file), chunk_size=args.chunk_size):
            summary = enhancement.build_project_search_summary(record)
            if not summary:
                summary = str(record.get(args.text_field, "") or "")

            record[args.text_field] = summary
            record["summary_model"] = "deterministic-search-summary-v1"
            record["summary_tokens"] = None
            record = validation_service._validate_and_enrich_record(record, {}, processed)

            batch.append(record)
            processed += 1

            if len(batch) >= args.record_batch_size:
                flush_batch(out)

            report_progress()

        flush_batch(out)
        report_progress(force=True)
        out.write("\n]\n")

    print(f"Repaired file: {output_path}")
    print(f"Records processed: {processed}")
    if args.skip_embeddings:
        print("Embeddings skipped and stale embedding fields removed.")
    else:
        print(f"Embeddings regenerated: {embeddings_generated}")
    print(f"Valid records: {validation_service.stats['records_valid']}")
    print(f"Invalid records: {validation_service.stats['records_invalid']}")
    print(f"Validation errors: {validation_service.stats['validation_errors']}")
    return 0


def cmd_add_geo_point(args: argparse.Namespace) -> int:
    from services.enhancement_service import EnhancementService
    from services.validation_enrichment_service import ValidationEnrichmentService

    enhancement = EnhancementService()
    reader = ValidationEnrichmentService()
    output_path = Path(args.output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    processed = 0
    updated = 0
    skipped = 0
    first_record = True
    started_at = time.time()

    with open(output_path, "w", encoding="utf-8") as out:
        out.write("[\n")
        for record in reader._iter_json_array(Path(args.input_file), chunk_size=args.chunk_size):
            processed += 1
            if enhancement.apply_location_field(record):
                updated += 1
            else:
                record.pop("location", None)
                skipped += 1

            if not first_record:
                out.write(",\n")
            json.dump(record, out, ensure_ascii=False)
            first_record = False

            if args.progress_interval and processed % args.progress_interval == 0:
                elapsed = max(time.time() - started_at, 0.001)
                rate = processed / elapsed
                print(
                    f"Processed {processed}; geo_point updated: {updated}; skipped: {skipped}; "
                    f"{rate:.2f} records/sec",
                    flush=True
                )

        out.write("\n]\n")

    print(f"Geo point output: {output_path}")
    print(f"Records processed: {processed}")
    print(f"Geo points updated: {updated}")
    print(f"Skipped without valid coordinates: {skipped}")
    return 0


def cmd_prepare_for_indexing(args: argparse.Namespace) -> int:
    from services.search_preparation_service import SearchPreparationService

    svc = SearchPreparationService()
    result = svc.prepare_for_indexing(
        input_file=str(args.input_file),
        output_file=str(args.output_file),
        embedding_field=args.embedding_field,
        expected_embedding_dim=args.expected_embedding_dim,
        summary_field=args.summary_field,
        chunk_size=args.chunk_size,
        progress_interval=args.progress_interval,
    )

    stats = result["stats"]
    print(f"Search-ready file: {result['output_file']}")
    print(f"Records processed: {result['count']}")
    print(f"Ready: {stats['ready']}")
    print(f"Not ready: {stats['not_ready']}")
    print(f"Locations: {stats['location_added']}")
    print(f"Missing locations: {stats['missing_location']}")
    print(f"Missing embeddings: {stats['missing_embedding']}")
    print(f"Bad embedding dimensions: {stats['bad_embedding_dimension']}")
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

    p_fix = sp.add_parser(
        "fix-enrichment",
        help="Rebuild validation/enrichment metadata for a large JSON array"
    )
    p_fix.add_argument("input_file", type=Path)
    p_fix.add_argument("output_file", type=Path)
    p_fix.add_argument("--chunk-size", type=int, default=1024 * 1024)
    p_fix.add_argument("--progress-interval", type=int, default=10000)
    p_fix.set_defaults(func=cmd_fix_enrichment)

    p_repair = sp.add_parser(
        "repair-summary-embeddings",
        help="Rebuild resumen and regenerate embeddings for a large JSON array"
    )
    p_repair.add_argument("input_file", type=Path)
    p_repair.add_argument("output_file", type=Path)
    p_repair.add_argument("--text-field", default="resumen")
    p_repair.add_argument("--embedding-field", default="embedding")
    p_repair.add_argument("--embedding-model", default="text-embedding-3-small")
    p_repair.add_argument("--chunk-size", type=int, default=1024 * 1024)
    p_repair.add_argument("--record-batch-size", type=int, default=64)
    p_repair.add_argument("--embedding-batch-size", type=int, default=64)
    p_repair.add_argument("--progress-interval", type=int, default=5000)
    p_repair.add_argument("--total-records", type=int, default=None)
    p_repair.add_argument("--status-file", type=Path, default=None)
    p_repair.add_argument(
        "--skip-embeddings",
        action="store_true",
        help="Only rebuild summaries and remove stale embedding fields"
    )
    p_repair.set_defaults(func=cmd_repair_summary_embeddings)

    p_geo = sp.add_parser(
        "add-geo-point",
        help="Add OpenSearch geo_point location from latitude/longitude to a JSON array"
    )
    p_geo.add_argument("input_file", type=Path)
    p_geo.add_argument("output_file", type=Path)
    p_geo.add_argument("--chunk-size", type=int, default=1024 * 1024)
    p_geo.add_argument("--progress-interval", type=int, default=10000)
    p_geo.set_defaults(func=cmd_add_geo_point)

    p_prepare = sp.add_parser(
        "prepare-for-indexing",
        help="Build the canonical search_ready.json file for OpenSearch/Neo4j loading"
    )
    p_prepare.add_argument("input_file", type=Path)
    p_prepare.add_argument("output_file", type=Path)
    p_prepare.add_argument("--summary-field", default="resumen")
    p_prepare.add_argument("--embedding-field", default="embedding")
    p_prepare.add_argument("--expected-embedding-dim", type=int, default=1536)
    p_prepare.add_argument("--chunk-size", type=int, default=1024 * 1024)
    p_prepare.add_argument("--progress-interval", type=int, default=10000)
    p_prepare.set_defaults(func=cmd_prepare_for_indexing)

    return ap


def main():
    ap = build_parser()
    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
