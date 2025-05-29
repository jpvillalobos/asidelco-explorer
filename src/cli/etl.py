
"""Thin CLI orchestrator for CFIA‑Explorer ETL.

Usage:
    python -m cfia_explorer.cli.etl --base-dir data --from-step extract
"""
import argparse, subprocess, os
from pathlib import Path
from datetime import datetime

STEPS = [
    ("extract_projects", "cfia_explorer.etl.extract.project_crawler"),
    ("extract_professionals", "cfia_explorer.etl.extract.professional_crawler"),
    ("html_to_json", "cfia_explorer.etl.extract.html_to_json"),
    ("parse_members", "cfia_explorer.etl.transform.parse_members"),
    ("enrich", "cfia_explorer.etl.transform.enrich"),
    ("openai_embed", "cfia_explorer.embeddings.openai"),
    ("merge_excel", "cfia_explorer.etl.transform.merge_excel"),
    ("to_excel", "cfia_explorer.etl.load.to_excel"),
    ("load_neo4j", "cfia_explorer.etl.load.neo4j"),
    ("load_opensearch", "cfia_explorer.etl.load.opensearch"),
]

def run_step(name, module, env, base_dir):
    marker = Path(base_dir) / f".DONE_{name}"
    if marker.exists():
        print(f"[SKIP] {name} (marker exists)")
        return
    print(f"[RUN ] {name}")
    code = f"import {module} as m; import sys, pathlib; " \
           f"sys.argv = [''] ; m.main(*m.__dict__.get('DEFAULT_ARGS', ()))"
    subprocess.check_call(["python", "-c", code], env=env)
    marker.touch()
    print(f"[DONE] {name}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-dir", default="data", help="root data folder")
    ap.add_argument("--from-step", choices=[n for n, _ in STEPS], default=STEPS[0][0])
    args = ap.parse_args()

    base = Path(args.base_dir).resolve()
    (base / "logs").mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["CFIA_BASE_DIR"] = str(base)
    idx = next(i for i,(n,_) in enumerate(STEPS) if n==args.from_step)
    for name, module in STEPS[idx:]:
        run_step(name, module, env, base)

if __name__ == "__main__":
    main()
