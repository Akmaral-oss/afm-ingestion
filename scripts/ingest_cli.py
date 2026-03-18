#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging

from app.logging_config import setup_logging
from app.config import Settings
from app.ingestion.pipeline import IngestionPipeline


def main():

    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--pg",
        required=True,
        help="postgresql+psycopg2://user:pass@host:5432/dbname"
    )

    ap.add_argument(
        "--model",
        default=None,
        help="Local embedding model path (e.g. models/bge-m3). Optional."
    )

    ap.add_argument(
        "--threshold",
        type=float,
        default=0.85,
        help="Embedding threshold for header->canonical mapping."
    )

    ap.add_argument(
        "--format_sim",
        type=float,
        default=0.92,
        help="Format registry embedding similarity threshold."
    )

    ap.add_argument(
        "--bank",
        default=None,
        help="Force bank label: halyk|kaspi|..."
    )

    ap.add_argument(
        "--rawjson",
        action="store_true",
        help="Store raw_row_json in transactions_core."
    )

    ap.add_argument(
        "--data",
        help="Root folder with bank subfolders (data/kaspi, data/halyk)"
    )

    ap.add_argument(
        "--loglevel",
        default="INFO",
        help="DEBUG|INFO|WARNING|ERROR"
    )

    ap.add_argument(
        "files",
        nargs="*",
        help="Direct XLSX files"
    )

    args = ap.parse_args()

    setup_logging(getattr(logging, args.loglevel.upper(), logging.INFO))

    settings = Settings(
        pg_dsn=args.pg,
        embedding_model_path=args.model,
        embedding_threshold=args.threshold,
        format_similarity_threshold=args.format_sim,
        store_raw_row_json=args.rawjson,
    )

    with IngestionPipeline(settings) as pipe:

        # -------- MODE 1: folder ingestion --------
        if args.data:

            pipe.ingest_data_folder(args.data)

        # -------- MODE 2: file ingestion --------
        elif args.files:

            for f in args.files:
                pipe.ingest_file(f, source_bank=args.bank)

        else:

            raise SystemExit(
                "Provide either --data <folder> or XLSX files"
            )


if __name__ == "__main__":
    main()
