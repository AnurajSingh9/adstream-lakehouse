"""Land raw JSONL into bronze. Bad rows go to quarantine — we don't silently drop money events."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from jsonschema import Draft202012Validator

from pipelines.common.paths import QUARANTINE, ROOT, bronze_dt

SCHEMA_PATH = ROOT / "schemas" / "ad_event.schema.json"


def _validator() -> Draft202012Validator:
    schema = json.loads(SCHEMA_PATH.read_text())
    return Draft202012Validator(schema)


def land_file(src: Path, ds: str) -> tuple[int, int]:
    v = _validator()
    out_dir = bronze_dt(ds)
    out_dir.mkdir(parents=True, exist_ok=True)
    q_dir = QUARANTINE / f"dt={ds}"
    q_dir.mkdir(parents=True, exist_ok=True)

    ok_path = out_dir / f"part-{src.stem}.jsonl"
    bad_path = q_dir / f"bad-{src.stem}.jsonl"

    n_ok = n_bad = 0
    with src.open() as fin, ok_path.open("w") as fout, bad_path.open("w") as fbad:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                v.validate(row)
                # stamp ingest if missing — producers sometimes forget
                row.setdefault("ingest_ts", datetime.now(timezone.utc).isoformat())
                fout.write(json.dumps(row, separators=(",", ":")) + "\n")
                n_ok += 1
            except Exception as exc:  # noqa: BLE001 — quarantine anything ugly
                fbad.write(json.dumps({"raw": line, "error": str(exc)}) + "\n")
                n_bad += 1

    if n_bad == 0 and bad_path.exists():
        bad_path.unlink()
    return n_ok, n_bad


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--src", required=True, type=Path)
    p.add_argument("--ds", required=True, help="partition date YYYY-MM-DD")
    args = p.parse_args()
    ok, bad = land_file(args.src, args.ds)
    print(f"landed ok={ok} quarantined={bad} → {bronze_dt(args.ds)}")


if __name__ == "__main__":
    main()
