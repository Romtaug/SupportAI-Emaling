#!/usr/bin/env python3
"""
run_weekly.py
--------------

Point d'entrée dédié à l'exécution hebdomadaire. Équivaut à
`python cli.py all update`, mais pensé pour être lancé par cron, Task
Scheduler Windows, ou une GitHub Action.

Comportement :
- Lance les 5 scrapers en séquence (update mode, upsert).
- Si un scraper plante, on log et on passe au suivant (les autres ne doivent
  pas être bloqués par un site qui tombe).
- En sortie : code 0 si tous les scrapers ont réussi, 1 si au moins un a
  échoué (pour que le runner CI puisse notifier).

Variables d'environnement optionnelles :
- PB_TEST=1      : mode test (volumes réduits)
- PB_VERTICALS=a,b,c : ne lancer que certains verticals (séparés par virgule)

Exemple :
    PB_TEST=1 python run_weekly.py
    PB_VERTICALS=hotels,immo python run_weekly.py
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

from core.utils import get_logger
from scrapers import REGISTRY


ROOT = Path(__file__).resolve().parent
LOG = get_logger("weekly", ROOT / "data" / "logs" / "weekly.log")


def main() -> int:
    test_mode = os.environ.get("PB_TEST", "").strip() in {"1", "true", "yes"}

    requested = os.environ.get("PB_VERTICALS", "").strip()
    if requested:
        verticals = [v.strip() for v in requested.split(",") if v.strip()]
        unknown = [v for v in verticals if v not in REGISTRY]
        if unknown:
            LOG.error("Verticals inconnus : %s", unknown)
            return 2
    else:
        verticals = sorted(REGISTRY)

    started = datetime.now(timezone.utc).isoformat()
    LOG.info("#" * 70)
    LOG.info("RUN HEBDO démarré à %s (test=%s)", started, test_mode)
    LOG.info("Verticals ciblés : %s", verticals)
    LOG.info("#" * 70)

    summary = {}
    exit_code = 0

    for vertical in verticals:
        LOG.info("")
        LOG.info(">>> %s", vertical.upper())

        try:
            scraper = REGISTRY[vertical](test_mode=test_mode)
            result = scraper.run(mode="update")
            summary[vertical] = {
                "status": "ok",
                "inserted": result.inserted,
                "updated": result.updated,
                "unchanged": result.unchanged,
            }
        except Exception as exc:
            LOG.error("Échec %s : %s\n%s",
                      vertical, exc, traceback.format_exc())
            summary[vertical] = {"status": "error", "error": repr(exc)}
            exit_code = 1

    ended = datetime.now(timezone.utc).isoformat()
    LOG.info("")
    LOG.info("#" * 70)
    LOG.info("RUN HEBDO terminé à %s", ended)
    for v, r in summary.items():
        LOG.info("  %-18s  %s", v, r)
    LOG.info("#" * 70)

    # Rapport JSON horodaté
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_dir = ROOT / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"weekly_{stamp}.json"

    report = {
        "started_at": started,
        "ended_at": ended,
        "test_mode": test_mode,
        "verticals": verticals,
        "summary": summary,
        "exit_code": exit_code,
    }
    report_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    LOG.info("Rapport JSON : %s", report_path)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
