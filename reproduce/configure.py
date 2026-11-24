"""Generate Gateway inputs for an Experiment 2 arm, without credentials."""
import argparse
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def save(path, data):
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("case", choices=("T1", "T2", "T3", "T4", "T5"))
    parser.add_argument("pair", type=int, choices=range(1, 6))
    parser.add_argument("sequence", choices=("reference", "adaptive"))
    parser.add_argument("run_id", help="Unique ID for Gateway and the external workload")
    parser.add_argument("--routing", type=Path, default=ROOT / "config/routing.json",
                        help="Deployment routing table prepared from the example")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", args.run_id):
        parser.error("run_id must contain 1 to 64 letters, digits, hyphens, or underscores")

    case = load(ROOT / "reproduce/cases" / (args.case + ".json"))
    pair, = [entry for entry in case["pairs"] if entry["pair"] == args.pair]
    plan = case["gateway_plan"]
    routing = load(args.routing)
    starting = plan["cases"][args.case].get("startingArchitecture", plan["defaultArchitecture"])
    if starting not in routing.get("backends", {}):
        parser.error(f"Routing table has no backend for the starting architecture {starting}")
    routing["active"] = starting
    plan["run"] = {
        "runId": args.run_id, "indicator": args.case, "workload": case["workload"], "sequence": args.sequence,
    }
    workload = {key: value for key, value in case.items() if key not in ("gateway_plan", "pairs")}
    workload.update(pair=pair, sequence=args.sequence, run_id=args.run_id)

    relative = Path("config/generated") / args.run_id
    output = ROOT / relative
    output.mkdir(parents=True, exist_ok=False)
    save(output / "routing.json", routing)
    save(output / "selection-experiment.json", plan)
    save(output / "workload.json", workload)
    (output / "experiment.env").write_text(
        "PDMS_GATEWAY_EXPERIMENT_ENABLED=true\n"
        "PDMS_GATEWAY_SELECTION_ALLOW_APPLY=true\n"
        f"PDMS_GATEWAY_RUN_ID={args.run_id}\n"
        f"PDMS_GATEWAY_ROUTING_FILE_HOST=./{relative.as_posix()}/routing.json\n"
        f"PDMS_GATEWAY_EXPERIMENT_FILE=file:/app/experiments/generated/{args.run_id}/selection-experiment.json\n",
        encoding="utf-8",
    )
    print(output)


if __name__ == "__main__":
    main()
