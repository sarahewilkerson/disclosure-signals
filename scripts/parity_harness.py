#!/usr/bin/env python3
from signals.cli import default_derived_db, parity_summary, vertical_slice_payload, repo_root


def main() -> None:
    fixture_dir = repo_root() / "tests" / "fixtures" / "vertical_slice"
    payload = vertical_slice_payload(str(default_derived_db()), fixture_dir)
    print(parity_summary(payload, fixture_dir))


if __name__ == "__main__":
    main()

