import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from logistics_ops.bootstrap import build_sync_use_case


def main() -> None:
    result = build_sync_use_case().execute()

    print("Dataset synchronization completed.")
    print(f"Bucket: {result.bucket}")
    print(f"Prefix: {result.prefix}")
    print(f"Total files discovered: {result.total_files}")
    print(f"Uploaded: {result.uploaded_files}")
    print(f"Skipped: {result.skipped_files}")


if __name__ == "__main__":
    main()
