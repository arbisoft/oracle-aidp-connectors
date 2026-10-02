"""Put tests/ on sys.path so tests can import fakes. The package itself is installed by uv or pip."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
