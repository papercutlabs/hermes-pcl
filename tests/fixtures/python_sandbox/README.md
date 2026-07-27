# Python sandbox fixtures

The test module builds its SQLite and CSV inputs under `tmp_path` from
deterministic row generators. Keeping the fixture recipe in Python avoids
committing mutable database binaries while preserving exact expected counts.
