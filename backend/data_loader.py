import pandas as pd
import sqlite3
from pathlib import Path
import sys
import re


def load_drugbank_data(data_path: str = None, sqlite_path: str = None):
    """Load DrugBank cleaned data (CSV or Excel) into a SQLite database.

    - If data_path is None, looks for common files in the repo root or `data/`:
      `drugbank_clean.csv`, `drugbank_cleaned.xlsx` or variants.
    - If sqlite_path is None, defaults to `db/polypharm.db` under repo root.

    Returns the sqlite3.Connection on success.
    """
    repo_root = Path(__file__).resolve().parents[1]

    # Determine data file
    candidates = []
    if data_path:
        candidates.append(Path(data_path))
    # common locations
    candidates += [
        repo_root / 'data' / 'drugbank_cleaned.xlsx',
        repo_root / 'data' / 'drugbank_cleaned.xls',
        repo_root / 'data' / 'drugbank_clean.xlsx',
        repo_root / 'data' / 'drugbank_clean.csv',
        repo_root / 'data' / 'drugbank_cleaned.csv',
        repo_root / 'drugbank_clean.csv',
        repo_root / 'drugbank_cleaned.xlsx',
        repo_root / 'drugbank_cleaned.csv',
    ]

    data_file = None
    for p in candidates:
        if p and p.exists():
            data_file = p
            break

    if data_file is None:
        raise FileNotFoundError(
            'Could not find drugbank cleaned data. Searched: ' + ', '.join(str(p) for p in candidates)
        )

    # Determine sqlite path
    if sqlite_path:
        sqlite_file = Path(sqlite_path)
    else:
        sqlite_file = repo_root / 'db' / 'polypharm.db'

    sqlite_file.parent.mkdir(parents=True, exist_ok=True)

    print(f'Loading data from: {data_file}')
    # Read using pandas
    if data_file.suffix.lower() in ['.csv']:
        try:
            df = pd.read_csv(data_file, low_memory=False, encoding='utf-8')
        except Exception:
            # try latin-1 as fallback
            df = pd.read_csv(data_file, low_memory=False, encoding='latin-1')
    else:
        # let pandas infer engine
        df = pd.read_excel(data_file)

    # Connect and write to SQL
    conn = sqlite3.connect(str(sqlite_file))
    df.to_sql('drugbank', conn, if_exists='replace', index=False)

    # Create indexes for speed if the columns exist. Quote column names that may contain hyphens.
    def quoted(col: str) -> str:
        return f'"{col}"'

    for col, idx_name in [("drugbank-id", 'idx_drug_id'), ('name', 'idx_name')]:
        if col in df.columns:
            sql = f'CREATE INDEX IF NOT EXISTS {idx_name} ON drugbank({quoted(col)})'
            try:
                conn.execute(sql)
            except Exception:
                # best-effort: continue if index creation fails
                print(f'Warning: failed to create index on column {col}')

    # Parse complex fields into relational tables
    try:
        parse_interactions(conn)
    except Exception as e:
        print('Warning: parse_interactions failed:', e)

    print(f'Cleaned DrugBank data loaded into SQLite at: {sqlite_file}')
    return conn


def parse_interactions(conn: sqlite3.Connection):
    """Parse the `drug-interactions` column from `drugbank` into a separate
    `drug_interactions` table with columns: drugbank-id, interacting_drugbank-id, effect.

    This is a best-effort parser that handles formats like:
      - DB00316:Increased risk,DB00635:Increased risk
      - DB00316,DB00635:Increased risk
      - DB00316 (Increased risk); DB00635 - Increased risk
    """
    # Read the two columns; quote hyphen-containing names for SQL
    try:
        df = pd.read_sql('SELECT "drugbank-id", "drug-interactions" FROM drugbank', conn)
    except Exception:
        # If the quoted names fail, try unquoted (older DBs)
        df = pd.read_sql('SELECT "drugbank-id", "drug-interactions" FROM drugbank', conn)

    interactions = []

    id_pattern = re.compile(r'\bDB\d+\b', flags=re.IGNORECASE)

    for _, row in df.iterrows():
        drug_id = row.get('drugbank-id')
        raw = row.get('drug-interactions')
        if pd.isna(raw) or not str(raw).strip():
            continue

        text = str(raw)

        # First, split into candidate chunks by common separators
        chunks = re.split(r'[;\n]|\|', text)
        # If chunks is one long chunk, also split by comma but carefully:
        if len(chunks) == 1:
            # But some effects themselves contain commas; try to split by ',DB' to keep effects with commas
            chunks = re.split(r',(?=\s*DB\d+)', text)

        for chunk in chunks:
            chunk = chunk.strip()
            if not chunk:
                continue

            # If there is a ':' assume id(s) before and effect after
            if ':' in chunk:
                left, right = chunk.split(':', 1)
                effect = right.strip()
                # left may have multiple IDs separated by commas or slashes
                ids = re.split(r'[,&/]|\s+', left)
                for iid in ids:
                    iid = iid.strip()
                    if not iid:
                        continue
                    # ensure it's an ID like DB12345; if not, try to extract
                    match = id_pattern.search(iid)
                    if match:
                        interacting = match.group(0)
                    else:
                        interacting = iid
                    interactions.append((drug_id, interacting, effect))
                continue

            # If no colon, try to find ID(s) and effect in parentheses or after a dash
            paren = re.search(r'\(([^)]+)\)', chunk)
            if paren:
                effect = paren.group(1).strip()
                ids_part = chunk[:paren.start()]
                ids = id_pattern.findall(ids_part)
                for interacting in ids:
                    interactions.append((drug_id, interacting, effect))
                continue

            # Try dash separator
            if ' - ' in chunk or ' — ' in chunk:
                parts = re.split(r'\s[-—]\s', chunk, maxsplit=1)
                if len(parts) == 2:
                    ids = id_pattern.findall(parts[0])
                    effect = parts[1].strip()
                    for interacting in ids:
                        interactions.append((drug_id, interacting, effect))
                    continue

            # As a last resort, extract any DB ids and store with empty effect
            ids = id_pattern.findall(chunk)
            for interacting in ids:
                interactions.append((drug_id, interacting, None))

    if not interactions:
        print('No interactions parsed (no data or unsupported format).')
        return

    interactions_df = pd.DataFrame(interactions, columns=['drugbank-id', 'interacting_drugbank-id', 'effect'])
    interactions_df.to_sql('drug_interactions', conn, if_exists='replace', index=False)

    # Create index on drugbank-id column in the new table
    try:
        conn.execute('CREATE INDEX IF NOT EXISTS idx_interact_id ON drug_interactions("drugbank-id")')
    except Exception:
        print('Warning: failed to create index idx_interact_id on drug_interactions')


if __name__ == '__main__':
    try:
        # Allow passing data path and sqlite path from the command line.
        # Usage:
        #   python data_loader.py [data_path] [sqlite_path]
        data_arg = None
        sqlite_arg = None
        if len(sys.argv) >= 2:
            data_arg = sys.argv[1]
        if len(sys.argv) >= 3:
            sqlite_arg = sys.argv[2]

        # If the file wasn't found by the default candidate search, it's helpful
        # to print existing candidate files under the repo root for debugging.
        try:
            conn = load_drugbank_data(data_arg, sqlite_arg)
        except FileNotFoundError as e:
            repo_root = Path(__file__).resolve().parents[1]
            print(str(e))
            print('\nFiles at project root:')
            for p in sorted(repo_root.glob('*')):
                print(' -', p.name)
            print('\nFiles in data/:')
            data_dir = repo_root / 'data'
            if data_dir.exists():
                for p in sorted(data_dir.glob('*')):
                    print(' -', p.name)
            sys.exit(2)

    except Exception as e:
        print('Error while loading DrugBank data:', e)
        sys.exit(1)