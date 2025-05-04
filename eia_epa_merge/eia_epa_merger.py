#!/usr/bin/env python
# eia_epa_pipeline.py  – 2025-04-26
# --------------------------------------------------------------------------
# 1. Download all EPA Electric-Power-Annual workbooks.
# 2. Parse every sheet (yearly tables + monthly blocks) to tidy form:
#    • wide-year  • matrix  • monthly-block
# 3. Write one parquet per sheet and a combined parquet.
# --------------------------------------------------------------------------
import re, time, requests
from pathlib import Path
from typing import List, Optional

import pandas as pd

# ------------------------- CONFIG -----------------------------------------
DATA_ROOT   = Path("eia_epa_all_xls")
OUTPUT_DIR  = Path("melted_epa_eia")
DATA_ROOT.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

BASE_URL    = "https://www.eia.gov/electricity/annual/xls/{name}.xlsx"
UA          = "Mozilla/5.0 (compatible; EPA-EIA-downloader/1.0)"
DELAY_S     = 2
RETRIES     = 3
BACKOFF     = 2
HEADER_SCAN = 25

YEAR_CAP_RE = re.compile(r"((?:19|20)\d{2})")  # capture yyyy
YEAR_FIND   = re.compile(r"(?:19|20)\d{2}")    # search yyyy
MONTHS      = ["january","february","march","april","may","june",
               "july","august","september","october","november","december"]

# --------------- FULL filename list (paste unchanged) ---------------------
TABLE_FILENAMES = [
    "epa_01_01","epa_01_02","epa_01_03","epa_02_01","epa_02_02","epa_02_11",
    "epa_03_01_a","epa_03_01_b","epa_03_02_a","epa_03_02_b","epa_03_03_a",
    "epa_03_03_b","epa_03_04_a","epa_03_04_b","epa_03_05_a","epa_03_05_b",
    "epa_03_06","epa_03_07","epa_03_08","epa_03_09","epa_03_10","epa_03_11",
    "epa_03_12","epa_03_13","epa_03_14","epa_03_15","epa_03_16","epa_03_17",
    "epa_03_18","epa_03_19","epa_03_20","epa_03_21","epa_03_22","epa_03_23",
    "epa_03_24","epa_03_25","epa_03_26","epa_03_27","epa_04_01","epa_04_02_a",
    "epa_04_02_b","epa_04_03","epa_04_04","epa_04_05","epa_04_06","epa_04_07_a",
    "epa_04_07_b","epa_04_07_c","epa_04_08_a","epa_04_08_b","epa_04_08_c",
    "epa_04_09_a","epa_04_09_b","epa_04_10","epa_04_11","epa_04_12","epa_04_13",
    "epa_04_14","epa_05_01_a","epa_05_01_b","epa_05_01_c","epa_05_01_d",
    "epa_05_01_e","epa_05_01_f","epa_05_02_a","epa_05_02_b","epa_05_02_c",
    "epa_05_02_d","epa_05_02_e","epa_05_02_f","epa_05_03_a","epa_05_03_b",
    "epa_05_03_c","epa_05_03_d","epa_05_03_e","epa_05_03_f","epa_05_04_a",
    "epa_05_04_b","epa_05_04_c","epa_05_04_d","epa_05_04_e","epa_05_04_f",
    "epa_05_05_d","epa_05_05_e","epa_05_05_f","epa_05_06_a","epa_05_06_b",
    "epa_05_06_c","epa_05_06_d","epa_05_06_e","epa_05_06_f","epa_05_07_a",
    "epa_05_07_b","epa_05_07_c","epa_05_07_d","epa_05_07_e","epa_05_07_f",
    "epa_05_08_d","epa_05_08_e","epa_05_08_f","epa_05_09","epa_05_10","epa_05_11",
    "epa_05_12","epa_05_13","epa_05_14","epa_10_01","epa_10_02","epa_10_03",
    "epa_10_04","epa_10_05"
]

# ---------------------- 1. DOWNLOAD ---------------------------------------
sess = requests.Session()
sess.headers.update({"User-Agent": UA})

def download(name: str):
    url  = BASE_URL.format(name=name)
    dest = DATA_ROOT / f"{name}.xlsx"
    if dest.exists() and dest.stat().st_size:
        print(f"↩️  {name}.xlsx present")
        return
    for attempt in range(1, RETRIES+1):
        try:
            r = sess.get(url, timeout=30)
            if r.status_code == 404:
                print(f"⚠️  {name}.xlsx 404")
                return
            r.raise_for_status()
            dest.write_bytes(r.content)
            print(f"✅  downloaded {name}.xlsx")
            return
        except requests.HTTPError:
            if attempt < RETRIES:
                time.sleep(BACKOFF*attempt)
            else:
                print(f"❌  HTTP error {name}")
                return
        except Exception as e:
            print(f"❌  {name}: {e}")
            return
    time.sleep(DELAY_S)

print(f"▶︎ Downloading {len(TABLE_FILENAMES)} workbooks …")
for fn in TABLE_FILENAMES:
    download(fn)
    time.sleep(DELAY_S)

# ---------------------- 2. HELPER FUNCTIONS -------------------------------
def detect_header(df: pd.DataFrame) -> int:
    for i in range(min(HEADER_SCAN, len(df))):
        row = df.iloc[i]
        if row.isna().all(): continue
        if row.astype(str).str.strip().str.lower().eq("year").any():
            return i
        if row.astype(str).str.contains(YEAR_FIND).sum() >= 3:
            return i
    return 0

def parse_monthly_blocks(raw: pd.DataFrame, path: Path, sheet: str) -> Optional[pd.DataFrame]:
    """Detect blocks that start with 'Year ####' then 12 month rows."""
    rows = []
    for idx, val in raw.iloc[:,0].items():
        m = re.match(r"\s*Year\s+((?:19|20)\d{2})\s*$", str(val))
        if not m: continue
        yr = int(m.group(1))
        block = raw.iloc[idx+1 : idx+13].copy()
        if block.empty: continue
        block.columns = [f"col{i}" for i in range(block.shape[1])]
        block.rename(columns={"col0":"month"}, inplace=True)
        block["month"] = block["month"].astype(str).str.strip().str.lower()
        block = block[block["month"].isin(MONTHS)]
        if block.empty: continue
        value_cols = [c for c in block.columns if c!="month"]
        melted = block.melt(id_vars="month", value_vars=value_cols,
                            var_name="category", value_name="value")
        melted["year"] = yr
        rows.append(melted)
    if rows:
        tidy = pd.concat(rows, ignore_index=True)
        tidy["value"] = pd.to_numeric(tidy["value"], errors="coerce")
        tidy["source_file"]  = path.stem
        tidy["source_sheet"] = sheet
        return tidy
    return None

# ---------------------- 3. TIDY PARSER ------------------------------------
def tidy_sheet(path: Path, sheet: str) -> Optional[pd.DataFrame]:
    raw = pd.read_excel(path, sheet_name=sheet, header=None, engine="openpyxl")

    # layout ➊ monthly-block
    monthly = parse_monthly_blocks(raw, path, sheet)
    if monthly is not None:
        return monthly

    # detect header & clean
    hdr = detect_header(raw)
    df  = pd.read_excel(path, sheet_name=sheet, header=hdr, engine="openpyxl")
    df.dropna(axis=0, how="all", inplace=True)
    df.dropna(axis=1, how="all", inplace=True)
    df.columns = [re.sub(r"\s+", " ", str(c)).strip() for c in df.columns]
    first = df.columns[0]

    # layout ➋ matrix  (first col starts with 'Year')
    if first.strip().lower().startswith("year"):
        df = df[df[first].astype(str).str.contains(YEAR_FIND, na=False)]
        df[first] = (df[first].astype(str)
                               .str.extract(YEAR_CAP_RE, expand=False)
                               .pipe(pd.to_numeric, errors="coerce"))
        df.dropna(subset=[first], inplace=True)
        df[first] = df[first].astype("int64")
        tidy = (df.melt(id_vars=first, var_name="category", value_name="value")
                  .rename(columns={first:"year"}))

    # layout ➌ wide-year
    else:
        # remove stray 'Year ####' spacer rows
        df = df[~df[first].astype(str)
                        .str.strip()
                        .str.lower()
                        .str.startswith("year")]
        year_map = {c:int(m.group(1))
                    for c in df.columns[1:]
                    if (m := YEAR_CAP_RE.search(str(c)))
                    and "through" not in str(c).lower()}
        if not year_map:
            return None
        df.rename(columns=year_map, inplace=True)
        tidy = (df.melt(id_vars=first, value_vars=list(year_map.values()),
                        var_name="year", value_name="value")
                  .rename(columns={first:"category"}))
        tidy["year"] = tidy["year"].astype("int64")

    tidy["value"] = pd.to_numeric(tidy["value"], errors="coerce")
    tidy["category"] = tidy["category"].astype(str)
    tidy["source_file"]  = path.stem
    tidy["source_sheet"] = sheet
    return tidy

# ---------------------- 4. PARSE ALL WORKBOOKS ----------------------------
print("\n▶︎ Parsing workbooks …")
tables: List[pd.DataFrame] = []
for wb in DATA_ROOT.glob("*.xlsx"):
    try:
        xls = pd.ExcelFile(wb, engine="openpyxl")
        for sh in xls.sheet_names:
            t = tidy_sheet(wb, sh)
            if t is not None and not t.empty:
                out = OUTPUT_DIR / f"melted_{wb.stem}_{sh}.parquet"
                t.to_parquet(out, index=False)
                print(f"✅  wrote {out}")
                tables.append(t)
    except Exception as e:
        print(f"❌  {wb.name}: {e}")

# ---------------------- 5. COMBINE ---------------------------------------
if tables:
    combined = pd.concat(tables, ignore_index=True)
    final = OUTPUT_DIR / "epa_eia_all_combined.parquet"
    combined.to_parquet(final, index=False)
    print(f"\n🎉  Combined parquet → {final}")
    print(f"    Rows: {len(combined):,}   Years: {combined['year'].nunique()}")
else:
    print("\n⚠️  No usable sheets parsed.")
