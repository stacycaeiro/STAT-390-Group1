#!/usr/bin/env python
# eia_epa_pipeline.py  – v1.5  (row_label never missing)
# -------------------------------------------------------------------------
import re, time, requests
from pathlib import Path
from typing import List, Optional
import pandas as pd

# ───────────────────────────────── CONFIG ────────────────────────────────
DATA_ROOT   = Path("eia_epa_all_xls")
OUTPUT_DIR  = Path("melted_epa_eia")
DATA_ROOT.mkdir(exist_ok=True); OUTPUT_DIR.mkdir(exist_ok=True)

BASE_URL = "https://www.eia.gov/electricity/annual/xls/{name}.xlsx"
UA       = "Mozilla/5.0 (EPA-EIA-downloader/1.5)"
DELAY_S, RETRIES, BACKOFF, SCAN_ROWS = 2, 3, 2, 30

YEAR_CAP  = re.compile(r"((?:19|20)\d{2})")
YEAR_FIND = re.compile(r"(?:19|20)\d{2}")
MONTHS    = [m.lower() for m in
             "January February March April May June July August "
             "September October November December".split()]

# <<< put your full filename list here >>>
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

# ─────────────────────────── DOWNLOAD ────────────────────────────────────
# sess = requests.Session(); sess.headers.update({"User-Agent": UA})

# def dl(name:str):
#     f = DATA_ROOT / f"{name}.xlsx"
#     if f.exists() and f.stat().st_size: print(f"↩️  {name}.xlsx present"); return
#     url = BASE_URL.format(name=name)
#     for i in range(1, RETRIES+1):
#         try:
#             r = sess.get(url, timeout=30); r.raise_for_status()
#             f.write_bytes(r.content); print(f"✅  {name}.xlsx"); return
#         except requests.HTTPError as e:
#             if r.status_code==404: print(f"⚠️  {name}.xlsx 404"); return
#             if i<RETRIES: time.sleep(BACKOFF*i)
#             else: print(f"❌  {name}: {e}"); return
#         except Exception as e:
#             print(f"❌  {name}: {e}"); return
#     time.sleep(DELAY_S)

# print(f"▶︎ Downloading {len(TABLE_FILENAMES)} workbooks …")
# for fn in TABLE_FILENAMES: dl(fn); time.sleep(DELAY_S)

# ─────────────────────────── HELPERS ─────────────────────────────────────
def header_row(raw):
    for i in range(min(SCAN_ROWS,len(raw))):
        r=raw.iloc[i]
        if r.isna().all(): continue
        if r.astype(str).str.lower().eq("year").any(): return i
        if r.astype(str).str.contains(YEAR_FIND).sum()>=3: return i
    return 0

def year_row(raw, hdr):
    for i in range(hdr+1, hdr+3):
        if i>=len(raw): break
        if raw.iloc[i].astype(str).str.contains(YEAR_FIND).sum()>=3: return i
    return None

def join_labels(row, cols):
    parts=[str(row[c]).strip() for c in cols if pd.notna(row[c]) and str(row[c]).strip()]
    return " | ".join(parts) if parts else "NA"

def fix_row_label(df: pd.DataFrame):
    """
    Final, nuclear-option guard:
    1. push ALL index levels to columns,
    2. create 'row_label' if still missing.
    """
    if "row_label" not in df.columns:
        df.reset_index(inplace=True)
    if "row_label" not in df.columns:
        df["row_label"] = "NA"

def monthly_blocks(raw, fp, sh):
    blocks=[]
    for idx,val in raw.iloc[:,0].items():
        m=re.match(r"\s*Year\s+((?:19|20)\d{2})\s*$", str(val))
        if not m: continue
        yr=int(m.group(1))
        blk=raw.iloc[idx+1:idx+13].copy()
        blk.columns=[f"c{i}" for i in range(blk.shape[1])]
        blk.rename(columns={"c0":"month"}, inplace=True)
        blk["month"]=blk["month"].astype(str).str.strip().str.lower()
        blk=blk[blk["month"].isin(MONTHS)]
        if blk.empty: continue
        melt=blk.melt(id_vars=["month"], var_name="measure", value_name="value")
        melt["row_label"]="monthly"; melt["year"]=yr; blocks.append(melt)
    if blocks:
        res=pd.concat(blocks,ignore_index=True)
        res["value"]=pd.to_numeric(res["value"], errors="coerce")
        res["source_file"]=fp.stem; res["source_sheet"]=sh
        return res
    return None

# ─────────────────────────── SHEET → TIDY ────────────────────────────────
def tidy_sheet(fp:Path, sh:str)->Optional[pd.DataFrame]:
    raw=pd.read_excel(fp, sheet_name=sh, header=None, engine="openpyxl")
    # layout A – monthly
    m=monthly_blocks(raw, fp, sh)
    if m is not None: return m

    hdr=header_row(raw); yrrow=year_row(raw,hdr)

    # layout D – measure row + year row
    if yrrow and yrrow>hdr:
        df=pd.read_excel(fp, sheet_name=sh, header=[hdr,yrrow], engine="openpyxl")
        df.dropna(axis=0,how="all",inplace=True); df.dropna(axis=1,how="all",inplace=True)
        desc=[c for c in df.columns if pd.isna(c[1])]
        yrs=[c for c in df.columns if not pd.isna(c[1]) and YEAR_CAP.match(str(c[1]))]
        if not yrs: return None
        if not desc: df["row_label"]="NA"
        else:        df["row_label"]=df.apply(lambda r: join_labels(r,desc),axis=1)

        fix_row_label(df)         # <── ultimate guarantee

        melt=df.melt(id_vars=["row_label"], value_vars=yrs,
                     var_name=["measure","yr"], value_name="value")
        melt["year"]=pd.to_numeric(melt["yr"],errors="coerce")
        melt=melt.dropna(subset=["year"]); melt["year"]=melt["year"].astype(int)
        melt.drop(columns=["yr"], inplace=True)

    else:  # layouts B & C
        df=pd.read_excel(fp, sheet_name=sh, header=hdr, engine="openpyxl")
        df.dropna(axis=0,how="all",inplace=True); df.dropna(axis=1,how="all",inplace=True)
        df.columns=[re.sub(r"\s+"," ",str(c)).strip() for c in df.columns]
        first=df.columns[0]
        year_cols=[c for c in df.columns if YEAR_CAP.search(str(c))]
        down=df[first].astype(str).str.contains(YEAR_FIND,na=False).sum()>=3

        if down and not year_cols:                 # layout C
            df=df[df[first].astype(str).str.contains(YEAR_FIND,na=False)]
            df["year"]=pd.to_numeric(df[first].astype(str).str.extract(YEAR_CAP)[0],errors="coerce")
            df=df.dropna(subset=["year"]); df["year"]=df["year"].astype(int)
            df.drop(columns=[first], inplace=True)
            melt=df.melt(id_vars=["year"], var_name="measure", value_name="value")
            melt["row_label"]="wide_row"

        elif year_cols:                            # layout B
            desc=[c for c in df.columns if c not in year_cols]
            if not desc: df["dummy"]=""; desc=["dummy"]
            df["row_label"]=df.apply(lambda r: join_labels(r,desc),axis=1)

            fix_row_label(df)      # <── ultimate guarantee

            melt=df.melt(id_vars=["row_label"], value_vars=year_cols,
                         var_name="measure", value_name="value")
            melt["year"]=pd.to_numeric(melt["measure"].str.extract(YEAR_CAP)[0],errors="coerce")
            melt=melt.dropna(subset=["year"]); melt["year"]=melt["year"].astype(int)
        else:
            return None

    melt["value"]=pd.to_numeric(melt["value"], errors="coerce")
    if "month" not in melt.columns: melt["month"]=pd.NA
    melt["source_file"]=fp.stem; melt["source_sheet"]=sh
    return melt[["year","month","row_label","measure","value","source_file","source_sheet"]]

# ─────────────────────────── MAIN LOOP ───────────────────────────────────
print("\n▶︎ Parsing workbooks …")
tables: List[pd.DataFrame]=[]
for wb in DATA_ROOT.glob("*.xlsx"):
    try:
        for sheet in pd.ExcelFile(wb, engine="openpyxl").sheet_names:
            t=tidy_sheet(wb,sheet)
            if t is not None and not t.empty:
                out=OUTPUT_DIR/f"melted_{wb.stem}_{sheet}.parquet"
                t.to_parquet(out,index=False)
                print(f"✅ wrote {out}")
                tables.append(t)
    except Exception as e:
        print(f"❌ {wb.name}: {e}")

if tables:
    combo=pd.concat(tables, ignore_index=True)
    final=OUTPUT_DIR/"epa_eia_all_combined.parquet"
    combo.to_parquet(final,index=False)
    print(f"\n🎉 Combined parquet → {final}"
          f"\n   rows={len(combo):,}   measures={combo['measure'].nunique()}")
else:
    print("\n⚠️  No usable sheets parsed.")
