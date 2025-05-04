import subprocess
import sys
import gc
import time

scripts = [
    "autodownload_main.py",
    "autodownload_auxiliary.py",
    "autodownload_CAMPD_Facility.py",
    "autodownload_ClimateWatch.py",
    "autodownload_EIA_923.py",
    "autodownload_EIA_AEO.py",
    "autodownload_EIA_EPA.py",
    "autodownload_EIA_Hourly_Electric_Grid.py",
    "autodownload_IRENA.py",
    "autodownload_USURDB.py",
    "build_wide_dataframe_full.py",
    "merge_chunk_6_safe.py",
    "merge_chunk_6_into_final.py",
    "eia_aeo2025_all_parquet.py",
    "merge2_aeo2025_w_lilly.py",
    "merge3_GHG_with_main.py",
    "merge_eia923_w_main.py",
    "usurdb_merge_test.py",
    "merge_CAMP_facilities_w_main.py",
    "cleaning_script.py"
]
print("\n🚀 Running selected merging scripts...\n")

for script in scripts:
    print(f"▶️ Running {script}...")
    
    result = subprocess.run(["python", script], capture_output=True, text=True)

    print(result.stdout)
    if result.stderr:
        print("❗ Errors:", result.stderr)

    if result.returncode != 0:
        print(f"❌ {script} exited with code {result.returncode}. Aborting.")
        sys.exit(result.returncode)

    # Memory cleanup after each script
    gc.collect()
    time.sleep(5)
    print("🧹 Memory cleaned and system rested.\n")

print("\n✅ All selected scripts completed successfully!")
