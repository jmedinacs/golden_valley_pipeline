"""
merge_corrected.py

Integrates corrected timecard entries into the cleaned dataset.

- Loads cleaned + corrected
- Normalizes keys
- Appends corrected after cleaned
- Drop-duplicates by (employee_id, date, clock_in) keeping the last
- Saves merged back to cleaned folder

Author: John Medina
Created on: August 5, 2025
"""

import pandas as pd
import util.main_data_io as mainIO
import preprocess.inspect_clean_raw as cleanRaw

KEYS = ["employee_id", "date", "clock_in"]  # 3-key to allow split shifts safely

def normalize_key_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize key columns: ID uppercase, date normalized, clock_in floored to minute."""
    out = df.copy()

    if "employee_id" in out.columns:
        out["employee_id"] = (
            out["employee_id"].astype("string").str.strip().str.upper()
        )

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()

    if "clock_in" in out.columns:
        # Keep time (don’t downcast to date); floor to minute for stable equality
        out["clock_in"] = pd.to_datetime(out["clock_in"], errors="coerce").dt.floor("min")

    return out


def append_and_deduplicate(df_clean: pd.DataFrame, df_corr: pd.DataFrame) -> pd.DataFrame:
    """
    Append cleaned + corrected (corrected last), then drop duplicates by KEYS keeping the last.
    Assumes both inputs are already normalized on KEYS.
    """
    combined = pd.concat([df_clean, df_corr], ignore_index=True)
    print(f"\nCombined rows (pre-dedup): {len(combined)}")

    # Keep corrected rows where keys match, preserve originals where they don't
    merged = combined.drop_duplicates(subset=KEYS, keep="last").reset_index(drop=True)

    print(f"Rows before: {len(df_clean)} | after merge: {len(merged)}")
    return merged


def merge_data(
    corr_filename: str = "missing_corr_filename",
    proc_filename: str = "missing_proc_filename",
    client_name: str = "missing_client_name",
    data_root: str = "../../data",
) -> pd.DataFrame:
    """Main entry: load → clean corrected → normalize → append+dedupe → save."""
    # Load datasets
    df_corr_raw = mainIO.load_corrected_data(client_name, corr_filename, data_root)
    df_clean_raw = mainIO.load_cleaned_data(client_name, proc_filename, data_root)

    # Clean corrected with your existing routine (text normalization, etc.)
    # (Pass client/filename if your cleaner writes aux reports)
    df_corr_clean = cleanRaw.clean_corrected_data(df_corr_raw, client_name=client_name, filename=corr_filename)

    # Normalize keys for both
    df_clean = normalize_key_columns(df_clean_raw)
    df_corr  = normalize_key_columns(df_corr_clean)

    # Optional: quick duplicate checks on inputs (comment out if noisy)
    # print("Cleaned dupes on KEYS:", df_clean.duplicated(subset=KEYS).sum())
    # print("Corrected dupes on KEYS:", df_corr.duplicated(subset=KEYS).sum())

    # Merge
    merged = append_and_deduplicate(df_clean, df_corr)

    # Post-condition (row count should not drop unless corrected intentionally removes a row)
    # If you require exact preservation, assert here:
    # assert len(merged) >= len(df_clean), "Merged result lost rows unexpectedly."

    # Save back to cleaned so DQ jobs can run without a corrected flag
    mainIO.save_cleaned_raw_data(merged, client_name, proc_filename)
    print("Merged dataset saved to cleaned.")

    return merged


if __name__ == "__main__":
    merge_data(
        corr_filename="jan_2024_test_data",
        proc_filename="jan_2024_test_data",
        client_name="test_client",
    )
