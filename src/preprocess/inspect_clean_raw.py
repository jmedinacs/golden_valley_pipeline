'''
inspect_clean_raw.py

Performs initial inspection and standardization of client timecard data, 
including null checks, value frequency summaries, and text normalization 
for select columns. Outputs a cleaned version to the processed data folder.

Created on Aug 9, 2025
@author: John Medina
'''


import util.main_data_io as mainIO
import pandas as pd
from pandas import StringDtype

# =========================
# 👇 CONFIG: KEYS & STRATEGY
# =========================
USE_THREE_KEY = True  # Set to True if split shifts may exist (recommended)
KEYS = ["employee_id", "date", "clock_in"] if USE_THREE_KEY else ["employee_id", "date"]

# De-duplication strategy when duplicates are detected:
#   - "error": raise with a preview (safest during development)
#   - "keep_last": keep the last occurrence for each key
#   - "keep_best_non_nulls": keep the row with the most non-null critical fields
DEDUP_STRATEGY = "keep_best_non_nulls"

# Columns that matter for "completeness" scoring when using keep_best_non_nulls
CRITICAL_COLS = [
    "clock_in", "clock_out", "lunch_start", "lunch_end",
    "second_lunch_start", "second_lunch_end",
    "wage_rate", "overtime_rate", "doubletime_rate",
    "pay_date", "first_meal_waiver_signed", "second_meal_waiver_signed",
    "employment_status", "exempt_status"
]

# -----------------------
# Predefined normalization mappings for text fields
# Keys must reflect post-clean values (lowercase, underscores, no slashes)
# -----------------------
MAPPINGS = {
    "employment_status": {
        "fulltime": "full_time",
        "f_t": "full_time",
        "pt": "part_time",
        "parttime": "part_time",
    },
    "exempt_status": {
        "nonexempt": "non_exempt",
        "non-exempt": "non_exempt",
        "n_ex": "non_exempt",
        "ne": "non_exempt",
    },
}

def inspect_data(df: pd.DataFrame):
    print(df.info())
    print("\nNumber of null entries per column:\n")
    print(df.isnull().sum())

def inspect_text_values(df: pd.DataFrame, text_cols: list):
    for col in text_cols:
        print(f"\n--- {col} ---")
        print(df[col].value_counts(dropna=False))

def standardize_text_columns(df: pd.DataFrame, mappings: dict) -> pd.DataFrame:
    df = df.copy()
    for col, map_dict in mappings.items():
        if col in df.columns:
            s = df[col].astype(StringDtype())
            s = (
                s.str.strip()
                 .str.lower()
                 .str.replace("-", "_", regex=False)
                 .str.replace(" ", "_", regex=False)
                 .str.replace("/", "_", regex=False)
            )
            if map_dict:
                s = s.replace(map_dict)
            df[col] = s
    return df

# ===========================================
# 👉 NEW: KEY NORMALIZATION + DEDUP FUNCTIONS
# ===========================================
def normalize_key_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize merge keys consistently.
    - employee_id: uppercase + trimmed
    - date: normalized to midnight datetime (keeps datetime dtype)
    - clock_in (if used): floor to minute to avoid microsecond drift
    """
    out = df.copy()
    if "employee_id" in out.columns:
        out["employee_id"] = out["employee_id"].astype(str).str.strip().str.upper()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    if USE_THREE_KEY and "clock_in" in out.columns:
        out["clock_in"] = pd.to_datetime(out["clock_in"], errors="coerce").dt.floor("min")
    return out

def duplicate_key_report(df: pd.DataFrame, dataset_name: str, export_csv: bool = True,
                         client_name: str = None, filename: str = None) -> pd.DataFrame:
    """
    Return a dataframe of duplicated keys (with count), print a quick preview,
    and optionally export a CSV for auditing.
    """
    dup_mask = df.duplicated(subset=KEYS, keep=False)
    dup_df = (df.loc[dup_mask, KEYS]
                .value_counts()
                .reset_index(name="count")
                .sort_values("count", ascending=False))
    total_dups = dup_mask.sum()
    if total_dups:
        print(f"\n{dataset_name}: found {total_dups} duplicate-key rows on {KEYS}.")
        print(dup_df.head(10).to_string(index=False))
        if export_csv and client_name and filename:
            # export beside cleaned file
            try:
                mainIO.save_aux_report(
                    dup_df, client_name, f"{filename}_duplicate_keys_report"
                )
                print("Duplicate key report saved (aux).")
            except Exception as e:
                print(f"Warning: could not save duplicate report: {e}")
    else:
        print(f"\n{dataset_name}: no duplicate keys found on {KEYS}.")
    return dup_df

def _keep_best_non_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each key group, keep the row with the highest non-null score across CRITICAL_COLS.
    Ties are resolved by 'last' occurrence.
    """
    work = df.copy()
    # non-null score
    present_cols = [c for c in CRITICAL_COLS if c in work.columns]
    score_name = "__nn_score__"
    work[score_name] = work[present_cols].notna().sum(axis=1)
    # stable ordering: original index -> last wins on ties
    work["__orig_idx__"] = range(len(work))
    # sort so best score then by original index ascending — we’ll take last to prefer latest
    work = work.sort_values([*KEYS, score_name, "__orig_idx__"])
    deduped = work.drop_duplicates(subset=KEYS, keep="last").drop(columns=[score_name, "__orig_idx__"])
    return deduped

def deduplicate_by_strategy(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Apply the configured de-dup strategy.
    """
    dup_mask = df.duplicated(subset=KEYS, keep=False)
    if not dup_mask.any():
        return df

    if DEDUP_STRATEGY == "error":
        dup_df = (df.loc[dup_mask, KEYS]
                    .value_counts()
                    .reset_index(name="count")
                    .sort_values("count", ascending=False))
        raise ValueError(
            f"[{dataset_name}] duplicate keys detected on {KEYS}. "
            f"Examples:\n{dup_df.head(15).to_string(index=False)}"
        )
    elif DEDUP_STRATEGY == "keep_last":
        return df.drop_duplicates(subset=KEYS, keep="last")
    elif DEDUP_STRATEGY == "keep_best_non_nulls":
        return _keep_best_non_nulls(df)
    else:
        raise ValueError(f"Unknown DEDUP_STRATEGY: {DEDUP_STRATEGY}")

# ===========================
# Existing high-level routines
# ===========================
def initial_inspection(client_name="missing_client", filename="missing_filename") -> pd.DataFrame:
    """
    Performs initial inspection and normalization of a client’s raw dataset.
    """
    df = mainIO.load_raw_data(client_name, filename)

    # Inspection
    inspect_data(df)
    text_columns = ["employment_status", "exempt_status"]
    inspect_text_values(df, text_columns)

    # Standardize text
    df_clean = standardize_text_columns(df, MAPPINGS)

    # 👉 Normalize merge keys & check dupes
    df_clean = normalize_key_columns(df_clean)
    _ = duplicate_key_report(df_clean, dataset_name="RAW→CLEAN (post-standardize)",
                             export_csv=True, client_name=client_name, filename=filename)
    df_clean = deduplicate_by_strategy(df_clean, dataset_name="RAW→CLEAN")

    print("\nAfter standardization:")
    inspect_text_values(df_clean, text_columns)

    # Save cleaned dataset
    mainIO.save_cleaned_raw_data(df_clean, client_name, filename)
    return df_clean

def clean_corrected_data(df_corrected: pd.DataFrame,
                         client_name: str = None,
                         filename: str = None) -> pd.DataFrame:
    """
    Cleans a corrected dataset consistently with RAW→CLEAN rules,
    normalizes keys, reports + resolves duplicates based on strategy.
    """
    inspect_data(df_corrected)
    text_columns = ["employment_status", "exempt_status"]
    inspect_text_values(df_corrected, text_columns)

    df_clean = standardize_text_columns(df_corrected, MAPPINGS)

    # 👉 Normalize keys & enforce uniqueness for corrected too
    df_clean = normalize_key_columns(df_clean)
    _ = duplicate_key_report(df_clean, dataset_name="CORRECTED (post-standardize)",
                             export_csv=True, client_name=client_name, filename=filename)
    df_clean = deduplicate_by_strategy(df_clean, dataset_name="CORRECTED")

    print("\nAfter standardization:")
    inspect_text_values(df_clean, text_columns)
    return df_clean

# For testing/debugging from CLI
if __name__ == '__main__':
    initial_inspection(client_name="test_client", filename="jan_2024_test_data")
