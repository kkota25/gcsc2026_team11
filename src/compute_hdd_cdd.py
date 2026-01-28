# src/compute_hdd_cdd.py
import pandas as pd

def add_degree_days(df: pd.DataFrame, t_col: str, tbase_h: float, tbase_c: float) -> pd.DataFrame:
    """Add daily HDD/CDD columns to a dataframe."""
    t = df[t_col].astype(float)
    df = df.copy()
    df["hdd"] = (tbase_h - t).clip(lower=0)
    df["cdd"] = (t - tbase_c).clip(lower=0)
    return df

def summarize_by_year(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Aggregate HDD/CDD by year."""
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col])
    out["year"] = out[date_col].dt.year
    return out.groupby("year", as_index=False)[["hdd", "cdd"]].sum()
