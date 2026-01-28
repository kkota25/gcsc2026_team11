# src/temp_jpn.py
from __future__ import annotations

from pathlib import Path
import pandas as pd

from compute_hdd_cdd import add_degree_days, summarize_by_year

# ---- 設定（JMA CSV仕様に合わせて固定） ----
# データは GitHub に上げない前提：data/raw/ 以下に配置
DATA_DIR = Path("data/raw/temp_jpn")
OUT_PATH = Path("outputs/tables/hdd_cdd_jpn.csv")

# JMA（日別値）: 6行目以降がデータ
ENCODING = "cp932"
SKIPROWS = 5
COL_NAMES = ["date", "tavg", "quality", "homog"]

DATE_COL = "date"  # 1列目：日付
TEMP_COL = "tavg"  # 2列目：日平均気温

# HDD/CDDの基準温度（必要に応じて変更）
TBASE_H = 18.0  # HDD base temperature
TBASE_C = 24.0  # CDD base temperature
# ---------------------------------------------


def read_jma_csv(path: str | Path) -> pd.DataFrame:
    """Read a JMA daily temperature CSV (with 5-line metadata header)."""
    df = pd.read_csv(
        path,
        encoding=ENCODING,
        skiprows=SKIPROWS,
        header=None,
        names=COL_NAMES,
    )
    # 型を整える
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    df[TEMP_COL] = pd.to_numeric(df[TEMP_COL], errors="coerce")
    return df


def read_all_csvs(data_dir: Path) -> list[Path]:
    """Recursively find all CSV files under the data directory."""
    return sorted(data_dir.rglob("*.csv"))


def infer_city_from_filename(path: Path) -> str:
    """Infer city name from filename like '1996-2025_tokyo.csv' -> 'tokyo'."""
    # 拡張子を除いた名前を "_" で分割して最後を都市名として扱う
    return path.stem.split("_")[-1]


def main() -> None:
    csv_paths = read_all_csvs(DATA_DIR)
    if not csv_paths:
        raise FileNotFoundError(f"No CSV files found under: {DATA_DIR.resolve()}")

    rows: list[pd.DataFrame] = []
    for p in csv_paths:
        # JMA CSVを適切に読み込む（encoding/skiprows/列名）
        df = read_jma_csv(p)

        # HDD/CDD（日次）を追加し、年次集計
        df = add_degree_days(df, t_col=TEMP_COL, tbase_h=TBASE_H, tbase_c=TBASE_C)
        yearly = summarize_by_year(df, date_col=DATE_COL)

        # 都市名を付与
        city = infer_city_from_filename(p)
        yearly.insert(0, "city", city)

        rows.append(yearly)

    out = pd.concat(rows, ignore_index=True).sort_values(["city", "year"]).reset_index(drop=True)

    # 出力フォルダが無ければ作る
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # 出力CSVを書き出し（= outputs/tables/hdd_cdd_jpn.csv を生成）
    out.to_csv(OUT_PATH, index=False, encoding="utf-8")
    print(f"Saved: {OUT_PATH} ({len(out)} rows)")


if __name__ == "__main__":
    main()
