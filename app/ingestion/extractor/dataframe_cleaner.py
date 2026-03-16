from __future__ import annotations
import pandas as pd
from app.utils.text_utils import norm_text


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all")
    df.columns = [norm_text(c) if c else "col" for c in df.columns]
    df = df.loc[:, ~pd.Index(df.columns).duplicated()]
    df = df.reset_index(drop=True)
    return df
