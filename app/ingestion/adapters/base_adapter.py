from __future__ import annotations

from typing import List
import pandas as pd


class BankAdapter:

    bank_name: str = "unknown"

    def list_files(self, data_root: str) -> List[str]:
        raise NotImplementedError()

    def extract(self, file_path: str) -> List[pd.DataFrame]:
        """
        Returns list of transaction DataFrames
        """
        raise NotImplementedError()