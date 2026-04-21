"""
Normalização de texto para export Parquet em UTF-8 a partir do SQL Server em ISO-8859-1.

- Colunas binárias são decodificadas como Latin-1.
- Strings já em Unicode: se os dados no banco forem UTF-8 armazenados em VARCHAR Latin-1
  (mojibake ao ler byte a byte como Latin-1), aplica-se encode(latin-1).decode(utf-8).
  Texto verdadeiramente Latin-1 permanece inalterado (decode UTF-8 falha e mantém o valor).
"""
from __future__ import annotations

import pandas as pd


def _normalize_cell(value):
    if value is None:
        return value
    if isinstance(value, float) and pd.isna(value):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("iso-8859-1", errors="replace")
    if isinstance(value, str):
        try:
            return value.encode("latin-1").decode("utf-8")
        except UnicodeError:
            return value
    return value


def normalize_text_columns_latin1_to_utf8(df: pd.DataFrame) -> pd.DataFrame:
    """Garante Unicode correto para gravação Parquet (UTF-8 via pyarrow/pandas)."""
    out = df.copy()
    for col in out.columns:
        s = out[col]
        if not (s.dtype == object or pd.api.types.is_string_dtype(s)):
            continue
        out[col] = s.map(_normalize_cell)
    return out
