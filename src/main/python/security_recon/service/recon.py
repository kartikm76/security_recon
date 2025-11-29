"""Diff utilities between data snapshots."""
from __future__ import annotations

from datetime import date
from typing import Any, List

import pandas as pd

from security_recon.domain.dictionary import AttributeRule, AttributeRuleSet

KEY_COLS = ["instrument_id", "as_of_date"]
ATTR_COLS = ["coupon", "cfi_code", "maturity_date", "callable_flag"]


class DataFrameDiffer:
    def __init__(self, rule_set: AttributeRuleSet):
        self.rule_set = rule_set

    @staticmethod    
    def normalize_value(value: Any, rule: AttributeRule) -> Any:
        if value is None:
            return None

        if rule.type == "float":
            return float(value)

        if rule.type == "boolean":
            return bool(value)

        if rule.type == "string":
            s = str(value)
            if rule.trim:
                s = s.strip()
            if rule.ignore_case:
                s = s.upper()
            return s
        return value

    @staticmethod
    def value_equal(src: Any, tgt: Any, rule: AttributeRule) -> bool:
        if src is None and tgt is None:
            return True
        if src is None or tgt is None:
            return False

        normalized_src = DataFrameDiffer.normalize_value(src, rule)
        normalized_tgt = DataFrameDiffer.normalize_value(tgt, rule)

        if rule.type == "float" and rule.tolerance is not None:
            return abs(float(normalized_src) - float(normalized_tgt)) <= rule.tolerance
        return normalized_src == normalized_tgt

    def build_exceptions_df(
        self,
        legacy_df: pd.DataFrame,
        strategic_df: pd.DataFrame,
        rules: AttributeRuleSet,
        run_id: str,
        as_of_date: date,
    ) -> pd.DataFrame:
        # full outer join on key columns
        legacy_df = legacy_df.copy()
        legacy_df.columns = [f"{c}_legacy" if c not in KEY_COLS else c for c in legacy_df.columns]

        strategic_df = strategic_df.copy()
        strategic_df.columns = [f"{c}_strategic" if c not in KEY_COLS else c for c in strategic_df.columns]

        merged_df = pd.merge(
            legacy_df,
            strategic_df,
            on = KEY_COLS,
            how = "outer",
            indicator = True,
        )

        records: List[dict] = []

        for _, row in merged_df.iterrows():
            key = {k: row[k] for k in KEY_COLS}
            side = row["_merge"]

            if side == "left_only":
                records.append(
                    {
                        "run_id": run_id,
                        "as_of_date": as_of_date,
                        "instrument_id": key["instrument_id"],
                        "attribute": "__record__",
                        "source_system": "legacy",
                        "target_system": "strategic",
                        "source_value": "present",
                        "target_value": "missing",
                        "difference_type": "ONLY_IN_LEGACY",
                    }
                )
                continue

            if side == "right_only":
                records.append(
                    {
                        "run_id": run_id,
                        "as_of_date": as_of_date,
                        "instrument_id": key["instrument_id"],
                        "attribute": "__record__",
                        "source_system": "legacy",
                        "target_system": "strategic",
                        "source_value": "missing",
                        "target_value": "present",
                        "difference_type": "ONLY_IN_STRATEGIC",
                    }
                )
                continue

            for attr in ATTR_COLS:
                rule = rules.get_rule(attr)
                src_value = row.get(f"{attr}_legacy")
                tgt_value = row.get(f"{attr}_strategic")

                if rule is None:
                    equal = src_value == tgt_value
                else:
                    equal = DataFrameDiffer.value_equal(src_value, tgt_value, rule)

                if not equal:
                    records.append(
                        {
                            "run_id": run_id,
                            "as_of_date": as_of_date,
                            "instrument_id": key["instrument_id"],
                            "attribute": attr,
                            "source_system": "legacy",
                            "target_system": "strategic",
                            "source_value": src_value,
                            "target_value": tgt_value,
                            "difference_type": "VALUE_MISMATCH",
                        }
                    )
        return pd.DataFrame.from_records(records)
