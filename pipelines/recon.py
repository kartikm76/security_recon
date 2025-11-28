"""Diff utilities between data snapshots."""
from typing import List
import pandas as pd
from .dictionary import AttributeRuleSet, AttributeRule

KEY_COLS = ["instrument_id", "as_of_date"]
ATTR_COLS = ["coupon", "cfi_code", "maturity_date", "callable_flag"]

class DataFrameDiffer:
    def __init__(self, rule_set: AttributeRuleSet):
        self.rule_set = rule_set
    
    def normalize_value(self, value, rule: AttributeRule):
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
            return 0
        return value
    
    def value_equal(src, tgt, rule: AttributeRule) -> bool:
        if src is None and tgt is None:
            return True
        if src is None or tgt is None:
            return False
        
        dfDiffer = DataFrameDiffer(rule)
        normalized_src = dfDiffer.normalize_value(src, rule)
        normalized_tgt = dfDiffer.normalize_value(tgt, rule)

        if rule.type == "float" and rule.tolerance is not None:
            return abs(normalized_src - normalized_tgt) <= rule.tolerance
        return normalized_src == normalized_tgt
     
    def build_exceptions_df(
            self,
            legacy_df: pd.DataFrame,
            strategic_df: pd.DataFrame,
            rules: AttributeRuleSet,
            run_id: str,
            as_of_date: str
    ) -> pd.DataFrame:
        # full outer join on key columns
        legacy_df = legacy_df.copy()
        legacy_df.columns = [f"{c}_legacy" if c not in KEY_COLS else c for c in legacy_df.columns]              ## Rename columns to indicate source
        
        strategic_df = strategic_df.copy()
        strategic_df.columns = [f"{c}_strategic" if c not in KEY_COLS else c for c in strategic_df.columns]   ## Rename columns to indicate target

        merged_df = pd.merge (
            legacy_df,
            strategic_df,
            on = KEY_COLS,
            how = "outer",
            indicator = True,
        )

        records: List[dict] = []                        ## List to hold exception records

        for _, row in merged_df.iterrows():             ## Iterate through merged rows
            key = {k: row[k] for k in KEY_COLS}         ## Extract key columns
            side = row["_merge"]                        ## Determine side of the record

            if side == "left_only":
                ## Record exists only in legacy
                for attr in ATTR_COLS:
                    records.append (
                        {
                            "run_id": run_id,
                            "as_of_date": as_of_date,
                            "instrument_id": key["instrument_id"],
                            "attribute": attr,
                            "source_system": "legacy",
                            "target_system": "strategic",
                            "source_value": row.get(f"{attr}_legacy"),
                            "target_value": None,
                            "difference_type": "ONLY_IN_LEGACY",
                        }
                    )
                continue

            if side == "right_only":
                ## Record exists only in legacy
                for attr in ATTR_COLS:
                    records.append (
                        {
                            "run_id": run_id,
                            "as_of_date": as_of_date,
                            "instrument_id": key["instrument_id"],
                            "attribute": attr,
                            "source_system": "legacy",
                            "target_system": "strategic",
                            "source_value": None,
                            "target_value": row.get(f"{attr}_strategic"),
                            "difference_type": "ONLY_IN_STRATEGIC",
                        }
                    )
                continue

            # both sides present
            for attr in ATTR_COLS:
                rule = rules.get_rule(attr)                 ## Get validation rule
                src_value = row.get(f"{attr}_legacy")       ## Source value
                tgt_value = row.get(f"{attr}_strategic")    ## Target value
                
                if rule is None:
                    equal = src_value == tgt_value          ## Fallback comparison
                else:
                    equal = DataFrameDiffer.value_equal(src_value, tgt_value, rule) ## Compare values based on rule
                
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