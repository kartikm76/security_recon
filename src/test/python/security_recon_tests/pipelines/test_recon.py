import pandas as pd
import pytest

from security_recon.domain.dictionary import AttributeRule, AttributeRuleSet
from security_recon.service.recon import DataFrameDiffer


@pytest.fixture(scope="module")
def define_source_data() -> pd.DataFrame:
    legacy_data = {
        "instrument_id": ["US0001", "US0002", "US0003"],
        "as_of_date": ["2023-12-29", "2023-12-29", "2023-12-29"],
        "coupon": [4.99999, 3.5, 4.0],
        "cfi_code": ["DXXXXXX", "DYYYYYY", "DZZZZZZ"],
        "maturity_date": ["2033-12-29", "2025-06-15", "2028-11-30"],
        "callable_flag": [True, False, True],
    }
    return pd.DataFrame(legacy_data)


@pytest.fixture(scope="module")
def define_target_data() -> pd.DataFrame:
    strategic_data = {
        "instrument_id": ["US0001", "US0002", "US0004"],
        "as_of_date": ["2023-12-29", "2023-12-29", "2023-12-29"],
        "coupon": [5.0, 3.6, 4.5],
        "cfi_code": ["DXXXXXX", "DYYYYYY", "DAAAAAA"],
        "maturity_date": ["2033-12-29", "2025-06-15", "2030-01-01"],
        "callable_flag": [True, False, False],
    }
    return pd.DataFrame(strategic_data)


def test_merged_dataframes(define_source_data: pd.DataFrame, define_target_data: pd.DataFrame) -> None:
    merged_df = pd.merge(
        define_source_data,
        define_target_data,
        on=["instrument_id", "as_of_date"],
        how="outer",
        suffixes=("_legacy", "_strategic"),
        indicator=True,
    )

    assert isinstance(merged_df, pd.DataFrame)
    assert merged_df.shape[0] == 4


def test_legacy_only_exceptions(define_source_data: pd.DataFrame, define_target_data: pd.DataFrame) -> None:
    merged_df = pd.merge(
        define_source_data,
        define_target_data,
        on=["instrument_id", "as_of_date"],
        how="outer",
        suffixes=("_legacy", "_strategic"),
        indicator=True,
    )
    legacy_only = merged_df[merged_df["_merge"] == "left_only"]
    assert legacy_only.shape[0] == 1
    assert legacy_only["instrument_id"].iloc[0] == "US0003"


def test_strategic_only_exceptions(
    define_source_data: pd.DataFrame, define_target_data: pd.DataFrame
) -> None:
    merged_df = pd.merge(
        define_source_data,
        define_target_data,
        on=["instrument_id", "as_of_date"],
        how="outer",
        suffixes=("_legacy", "_strategic"),
        indicator=True,
    )
    strategic_only = merged_df[merged_df["_merge"] == "right_only"]
    assert strategic_only.shape[0] == 1
    assert strategic_only["instrument_id"].iloc[0] == "US0004"


def test_attribute_mismatch_exceptions(
    define_source_data: pd.DataFrame, define_target_data: pd.DataFrame
) -> None:
    merged_df = pd.merge(
        define_source_data,
        define_target_data,
        on=["instrument_id", "as_of_date"],
        how="outer",
        suffixes=("_legacy", "_strategic"),
        indicator=True,
    )
    both_sides = merged_df[merged_df["_merge"] == "both"]
    assert both_sides.shape[0] == 2
    assert both_sides["instrument_id"].tolist() == ["US0001", "US0002"]
    assert both_sides["coupon_legacy"].iloc[0] == 4.99999
    assert both_sides["coupon_strategic"].iloc[0] == 5.0
    assert both_sides["coupon_legacy"].iloc[1] == 3.5
    assert both_sides["coupon_strategic"].iloc[1] == 3.6
    assert both_sides["cfi_code_legacy"].iloc[0] == "DXXXXXX"
    assert both_sides["cfi_code_strategic"].iloc[0] == "DXXXXXX"


def test_dataframe_differ(define_source_data: pd.DataFrame, define_target_data: pd.DataFrame) -> None:
    rules = AttributeRuleSet(
        {
            "coupon": AttributeRule(name="coupon", type="float", tolerance=0.01),
            "cfi_code": AttributeRule(name="cfi_code", type="string", ignore_case=True, trim=True),
            "maturity_date": AttributeRule(name="maturity_date", type="date"),
            "callable_flag": AttributeRule(name="callable_flag", type="boolean"),
        }
    )
    differ = DataFrameDiffer(rules)
    exceptions = differ.build_exceptions_df(
        define_source_data,
        define_target_data,
        rules,
        run_id="test-run",
        as_of_date=pd.Timestamp("2023-12-29").date(),
    )
    assert not exceptions.empty
    assert set(exceptions["difference_type"].unique()) <= {
        "ONLY_IN_LEGACY",
        "ONLY_IN_STRATEGIC",
        "VALUE_MISMATCH",
    }
