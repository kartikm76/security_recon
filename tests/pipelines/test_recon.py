import pandas as pd
from pytest import fixture 
from pipelines.recon import DataFrameDiffer
from pipelines.dictionary import AttributeRuleSet, AttributeRule

@fixture(scope="module")
def define_source_data():
    ## define legacy data frame    
    legacy_data = {
        "instrument_id": ["US0001", "US0002", "US0003"],
        "as_of_date": ["2023-12-29", "2023-12-29", "2023-12-29"],
        "coupon": [4.99999, 3.5, 4.0],
        "cfi_code": ["DXXXXXX", "DYYYYYY", "DZZZZZZ"],
        "maturity_date": ["2033-12-29", "2025-06-15", "2028-11-30"],
        "callable_flag": [True, False, True],
    }
    legacy_df = pd.DataFrame(legacy_data)
    return legacy_df

@fixture(scope="module")
def define_target_data():
    ## define strategic data frame    
    strategic_data = {
        "instrument_id": ["US0001", "US0002", "US0004"],
        "as_of_date": ["2023-12-29", "2023-12-29", "2023-12-29"],
        "coupon": [5.0, 3.6, 4.5],
        "cfi_code": ["DXXXXXX", "DYYYYYY", "DAAAAAA"],
        "maturity_date": ["2033-12-29", "2025-06-15", "2030-01-01"],
        "callable_flag": [True, False, False],
    }
    strategic_df = pd.DataFrame(strategic_data)
    return strategic_df

def test_merged_dataframes(define_source_data, define_target_data):
    legacy_df = define_source_data
    strategic_df = define_target_data
    
    merged_df = pd.merge (
        legacy_df,
        strategic_df,
        on = ["instrument_id", "as_of_date"],
        how = "outer",
        suffixes = ("_legacy", "_strategic"),
        indicator = True,
    )

    assert isinstance(merged_df, pd.DataFrame)      ## Check type
    assert merged_df.shape[0] == 4                  ## 3 unique instrument_ids in legacy and 3 in strategic, with 2 overlaps

    
def test_legacy_only_exceptions(define_source_data, define_target_data):    
    legacy_df = define_source_data
    strategic_df = define_target_data
    merged_df = pd.merge (
        legacy_df,
        strategic_df,
        on = ["instrument_id", "as_of_date"],
        how = "outer",
        suffixes = ("_legacy", "_strategic"),
        indicator = True,
    )
    print (merged_df)
    legacy_only = merged_df[merged_df["_merge"] == "left_only"]     ## add test for legacy only exceptions
    assert legacy_only.shape[0] == 1          ## Only US0003 is legacy only
    assert legacy_only["instrument_id"].iloc[0] == "US0003"   

def test_strategic_only_exceptions(define_source_data, define_target_data):
    legacy_df = define_source_data
    strategic_df = define_target_data
    merged_df = pd.merge (
        legacy_df,
        strategic_df,
        on = ["instrument_id", "as_of_date"],
        how = "outer",
        suffixes = ("_legacy", "_strategic"),
        indicator = True,
    )
    print (merged_df)
    ## add test for strategic only exceptions
    strategic_only = merged_df[merged_df["_merge"] == "right_only"] ## add test for strategic only exceptions    
    assert strategic_only.shape[0] == 1          ## Only US0004 is strategic only
    assert strategic_only["instrument_id"].iloc[0] == "US0004"

def test_attribute_mismatch_exceptions(define_source_data, define_target_data):
    legacy_df = define_source_data
    strategic_df = define_target_data
    merged_df = pd.merge (
        legacy_df,
        strategic_df,
        on = ["instrument_id", "as_of_date"],
        how = "outer",
        suffixes = ("_legacy", "_strategic"),
        indicator = True,
    )
    both_sides = merged_df[merged_df["_merge"] == "both"]   ## Records present on both sides
    assert both_sides.shape[0] == 2                        ## US0001 and US0002 present on both sides
    assert both_sides["instrument_id"].tolist() == ["US0001", "US0002"]
    ## Further checks for attribute mismatches can be added here
    assert both_sides["coupon_legacy"].iloc[0] == 4.99999
    assert both_sides["coupon_strategic"].iloc[0] == 5.0
    assert both_sides["coupon_legacy"].iloc[1] == 3.5
    assert both_sides["coupon_strategic"].iloc[1] == 3.6
    # Further attribute checks can be added as needed
    assert both_sides["cfi_code_legacy"].iloc[0] == "DXXXXXX"
    assert both_sides["cfi_code_strategic"].iloc[0] == "DXXXXXX"