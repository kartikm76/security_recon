import re
from pytest import fixture 

def test_string_matches_expected():
    message = "Hello, World!"
    assert message == "Hello, World!"
    assert isinstance(message, str)
    assert len(message) > 0
    assert message.startswith("Hello")
    assert message.endswith("World!")
    assert re.fullmatch(r"Hello, World!", message)

def test_get_legacy_securities_by_date():
    from datetime import date
    from pipelines.extract import LegacySecurityRepository
    import pandas as pd

    repo = LegacySecurityRepository()
    as_of_date = date(2023, 12, 29)
    df = repo.load_by_date(as_of_date)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    expected_columns = {
        "instrument_id",
        "as_of_date",
        "isin",
        "cfi_code",
        "coupon",
        "maturity_date",
        "currency",
        "callable_flag",
    }
    assert set(df.columns) == expected_columns
    assert pd.to_datetime(df["as_of_date"]).dt.date.eq(as_of_date).all()
    assert pd.to_datetime(df["as_of_date"]).eq(pd.Timestamp(as_of_date)).all()
    assert df["instrument_id"].iloc[0]  == "US0001"     ## Check first entry
    assert df["instrument_id"].eq("US0001").any()       ## Check if at least one entry matches

def test_strategic_securities_load_by_date():
    from datetime import date
    from pipelines.extract import StrategicSecurityRepository
    import pandas as pd

    repo = StrategicSecurityRepository()
    as_of_date = date(2023, 12, 29)
    df = repo.load_by_date(as_of_date)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    expected_columns = {
        "instrument_id",
        "as_of_date",
        "isin",
        "cfi_code",
        "coupon",
        "maturity_date",
        "currency",
        "callable_flag",
    }
    assert set(df.columns) == expected_columns
    assert pd.to_datetime(df["as_of_date"]).dt.date.eq(as_of_date).all()
    assert pd.to_datetime(df["as_of_date"]).eq(pd.Timestamp(as_of_date)).all()
    assert df["instrument_id"].iloc[0]  == "US0001"     ## Check first entry
    assert df["instrument_id"].eq("US0001").any()       ## Check if at least one entry matches