"""Tests for the HLDataFrame class."""

from io import StringIO

import polars as pl

from hledgertools.hldataframe import HLDataFrame


def _make_balance_df() -> HLDataFrame:
    """Create a small balance-style HLDataFrame for testing."""
    return HLDataFrame(
        {
            "account": [
                "expenses:food",
                "expenses:car",
                "revenues:salary",
            ],
            "2025-01": ["100.0", "50.0", "-2000.0"],
            "2025-02": ["120.0", "30.0", "-2000.0"],
        }
    )


def _make_currency_df() -> HLDataFrame:
    """Create a HLDataFrame with currency-formatted strings."""
    return HLDataFrame(
        {
            "account": ["expenses:food", "expenses:car"],
            "jan": ["£100.00", "£50.50"],
            "feb": ["£120.00", "£30.00"],
        }
    )


class TestHLDataFrameSubclassing:
    """Tests that HLDataFrame operations return HLDataFrame instances."""

    def test_from_dict_returns_hldf(self):
        df = HLDataFrame({"a": [1, 2], "b": [3, 4]})
        assert isinstance(df, HLDataFrame)

    def test_rename_returns_hldf(self):
        df = _make_balance_df()
        result = df.rename({"account": "acct"})
        assert isinstance(result, HLDataFrame)

    def test_filter_returns_hldf(self):
        df = _make_balance_df()
        result = df.filter(pl.col("account") == "expenses:food")
        assert isinstance(result, HLDataFrame)
        assert len(result) == 1

    def test_with_columns_returns_hldf(self):
        df = _make_balance_df()
        result = df.with_columns(pl.lit(0).alias("new_col"))
        assert isinstance(result, HLDataFrame)

    def test_select_returns_hldf(self):
        df = _make_balance_df()
        result = df.select("account")
        assert isinstance(result, HLDataFrame)


class TestFromCsv:
    """Tests for the from_csv classmethod."""

    def test_from_csv_returns_hldf(self):
        csv_text = "account,balance\nexpenses:food,100.0\n"
        df = HLDataFrame.from_csv(StringIO(csv_text), infer_schema=False)
        assert isinstance(df, HLDataFrame)
        assert df.columns == ["account", "balance"]
        assert len(df) == 1

    def test_from_csv_with_separator(self):
        csv_text = "account;balance\nexpenses:food;100.0\n"
        df = HLDataFrame.from_csv(StringIO(csv_text), infer_schema=False, separator=";")
        assert isinstance(df, HLDataFrame)
        assert df.columns == ["account", "balance"]


class TestFilterAccounts:
    """Tests for filter_accounts method."""

    def test_filter_single_pattern(self):
        df = _make_balance_df()
        result = df.filter_accounts("expenses:food")
        assert isinstance(result, HLDataFrame)
        assert len(result) == 1
        assert result["account"].to_list() == ["expenses:food"]

    def test_filter_multiple_patterns(self):
        df = _make_balance_df()
        result = df.filter_accounts(["expenses:food", "expenses:car"])
        assert len(result) == 2

    def test_filter_regex_pattern(self):
        df = _make_balance_df()
        result = df.filter_accounts("expenses:.*")
        assert len(result) == 2

    def test_filter_exclude(self):
        df = _make_balance_df()
        result = df.filter_accounts("expenses:.*", exclude=True)
        assert len(result) == 1
        assert result["account"].to_list() == ["revenues:salary"]

    def test_filter_no_match(self):
        df = _make_balance_df()
        result = df.filter_accounts("assets:.*")
        assert len(result) == 0

    def test_filter_custom_column(self):
        df = HLDataFrame({"acct": ["expenses:food", "revenues:salary"], "val": [1, 2]})
        result = df.filter_accounts("expenses:.*", account_col="acct")
        assert len(result) == 1


class TestColToDatetime:
    """Tests for col_to_datetime method."""

    def test_col_to_datetime_default_format(self):
        df = HLDataFrame({"date": ["2025-01", "2025-02"], "value": [100, 200]})
        result = df.col_to_datetime()
        assert isinstance(result, HLDataFrame)
        assert result["date"].dtype == pl.Datetime

    def test_col_to_datetime_custom_format(self):
        df = HLDataFrame({"date": ["2025-01-15", "2025-02-15"], "value": [100, 200]})
        result = df.col_to_datetime(date_format="%Y-%m-%d")
        assert result["date"].dtype == pl.Datetime

    def test_col_to_datetime_custom_col_name(self):
        df = HLDataFrame({"period": ["2025-01", "2025-02"], "value": [100, 200]})
        result = df.col_to_datetime(datecol_name="period")
        assert result["period"].dtype == pl.Datetime


class TestTranspose:
    """Tests for the transpose method."""

    def test_transpose_auto_name_account(self):
        df = HLDataFrame(
            {
                "account": ["expenses:food", "expenses:car"],
                "2025-01": [100.0, 50.0],
                "2025-02": [120.0, 30.0],
            }
        )
        result = df.transpose()
        assert isinstance(result, HLDataFrame)
        assert "date" in result.columns
        assert "expenses:food" in result.columns
        assert "expenses:car" in result.columns
        assert len(result) == 2

    def test_transpose_auto_name_date(self):
        df = HLDataFrame(
            {
                "date": ["2025-01", "2025-02"],
                "expenses:food": [100.0, 120.0],
            }
        )
        result = df.transpose()
        assert "account" in result.columns
        assert "2025-01" in result.columns
        assert "2025-02" in result.columns
        assert len(result) == 1
        assert result["account"].to_list() == ["expenses:food"]

    def test_transpose_standard_mode(self):
        df = HLDataFrame({"col1": ["a", "b"], "col2": [1, 2], "col3": [3, 4]})
        result = df.transpose(auto_name=False)
        assert isinstance(result, HLDataFrame)

    def test_transpose_account_capital(self):
        """Test that 'Account' (capital) is normalised to 'account'."""
        df = HLDataFrame(
            {
                "Account": ["expenses:food"],
                "2025-01": [100.0],
            }
        )
        result = df.transpose()
        assert "date" in result.columns
        assert "expenses:food" in result.columns


class TestCurrencyToNumber:
    """Tests for currency_to_number method."""

    def test_currency_to_number_basic(self):
        df = _make_currency_df()
        result = df.currency_to_number(preserve_cols={"account"})
        assert isinstance(result, HLDataFrame)
        assert result["jan"].dtype == pl.Float64
        assert result["feb"].dtype == pl.Float64
        assert result["jan"].to_list() == [100.0, 50.5]

    def test_currency_to_number_custom_symbol(self):
        df = HLDataFrame(
            {
                "account": ["a", "b"],
                "val": ["$100.00", "$50.00"],
            }
        )
        result = df.currency_to_number(currency_symbol="$", preserve_cols={"account"})
        assert result["val"].to_list() == [100.0, 50.0]

    def test_currency_to_number_change_cols(self):
        df = _make_currency_df()
        result = df.currency_to_number(
            preserve_cols={"account"},
            change_cols={"jan"},
        )
        assert result["jan"].dtype == pl.Float64
        assert result["feb"].dtype == pl.Utf8

    def test_currency_to_number_with_commas(self):
        df = HLDataFrame(
            {
                "account": ["a"],
                "val": ["£1,234.56"],
            }
        )
        result = df.currency_to_number(preserve_cols={"account"})
        assert result["val"].to_list() == [1234.56]

    def test_currency_to_number_already_float(self):
        df = HLDataFrame(
            {
                "account": ["a", "b"],
                "val": [100.0, 50.5],
            }
        )
        result = df.currency_to_number(preserve_cols={"account"})
        assert result["val"].dtype == pl.Float64
        assert result["val"].to_list() == [100.0, 50.5]
