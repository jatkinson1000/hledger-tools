"""Utilities for manipulating hledger dataframes."""

import polars as pl


class HLDataFrame(pl.DataFrame):
    """A Polars DataFrame subclass with utilities for hledger data manipulation."""

    # ======================================
    # Common overrides to return HLDataFrame
    # ======================================

    @classmethod
    def _from_pydf(cls, py_df):
        """Override to ensure operations return HLDataFrame instances."""
        df = super()._from_pydf(py_df)
        df.__class__ = cls
        return df

    def rename(self, mapping: dict, **kwargs) -> "HLDataFrame":
        """Override rename to return HLDataFrame."""
        return self.__class__(super().rename(mapping, **kwargs))

    def filter(self, *args, **kwargs) -> "HLDataFrame":
        """Override filter to return HLDataFrame."""
        return self.__class__(super().filter(*args, **kwargs))

    def with_columns(self, *args, **kwargs) -> "HLDataFrame":
        """Override with_columns to return HLDataFrame."""
        return self.__class__(super().with_columns(*args, **kwargs))

    def select(self, *args, **kwargs) -> "HLDataFrame":
        """Override select to return HLDataFrame."""
        return self.__class__(super().select(*args, **kwargs))

    @classmethod
    def from_csv(
        cls, csv_text: str, infer_schema: bool = False, **kwargs
    ) -> "HLDataFrame":
        """
        Read CSV text and return as HLDataFrame.

        Parameters
        ----------
        csv_text : str
            CSV formatted string or file path
        infer_schema : bool, default False
            Whether to infer column types. False keeps as strings (safer for hledger)
        **kwargs
            Additional arguments passed to pl.read_csv()

        Returns
        -------
        HLDataFrame
            HLDataFrame instance
        """
        return cls(pl.read_csv(csv_text, infer_schema=infer_schema, **kwargs))

    # ===============================
    # DATAFRAME MANIPULATION
    # ===============================

    def filter_accounts(
        self,
        patterns: list[str] | str,
        account_col: str = "account",
        exclude: bool = False,
    ) -> "HLDataFrame":
        """
        Filter rows by account name pattern(s).

        Parameters
        ----------
        patterns : list of str or str
            Account name pattern(s) to match. Supports regex.
        account_col : str, default "account"
            Name of the account column
        exclude : bool, default False
            If True, exclude matching accounts instead of including them

        Returns
        -------
        HLDataFrame
            Filtered HLDataFrame

        Examples
        --------
        >>> df.filter_accounts("expenses:food")
        >>> df.filter_accounts(["assets:bank", "assets:cash"])
        >>> df.filter_accounts("expenses:.*", exclude=True)
        """
        if isinstance(patterns, str):
            patterns = [patterns]

        pattern = "|".join(patterns)
        condition = pl.col(account_col).str.contains(pattern)

        if exclude:
            condition = ~condition

        return self.__class__(self.filter(condition))

    # ===============================
    # DATA MANIPULATION
    # ===============================

    def col_to_datetime(
        self,
        datecol_name="date",
        date_format: str = "%Y-%m",
    ) -> "HLDataFrame":
        """
        Convert string date column to datetime format.

        Parameters
        ----------
        datecol_name : str, default "date"
            Name of the column containing date strings
        date_format : str, default "%Y-%m"
            strftime format string for parsing dates

        Returns
        -------
        HLDataFrame
            HLDataFrame with date column converted to datetime type
        """
        return self.__class__(
            self.with_columns(pl.col(datecol_name).str.to_datetime(date_format))
        )

    def transpose(
        self,
        id_col: str | None = None,
        include_header: bool = True,
        auto_name: bool = True,
    ) -> "HLDataFrame":
        """
        Transpose DataFrame with optional intelligent column naming for hledger data.

        Enhanced version of Polars transpose that can automatically handle hledger's
        account/date structure when auto_name=True. Otherwise behaves like standard
        transpose.

        Parameters
        ----------
        id_col : str, optional
            Name of the column to use as identifier (will become first column after
            transpose). If None and auto_name=True, auto-detects "account" or "date".
            If None and auto_name=False, performs standard transpose.
        include_header : bool, default True
            Whether to include the header in transpose
        auto_name : bool, default True
            Whether to automatically name columns based on hledger conventions.
            If False, behaves like standard Polars transpose.

        Returns
        -------
        HLDataFrame
            Transposed HLDataFrame

        Notes
        -----
        When auto_name=False, this behaves identically to parent Polars transpose().
        When auto_name=True, it attempts to intelligently handle account/date columns.
        Date columns will be string type - use col_to_datetime() to convert if needed.
        hledger provides a `--transpose` option that performs a similar transformation.

        Examples
        --------
        Smart hledger transpose (auto_name=True):
            Input:  account,2024-01,2024-02
                    expenses:food,£100,£150
            Output: date,expenses:food
                    2024-01,£100
                    2024-02,£150

        Standard transpose (auto_name=False):
            Input:  col1,col2,col3
                    a,b,c
            Output: column_0,a
                    col1,a
                    col2,b
                    col3,c
        """
        # If auto_name is disabled or no id_col heuristic applies, use parent method
        if not auto_name:
            return self.__class__(super().transpose(include_header=include_header))

        # Auto-detect id column if not specified
        if id_col is None:
            if "account" in self.columns or "Account" in self.columns:
                id_col = "account"
            elif "date" in self.columns:
                id_col = "date"
            else:
                # Fallback to standard transpose if can't auto-detect
                return self.__class__(super().transpose(include_header=include_header))

        # Normalize "Account" to "account" if needed (hledger is not consistent in name)
        df = self.rename({"Account": "account"}) if "Account" in self.columns else self

        # Get values from id column for new column headers
        id_values = df[id_col].to_list()

        # Determine new first column name (opposite of current)
        new_id_col = "date" if id_col == "account" else "account"

        # Transpose dataframe after removing id column and set headers
        df_transposed = df.drop(id_col).transpose(include_header=include_header)
        df_transposed.columns = [new_id_col, *id_values]

        return self.__class__(df_transposed)

    def currency_to_number(
        self,
        currency_symbol: str = "£",
        preserve_cols: set[str] | None = None,
        change_cols: set[str] | None = None,
    ) -> "HLDataFrame":
        """
        Convert currency string columns to numeric Float64 values.

        Removes currency symbols and converts string representations to numbers.
        Useful for processing hledger output with currency formatting.

        Parameters
        ----------
        currency_symbol : str, default "£"
            Currency symbol to remove from strings
        preserve_cols : set of str, optional
            Set of column names to exclude from conversion
        change_cols : set of str, optional
            Set of column names to convert. If None, converts all columns
            except those in preserve_cols

        Returns
        -------
        HLDataFrame
            HLDataFrame with specified columns converted to Float64 numeric type

        Examples
        --------
        >>> df.currency_to_number(currency_symbol="£", preserve_cols={"date", "acct"})
        """
        if change_cols is None:
            change_cols = set(self.columns)
        if preserve_cols is None:
            preserve_cols = set()

        return self.__class__(
            self.with_columns(
                [
                    pl.col(col).str.replace(currency_symbol, "").cast(pl.Float64)
                    for col in change_cols
                    if col not in preserve_cols
                ]
            )
        )

    # def fill_zero_for_missing_dates(
    #     self,
    #     date_col: str = "date",
    #     freq: str = "1mo",
    # ) -> "HLDataFrame":
    #     """
    #     Fill missing dates with zero values for time series analysis.

    #     Useful when hledger output has date gaps (e.g., months with no transactions).

    #     Parameters
    #     ----------
    #     date_col : str, default "date"
    #         Name of the date column
    #     freq : str, default "1mo"
    #         Frequency string for filling dates (e.g., "1d", "1mo", "1y")

    #     Returns
    #     -------
    #     HLDataFrame
    #         HLDataFrame with continuous date range, missing values filled with 0

    #     Notes
    #     -----
    #     Assumes date column is already in datetime format.
    #     """
    #     # Create complete date range
    #     date_range = pl.date_range(
    #         self[date_col].min(),
    #         self[date_col].max(),
    #         interval=freq,
    #         eager=True,
    #     ).alias(date_col)
    #
    #     # Join with complete range and fill nulls with 0
    #     complete_df = (
    #         pl.DataFrame({date_col: date_range})
    #         .join(self, on=date_col, how="left")
    #         .fill_null(0)
    #     )
    #
    #     return self.__class__(complete_df)
