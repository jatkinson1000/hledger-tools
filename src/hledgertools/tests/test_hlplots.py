"""Tests for the hlplots plotting functions."""

import polars as pl
import pytest
from matplotlib.figure import Figure
from plotly.graph_objects import Figure as PlotlyFigure

from hledgertools.hlplots import (
    plot_budget_vs_actual,
    plot_essentials_discretionary,
    plot_matrix_heatmap,
    plot_monthly_categories,
    plot_net_worth,
    plot_sankey_cashflow,
)

# ========================
# Fixtures
# ========================


@pytest.fixture
def sankey_df() -> pl.DataFrame:
    """Single-period balance DataFrame for Sankey testing."""
    return pl.DataFrame(
        {
            "account": [
                "revenues:salary",
                "revenues:market-pay",
                "revenues:interest",
                "expenses:tax:paye",
                "expenses:tax:ni",
                "expenses:pension:uss",
                "expenses:groceries",
                "expenses:utilities",
                "expenses:car",
                "expenses:entertainment",
                "expenses:home",
            ],
            "balance": [
                "-50000",
                "-5000",
                "-100",
                "10000",
                "3000",
                "2000",
                "2000",
                "1500",
                "1000",
                "800",
                "500",
            ],
        }
    )


@pytest.fixture
def monthly_df() -> pl.DataFrame:
    """Transposed monthly balance DataFrame for monthly plot testing."""
    return pl.DataFrame(
        {
            "date": ["2025-04", "2025-05", "2025-06"],
            "expenses:groceries": [150.0, 120.0, 130.0],
            "expenses:utilities": [100.0, 90.0, 110.0],
            "expenses:car": [50.0, 60.0, 70.0],
        }
    )


@pytest.fixture
def budget_df() -> pl.DataFrame:
    """Budget vs actual DataFrame."""
    return pl.DataFrame(
        {
            "Account": [
                "expenses:groceries",
                "expenses:utilities",
                "expenses:car",
            ],
            "Commodity": ["£", "£", "£"],
            "2025-04-01..2026-03-31": ["2000", "1500", "1000"],
            "budget": ["1950", "1572", "1570"],
        }
    )


@pytest.fixture
def net_worth_df() -> pl.DataFrame:
    """Monthly balance DataFrame for net worth testing."""
    return pl.DataFrame(
        {
            "date": ["2025-04", "2025-05", "2025-06"],
            "assets": [50000.0, 52000.0, 55000.0],
            "liabilities": [-150000.0, -149000.0, -148000.0],
        }
    )


# ========================
# Sankey tests
# ========================


class TestPlotSankeyCashflow:
    """Tests for plot_sankey_cashflow."""

    def test_returns_plotly_figure(self, sankey_df):
        fig = plot_sankey_cashflow(sankey_df)
        assert isinstance(fig, PlotlyFigure)

    def test_has_sankey_trace(self, sankey_df):
        fig = plot_sankey_cashflow(sankey_df)
        assert fig.data[0].type == "sankey"

    def test_with_min_flow(self, sankey_df):
        fig = plot_sankey_cashflow(sankey_df, min_flow=500.0)
        assert isinstance(fig, PlotlyFigure)

    def test_custom_deduction_patterns(self, sankey_df):
        fig = plot_sankey_cashflow(sankey_df, deduction_patterns=["expenses:tax"])
        assert isinstance(fig, PlotlyFigure)

    def test_raises_on_no_revenue(self, sankey_df):
        expense_only = sankey_df.filter(pl.col("account").str.contains("expenses:"))
        with pytest.raises(ValueError, match="No revenue accounts"):
            plot_sankey_cashflow(expense_only)

    def test_static_image_export(self, sankey_df):
        fig = plot_sankey_cashflow(sankey_df, min_flow=200.0)
        img = fig.to_image(format="png", width=400, height=300)
        assert len(img) > 0

    def test_depth_disambiguates_nodes(self):
        df = pl.DataFrame(
            {
                "account": [
                    "revenues:salary",
                    "expenses:car:insurance",
                    "expenses:home:insurance",
                ],
                "balance": ["-50000", "1000", "2000"],
            }
        )
        fig = plot_sankey_cashflow(df, depth=2)
        labels = list(fig.data[0].node.label)
        assert "Car Insurance" in labels
        assert "Home Insurance" in labels


# ========================
# Monthly categories tests
# ========================


class TestPlotMonthlyCategories:
    """Tests for plot_monthly_categories."""

    def test_returns_figure(self, monthly_df):
        fig = plot_monthly_categories(
            monthly_df, ["expenses:groceries", "expenses:utilities"]
        )
        assert isinstance(fig, Figure)

    def test_grouped_kind(self, monthly_df):
        fig = plot_monthly_categories(
            monthly_df,
            ["expenses:groceries", "expenses:utilities"],
            kind="grouped",
        )
        assert isinstance(fig, Figure)

    def test_stacked_kind(self, monthly_df):
        fig = plot_monthly_categories(
            monthly_df,
            ["expenses:groceries", "expenses:utilities"],
            kind="stacked",
        )
        assert isinstance(fig, Figure)

    def test_custom_title(self, monthly_df):
        fig = plot_monthly_categories(
            monthly_df,
            ["expenses:groceries"],
            title="My Title",
        )
        assert fig.axes[0].get_title() == "My Title"

    def test_depth_disambiguates_labels(self):
        df = pl.DataFrame(
            {
                "date": ["2025-04", "2025-05"],
                "expenses:car:insurance": [100.0, 110.0],
                "expenses:home:insurance": [200.0, 210.0],
            }
        )
        fig = plot_monthly_categories(
            df,
            ["expenses:car:insurance", "expenses:home:insurance"],
            depth=2,
        )
        labels = [t.get_text() for t in fig.axes[0].get_legend().get_texts()]
        assert "Car Insurance" in labels
        assert "Home Insurance" in labels

    def test_filters_commodity_row(self):
        df = pl.DataFrame(
            {
                "date": ["commodity", "2025-04"],
                "expenses:groceries": ["£", "150.0"],
            }
        )
        fig = plot_monthly_categories(df, ["expenses:groceries"])
        assert isinstance(fig, Figure)


# ========================
# Budget vs actual tests
# ========================


class TestPlotBudgetVsActual:
    """Tests for plot_budget_vs_actual."""

    def test_returns_figure(self, budget_df):
        fig = plot_budget_vs_actual(budget_df)
        assert isinstance(fig, Figure)

    def test_auto_detect_actual_col(self, budget_df):
        fig = plot_budget_vs_actual(budget_df)
        assert isinstance(fig, Figure)

    def test_explicit_actual_col(self, budget_df):
        fig = plot_budget_vs_actual(budget_df, actual_col="2025-04-01..2026-03-31")
        assert isinstance(fig, Figure)

    def test_filters_zero_rows(self):
        df = pl.DataFrame(
            {
                "Account": ["expenses:food", "expenses:empty"],
                "Commodity": ["£", "£"],
                "actual": ["100", "0"],
                "budget": ["0", "0"],
            }
        )
        fig = plot_budget_vs_actual(df, actual_col="actual")
        assert isinstance(fig, Figure)

    def test_custom_title(self, budget_df):
        fig = plot_budget_vs_actual(budget_df, title="Budget Test")
        assert fig.axes[0].get_title() == "Budget Test"


# ========================
# Essentials vs discretionary tests
# ========================


class TestPlotEssentialsDiscretionary:
    """Tests for plot_essentials_discretionary."""

    def test_returns_figure(self, sankey_df):
        essentials = ["expenses:groceries", "expenses:utilities"]
        fig = plot_essentials_discretionary(sankey_df, essentials)
        assert isinstance(fig, Figure)

    def test_with_regex_patterns(self, sankey_df):
        essentials = ["expenses:groceries", "expenses:car"]
        fig = plot_essentials_discretionary(sankey_df, essentials)
        assert isinstance(fig, Figure)

    def test_all_essential(self):
        df = pl.DataFrame(
            {
                "account": ["expenses:groceries", "expenses:utilities"],
                "balance": ["100", "200"],
            }
        )
        fig = plot_essentials_discretionary(
            df, ["expenses:groceries", "expenses:utilities"]
        )
        assert isinstance(fig, Figure)

    def test_all_discretionary(self):
        df = pl.DataFrame(
            {
                "account": ["expenses:entertainment", "expenses:holiday"],
                "balance": ["100", "200"],
            }
        )
        fig = plot_essentials_discretionary(df, ["expenses:groceries"])
        assert isinstance(fig, Figure)

    def test_custom_title(self, sankey_df):
        essentials = ["expenses:groceries"]
        fig = plot_essentials_discretionary(
            sankey_df, essentials, title="Essentials Test"
        )
        assert fig.axes[0].get_title() == "Essentials Test"


# ========================
# Net worth tests
# ========================


class TestPlotNetWorth:
    """Tests for plot_net_worth."""

    def test_returns_figure(self, net_worth_df):
        fig = plot_net_worth(net_worth_df)
        assert isinstance(fig, Figure)

    def test_auto_detect_cols(self, net_worth_df):
        fig = plot_net_worth(net_worth_df)
        assert isinstance(fig, Figure)

    def test_explicit_cols(self, net_worth_df):
        fig = plot_net_worth(
            net_worth_df,
            asset_cols=["assets"],
            liability_cols=["liabilities"],
        )
        assert isinstance(fig, Figure)

    def test_filters_commodity_row(self):
        df = pl.DataFrame(
            {
                "date": ["commodity", "2025-04"],
                "assets": ["£", "50000"],
                "liabilities": ["£", "-150000"],
            }
        )
        fig = plot_net_worth(df)
        assert isinstance(fig, Figure)

    def test_custom_title(self, net_worth_df):
        fig = plot_net_worth(net_worth_df, title="Net Worth Test")
        assert fig.axes[0].get_title() == "Net Worth Test"


# ========================
# Heatmap tests
# ========================


class TestPlotMatrixHeatmap:
    """Tests for plot_matrix_heatmap."""

    def test_returns_figure_date_oriented(self, monthly_df):
        fig = plot_matrix_heatmap(monthly_df)
        assert isinstance(fig, Figure)

    def test_account_oriented(self):
        df = pl.DataFrame(
            {
                "account": ["expenses:food", "expenses:car"],
                "2025-04": [100.0, 50.0],
                "2025-05": [120.0, 30.0],
            }
        )
        fig = plot_matrix_heatmap(df)
        assert isinstance(fig, Figure)

    def test_filters_commodity_row(self):
        df = pl.DataFrame(
            {
                "date": ["commodity", "2025-04"],
                "expenses:food": ["£", "100"],
            }
        )
        fig = plot_matrix_heatmap(df)
        assert isinstance(fig, Figure)

    def test_custom_title(self, monthly_df):
        fig = plot_matrix_heatmap(monthly_df, title="Heatmap Test")
        assert fig.axes[0].get_title() == "Heatmap Test"

    def test_raises_on_no_index_col(self):
        df = pl.DataFrame({"foo": [1], "bar": [2]})
        with pytest.raises(ValueError, match="account.*date"):
            plot_matrix_heatmap(df)
