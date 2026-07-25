"""Plotting utilities for hledger data visualisation."""

import matplotlib.pyplot as plt
import numpy as np
import plotly.graph_objects as go
import polars as pl
from matplotlib import ticker
from matplotlib.figure import Figure

from hledgertools.hldataframe import HLDataFrame

# ========================
# Colour constants
# ========================
_INCOME_COLOR = "#2ca02c"
_GROSS_COLOR = "#1f77b4"
_DEDUCTION_COLOR = "#d62728"
_NET_COLOR = "#1f77b4"
_EXPENSE_COLOR = "#ff7f0e"
_SAVINGS_COLOR = "#2ca02c"

_INCOME_LINK = "rgba(44, 160, 44, 0.25)"
_DEDUCTION_LINK = "rgba(214, 39, 40, 0.25)"
_NET_LINK = "rgba(31, 119, 180, 0.25)"
_EXPENSE_LINK = "rgba(255, 127, 14, 0.25)"
_SAVINGS_LINK = "rgba(44, 160, 44, 0.25)"


# ========================
# Helpers
# ========================


def _clean_account_name(name: str, depth: int = 1) -> str:
    """Shorten an hledger account name for display labels.

    Takes the last ``depth`` colon-separated segments, joins them with spaces,
    and title-cases the result. A deeper ``depth`` disambiguates accounts that
    share a common final segment (e.g. ``car:insurance`` vs ``home:insurance``).

    Parameters
    ----------
    name : str
        Full hledger account name (e.g. ``"expenses:car:fuel"``).
    depth : int, default 1
        Number of trailing segments to keep. ``1`` yields ``"Fuel"``;
        ``2`` yields ``"Car Fuel"``.

    Returns
    -------
    str
        Cleaned display name.

    Examples
    --------
    >>> _clean_account_name("expenses:car:fuel")
    'Fuel'
    >>> _clean_account_name("expenses:car:insurance", depth=2)
    'Car Insurance'
    """
    parts = name.split(":")
    selected = parts[-depth:] if depth >= 1 else parts
    return " ".join(selected).replace("-", " ").title()


def _group_small(
    items: list[tuple[str, float]],
    min_flow: float,
    other_label: str,
) -> list[tuple[str, float]]:
    """Group items with value below ``min_flow`` into a single aggregated item.

    Parameters
    ----------
    items : list of (str, float)
        Ordered list of (name, value) pairs.
    min_flow : float
        Threshold below which items are aggregated into ``other_label``.
    other_label : str
        Label for the aggregated small-flow item.

    Returns
    -------
    list of (str, float)
        Filtered list with small items replaced by a single ``other_label`` entry.
    """
    if min_flow <= 0:
        return items
    large = [(name, val) for name, val in items if val >= min_flow]
    small_total = sum(val for _, val in items if val < min_flow)
    if small_total > 0:
        large.append((other_label, small_total))
    return large


# ========================
# Heatmap
# ========================


def plot_matrix_heatmap(  # noqa: PLR0913, PLR0917
    df: pl.DataFrame,
    title: str = "Values by Category and Month",
    xlabel: str = "Month",
    ylabel: str = "Category",
    cbar_label: str = "£ Value",
    depth: int = 1,
) -> Figure:
    """Render a heatmap of account balances across periods.

    Accepts either orientation: accounts-as-rows (with an ``account`` column)
    or dates-as-rows (with a ``date`` column, which is transposed internally).
    The ``commodity`` row produced by hledger CSV output is filtered out.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame from a periodic hledger balance CSV (``--layout=bare``).
        Must contain either an ``account`` or ``date`` column.
    title : str, default "Values by Category and Month"
        Title for the heatmap.
    xlabel : str, default "Month"
        Label for the x-axis.
    ylabel : str, default "Category"
        Label for the y-axis.
    cbar_label : str, default "£ Value"
        Label for the colour bar.
    depth : int, default 1
        Number of trailing account-name segments to keep for row/column
        labels (see ``_clean_account_name``).

    Returns
    -------
    Figure
        Matplotlib Figure containing the heatmap.
    """
    if "date" in df.columns:
        index_col = "date"
    elif "account" in df.columns:
        index_col = "account"
    else:
        msg = "DataFrame must contain an 'account' or 'date' column."
        raise ValueError(msg)

    cleaned = df.filter(pl.col(index_col) != "commodity")
    value_cols = [c for c in cleaned.columns if c != index_col]
    cleaned = HLDataFrame(cleaned).currency_to_number(preserve_cols={index_col})

    if index_col == "date":
        row_labels = [_clean_account_name(c, depth=depth) for c in value_cols]
        col_labels = cleaned[index_col].to_list()
        matrix = cleaned.select(value_cols).to_numpy().T
    else:
        row_labels = [
            _clean_account_name(r[index_col], depth=depth)
            for r in cleaned.to_dicts()
        ]
        col_labels = value_cols
        matrix = cleaned.select(value_cols).to_numpy()

    fig, ax = plt.subplots(figsize=(14, min(0.6 * len(row_labels), 10)))
    im = ax.imshow(matrix, cmap="Reds", aspect="auto")

    n_rows, n_cols = matrix.shape
    for i in range(n_rows):
        for j in range(n_cols):
            ax.text(
                j,
                i,
                f"{matrix[i, j]:.0f}",
                ha="center",
                va="center",
                fontsize=8,
            )

    ax.set_xticks(range(n_cols))
    ax.set_xticklabels(col_labels, rotation=45, ha="right")
    ax.set_yticks(range(n_rows))
    ax.set_yticklabels(row_labels)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    fig.colorbar(im, ax=ax, label=cbar_label)
    fig.tight_layout()
    return fig


# ========================
# Sankey (Plotly)
# ========================


def plot_sankey_cashflow(  # noqa: PLR0913, PLR0915
    df: pl.DataFrame,
    account_col: str = "account",
    balance_col: str = "balance",
    deduction_patterns: list[str] | None = None,
    savings_label: str = "Savings & Investments",
    min_flow: float = 0.0,
    title: str = "Cash Flow Sankey",
    depth: int = 1,
) -> go.Figure:
    """Create a Sankey diagram showing cash flow from income to expenses.

    Processes a single-period balance DataFrame to visualise how income flows
    through payroll deductions (tax, pension) to net disposable income, then
    to living expenses and savings.

    The diagram has four layers::

        Income sources -> Gross Income -> Deductions + Net Income
                                          Net Income -> Living expenses + Savings

    ``Savings`` represents income not consumed by expenses (includes debt
    principal repayment and asset accumulation).

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame with account names and balances from a single-period
        hledger ``balance`` command (``--layout=bare``).
    account_col : str, default "account"
        Name of the column containing account names.
    balance_col : str, default "balance"
        Name of the column containing balance values (strings or floats).
    deduction_patterns : list of str, optional
        Regex patterns for expense accounts treated as payroll deductions.
        Defaults to ``["expenses:tax", "expenses:pension"]``.
    savings_label : str, default "Savings & Investments"
        Label for the savings node in the diagram.
    min_flow : float, default 0.0
        Minimum flow value to show as a separate node. Flows below this
        threshold are aggregated into an "Other" node per layer.
    title : str, default "Cash Flow Sankey"
        Title for the diagram.
    depth : int, default 1
        Number of trailing account-name segments to keep for node labels
        (see ``_clean_account_name``).

    Returns
    -------
    go.Figure
        Plotly Figure containing the Sankey diagram. Call ``.show()`` for
        interactive display or ``.to_image(format="png")`` for static export.

    Raises
    ------
    ValueError
        If no revenue accounts are found in the DataFrame.
    """
    if deduction_patterns is None:
        deduction_patterns = ["expenses:tax", "expenses:pension"]
    deduction_regex = "|".join(deduction_patterns)

    work = HLDataFrame(df).currency_to_number(preserve_cols={account_col})

    income_df = work.filter(
        pl.col(account_col).str.contains("revenues:") & (pl.col(balance_col) < 0)
    ).with_columns((-pl.col(balance_col)).alias(balance_col))

    if income_df.is_empty():
        msg = "No revenue accounts found in DataFrame."
        raise ValueError(msg)

    deduction_df = work.filter(
        pl.col(account_col).str.contains("expenses:")
        & pl.col(account_col).str.contains(deduction_regex)
        & (pl.col(balance_col) > 0)
    )

    living_df = work.filter(
        pl.col(account_col).str.contains("expenses:")
        & ~pl.col(account_col).str.contains(deduction_regex)
        & (pl.col(balance_col) > 0)
    )

    total_income = income_df[balance_col].sum()
    total_deductions = deduction_df[balance_col].sum()
    total_living = living_df[balance_col].sum()
    savings = total_income - total_deductions - total_living

    income_items = _group_small(
        sorted(
            [
                (_clean_account_name(r[account_col], depth=depth), r[balance_col])
                for r in income_df.to_dicts()
            ],
            key=lambda x: x[1],
            reverse=True,
        ),
        min_flow,
        "Other Income",
    )
    deduction_items = sorted(
        [
            (_clean_account_name(r[account_col], depth=depth), r[balance_col])
            for r in deduction_df.to_dicts()
        ],
        key=lambda x: x[1],
        reverse=True,
    )
    living_items = _group_small(
        sorted(
            [
                (_clean_account_name(r[account_col], depth=depth), r[balance_col])
                for r in living_df.to_dicts()
            ],
            key=lambda x: x[1],
            reverse=True,
        ),
        min_flow,
        "Other Expenses",
    )

    # Build nodes and links
    labels: list[str] = []
    x_pos: list[float] = []
    node_colors: list[str] = []
    link_sources: list[int] = []
    link_targets: list[int] = []
    link_values: list[float] = []
    link_colors: list[str] = []

    # Layer 0: income sources
    for name, _ in income_items:
        labels.append(name)
        x_pos.append(0.0)
        node_colors.append(_INCOME_COLOR)

    # Layer 1: Gross Income
    gross_idx = len(labels)
    labels.append("Gross Income")
    x_pos.append(0.3)
    node_colors.append(_GROSS_COLOR)

    # Layer 2: deductions + Net Income
    deduction_indices: list[int] = []
    for name, _ in deduction_items:
        deduction_indices.append(len(labels))
        labels.append(name)
        x_pos.append(0.6)
        node_colors.append(_DEDUCTION_COLOR)

    net_idx = len(labels)
    labels.append("Net Income")
    x_pos.append(0.6)
    node_colors.append(_NET_COLOR)

    # Layer 3: living expenses + savings
    expense_indices: list[int] = []
    for name, _ in living_items:
        expense_indices.append(len(labels))
        labels.append(name)
        x_pos.append(0.9)
        node_colors.append(_EXPENSE_COLOR)

    savings_idx: int | None = None
    if savings > 0:
        savings_idx = len(labels)
        labels.append(savings_label)
        x_pos.append(0.9)
        node_colors.append(_SAVINGS_COLOR)

    # Links: income -> gross
    for i, (_, val) in enumerate(income_items):
        link_sources.append(i)
        link_targets.append(gross_idx)
        link_values.append(val)
        link_colors.append(_INCOME_LINK)

    # Links: gross -> deductions
    for j, idx in enumerate(deduction_indices):
        link_sources.append(gross_idx)
        link_targets.append(idx)
        link_values.append(deduction_items[j][1])
        link_colors.append(_DEDUCTION_LINK)

    # Link: gross -> net
    net_income = total_income - total_deductions
    link_sources.append(gross_idx)
    link_targets.append(net_idx)
    link_values.append(net_income)
    link_colors.append(_NET_LINK)

    # Links: net -> living expenses
    for j, idx in enumerate(expense_indices):
        link_sources.append(net_idx)
        link_targets.append(idx)
        link_values.append(living_items[j][1])
        link_colors.append(_EXPENSE_LINK)

    # Link: net -> savings
    if savings_idx is not None:
        link_sources.append(net_idx)
        link_targets.append(savings_idx)
        link_values.append(savings)
        link_colors.append(_SAVINGS_LINK)

    fig = go.Figure(
        data=[
            go.Sankey(
                arrangement="snap",
                node={
                    "label": labels,
                    "x": x_pos,
                    "color": node_colors,
                    "pad": 15,
                    "thickness": 20,
                },
                link={
                    "source": link_sources,
                    "target": link_targets,
                    "value": link_values,
                    "color": link_colors,
                },
            )
        ]
    )
    fig.update_layout(
        title=title,
        font_size=12,
        width=1000,
        height=600,
    )
    return fig


# ========================
# Monthly categories (matplotlib)
# ========================


def plot_monthly_categories(  # noqa: PLR0913
    df: pl.DataFrame,
    categories: list[str],
    date_col: str = "date",
    kind: str = "grouped",
    figsize: tuple[float, float] = (12.0, 5.0),
    title: str | None = None,
    ylabel: str = "Amount (£)",
    depth: int = 1,
) -> Figure:
    """Plot monthly expenditure for selected categories as a bar chart.

    Parameters
    ----------
    df : pl.DataFrame
        Transposed monthly balance DataFrame with a date column and account
        columns (from hledger ``balance --transpose --layout=bare``).
        The ``commodity`` row is filtered out automatically.
    categories : list of str
        Column names to plot (e.g. ``["expenses:groceries", "expenses:utilities"]``).
    date_col : str, default "date"
        Name of the column containing period labels or dates.
    kind : str, default "grouped"
        Chart style: ``"grouped"`` for side-by-side bars or ``"stacked"``
        for stacked bars.
    figsize : tuple of float, default (12.0, 5.0)
        Figure size in inches.
    title : str, optional
        Chart title. If ``None``, a default is generated.
    ylabel : str, default "Amount (£)"
        Label for the y-axis.
    depth : int, default 1
        Number of trailing account-name segments to keep for legend labels
        (see ``_clean_account_name``).

    Returns
    -------
    Figure
        Matplotlib Figure containing the bar chart.
    """
    cleaned = df.filter(pl.col(date_col) != "commodity")
    cleaned = HLDataFrame(cleaned).currency_to_number(preserve_cols={date_col})
    n_months = len(cleaned)
    n_cats = len(categories)
    x = np.arange(n_months)
    bar_width = 0.8 / max(n_cats, 1)

    fig, ax = plt.subplots(figsize=figsize)

    if kind == "stacked":
        bottom = np.zeros(n_months)
        for cat in categories:
            values = cleaned[cat].to_numpy()
            ax.bar(
                x,
                values,
                bar_width,
                bottom=bottom,
                label=_clean_account_name(cat, depth=depth),
            )
            bottom += values
    else:
        for i, cat in enumerate(categories):
            offset = (i - n_cats / 2 + 0.5) * bar_width
            values = cleaned[cat].to_numpy()
            ax.bar(
                x + offset,
                values,
                bar_width,
                label=_clean_account_name(cat, depth=depth),
            )

    date_values = cleaned[date_col].to_list()
    ax.set_xticks(x)
    ax.set_xticklabels(date_values, rotation=45, ha="right")
    ax.yaxis.set_major_formatter(ticker.StrMethodFormatter("£{x:,.0f}"))
    ax.set_ylabel(ylabel)
    if title is not None:
        ax.set_title(title)
    ax.grid(axis="y", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    return fig


# ========================
# Budget vs Actual (matplotlib)
# ========================


def plot_budget_vs_actual(  # noqa: PLR0913
    df: pl.DataFrame,
    account_col: str = "Account",
    actual_col: str | None = None,
    budget_col: str = "budget",
    figsize: tuple[float, float] = (12.0, 6.0),
    title: str | None = None,
    depth: int = 1,
) -> Figure:
    """Plot actual versus budgeted amounts as a horizontal bar chart.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame from hledger ``balance --budget --output-format=csv
        --layout=bare``. Must contain an account column, a budget column,
        and an actual column.
    account_col : str, default "Account"
        Name of the column containing account names.
    actual_col : str, optional
        Name of the column containing actual values. If ``None``, the first
        column that is not ``account_col``, ``"Commodity"``, or ``budget_col``
        is used.
    budget_col : str, default "budget"
        Name of the column containing budget values.
    figsize : tuple of float, default (12.0, 6.0)
        Figure size in inches.
    title : str, optional
        Chart title. If ``None``, a default is used.
    depth : int, default 1
        Number of trailing account-name segments to keep for y-axis labels
        (see ``_clean_account_name``).

    Returns
    -------
    Figure
        Matplotlib Figure containing the horizontal bar chart.
    """
    if actual_col is None:
        skip = {account_col.lower(), "commodity", budget_col.lower()}
        for col in df.columns:
            if col.lower() not in skip:
                actual_col = col
                break

    if actual_col is None:
        msg = "Could not identify actual column in DataFrame."
        raise ValueError(msg)

    preserve = {account_col, "Commodity"}
    work = (
        HLDataFrame(df)
        .currency_to_number(preserve_cols=preserve)
        .filter((pl.col(actual_col) != 0) | (pl.col(budget_col) != 0))
        .with_columns(
            (pl.col(actual_col) - pl.col(budget_col)).abs().alias("_abs_var")
        )
        .sort("_abs_var", descending=True)
    )

    accounts = [
        _clean_account_name(r[account_col], depth=depth)
        for r in work.to_dicts()
    ]
    actuals = work[actual_col].to_list()
    budgets = work[budget_col].to_list()
    n = len(accounts)
    y = np.arange(n)
    bar_height = 0.35

    fig, ax = plt.subplots(figsize=figsize)
    bar_colors = [
        "#2ca02c" if a <= b else "#d62728"
        for a, b in zip(actuals, budgets, strict=False)
    ]
    ax.barh(
        y + bar_height / 2,
        actuals,
        bar_height,
        label="Actual",
        color=bar_colors,
    )
    ax.barh(
        y - bar_height / 2,
        budgets,
        bar_height,
        label="Budget",
        color="#999999",
        alpha=0.6,
    )
    ax.set_yticks(y)
    ax.set_yticklabels(accounts)
    ax.xaxis.set_major_formatter(ticker.StrMethodFormatter("£{x:,.0f}"))
    ax.invert_yaxis()
    ax.set_xlabel("Amount (£)")
    if title is not None:
        ax.set_title(title)
    ax.grid(axis="x", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    return fig


# ========================
# Essentials vs Discretionary (matplotlib)
# ========================


def plot_essentials_discretionary(  # noqa: PLR0913
    df: pl.DataFrame,
    essentials: list[str],
    account_col: str = "account",
    balance_col: str = "balance",
    figsize: tuple[float, float] = (8.0, 8.0),
    title: str | None = None,
) -> Figure:
    """Plot a donut chart splitting expenses into essentials and discretionary.

    Essential accounts are identified by matching against a list of patterns
    (e.g. loaded from a file). All other expense accounts are treated as
    discretionary.

    Parameters
    ----------
    df : pl.DataFrame
        Single-period balance DataFrame with account and balance columns.
    essentials : list of str
        Account name patterns to classify as essential (supports regex).
    account_col : str, default "account"
        Name of the column containing account names.
    balance_col : str, default "balance"
        Name of the column containing balance values.
    figsize : tuple of float, default (8.0, 8.0)
        Figure size in inches.
    title : str, optional
        Chart title. If ``None``, a default is used.

    Returns
    -------
    Figure
        Matplotlib Figure containing the donut chart.
    """
    work = HLDataFrame(df).currency_to_number(preserve_cols={account_col})

    expense_df = work.filter(
        pl.col(account_col).str.contains("expenses:") & (pl.col(balance_col) > 0)
    )

    essential_regex = "|".join(essentials)
    essential_df = expense_df.filter(pl.col(account_col).str.contains(essential_regex))
    discretionary_df = expense_df.filter(
        ~pl.col(account_col).str.contains(essential_regex)
    )

    essential_df = essential_df.with_columns(
        pl.col(account_col).str.split(":").list.get(1).alias("group")
    )
    essential_groups = (
        essential_df.group_by("group").agg(pl.col(balance_col).sum()).sort("group")
    )

    sizes = essential_groups[balance_col].to_list()
    labels = [_clean_account_name(g) for g in essential_groups["group"].to_list()]
    disc_total = discretionary_df[balance_col].sum()
    if disc_total > 0:
        sizes.append(disc_total)
        labels.append("Discretionary")

    colors = list(plt.get_cmap("Set3")(np.linspace(0, 1, len(labels))))

    fig, ax = plt.subplots(figsize=figsize)
    wedges, _texts, autotexts = ax.pie(  # type: ignore[misc]
        sizes,
        labels=labels,
        autopct="%1.1f%%",
        colors=colors,
        startangle=90,
        wedgeprops={"width": 0.4},
        pctdistance=0.8,
    )
    for txt in autotexts:
        txt.set_fontsize(9)

    total = sum(sizes)
    ax.text(0, 0, f"£{total:,.0f}", ha="center", va="center", fontsize=14)
    if title is not None:
        ax.set_title(title)
    fig.tight_layout()
    return fig


# ========================
# Net Worth (matplotlib)
# ========================


def plot_net_worth(  # noqa: PLR0913
    df: pl.DataFrame,
    date_col: str = "date",
    asset_cols: list[str] | None = None,
    liability_cols: list[str] | None = None,
    figsize: tuple[float, float] = (12.0, 5.0),
    title: str | None = None,
) -> Figure:
    """Plot net worth (assets + liabilities) over time as a line chart.

    Parameters
    ----------
    df : pl.DataFrame
        Transposed monthly balance DataFrame from hledger ``balance`` with
        ``--transpose --layout=bare --cumulative --historical``. Must contain
        a date column and asset/liability account columns.
    date_col : str, default "date"
        Name of the column containing period labels or dates.
    asset_cols : list of str, optional
        Column names for asset accounts. If ``None``, all columns starting
        with ``"assets"`` are used.
    liability_cols : list of str, optional
        Column names for liability accounts. If ``None``, all columns
        starting with ``"liabilities"`` are used.
    figsize : tuple of float, default (12.0, 5.0)
        Figure size in inches.
    title : str, optional
        Chart title. If ``None``, a default is used.

    Returns
    -------
    Figure
        Matplotlib Figure containing the net worth line chart.
    """
    cleaned = df.filter(pl.col(date_col) != "commodity")

    if asset_cols is None:
        asset_cols = [c for c in cleaned.columns if c.startswith("assets")]
    if liability_cols is None:
        liability_cols = [c for c in cleaned.columns if c.startswith("liabilities")]

    all_cols = asset_cols + liability_cols
    cleaned = HLDataFrame(cleaned).currency_to_number(preserve_cols={date_col})

    net_expr = pl.lit(0.0)
    for col in all_cols:
        net_expr = net_expr + pl.col(col)
    cleaned = cleaned.with_columns(net_expr.alias("_net_worth"))

    dates = cleaned[date_col].to_list()
    net_worth = cleaned["_net_worth"].to_list()

    fig, ax = plt.subplots(figsize=figsize)
    ax.fill_between(range(len(dates)), net_worth, alpha=0.2, color=_GROSS_COLOR)
    ax.plot(range(len(dates)), net_worth, marker="o", color=_GROSS_COLOR)

    ax.set_xticks(range(len(dates)))
    ax.set_xticklabels(dates, rotation=45, ha="right")
    ax.yaxis.set_major_formatter(ticker.StrMethodFormatter("£{x:,.0f}"))
    ax.set_ylabel("Net Worth (£)")
    ax.grid(axis="y", alpha=0.3)
    if title is not None:
        ax.set_title(title)
    fig.tight_layout()
    return fig
