# hledgertools

A Python package for extracting and manipulating [hledger](https://hledger.org/)
data using [Polars](https://pola.rs/) DataFrames.

## Installation

Install from source by cloning the repository and using `pip`:

```bash
git clone https://github.com/jatkinson1000/hledger-tools.git
cd hledger-tools
pip install .
```

For development (includes linting and test dependencies):

```bash
pip install -e ".[dev]"
```

## Overview

The package provides three modules:

- **`HledgerCommand`** — Construct and execute hledger subprocess calls with
  flexible options for date ranges, periodicity, output format, and account
  filtering.
- **`HLDataFrame`** — A Polars DataFrame subclass with utilities tailored to
  hledger data: account filtering, date conversion, transposition, and currency
  parsing.
- **`hlplots`** — Plotting functions for visualising hledger data, including
  Sankey cash-flow diagrams, monthly category bars, budget vs actual
  comparisons, essentials vs discretionary splits, net-worth trajectories, and
  heatmaps.

## Usage

### Running hledger commands

```python
import hledgertools as hlt

cmd = hlt.HledgerCommand(
    ledgerfile="2025.journal",
    begin_date="2025-04-01",
    end_date="2026-04-01",
    periodic="monthly",
    output_format="csv",
    other_options=["--transpose", "--layout=bare", "--no-total"],
)
result = cmd.run("balance", accounts=["expenses:groceries"])
```

When `output_format` is set, `run()` returns a `StringIO` object suitable for
passing directly to `polars.read_csv()`. Otherwise it returns the raw stdout
string.

### Working with HLDataFrame

```python
import polars as pl
import hledgertools as hlt

# Read hledger CSV output
df = hlt.HLDataFrame.from_csv(result, infer_schema=False)

# Filter to specific accounts (supports regex)
expenses = df.filter_accounts("expenses:.*")

# Convert currency strings to floats
numeric = df.currency_to_number(preserve_cols={"account", "date"})

# Transpose account/date layout
transposed = df.transpose()  # auto-detects orientation
```

### Plotting

```python
from hledgertools.hlplots import (
    plot_sankey_cashflow,
    plot_monthly_categories,
    plot_budget_vs_actual,
    plot_essentials_discretionary,
    plot_net_worth,
    plot_matrix_heatmap,
)

# Sankey diagram (Plotly) - income -> deductions -> expenses -> savings
fig = plot_sankey_cashflow(df, min_flow=200.0)
fig.show()
# Static export for PDF embedding:
# img = fig.to_image(format="png")

# Monthly bar chart (matplotlib)
fig = plot_monthly_categories(
    monthly_df,
    ["expenses:groceries", "expenses:utilities"],
    kind="grouped",
)

# Budget vs actual comparison
fig = plot_budget_vs_actual(budget_df)

# Essentials vs discretionary donut chart
essentials = open("essentials.txt").read().splitlines()
fig = plot_essentials_discretionary(df, essentials)

# Net worth over time
fig = plot_net_worth(monthly_balance_df)

# Heatmap of all categories x months
fig = plot_matrix_heatmap(monthly_df)
```

## Assumptions

The package makes several assumptions about the structure of your hledger
ledger:

- **Account hierarchy** — Accounts use the standard hledger colon-separated
  hierarchy with top-level groups: `revenues:`, `expenses:`, `assets:`, and
  `liabilities:`.
- **Balance normality** — Revenue accounts carry negative balances (credit
  normal) and expense accounts carry positive balances (debit normal), as
  produced by hledger's `balance` command. Asset and liability accounts follow
  their conventional normality.
- **Expense grouping** — `plot_essentials_discretionary` groups expenses by the
  second segment of the account name (e.g. `expenses:groceries` -> `Groceries`),
  so expense accounts are expected to have at least two colon-separated levels.

## Development

```bash
# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest

# Lint and format
ruff check src/
ruff format src/

# Type checking
mypy src/
```

## License

GNU General Public License v3 or later (GPLv3+).
