"""Tests for the HledgerCommand class."""

from io import StringIO
from unittest.mock import MagicMock, patch

from hledgertools.hlcommand import HledgerCommand


class TestHledgerCommandInit:
    """Tests for HledgerCommand initialisation and attribute storage."""

    def test_init_defaults(self):
        cmd = HledgerCommand()
        assert cmd.command == "hledger"
        assert cmd.ledgerfile is None
        assert cmd.begin_date is None
        assert cmd.end_date is None
        assert cmd.period is None
        assert cmd.periodic is None
        assert cmd.output_format is None
        assert cmd.other_options is None

    def test_init_with_params(self):
        cmd = HledgerCommand(
            ledgerfile="test.journal",
            begin_date="2025-01-01",
            end_date="2025-12-31",
            periodic="monthly",
            output_format="csv",
            other_options=["--empty"],
        )
        assert cmd.ledgerfile == "test.journal"
        assert cmd.begin_date == "2025-01-01"
        assert cmd.end_date == "2025-12-31"
        assert cmd.periodic == "monthly"
        assert cmd.output_format == "csv"
        assert cmd.other_options == ["--empty"]


class TestHledgerCommandRun:
    """Tests for HledgerCommand.run command construction."""

    @patch("hledgertools.hlcommand.subprocess.run")
    def test_run_basic_balance(self, mock_run):
        mock_run.return_value = MagicMock(stdout="output", stderr="")
        cmd = HledgerCommand(ledgerfile="test.journal")
        result = cmd.run("balance")
        assert result == "output"
        called_args = mock_run.call_args[0][0]
        assert called_args[0:2] == ["hledger", "balance"]
        assert "--file=test.journal" in called_args

    @patch("hledgertools.hlcommand.subprocess.run")
    def test_run_with_dates(self, mock_run):
        mock_run.return_value = MagicMock(stdout="output", stderr="")
        cmd = HledgerCommand(
            ledgerfile="test.journal",
            begin_date="2025-01-01",
            end_date="2025-12-31",
        )
        cmd.run("balance")
        called_args = mock_run.call_args[0][0]
        assert "--begin=2025-01-01" in called_args
        assert "--end=2025-12-31" in called_args

    @patch("hledgertools.hlcommand.subprocess.run")
    def test_run_with_periodic(self, mock_run):
        mock_run.return_value = MagicMock(stdout="output", stderr="")
        cmd = HledgerCommand(ledgerfile="test.journal", periodic="monthly")
        cmd.run("balance")
        called_args = mock_run.call_args[0][0]
        assert "--monthly" in called_args

    @patch("hledgertools.hlcommand.subprocess.run")
    def test_run_with_period(self, mock_run):
        mock_run.return_value = MagicMock(stdout="output", stderr="")
        cmd = HledgerCommand(ledgerfile="test.journal", period="2025-Q1")
        cmd.run("balance")
        called_args = mock_run.call_args[0][0]
        assert "--period=2025-Q1" in called_args

    @patch("hledgertools.hlcommand.subprocess.run")
    def test_run_with_output_format_returns_stringio(self, mock_run):
        mock_run.return_value = MagicMock(stdout="csv,data\n", stderr="")
        cmd = HledgerCommand(ledgerfile="test.journal", output_format="csv")
        result = cmd.run("balance")
        assert isinstance(result, StringIO)
        assert result.read() == "csv,data\n"

    @patch("hledgertools.hlcommand.subprocess.run")
    def test_run_without_output_format_returns_str(self, mock_run):
        mock_run.return_value = MagicMock(stdout="text output", stderr="")
        cmd = HledgerCommand(ledgerfile="test.journal")
        result = cmd.run("balance")
        assert isinstance(result, str)
        assert result == "text output"

    @patch("hledgertools.hlcommand.subprocess.run")
    def test_run_with_other_options(self, mock_run):
        mock_run.return_value = MagicMock(stdout="output", stderr="")
        cmd = HledgerCommand(
            ledgerfile="test.journal",
            other_options=["--empty", "--depth=2"],
        )
        cmd.run("balance")
        called_args = mock_run.call_args[0][0]
        assert "--empty" in called_args
        assert "--depth=2" in called_args

    @patch("hledgertools.hlcommand.subprocess.run")
    def test_run_with_extra_options(self, mock_run):
        mock_run.return_value = MagicMock(stdout="output", stderr="")
        cmd = HledgerCommand(ledgerfile="test.journal")
        cmd.run("balance", extra_options=["--no-total"])
        called_args = mock_run.call_args[0][0]
        assert "--no-total" in called_args

    @patch("hledgertools.hlcommand.subprocess.run")
    def test_run_with_accounts(self, mock_run):
        mock_run.return_value = MagicMock(stdout="output", stderr="")
        cmd = HledgerCommand(ledgerfile="test.journal")
        cmd.run("balance", accounts=["expenses:food", "expenses:car"])
        called_args = mock_run.call_args[0][0]
        assert "expenses:food" in called_args
        assert "expenses:car" in called_args

    @patch("hledgertools.hlcommand.subprocess.run")
    def test_run_with_ignore(self, mock_run):
        mock_run.return_value = MagicMock(stdout="output", stderr="")
        cmd = HledgerCommand(ledgerfile="test.journal")
        cmd.run("balance", ignore=["expenses:tax"])
        called_args = mock_run.call_args[0][0]
        assert "not:expenses:tax" in called_args

    @patch("hledgertools.hlcommand.subprocess.run")
    def test_run_checks_true(self, mock_run):
        mock_run.return_value = MagicMock(stdout="output", stderr="")
        cmd = HledgerCommand(ledgerfile="test.journal")
        cmd.run("balance")
        assert mock_run.call_args[1]["check"] is True

    @patch("hledgertools.hlcommand.subprocess.run")
    def test_run_full_command_order(self, mock_run):
        """Verify the full command is constructed in the expected order."""
        mock_run.return_value = MagicMock(stdout="output", stderr="")
        cmd = HledgerCommand(
            ledgerfile="ledger.journal",
            begin_date="2025-01-01",
            end_date="2025-12-31",
            periodic="monthly",
            output_format="csv",
            other_options=["--empty"],
        )
        cmd.run(
            "balance",
            accounts=["expenses:food"],
            ignore=["expenses:tax"],
            extra_options=["--no-total"],
        )
        called_args = mock_run.call_args[0][0]
        assert called_args == [
            "hledger",
            "balance",
            "--file=ledger.journal",
            "--begin=2025-01-01",
            "--end=2025-12-31",
            "--monthly",
            "--output-format=csv",
            "--empty",
            "--no-total",
            "expenses:food",
            "not:expenses:tax",
        ]
