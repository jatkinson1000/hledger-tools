"""Defines the base HledgerCommand class."""

import subprocess
from io import StringIO


class HledgerCommand:
    """Class to construct and execute hledger subprocess calls with flexible options."""

    def __init__(
        self,
        ledgerfile: str | None = None,
        begin_date: str | None = None,
        end_date: str | None = None,
        period: str | None = None,
        periodic: str | None = None,
        output_format: str | None = None,
        other_options: list[str] | None = None,
    ):
        """
        Initialize the HledgerCommand class.

        :param command: The base hledger command (default: "hledger").
        """
        self.command = "hledger"
        self.ledgerfile = ledgerfile
        self.begin_date = begin_date
        self.end_date = end_date
        self.period = period
        self.periodic = periodic
        self.output_format = output_format
        self.other_options = other_options

    def run(  # noqa:PLR0912
        self,
        subcommand: str,
        accounts: list[str] | None = None,
        ignore: list[str] | None = None,
        extra_options: list[str] | None = None,
    ) -> str:
        """
        Run a hledger subcommand with the specified options.

        Parameters
        ----------
        subcommand : str
            The hledger subcommand to execute (e.g., "balance", "print", "bs", "is").
        accounts : list[str] | None
            list of the accounts to output for
        ignore : list[str] | None
            list of the accounts to ignore
        extra_options : list[str] | None
            list of any other command line options to be passed to hledger call as str
        """
        # Construct the base command
        full_command = ["hledger", subcommand]

        # Add input file if specified
        if self.ledgerfile:
            full_command.append(f"--file={self.ledgerfile}")

        # Add date filters if provided
        if self.period:
            full_command.append(f"--period={self.period}")
        if self.begin_date:
            full_command.append(f"--begin={self.begin_date}")
        if self.end_date:
            full_command.append(f"--end={self.end_date}")

        # Specify whether to split report into multiple periods
        if self.periodic:
            full_command.append(f"--{self.periodic}")

        # Set display format if provided
        if self.output_format:
            full_command.append(f"--output-format={self.output_format}")

        # Specify any other options not catered for by default
        # First from overall options
        if self.other_options:
            for opt in self.other_options:
                full_command.append(opt)
        # Second from those passed into this command
        if extra_options:
            for opt in extra_options:
                full_command.append(opt)

        # Specify accounts requested
        if accounts:
            for acct in accounts:
                full_command.append(acct)
        # Ignore accounts requested
        if ignore:
            for acct in ignore:
                full_command.append(f"not:{acct}")

        # Execute the command and capture the output
        result = subprocess.run(  # noqa:S603
            full_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )

        if self.output_format:
            return StringIO(result.stdout)

        return result.stdout
