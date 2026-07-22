"""Pytest configuration and fixtures for hledgertools tests."""

import matplotlib as mpl

mpl.use("Agg")

import matplotlib.pyplot as plt
import pytest


@pytest.fixture(autouse=True)
def _close_figures():
    """Close all matplotlib figures after each test to avoid memory leaks."""
    yield
    plt.close("all")
