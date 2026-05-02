"""
AWS Cost Lens - AWS Cost Analysis Tool

A tool for analyzing and visualizing AWS costs by service and usage type.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("aws-cost-lens")
except PackageNotFoundError:
    __version__ = "0.0.0"

from .core import (
    AWSService,
    analyze_costs_detailed,
    analyze_costs_simple,
    get_cost_data,
    list_available_services,
)

__all__ = [
    "AWSService",
    "__version__",
    "analyze_costs_detailed",
    "analyze_costs_simple",
    "get_cost_data",
    "list_available_services",
] 