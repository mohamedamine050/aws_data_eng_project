"""Pytest configuration and fixtures."""
import sys
from unittest.mock import MagicMock

# Mock awsglue module before importing glue_ecommerce_processing
sys.modules['awsglue'] = MagicMock()
sys.modules['awsglue.utils'] = MagicMock()

# Mock getResolvedOptions to return a dict
def mock_get_resolved_options(args, option_names):
    """Mock implementation of getResolvedOptions."""
    return {opt: f"mock_{opt}" for opt in option_names}

sys.modules['awsglue.utils'].getResolvedOptions = mock_get_resolved_options
