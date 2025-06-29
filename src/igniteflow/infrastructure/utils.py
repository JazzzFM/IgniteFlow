"""
Utility functions for the IgniteFlow framework.

This module provides common utility functions for path management,
configuration, and other core services.
"""

import os
import sys
from pathlib import Path
from typing import List

from .exceptions import IgniteFlowError


def setup_project_path() -> None:
    """
    Set up Python path for IgniteFlow modules.
    
    This ensures that all IgniteFlow modules can be imported correctly
    regardless of the execution context.
    """
    # Get project root directory
    current_dir = Path(__file__).parent
    project_root = current_dir.parent.parent
    src_path = project_root / "src"
    
    # Add to Python path if not already present
    src_path_str = str(src_path)
    if src_path_str not in sys.path:
        sys.path.insert(0, src_path_str)


def auto_detect_config_path() -> str:
    """
    Auto-detect configuration path based on project structure.
    
    Returns:
        Path to configuration directory
        
    Raises:
        IgniteFlowError: If config path cannot be determined
    """
    # Try environment variable first
    if config_path := os.getenv("IGNITEFLOW_CONFIG_PATH"):
        return config_path
    
    # Auto-detect based on script location
    current_dir = Path(__file__).parent
    project_root = current_dir.parent.parent
    config_path = project_root / "src" / "config"
    
    if config_path.exists():
        return str(config_path)
    
    # Fallback to legacy path
    legacy_config_path = current_dir.parent / "config"
    if legacy_config_path.exists():
        return str(legacy_config_path)
    
    raise IgniteFlowError(f"Could not auto-detect config path. Please specify --config-path")
