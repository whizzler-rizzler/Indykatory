#!/usr/bin/env python3
"""
Railway entry point - uruchamia Indicators Service
"""

import subprocess
import sys

if __name__ == "__main__":
    subprocess.run([sys.executable, "indicators_service.py"])
