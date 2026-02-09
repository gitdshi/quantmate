#!/usr/bin/env python3
"""DB->file sync disabled.

This helper previously pushed database strategies to files by calling
the API. That behavior is disabled to avoid automatic file creation.
This stub exits with a message so CI/docs referencing the script won't fail.
"""
import sys

print("DB->file synchronization is disabled. Remove or edit this script to re-enable.")
sys.exit(0)
