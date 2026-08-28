"""Put the package root on sys.path so `import wiggle` works when running `pytest` from the repo
root without installing the package first."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
