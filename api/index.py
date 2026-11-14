"""Serverless entry point of the hosted site: serves the /api routes (archive queries) with the same code
that the local builder uses."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "local-builder"))

from app import app  # noqa: E402,F401  (Vercel looks for the ASGI object called "app")
