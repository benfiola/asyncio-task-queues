#!/bin/sh -e
black --check .
pytest tests
