#!/bin/zsh -e
rm -rf dist
python -m build --sdist --wheel .
twine upload dist/*
