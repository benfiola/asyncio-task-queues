#!/bin/sh -e
action="$1"

if [ "$action" = "tests" ]; then
    black --check .
    pytest tests
elif [ "$action" = "docs" ]; then
    mkdocs build
else
    exec "$@"
fi
