#!/bin/sh -e
python_version="$1"
if [ "$python_version" = "" ]; then
    echo 1>&2 "usage: $0 <python_version>"
    exit 1
fi

dockerfile="./test.Dockerfile"
if [ ! -f "$dockerfile" ]; then
    echo 1>&2 echo "file not found: ${dockerfile}"
    exit 1
fi

docker build --build-arg "PYTHON_VERSION=${python_version}" --file "${dockerfile}" .
