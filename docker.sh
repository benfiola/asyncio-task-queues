#!/bin/sh -e
python_version="${PYTHON_VERSION:-3.9}"

echo "building dev image (python version: ${python_version})"
image="$(docker build --quiet --build-arg "PYTHON_VERSION=${python_version}" .)"
echo "build finished (image: ${image})"

interactive=""
if test -t 0 -a -t 1 -a -t 2; then
    echo "tty detected - using tty arguments with docker"
    interactive="--interactive --tty"
fi
echo "running command (command: $@)"
docker run $interactive --volume "$(pwd)/site:/app/site" --rm "${image}" $@
