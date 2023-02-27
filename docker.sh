#!/bin/sh -e
python_version="${PYTHON_VERSION:-3.9}"

echo "building dev image (python version: ${python_version})"
image="$(docker build --quiet --build-arg "PYTHON_VERSION=${python_version}" .)"
echo "build finished (image: ${image})"

is_interactive="0"
interactive=""
if test -t 0 -a -t 1 -a -t 2; then
    is_interactive="1"
    interactive="--interactive --tty"
fi

echo "running command (command: $@, is_interactive: ${is_interactive})"
docker run $interactive --user "${uid}:${uid}" --rm "${image}" $@
