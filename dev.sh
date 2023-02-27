#!/bin/sh -e
dockerfile="./dev.Dockerfile"
if [ ! -f "${dockerfile}" ]; then
    1>&2 echo "error: dockerfile not found: ${dockerfile}"
    exit 1
fi

python_version="${PYTHON_VERSION:-3.9}"
echo "building dev image (python version: ${python_version})"
image="$(docker build --quiet --file "${dockerfile}" --build-arg "PYTHON_VERSION=${python_version}" .)"
echo "build finished (image: ${image})"
docker run --rm --volume "$(pwd)/site:/app/site" "${image}" $@
