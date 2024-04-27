#!/usr/bin/env bash -x
#
# Run Docker on the test image, with an interactive shell.

docker run -ti -v $HOME:/home/bmc/bmc bclapper/py-sqlshell-test
