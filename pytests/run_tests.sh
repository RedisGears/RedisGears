#!/usr/bin/env sh
#
# Requires:
#
# - Python 3 interpreter.
# - Python 3 pip.
# - coreutils

FULL_PATH_TO_SCRIPT="$(realpath "${BASH_SOURCE[-1]}")"
SCRIPT_DIRECTORY="$(dirname "$FULL_PATH_TO_SCRIPT")"
CALLING_DIRECTORY=`pwd`

printf "\e[31m Python will now install the dependencies system-wide.\e[0m\n"

pip install -r $SCRIPT_DIRECTORY/requirements.txt --break-system-packages

cd $SCRIPT_DIRECTORY

RLTest $@

cd $CALLING_DIRECTORY
