#!/bin/bash

# Run mypy checks
mypy --config-file mypy.ini ./conduit_p2p --python-version 3.10 --namespace-packages --explicit-package-bases

# Run pylint checks
python3 -m pylint --rcfile ./.pylintrc ./conduit_flat_file_db
