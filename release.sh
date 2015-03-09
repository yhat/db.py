#!/bin/bash

python -m unittest discover db/tests/
pandoc -f markdown -t rst README.md > README.rst
python setup.py install sdist $1
