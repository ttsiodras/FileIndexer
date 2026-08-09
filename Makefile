# Makefile for the FileIndexer project.
#
# Targets:
#   make            - set up the virtualenv, then run flake8, pylint and mypy
#   make flake8     - lint with flake8
#   make pylint     - lint with pylint
#   make mypy       - static type-check with mypy
#   make test       - run the test suite (test_indexer.py)
#   make coverage   - run the tests under coverage and print a report
#   make clean      - remove build artifacts

TARGET:=indexer.py
PYTHON:=python3
VENV:=.venv

all:	.setup .analysed

.analysed:	${TARGET}
	$(MAKE) flake8
	$(MAKE) pylint
	$(MAKE) mypy
	@touch $@

flake8: dev-install
	@echo "============================================"
	@echo " Running flake8..."
	@echo "============================================"
	${VENV}/bin/flake8 ${TARGET}

pylint: dev-install
	@echo "============================================"
	@echo " Running pylint..."
	@echo "============================================"
	${VENV}/bin/pylint --rcfile=pylint.cfg ${TARGET}

mypy: dev-install
	@echo "============================================"
	@echo " Running mypy..."
	@echo "============================================"
	${VENV}/bin/mypy ${TARGET}

test: dev-install
	${VENV}/bin/python3 test_indexer.py

coverage: dev-install
	@echo "============================================" ; \
	 echo " Running tests under coverage..." ; \
	 echo "============================================" ; \
	 echo '[run]' > .coveragerc ; \
	 echo "source = $(CURDIR)" >> .coveragerc ; \
	 echo 'parallel = True' >> .coveragerc ; \
	 echo 'concurrency = multiprocessing' >> .coveragerc ; \
	 export COVERAGE_PROCESS_START=$(CURDIR)/.coveragerc \
	        COVERAGE_FILE=$(CURDIR)/.coverage ; \
	 ${VENV}/bin/coverage erase ; \
	 ${VENV}/bin/coverage run test_indexer.py ; \
	 ${VENV}/bin/coverage combine -q ; \
	 ${VENV}/bin/coverage report -m --include="$(CURDIR)/indexer.py" ; \
	 rm -f .coveragerc .coverage

dev-install:	.setup | prereq

prereq:
	@${PYTHON} -c 'import sys; sys.exit(1 if sys.version_info < (3, 6) else 0)' || { \
	    echo "=============================================" ; \
	    echo "[x] You need at least Python 3.6 to run this." ; \
	    echo "=============================================" ; \
	    exit 1 ; \
	}

.setup:	requirements-dev.txt
	@if [ ! -d ${VENV} ] ; then                            \
	    echo "[-] Installing VirtualEnv environment..." ;  \
	    ${PYTHON} -m venv ${VENV} || exit 1 ;              \
	fi
	echo "[-] Installing packages inside environment..." ; \
	. ${VENV}/bin/activate || exit 1 ;                     \
	${PYTHON} -m pip install -r requirements-dev.txt || exit 1
	touch $@

clean:
	rm -rf .mypy_cache/ __pycache__/ .pytest_cache/ .analysed .setup .coverage .coveragerc

.PHONY: all flake8 pylint mypy test coverage clean dev-install prereq
