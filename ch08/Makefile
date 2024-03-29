#
# This GNU/Makefile is an optional tool in this directory to help with building
# and setting up the code examples
#

.PHONY: help
help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

### SETUP
VIRTUALENV=.virtualenv
.PHONY: setup clean python-deps

setup: $(VIRTUALENV) rust-deps python-deps ## Do all the environment set up for build/test

clean: ## Clean up development resourcs
	rm -rf $(VIRTUALENV)

$(VIRTUALENV): #nodoc: Create a Python virtualenv for development
	python3 -m venv $(VIRTUALENV)

python-deps: $(VIRTUALENV) $(VIRTUALENV)/.installed ## Install the Python dependencies

## Using this file to denote that the install has successfully run
$(VIRTUALENV)/.installed:
	$(VIRTUALENV)/bin/pip install deltalake pandas
	touch $(VIRTUALENV)/.installed

# These variables only exist to test command presence
CARGO:=$(shell which cargo)
CARGO_LAMBDA:=$(shell which cargo-lambda)
rust-deps: ## Install the Rust dependencies
ifndef CARGO
	$(error Cargo must be installed, please visit <https://rustup.rs/> to install)
endif
ifndef CARGO_LAMBDA
	$(info cargo-lambda not found in PATH, installing)
	cargo install cargo-lambda
endif

### BUILD
.PHONY: build build-zips

build: rust-deps ## Build and compile the lambdas
	(cd rust-example && cargo build)

build-zips: rust-deps ## Build the uploadable Lambda zips
	(cd rust-example && cargo lambda build --release --output-format zip)


### TEST
.PHONY: test-python test
test: test-python ## Run all tests

test-python: python-deps ## Test the python specific code
	# Before testing, clean the generated data from ./data
	git clean -Xf -- ./data/gen
	python -m doctest -v -f python-snippets.md


#@# DOWNLOAD DATA
.PHONY: download
download: data/co2_mm_mlo.csv ## Download some datasets for experimentation

# Downloads the last month's observation from NOAA on atmospheric CO2
data/co2_mm_mlo.csv:
	mkdir -p data && cd data && curl -O https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_mlo.csv
