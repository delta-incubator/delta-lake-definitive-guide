.DEFAULT_GOAL := help
.PHONY: clean depends deploy help zip

TOFU:=$(shell which tofu || which terraform)

.precheck: ## Ensure that the necessary dependencies are on the system
	echo ">> Checking for ncessary tools"
	@which virtualenv || (echo "You must have virtualenv in the PATH" && exit 1)
	@which tofu || which terraform || (echo "You must have OpenTofu or Terraform installed" && exit 1)
	touch .precheck

depends: .depends ## Set up the Python dependencies

.depends: .precheck venv requirements.txt ## Set up the Python dependencies with an output to allow Make to skip
	. venv/bin/activate && pip install -r requirements.txt && touch .depends

venv: ## Set up the virtualenv
	virtualenv venv

build: target/lambda_function.zip target/lambda_layer.zip ## Create the zip package for upload
	@# Sanity check to save time uploading the fiule
	@test $(shell unzip -l target/lambda_layer.zip | tail -1 | xargs | cut -d' ' -f1) -le 250000000 || (echo "The zip file is too large to be unpacked in a Lambda layer!" && exit 1)

target/lambda_function.zip: target lambda_function.py
	(cd target && zip -qr lambda_function.zip ../lambda_function.py)
target/lambda_layer.zip:  target requirements.txt
	. venv/bin/activate && pip install -r requirements.txt --no-dependencies --target ./target/python
	(cd target && zip -qr lambda_layer.zip python)
target:
	mkdir -p target/python

clean: destroy ## Clean up temporary state
	rm -f .precheck .depends
	rm -rf deploy/.terraform/ venv
	rm -f deploy/.terraform*

destroy:  ## Destroy the terraform stack if it exists
	if [ -f deploy/terraform.tfstate ]; then \
		(cd deploy && $(TOFU) destroy); \
	fi;

format: .precheck ## Run all the formatters
	(cd deploy && $(TOFU) fmt)

deploy: deploy/.terraform build ## Deploy the Lambda to your AWS account
	(cd deploy && $(TOFU) apply)

deploy/.terraform: deploy/providers.tf
	(cd deploy && $(TOFU) init)

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
