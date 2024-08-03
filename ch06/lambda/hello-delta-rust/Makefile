.DEFAULT_GOAL := help
.PHONY: clean build deploy help zip

TOFU:=$(shell which tofu || which terraform)
ZIPFILE:=target/lambda/hello-delta-rust/bootstrap.zip

.precheck: ## Ensure that the necessary dependencies are on the system
	echo ">> Checking for ncessary tools"
	@cargo lambda -V || (echo "Must have cargo-lambda installed" && exit 1)
	@which tofu || which terraform || (echo "You must have OpenTofu or Terraform installed" && exit 1)
	touch .precheck

clean: destroy ## Clean up temporary state
	rm -f .precheck Cargo.lock
	rm -rf deploy/.terraform/
	rm -f deploy/.terraform*

destroy:  ## Destroy the terraform stack if it exists
	if [ -f deploy/terraform.tfstate ]; then \
		(cd deploy && $(TOFU) destroy); \
	fi;

format: .precheck ## Run all the formatters
	cargo fmt
	(cd deploy && $(TOFU) fmt)

build: Cargo.lock src/main.rs ## Build the Rust project
	cargo build

$(ZIPFILE): src/main.rs ## Produce the zip file for deploying into AWS Lambda
	@echo ">> A bootstrap.zip is needed, compiling.."
	cargo lambda build --release --output-format zip

deploy: deploy/.terraform $(ZIPFILE) ## Deploy the Lambda to your AWS account
	(cd deploy && $(TOFU) apply)

deploy/.terraform: deploy/providers.tf
	(cd deploy && $(TOFU) init)

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
