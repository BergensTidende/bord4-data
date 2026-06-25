.PHONY: data clean help

data: ## Build generated data files
	python3 scripts/build_region_transitions.py
	python3 scripts/build_current_municipalities.py
	python3 scripts/build_regions_history.py

clean: ## Delete Python cache files
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z_-]+:.*## / {printf "%-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
