DEFAULT_BACKUP := data/raw/dataset_raw.sql

# Full pipeline: collect + analyze
all:
	python src/main.py --token $(or $(token),$(GITHUB_TOKEN)) \
	               --repos $(or $(repos),50) \
	               --workers $(or $(workers),10) \
	               --analyze-workers $(or $(analyze_workers),4) \
	               --database-url $(or $(database_url),postgresql://postgres:password@localhost/1)

# Dataset collection only
collect:
	python src/main.py collect --token $(or $(token),$(GITHUB_TOKEN)) \
	               --repos $(or $(repos),50) \
	               --workers $(or $(workers),10) \
	               --database-url $(or $(database_url),postgresql://postgres:password@localhost/1)

# Analysis only
analyze:
	python src/main.py analyze --database-url $(or $(database_url),postgresql://postgres:password@localhost/1) \
	                       --workers $(or $(workers),4)

# Restore database from backup
restore:
	bash scripts/restore_backup.sh $(or $(file),$(DEFAULT_BACKUP))

# Help
help:
	@echo "Available commands:"
	@echo "  make all          - Run full pipeline: collect + analyze"
	@echo "  make collect  - Collect GitHub dataset only"
	@echo "  make analyze      - Run analysis only"
	@echo "  make restore      - Restore database from backup"
	@echo ""
	@echo "Examples:"
	@echo "  make all token=ghp_xxx repos=100 workers=10 analyze_workers=4"
	@echo "  make get_dataset token=ghp_xxx repos=100"
	@echo "  make analyze workers=4"
	@echo "  make restore file=backups/custom.sql"

.PHONY: all get_dataset analyze restore help