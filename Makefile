
up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f

ps:
	docker compose ps

init:
	bash init.sh

backfill:
	@if [ -z "$(START)" ] || [ -z "$(END)" ]; then \
		echo "❗ Please provide START and END dates. Usage: make backfill START=2025-05-01 END=2025-05-03"; \
	exit 1; \
	fi; \
	docker compose run --rm airflow-scheduler airflow dags backfill nyc_311_etl_pipeline --start-date $(START) --end-date $(END)

airflow:
	@echo "🌐 Opening Airflow UI at http://localhost:8080 ..."
	@open http://localhost:8080

jupyter:
	@echo "📁 Creating notebooks directory (if missing)..."
	@test -d notebooks || mkdir notebooks
	@echo "🚀 Starting JupyterLab container ..."
	docker compose up -d jupyter
	@sleep 3
	@echo "🌐 Opening http://localhost:8888/lab ..."
	open http://localhost:8888/lab

filetree:
	@echo "📂 Generating project file tree ... ("brew install tree" in Mac)"
	tree -I '__pycache__|.git|.venv|*.pyc|__init__.py|logs|pgdata' -L 3 > filetree.md
	@echo "✅ File tree written to filetree.md"

help:
	@echo "Usage:"
	@echo "  make up          # Start services"
	@echo "  make down        # Stop services"
	@echo "  make restart     # Restart services"
	@echo "  make logs        # Tail logs"
	@echo "  make ps          # Show service status"
	@echo "  make init        # Run project initialization"
	@echo "  make backfill START=YYYY-MM-DD END=YYYY-MM-DD   # Run Airflow backfill"
	@echo "  web         Open Airflow UI in browser (localhost:8080)"
	@echo "  jupyter     Open JupyterLab (localhost:8888)"
	@echo "  filetree    Generate file tree structure as markdown"