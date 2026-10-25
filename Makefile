.PHONY: run inspect test clean

run:
	python scripts/run_pipeline.py

inspect:
	python scripts/inspect_gold.py

test:
	pytest -q

clean:
	rm -rf data/lake/bronze data/lake/silver data/lake/gold reports/quality_report.json
