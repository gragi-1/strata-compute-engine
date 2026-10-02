.PHONY: install check test up down proto cpp benchmark demo
install:
	uv sync --locked
check:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy
test:
	uv run pytest --cov --cov-report=term-missing --cov-report=xml
up:
	docker compose up --build -d
down:
	docker compose down
proto:
	uv run python scripts/generate_proto.py
cpp:
	cmake -S worker_cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release
	cmake --build build/cpp -j2
	ctest --test-dir build/cpp --output-on-failure
benchmark:
	uv run python benchmarks/run.py --jobs 1000
demo:
	uv run python scripts/failure_demo.py
