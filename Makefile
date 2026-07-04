.PHONY: install run produce help clean

install:
	uv sync

run:
	uv run arise produce --rate 100 --duration 30s

produce:
	uv run arise produce $(ARGS)

help:
	uv run arise --help

clean:
	rm -rf .venv uv.lock
