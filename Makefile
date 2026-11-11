.PHONY: run test
run:
	python -m davano.app

test:
	python -m unittest discover -s tests -v
