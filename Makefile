#!make

export PYTHON_VERSION := 3.7.7

#SHELL := /bin/bash

install-dev:
	@+pipenv install --python ${PYTHON_VERSION} --dev -e .

install:
	@+pipenv install --python ${PYTHON_VERSION} -e .

.PHONY: test
test:
	pipenv run pytest

docker-up:
	{ \
   	cd redis-docker ;\
   	docker-compose up -d ;\
	}

docker-down:
	{ \
	cd redis-docker ;\
	docker-compose down ;\
  	}

local-setup:
	python scripts/local_setup.py
