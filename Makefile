.PHONY: setup test

setup:
	pip3 install virtualenv
	virtualenv env
	source env/bin/activate; \
	pip3 install -r requirements.txt

test:
	source env/bin/activate; \
	SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=/src:./src/ python3 -m pytest ${target} -v

