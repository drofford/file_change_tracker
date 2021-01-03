targets:
	@echo Valid targets are: `egrep '^[^ :]+:' Makefile | cut -d: -f1 | grep -v '\<targets\>' | grep -v ^_ | sort | uniq`

clean:
	@find . -depth \( -name \*.pyc -o -name __pycache__ -o -name \*~ \) -exec /bin/rm -fr {} \;

format:
	@black `find . -name \*.py`

pytest:
	@PYTHONPATH=src poetry run pytest -v

checkin:
	@git commit -a -m "Saving changes"

push:
	@git push origin `git branch --show-current`

puff:
	@git pull --ff-only origin `git branch --show-current`

pull:
	@git pull origin `git branch --show-current`







