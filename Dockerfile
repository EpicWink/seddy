FROM python:alpine
RUN pip install seddy coloredlogs pyyaml python-json-logger
ENTRYPOINT ["seddy"]
