FROM python:alpine
ARG SEDDY_REQUIREMENT='> 0.0'
RUN pip install "seddy $SEDDY_REQUIREMENT" coloredlogs pyyaml python-json-logger
ENTRYPOINT ["seddy"]
