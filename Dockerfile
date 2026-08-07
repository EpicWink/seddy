FROM python:alpine
ARG SEDDY_REQUIREMENT='> 0.0'
RUN pip install "seddy [yaml,json-logging,colored-logging] $SEDDY_REQUIREMENT"
ENTRYPOINT ["seddy"]
