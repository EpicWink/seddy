FROM python:alpine
RUN pip install seddy coloredlogs pyyaml
ENTRYPOINT ["seddy"]
