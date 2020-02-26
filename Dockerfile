FROM python:alpine
RUN pip install seddy coloredlogs
ENTRYPOINT ["seddy"]
