FROM python:3.9.1

RUN pip install pandas

WORKDIR /usr/app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py"]