FROM --platform=linux/amd64 python:3.12

COPY tap_mubi/ ./tap_mubi/
COPY meltano.yml setup.py ./

RUN pip install -e .
RUN pip install meltano

RUN meltano lock --update --all
RUN meltano install extractor tap-mubi
RUN meltano install loader target-postgres

CMD ["meltano", "run", "tap-mubi", "target-postgres"]
