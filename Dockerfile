FROM python:3.10.8-slim-bullseye
LABEL maintainer="Anton Bakker <anton.bakker@kadaster.nl>"
COPY . /src

RUN apt-get update && \
    apt-get install -y \
        jq \
        moreutils && \
    pip install --upgrade setuptools && \
    pip install /src

ENTRYPOINT [ "ngr-spider" ]
