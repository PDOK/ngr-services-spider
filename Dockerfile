FROM python:3.10.8-bullseye

LABEL maintainer="Anton Bakker <anton.bakker@kadaster.nl>"
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv --system-site-packages /opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN pip3 install --no-cache-dir setuptools pip --upgrade
RUN apt-get update && \
    apt-get install -y jq
ADD . /src
WORKDIR /src
RUN pip3 install --no-cache-dir .
ENTRYPOINT [ "ngr-spider" ]
