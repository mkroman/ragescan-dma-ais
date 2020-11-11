FROM python:3.9-alpine
MAINTAINER Mikkel Kroman <mk@maero.dk>

WORKDIR /usr/src

ADD https://github.com/mkroman/zip2zstd/releases/download/v0.1.0/zip2zstd-x86_64-unknown-linux-musl /usr/bin/zip2zstd
RUN chmod +x /usr/bin/zip2zstd

RUN apk add --no-cache zstd python3 curl

COPY . .

RUN pip install pipenv
RUN pipenv install --deploy --system

ENTRYPOINT ["python", "app.py"]
