FROM python:3.10

ENV PIP_VERSION=23.0.1
ENV POETRY_VERSION=1.4.2
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/app/config/application_default_credentials.json
ENV PYTHONPATH=$PYTHONPATH:/opt/app

ENV AMORA_PROJECT_PATH=/opt/app/examples/amora_project
ENV AMORA_TARGET_PROJECT=amora-data-build-tool
ENV AMORA_TARGET_SCHEMA=amora

WORKDIR /opt/app

COPY . .

RUN pip install -U pip==$PIP_VERSION poetry==$POETRY_VERSION && \
    poetry config virtualenvs.create false && \
    poetry config --list && \
    poetry install --without dev --no-interaction --no-ansi --all-extras

CMD ["amora", "dash", "serve"]