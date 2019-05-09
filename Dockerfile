FROM python:3.7.0-slim-stretch

# Upgrade pip
RUN pip install --upgrade pip

# Install libraries.
RUN pip install kazoo
RUN pip install slackclient
RUN pip install PyYAML

COPY . /

ENV PYTHONPATH=/

CMD ["python3", "-u", "/koolie/go.py", "--help"]