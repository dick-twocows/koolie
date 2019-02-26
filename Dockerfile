FROM python:3.7.0-slim-stretch

# Upgrade pip
RUN pip install --upgrade pip

# Install libraries.
RUN pip install kazoo==2.5.0
RUN pip install slackclient==1.0.0
RUN pip install PyYAML==3.13

COPY . /

ENV PYTHONPATH=/

CMD ["python3", "-u", "/koolie/go.py", "--help"]