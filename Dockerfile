FROM quay.io/keboola/docker-custom-python:latest

COPY . /code/
WORKDIR /data/

RUN pip install -r /code/requirements.txt

CMD ["python", "-u", "/code/src/component.py"]
