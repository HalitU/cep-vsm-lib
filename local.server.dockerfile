FROM python:3.10.10-slim

WORKDIR /code

# Installing the pre-requisite libraries
RUN apt-get update
RUN apt-get -y install apt-utils
RUN apt-get -y install gcc

# Copying the source code
COPY /server_app/requirements.txt .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY /cep_library ./cep_library
COPY /server_app .

# Starting the fastapi application
CMD ["python", "web_api_server.py"]
