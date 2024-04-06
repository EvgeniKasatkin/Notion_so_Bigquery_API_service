FROM python:3.8.0-buster
ENV TZ=Europe/Moscow

RUN apt-get update

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN mkdir /templates
COPY ./templates/ /app/templates/

ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . .

COPY /app .
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 main:app




