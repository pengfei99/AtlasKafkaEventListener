#
FROM python:3.8-bullseye

# set api as the current work dir
WORKDIR /app

# copy the requirements lists
COPY ./requirements.txt /app/requirements.txt

# install all the requirements
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# copy the main code
COPY atlas_kafka_event_listener /app/atlas_kafka_event_listener
COPY command/run.sh /app/command/run.sh

# set up python path for the added source
ENV PYTHONPATH "${PYTHONPATH}:/app/atlas_kafka_event_listener"

# call the function
CMD ["sh command/run.sh"]