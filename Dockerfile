# Use an official Python runtime as a parent image
FROM python:3.6-slim
MAINTAINER enviroDGI@gmail.com

RUN apt-get update && apt-get install -y --no-install-recommends \
    git gcc g++

# Set the working directory to /app
WORKDIR /app
ARG WEB_MONITORING_DB_URL
ARG WEB_MONITORING_DB_PASSWORD
ARG WEB_MONITORING_DB_EMAIL

# Copy the requirements.txt alone into the container at /app
# so that they can be cached more aggressively than the rest of the source.
ADD requirements.txt /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Copy the rest of the source.
ADD . /app

# Install package.
RUN pip install .

# Make port 80 available to the world outside this container.
EXPOSE 80

# Run server on port 80 when the container launches.
CMD ["ia_healthcheck"]
