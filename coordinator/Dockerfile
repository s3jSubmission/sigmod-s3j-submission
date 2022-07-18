FROM python:3.8.10-slim-buster

# Install python deps
COPY ./requirements.txt /requirements.txt
RUN pip3 install -r /requirements.txt

# Copy over app
COPY ./src /src

# Copy over jars
COPY ./jars /jars

# Copy over resources
# COPY ./resources /resources

ENV PYTHONUNBUFFERED=1
CMD ["python", "/src/app.py"]