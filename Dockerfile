FROM quay.io/astronomer/astro-runtime:10.1.0

USER root

COPY requirements.txt /app/

RUN python -m venv spark_venv && \
    source spark_venv/bin/activate && \
    pip install --no-cache-dir --default-timeout=100 -r /app/requirements.txt
#&& deactivate

# Install Java 17
RUN apt-get update && \
    apt-get install -y sudo && \
    sudo apt-get install -y openjdk-17-jdk && \
    sudo rm -rf /var/lib/apt/lists/*

# Set the JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64