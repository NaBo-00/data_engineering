FROM apache/airflow:2.9.1

# Install Python3 and necessary packages
USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip procps

RUN apt-get update && \
    curl -L -o OpenJDK11U-jdk_x64_linux_11.0.11_9.tar.gz https://github.com/AdoptOpenJDK/openjdk11-upstream-binaries/releases/download/jdk-11.0.11%2B9/OpenJDK11U-jdk_x64_linux_11.0.11_9.tar.gz && \
    tar -xzf OpenJDK11U-jdk_x64_linux_11.0.11_9.tar.gz -C /opt/ && \
    rm OpenJDK11U-jdk_x64_linux_11.0.11_9.tar.gz

RUN apt-get update && \
    apt-get install -y ant

RUN rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /opt/openjdk-11.0.11_9
ENV PATH $JAVA_HOME/bin:$PATH

# Switch to airflow user
USER airflow

# Copy requirements.txt into the Docker container
COPY requirements.txt /sources/requirements.txt

# Upgrade pip and install required packages
RUN pip install --upgrade pip && \ 
    pip install --no-cache-dir -r /sources/requirements.txt
