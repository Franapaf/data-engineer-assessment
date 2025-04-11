FROM openjdk:11-slim

# Install Python and system tools
RUN apt-get update && apt-get install -y \
    python3.10 \
    python3-pip \
    curl \
    wget \
    git \
    procps \
    ca-certificates \
    && apt-get clean

# Fix Spark's expected Java path
RUN mkdir -p /usr/lib/jvm && \
    ln -s /usr/local/openjdk-11 /usr/lib/jvm/java-11-openjdk-amd64

# Set environment variables
ENV JAVA_HOME=/usr/local/openjdk-11
ENV SPARK_VERSION=3.5.0
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

COPY src/jars/ /opt/spark/jars/


WORKDIR /app/src

# Default command
CMD ["python3", "src/main.py"]
