FROM openjdk:8

# Install Python3
RUN \
    apt-get update && \
    apt-get install -y python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Install PySpark and Numpy
RUN \
    pip3 install --upgrade pip && \
    pip install numpy && \
    pip install pyspark

# Define working directory
WORKDIR /data

# Define default command
CMD ["bash"]