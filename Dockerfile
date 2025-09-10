FROM apache/airflow:2.9.1-python3.11

USER root

# Install dependencies for building Python 3.11.8
RUN apt-get update && \
    apt-get install -y \
        wget \
        build-essential \
        libssl-dev \
        zlib1g-dev \
        libncurses5-dev \
        libncursesw5-dev \
        libreadline-dev \
        libsqlite3-dev \
        libgdbm-dev \
        libdb5.3-dev \
        libbz2-dev \
        libexpat1-dev \
        liblzma-dev \
        tk-dev \
        uuid-dev \
        libffi-dev \
        curl \
        openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

# Build and install Python 3.11.8 manually
RUN cd /usr/src && \
    wget https://www.python.org/ftp/python/3.11.8/Python-3.11.8.tgz && \
    tar xzf Python-3.11.8.tgz && \
    cd Python-3.11.8 && \
    ./configure --enable-optimizations && \
    make -j"$(nproc)" && \
    make altinstall && \
    ln -sf /usr/local/bin/python3.11 /usr/local/bin/python3 && \
    ln -sf /usr/local/bin/pip3.11 /usr/local/bin/pip3 && \
    python3 --version && pip3 --version

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt /

USER airflow

# Install Python packages
RUN pip3 install --no-cache-dir -r /requirements.txt
