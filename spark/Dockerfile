FROM bitnami/spark:4.0.0
USER root

# Cài Python 3.11 và pip
RUN apt-get update && apt-get install -y \
    python3.11 python3.11-venv python3.11-distutils python3-pip curl \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 \
    && update-alternatives --set python3 /usr/bin/python3.11

# Đặt PATH ưu tiên /usr/bin trước
ENV PATH=/usr/bin:$PATH

# Set mặc định cho Spark dùng Python 3.11
ENV PYSPARK_PYTHON=/usr/bin/python3.11 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11

# Kiểm tra phiên bản Python và Spark
RUN python3 --version \
    && $SPARK_HOME/bin/spark-submit --version

USER 1001
