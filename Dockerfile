# Use the official Airflow image with Python 3.11
FROM apache/airflow:latest-python3.11

# Switch to root to install additional system packages
USER root

# Install OpenJDK 17 and other dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && apt-get install -y --no-install-recommends gdal-bin libgdal-dev libspatialindex-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Miniconda
RUN curl -sS https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -b -p /opt/conda \
    && rm -f /tmp/miniconda.sh \
    && /opt/conda/bin/conda clean --all --yes

# Add conda to PATH
ENV PATH="/opt/conda/bin:$PATH"

# Switch back to airflow user
USER airflow

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Install Airflow and required providers
RUN pip install --no-cache-dir \
    apache-airflow \
    apache-airflow[statsd] \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-apache-hive \
    folium \
    SQLAlchemy \
    shapely \
    boto3 \
    branca \
    geopandas

# Install GeoPandas dependencies and clean up
RUN conda install --yes --channel conda-forge \
    geopandas=0.14.0 \
    pandas=2.0.3 \
    numpy=1.24 \
    python-dateutil=2.8.2 \
    pyproj=3.6.1 \
    fiona=1.9.5 \
    shapely=2.0.1 \
    rtree=1.0.1 \
    && conda clean --all --yes

# Verify GeoPandas installation
RUN python -c "import geopandas as gpd; print('GeoPandas version:', gpd.__version__)"
