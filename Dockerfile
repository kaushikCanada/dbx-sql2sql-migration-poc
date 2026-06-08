FROM apache/spark:4.0.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gnupg2 unixodbc-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc \
       | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] \
       https://packages.microsoft.com/debian/12/prod bookworm main" \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PATH="$PATH:/opt/mssql-tools18/bin"
ENV PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip:/app"
ENV PYSPARK_PYTHON="python3"

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .
