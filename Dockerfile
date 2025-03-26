
FROM veighna/veighna:3.9.4 AS base 


# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3-slim AS app


# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Install lib which is required by vnpy
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc wget build-essential \
    && rm -rf /var/lib/apt/lists/*
# Clean up and remove gcc
#    && apt-get purge -y --auto-remove gcc build-essential

# Install TA-Lib
RUN wget "https://github.com/ta-lib/ta-lib/releases/download/v0.6.4/ta-lib_0.6.4_amd64.deb" \
    && dpkg -i ta-lib_0.6.4_amd64.deb \
    && rm -rf ta-lib_0.6.4_amd64.deb

# Install pip requirements
COPY requirements.txt .
# RUN python -m pip install -i "https://pypi.tuna.tsinghua.edu.cn/simple" --default-timeout=500 -r requirements.txt
RUN python -m pip install --default-timeout=500 -r requirements.txt

WORKDIR /app
COPY . /app

# Creates a non-root user with an explicit UID and adds permission to access the /app folder
# For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["python", "app/main.py"]
