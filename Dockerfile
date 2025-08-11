# Use a small, modern Python base
FROM python:3.11-slim

# set a working directory
WORKDIR /app

# install build deps (if any), then install pip packages
# keep layer minimal
COPY requirements.txt /app/requirements.txt
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev build-essential \
    && pip install --upgrade pip \
    && pip install -r /app/requirements.txt \
    && apt-get remove -y gcc build-essential libpq-dev \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# copy app files
COPY . /app

# expose port for FastAPI/uvicorn
EXPOSE 8000

# default command: run uvicorn. Override in `docker run` or docker-compose for celery worker
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

