FROM python:3.11-slim

# 필요한 리눅스 패키지 및 dbt 설치
RUN apt-get update && apt-get install -y git \
    && pip install --no-cache-dir dbt-postgres

WORKDIR /dbt