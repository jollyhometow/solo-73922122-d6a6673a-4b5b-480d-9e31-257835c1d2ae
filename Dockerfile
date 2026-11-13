FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv/app
COPY requirements.txt /srv/app/requirements.txt
RUN pip install -r requirements.txt

COPY app /srv/app/app

RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000
# 两个 Web 进程由 compose 分别启动；容器内单 worker，保证双进程形态可演示
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
