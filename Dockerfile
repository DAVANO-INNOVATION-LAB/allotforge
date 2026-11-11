FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY davano ./davano
COPY fixtures ./fixtures
COPY web ./web
RUN mkdir -p /data && chown 10001:0 /data && chmod 0770 /data && chmod -R a+rX /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 DAVANO_DB_PATH=/data/davano.db DAVANO_BIND=0.0.0.0 PORT=8080
USER 10001:0
EXPOSE 8080
CMD ["python", "-m", "davano.app"]
