FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /srv
RUN pip install --no-cache-dir fastapi==0.146.0 "uvicorn[standard]==0.54.0"
COPY backend/mock_services /srv/mock_services
EXPOSE 9000
CMD ["uvicorn", "mock_services.app:app", "--host", "0.0.0.0", "--port", "9000"]
