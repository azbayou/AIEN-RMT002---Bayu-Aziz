### 1. Setup Docker
```powershell
docker --version
docker compose up -d
docker compose up -d nl2sql-api analytics-ui
```

Airflow   : http://localhost:8080
Streamlit : http://localhost:8501
FastAPI   : http://localhost:8000/docs