# afm-ingestion

Backend API for transaction ingestion, analytics, admin, and NL2SQL chat.

## Requirements

- Python 3.11+
- PowerShell on Windows
- Configured [`.env`](D:/afm-ingestion/afm-ingestion/.env)

## Run Backend

Current frontend is configured to call the backend on `http://127.0.0.1:8003`.

```powershell
cd D:\afm-ingestion\afm-ingestion
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m uvicorn app.main:app --host 127.0.0.1 --port 8003 --reload
```

If you do not want to activate the virtual environment:

```powershell
cd D:\afm-ingestion\afm-ingestion
.\venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8003 --reload
```

## Health Check

```powershell
Invoke-WebRequest http://127.0.0.1:8003/health
```

Expected response:

```json
{"status":"ok","service":"afm-ingestion-api"}
```

## Notes

- FastAPI app entrypoint: `app.main:app`
- Admin panel: `http://127.0.0.1:8003/admin`
- API docs: `http://127.0.0.1:8003/docs`
