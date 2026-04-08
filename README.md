# shortener_service

FastAPI URL shortener adapted for classic deployment via Ansible + systemd + nginx.

## Runtime

The service reads configuration from environment variables:

- `DATABASE_URL`
- `REDIS_URL`
- `BASE_URL`

Default values are compatible with the accompanying `ansible-lab.zip` deployment.

## Local run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```
