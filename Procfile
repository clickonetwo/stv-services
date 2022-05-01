release: python release.py
web: gunicorn -k uvicorn.workers.UvicornWorker stv_services.web.main:app
