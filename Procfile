release: alembic upgrade head
web: gunicorn -k uvicorn.workers.UvicornWorker stv_services.web.main:app
