FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    pyarrow \
    google-auth \
    requests

COPY src/*.py ./

CMD ["python3", "entrypoint.py"]
