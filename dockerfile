FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

COPY requirements.txt /tmp/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --requirement /tmp/requirements.txt

COPY ./app /app

# added line in order to be able to have access to all paths, from the deployed container
CMD uvicorn appMonTrajet:app --host 0.0.0.0 --port 80