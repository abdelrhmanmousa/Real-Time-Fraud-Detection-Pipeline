FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set the working directory in the container
WORKDIR /app

# Copy the dependencies file and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

CMD exec gunicorn -k uvicorn.workers.UvicornWorker --workers 1 --threads 8 --timeout 0 --bind 0.0.0.0:$PORT main:app