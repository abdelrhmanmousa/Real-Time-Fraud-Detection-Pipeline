FROM python:3.10-slim

WORKDIR /fraud_detection

COPY ./requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY ./fraud_detection/ ./

# Define default environment variables
# These can be overridden at runtime (e.g., docker run -e DATA_PATH=/custom/data.csv ...)
ENV DATA_PATH=""
ENV ARTIFACTS_DIR="/fraud_detection/artifacts" 
ENV TEST_SIZE_RATIO="0.2"
ENV N_TRIALS="3"
ENV WANDB_API_KEY="" 

ENTRYPOINT ["python", "-m", "main"]
