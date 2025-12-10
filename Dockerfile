# Use a slim and stable base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

ENV RUN_DOCKER=true

# Add project to PYTHONPATH so that 'etl' module is visible
ENV PYTHONPATH=/app

#ENV PREFECT_API_MODE=OFF
#ENV PREFECT_LOCAL_SERVER_PROCESS="disabled"
#prefect3 always tries to connect to a server so above commands do not work

# Copy only requirements first (Docker layer caching optimization)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project
COPY . .

# Create the database folder if needed
RUN mkdir -p db

# Default command: run the ETL pipeline
CMD ["python", "etl/run_local_no_prefect.py"]