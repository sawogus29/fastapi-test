# app3/Dockerfile

FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY app3.py .

# Expose the port
EXPOSE 8003

# Command to run the application
CMD ["uvicorn", "app3:app", "--host", "0.0.0.0", "--port", "8003"]