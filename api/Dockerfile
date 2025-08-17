FROM python:3.11-slim
WORKDIR /app

# Install shared dependencies
COPY ../requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy API code
COPY . .

# Expose port and run FastAPI via Uvicorn
EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]