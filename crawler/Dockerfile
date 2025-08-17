### crawler/Dockerfile
```dockerfile
FROM python:3.11-slim
WORKDIR /app

# Install shared dependencies
COPY ../requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy crawler code
COPY . .

# Default command
CMD ["python", "-m", "crawler.main"]