FROM python:3.11-slim
WORKDIR /app
COPY requirement.txt .
RUN pip install -r requirement.txt
COPY main.py .
CMD ["functions-framework", "--target", "main_handler", "--signature-type", "cloudevent", "--port", "8080"]