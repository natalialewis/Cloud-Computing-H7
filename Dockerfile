FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY consumer.py .
COPY retriever.py .
COPY storage.py .

# This is the default command that will run when the container starts
CMD ["python", "consumer.py", "--storage", "dynamodb", "--table-name", "widgets", "--queue-name", "cs5270-requests"]