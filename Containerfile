FROM python:3.10.13-slim-bullseye

LABEL maintainer="RBS-HARPA-PROJECT"
LABEL version="1.0"
LABEL description="Containerfile for the RBS-HARPA-PROJECT"

# Set the working directory to /app
WORKDIR /app

# Copy python files into the container at /app
COPY main.py /app
COPY meteoAPI.py /app
COPY calculate_consumption.py /app
COPY requirements.txt /app

# Create Dataset directory
COPY Dataset /app/Dataset

# Create dataset_result directory empty
RUN mkdir /app/dataset_result
RUN mkdir /app/dataset_result/meteo 
RUN mkdir /app/dataset_result/meteo_consumption

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the app.py when the container launches
CMD ["python", "main.py"]




