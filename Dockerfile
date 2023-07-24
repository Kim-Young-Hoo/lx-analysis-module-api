# Use the official Python image as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the ChromeDriver binary from your project folder into the container
# COPY chromedriver /usr/local/bin/chromedriver

# Set execute permissions for the ChromeDriver binary
# RUN chmod +x /usr/local/bin/chromedriver

# Install necessary packages, including fonts-nanum, apt-utils, fontconfig, and chromium-chromedriver
RUN apt-get update && \
    apt-get install -y fonts-nanum* apt-utils fontconfig chromium-driver && \
    fc-cache -fv

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install the required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire app directory into the container
COPY . .

# Add the font configuration for matplotlib
RUN python -c "import matplotlib; print(matplotlib.__file__)" && \
    cp /usr/share/fonts/truetype/nanum/Nanum* /usr/local/lib/python3.9/site-packages/matplotlib/mpl-data/fonts/ttf/ && \
    rm -rf ~/.cache/matplotlib/*

# Expose the port your FastAPI app is listening on
EXPOSE 11100

# Start the FastAPI app using uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "11100"]
