FROM python:3.11-slim

# Install system dependencies for audio and file handling
RUN apt-get update && apt-get install -y \
    libasound2-dev \
    libportaudio2 \
    libportaudiocpp0 \
    portaudio19-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /home/pi/music_backup /media/ssd/synced /media/ssd/unsynced

# Expose port
EXPOSE 5000

# Run the application
CMD ["python", "music_player.py"]