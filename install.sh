#!/bin/bash
# Installation script for Resort Music Player on Raspberry Pi

set -e

echo "========================================="
echo "Resort Music Player - Installation"
echo "========================================="

# Check if running as root
if [ "$EUID" -eq 0 ]; then 
   echo "Please do not run as root. Run as pi user."
   exit 1
fi

# Update system
echo "Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install system dependencies
echo "Installing system dependencies..."
sudo apt install -y \
    python3 \
    python3-pip \
    python3-venv \
    sqlite3 \
    libasound2-dev \
    libportaudio2 \
    build-essential \
    git

# Create project directory
PROJECT_DIR="/home/pi/resort-music-player"
echo "Creating project directory: $PROJECT_DIR"
mkdir -p $PROJECT_DIR
cd $PROJECT_DIR

# Copy files
echo "Copying project files..."
cp -r backend $PROJECT_DIR/
cp .env.example $PROJECT_DIR/.env

# Create Python virtual environment
echo "Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r backend/requirements.txt

# Create necessary directories
echo "Creating storage directories..."
sudo mkdir -p /media/ssd/synced
sudo mkdir -p /media/ssd/unsynced
sudo mkdir -p /media/ssd/special
sudo mkdir -p /home/pi/music_backup

# Set permissions
sudo chown -R pi:pi /media/ssd
sudo chown -R pi:pi /home/pi/music_backup

# Configure audio
echo "Configuring audio output..."
sudo amixer cset numid=3 1  # Force 3.5mm jack

# Create systemd service
echo "Creating systemd service..."
sudo tee /etc/systemd/system/resort-music.service > /dev/null <<EOF
[Unit]
Description=Resort Music Player Service
After=network.target

[Service]
Type=simple
User=pi
WorkingDirectory=$PROJECT_DIR
Environment="PATH=$PROJECT_DIR/venv/bin"
ExecStart=$PROJECT_DIR/venv/bin/python backend/app.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd
sudo systemctl daemon-reload

# Enable service
echo "Enabling service to start on boot..."
sudo systemctl enable resort-music.service

echo ""
echo "========================================="
echo "Installation Complete!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Edit .env file with your configuration:"
echo "   nano $PROJECT_DIR/.env"
echo ""
echo "2. Add your Gayatri Mantra file:"
echo "   cp your_file.mp3 /media/ssd/special/gayatri_mantra.mp3"
echo ""
echo "3. Start the service:"
echo "   sudo systemctl start resort-music.service"
echo ""
echo "4. Check status:"
echo "   sudo systemctl status resort-music.service"
echo ""
echo "5. View logs:"
echo "   sudo journalctl -u resort-music.service -f"
echo ""
