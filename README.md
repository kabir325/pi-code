# Resort Music Player - Raspberry Pi Code

Backend code for running the music player on Raspberry Pi with Ubuntu.

## Features

- 🎵 Automatic music playback with scheduling
- 💾 Intelligent storage failover (SSD → SD card)
- 🔄 Auto-restart on system reboot
- 🌐 REST API for remote control
- 📊 Real-time status monitoring
- 🔐 Secure access via Tailscale

## Quick Installation

1. Copy this folder to your Raspberry Pi:
```bash
scp -r pi-code pi@your-pi-ip:/home/pi/
```

2. SSH into your Pi:
```bash
ssh pi@your-pi-ip
```

3. Run the installation script:
```bash
cd /home/pi/pi-code
chmod +x install.sh
./install.sh
```

4. Configure your settings:
```bash
nano /home/pi/resort-music-player/.env
```

**Important**: Customize the schedule for each Pi:
- Pi 1: Keep default times (7 AM - 11 PM)
- Pi 2: Modify START_TIME, END_TIME, GAYATRI_TIMES as needed

5. Add Gayatri Mantra file:
```bash
cp your_gayatri_mantra.mp3 /media/ssd/special/gayatri_mantra.mp3
```

6. Start the service:
```bash
sudo systemctl start resort-music.service
```

## Service Management

### Check Status
```bash
sudo systemctl status resort-music.service
```

### View Logs
```bash
sudo journalctl -u resort-music.service -f
```

### Restart Service
```bash
sudo systemctl restart resort-music.service
```

### Stop Service
```bash
sudo systemctl stop resort-music.service
```

### Disable Auto-start
```bash
sudo systemctl disable resort-music.service
```

## API Endpoints

The service runs on port 5000 and provides these endpoints:

- `GET /api/status` - Get current player status
- `POST /api/control` - Control playback (play/pause/skip/volume)
- `GET /api/stats` - Get music library statistics
- `GET /api/health` - System health check
- `POST /api/upload/*` - Upload music files
- `GET /api/storage/*` - Storage monitoring

## Tailscale Setup

1. Install Tailscale on your Pi:
```bash
curl -fsSL https://tailscale.com/install.sh | sh
```

2. Authenticate:
```bash
sudo tailscale up
```

3. Get your Tailscale IP:
```bash
tailscale ip -4
```

4. Use this IP in your frontend configuration

## Directory Structure

```
/home/pi/resort-music-player/
├── backend/                 # Python Flask application
│   ├── app.py              # Main application
│   ├── config.py           # Configuration
│   ├── enhanced_music_player.py
│   ├── api/                # API routes
│   └── services/           # Business logic
├── venv/                   # Python virtual environment
└── .env                    # Your configuration

/media/ssd/
├── synced/                 # Processed music files
├── unsynced/               # Upload staging
└── special/                # Gayatri Mantra

/home/pi/music_backup/      # Fallback storage (SD card)
```

## Troubleshooting

### Service won't start
```bash
# Check logs for errors
sudo journalctl -u resort-music.service -n 50

# Check if port 5000 is available
sudo netstat -tulpn | grep 5000

# Verify Python dependencies
source /home/pi/resort-music-player/venv/bin/activate
pip list
```

### No audio output
```bash
# Test audio
aplay /usr/share/sounds/alsa/Front_Left.wav

# Configure audio output
sudo raspi-config
# Select: Advanced Options → Audio → Force 3.5mm jack

# Check volume
amixer get Master
```

### Storage issues
```bash
# Check mounted drives
lsblk
df -h

# Verify permissions
ls -la /media/ssd
ls -la /home/pi/music_backup
```

### Can't connect from frontend
```bash
# Check if service is running
sudo systemctl status resort-music.service

# Check firewall (if enabled)
sudo ufw status
sudo ufw allow 5000

# Test API locally
curl http://localhost:5000/api/status

# Check Tailscale connection
tailscale status
```

## Customization for Multiple Pis

### Pi 1 Configuration (.env)
```bash
START_TIME=07:00
END_TIME=23:00
GAYATRI_TIMES=07:00,19:00
```

### Pi 2 Configuration (.env)
```bash
START_TIME=08:00
END_TIME=22:00
GAYATRI_TIMES=08:00,18:00
```

Each Pi can have its own schedule while using the same codebase.

## Auto-Start on Boot

The installation script automatically configures the service to start on boot. The service will:
- Start automatically when the Pi boots
- Restart automatically if it crashes
- Wait 10 seconds between restart attempts
- Run as the `pi` user

## Updating

To update the code:

1. Stop the service:
```bash
sudo systemctl stop resort-music.service
```

2. Update files:
```bash
cd /home/pi/resort-music-player
# Copy new files or git pull
```

3. Update dependencies if needed:
```bash
source venv/bin/activate
pip install -r backend/requirements.txt
```

4. Restart service:
```bash
sudo systemctl start resort-music.service
```

## Support

For issues or questions, check:
- Service logs: `sudo journalctl -u resort-music.service -f`
- Application logs: `tail -f /home/pi/music_player.log`
- System status: `curl http://localhost:5000/api/health`
