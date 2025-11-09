import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    # Storage Configuration
    SYNCED_FOLDER = os.getenv('SYNCED_FOLDER', '/media/ssd/synced')
    UNSYNCED_FOLDER = os.getenv('UNSYNCED_FOLDER', '/media/ssd/unsynced')
    BACKUP_FOLDER = os.getenv('BACKUP_FOLDER', '/home/pi/music_backup')
    DATABASE_PATH = os.getenv('DATABASE_PATH', '/home/pi/music_player.db')
    
    # Special Files
    GAYATRI_MANTRA_PATH = os.getenv('GAYATRI_MANTRA_PATH', '/media/ssd/special/gayatri_mantra.mp3')
    
    # Logging
    LOG_FILE = os.getenv('LOG_FILE', '/home/pi/music_player.log')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Web Server
    WEB_PORT = int(os.getenv('WEB_PORT', 5000))
    WEB_HOST = os.getenv('WEB_HOST', '0.0.0.0')
    
    # Scheduling
    START_TIME = os.getenv('START_TIME', '07:00')
    END_TIME = os.getenv('END_TIME', '23:00')
    GAYATRI_TIMES = os.getenv('GAYATRI_TIMES', '07:00,19:00').split(',')
    GAYATRI_DURATION = int(os.getenv('GAYATRI_DURATION', 240))  # 4 minutes
    
    # File Upload
    MAX_UPLOAD_SIZE = int(os.getenv('MAX_UPLOAD_SIZE', 100 * 1024 * 1024))  # 100MB
    ALLOWED_EXTENSIONS = {'mp3', 'wav', 'flac', 'm4a', 'aac', 'ogg'}
    UPLOAD_CHUNK_SIZE = int(os.getenv('UPLOAD_CHUNK_SIZE', 1024 * 1024))  # 1MB chunks
    
    # Email Configuration
    SMTP_HOST = os.getenv('SMTP_HOST', 'smtp.gmail.com')
    SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
    SMTP_USER = os.getenv('SMTP_USER')
    SMTP_PASS = os.getenv('SMTP_PASS')
    ADMIN_EMAILS = os.getenv('ADMIN_EMAILS', '').split(',')
    
    # Security
    SECRET_KEY = os.getenv('SECRET_KEY', 'resort-music-player-secret-key-change-in-production')
    ACCESS_TOKEN_EXPIRE_HOURS = int(os.getenv('ACCESS_TOKEN_EXPIRE_HOURS', 24))
    
    # Storage Monitoring
    STORAGE_CHECK_INTERVAL = int(os.getenv('STORAGE_CHECK_INTERVAL', 30))  # seconds
    STORAGE_WARNING_THRESHOLD = float(os.getenv('STORAGE_WARNING_THRESHOLD', 0.9))  # 90% full
    
    # WebSocket
    WEBSOCKET_PING_INTERVAL = int(os.getenv('WEBSOCKET_PING_INTERVAL', 25))
    WEBSOCKET_PING_TIMEOUT = int(os.getenv('WEBSOCKET_PING_TIMEOUT', 60))
    
    @classmethod
    def create_directories(cls):
        """Create necessary directories if they don't exist"""
        directories = [
            cls.SYNCED_FOLDER,
            cls.UNSYNCED_FOLDER,
            cls.BACKUP_FOLDER,
            os.path.dirname(cls.DATABASE_PATH),
            os.path.dirname(cls.LOG_FILE),
            os.path.dirname(cls.GAYATRI_MANTRA_PATH)
        ]
        
        for directory in directories:
            if directory:
                Path(directory).mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def validate_config(cls):
        """Validate critical configuration"""
        errors = []
        
        if not cls.SMTP_USER or not cls.SMTP_PASS:
            errors.append("SMTP credentials not configured")
        
        if not cls.ADMIN_EMAILS or cls.ADMIN_EMAILS == ['']:
            errors.append("Admin emails not configured")
        
        if not os.path.exists(cls.GAYATRI_MANTRA_PATH):
            errors.append(f"Gayatri Mantra file not found: {cls.GAYATRI_MANTRA_PATH}")
        
        return errors