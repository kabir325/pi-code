-- Enhanced database schema for resort music player
-- Single songs table with storage location tracking

-- Upload sessions table for chunked uploads
CREATE TABLE upload_sessions (
    id TEXT PRIMARY KEY,           -- UUID for upload session
    filename TEXT NOT NULL,        -- Secure filename
    original_filename TEXT NOT NULL, -- Original filename from user
    file_size INTEGER NOT NULL,
    bytes_uploaded INTEGER DEFAULT 0,
    progress_percent REAL DEFAULT 0,
    status TEXT DEFAULT 'pending', -- pending/uploading/completed/failed
    temp_path TEXT,                -- Temporary file path during upload
    final_path TEXT,               -- Final file path after completion
    checksum TEXT,                 -- MD5 checksum of completed file
    error_message TEXT,            -- Error message if failed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,          -- When first chunk was uploaded
    last_chunk_at TIMESTAMP,       -- When last chunk was uploaded
    completed_at TIMESTAMP         -- When upload was completed
);

CREATE TABLE songs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT UNIQUE NOT NULL,
    filepath TEXT NOT NULL,
    title TEXT,
    artist TEXT,
    album TEXT,
    duration INTEGER,
    file_size INTEGER,
    format TEXT,                    -- Audio format (mp3, wav, flac, m4a)
    bitrate INTEGER,               -- Audio bitrate
    date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    play_count INTEGER DEFAULT 0,
    last_played TIMESTAMP,
    
    -- Storage tracking
    storage_location TEXT DEFAULT 'primary',  -- 'primary', 'fallback', 'both'
    primary_path TEXT,             -- Path on SSD
    fallback_path TEXT,            -- Path on SD card
    is_backup_synced BOOLEAN DEFAULT FALSE,   -- Is this song backed up to SD?
    backup_date TIMESTAMP,         -- When was it last backed up
    
    -- Metadata
    checksum TEXT,                 -- File checksum for integrity
    is_available BOOLEAN DEFAULT TRUE  -- Is file currently accessible
);

-- Index for performance
CREATE INDEX idx_songs_storage_location ON songs(storage_location);
CREATE INDEX idx_songs_available ON songs(is_available);
CREATE INDEX idx_songs_backup_synced ON songs(is_backup_synced);

-- Backup sync tracking table
CREATE TABLE backup_sync_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    song_id INTEGER,
    action TEXT NOT NULL,          -- 'backup_created', 'backup_updated', 'backup_failed'
    source_path TEXT,
    destination_path TEXT,
    file_size INTEGER,
    checksum TEXT,
    sync_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT,
    FOREIGN KEY (song_id) REFERENCES songs (id)
);

-- Storage status table for monitoring
CREATE TABLE storage_status (
    id INTEGER PRIMARY KEY,
    storage_type TEXT NOT NULL,    -- 'primary' or 'fallback'
    mount_point TEXT,
    is_available BOOLEAN DEFAULT FALSE,
    capacity_gb REAL,
    used_gb REAL,
    free_gb REAL,
    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    health_status TEXT DEFAULT 'unknown'  -- 'healthy', 'warning', 'error'
);

-- Initialize storage status records
INSERT OR IGNORE INTO storage_status (id, storage_type, mount_point) VALUES 
(1, 'primary', '/media/ssd'),
(2, 'fallback', '/home/pi/music_backup');

-- Example queries for different scenarios:

-- Get all songs available on primary storage
-- SELECT * FROM songs WHERE storage_location IN ('primary', 'both') AND is_available = TRUE;

-- Get all songs available on fallback storage  
-- SELECT * FROM songs WHERE storage_location IN ('fallback', 'both') AND is_available = TRUE;

-- Get songs that need backup
-- SELECT * FROM songs WHERE storage_location = 'primary' AND is_backup_synced = FALSE;

-- Get random song from available storage
-- SELECT * FROM songs WHERE 
--   (storage_location = 'primary' AND is_available = TRUE) OR 
--   (storage_location = 'fallback' AND is_available = TRUE) OR
--   (storage_location = 'both' AND is_available = TRUE)
-- ORDER BY RANDOM() LIMIT 1;