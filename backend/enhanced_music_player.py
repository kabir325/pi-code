#!/usr/bin/env python3
"""
Enhanced Resort Music Player System
Integrates with storage monitoring and provides robust failover capabilities
"""

import os
import sqlite3
import json
import random
import threading
import time
import logging
from datetime import datetime, time as dt_time
from pathlib import Path
import schedule
import pygame
from contextlib import contextmanager

from config import Config
from services.storage_monitor import StorageMonitor
from services.storage_health_checker import StorageHealthChecker
from services.backup_manager import BackupManager

logger = logging.getLogger(__name__)

class EnhancedMusicDatabase:
    """Enhanced database operations with storage location awareness"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize the database with enhanced schema"""
        with self.get_connection() as conn:
            # Enhanced songs table with storage tracking
            conn.execute('''
                CREATE TABLE IF NOT EXISTS songs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT UNIQUE NOT NULL,
                    filepath TEXT NOT NULL,
                    title TEXT,
                    artist TEXT,
                    album TEXT,
                    duration INTEGER,
                    file_size INTEGER,
                    format TEXT,
                    bitrate INTEGER,
                    date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    play_count INTEGER DEFAULT 0,
                    last_played TIMESTAMP,
                    storage_location TEXT DEFAULT 'primary',
                    primary_path TEXT,
                    fallback_path TEXT,
                    is_backup_synced BOOLEAN DEFAULT FALSE,
                    backup_date TIMESTAMP,
                    checksum TEXT,
                    is_available BOOLEAN DEFAULT TRUE
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS play_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    song_id INTEGER,
                    played_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    duration_played INTEGER,
                    storage_used TEXT,
                    playback_quality TEXT,
                    FOREIGN KEY (song_id) REFERENCES songs (id)
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS system_status (
                    id INTEGER PRIMARY KEY,
                    is_playing BOOLEAN DEFAULT FALSE,
                    current_song_id INTEGER,
                    volume REAL DEFAULT 0.7,
                    storage_mode TEXT DEFAULT 'primary',
                    last_health_check TIMESTAMP,
                    upload_in_progress BOOLEAN DEFAULT FALSE,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Initialize system status if not exists
            conn.execute('INSERT OR IGNORE INTO system_status (id) VALUES (1)')
            conn.commit()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def get_random_song(self, storage_mode: str = None) -> dict:
        """Get a random song from available storage"""
        with self.get_connection() as conn:
            if storage_mode == 'primary':
                # Only get songs from primary storage
                cursor = conn.execute('''
                    SELECT * FROM songs 
                    WHERE storage_location IN ('primary', 'both') 
                    AND is_available = TRUE
                    ORDER BY RANDOM() 
                    LIMIT 1
                ''')
            elif storage_mode == 'fallback':
                # Only get songs from fallback storage
                cursor = conn.execute('''
                    SELECT * FROM songs 
                    WHERE storage_location IN ('fallback', 'both') 
                    AND is_available = TRUE
                    ORDER BY RANDOM() 
                    LIMIT 1
                ''')
            else:
                # Get from any available storage
                cursor = conn.execute('''
                    SELECT * FROM songs 
                    WHERE is_available = TRUE
                    ORDER BY RANDOM() 
                    LIMIT 1
                ''')
            
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_song_path(self, song: dict, preferred_storage: str = 'primary') -> str:
        """Get the appropriate file path for a song based on storage preference"""
        if preferred_storage == 'primary' and song.get('primary_path'):
            if os.path.exists(song['primary_path']):
                return song['primary_path']
        
        if preferred_storage == 'fallback' and song.get('fallback_path'):
            if os.path.exists(song['fallback_path']):
                return song['fallback_path']
        
        # Fallback to any available path
        if song.get('primary_path') and os.path.exists(song['primary_path']):
            return song['primary_path']
        elif song.get('fallback_path') and os.path.exists(song['fallback_path']):
            return song['fallback_path']
        else:
            return song['filepath']  # Legacy fallback
    
    def update_play_stats(self, song_id: int, duration_played: int = None, 
                         storage_used: str = None, playback_quality: str = 'normal'):
        """Update play statistics with enhanced tracking"""
        with self.get_connection() as conn:
            conn.execute('''
                UPDATE songs 
                SET play_count = play_count + 1, last_played = CURRENT_TIMESTAMP 
                WHERE id = ?
            ''', (song_id,))
            
            conn.execute('''
                INSERT INTO play_history 
                (song_id, duration_played, storage_used, playback_quality) 
                VALUES (?, ?, ?, ?)
            ''', (song_id, duration_played, storage_used, playback_quality))
            
            conn.commit()
    
    def mark_song_unavailable(self, song_id: int):
        """Mark a song as unavailable"""
        with self.get_connection() as conn:
            conn.execute('''
                UPDATE songs 
                SET is_available = FALSE 
                WHERE id = ?
            ''', (song_id,))
            conn.commit()
    
    def get_system_status(self):
        """Get current system status"""
        with self.get_connection() as conn:
            cursor = conn.execute('SELECT * FROM system_status WHERE id = 1')
            return dict(cursor.fetchone())
    
    def update_system_status(self, **kwargs):
        """Update system status"""
        with self.get_connection() as conn:
            set_clause = ', '.join([f"{key} = ?" for key in kwargs.keys()])
            values = list(kwargs.values()) + [1]
            conn.execute(f'''
                UPDATE system_status 
                SET {set_clause}, last_updated = CURRENT_TIMESTAMP 
                WHERE id = ?
            ''', values)
            conn.commit()

class EnhancedMusicPlayer:
    """Enhanced music player with storage monitoring integration"""
    
    def __init__(self):
        self.db = EnhancedMusicDatabase(Config.DATABASE_PATH)
        self.storage_monitor = StorageMonitor()
        self.health_checker = StorageHealthChecker(self.storage_monitor)
        self.backup_manager = BackupManager()
        
        # Player state
        self.current_song = None
        self.is_playing = False
        self.is_special_play = False
        self.volume = 0.7
        self.stop_requested = False
        self.playback_errors = 0
        self.max_playback_errors = 3
        
        # Storage state
        self.current_storage_mode = 'primary'
        self.last_storage_check = None
        self.storage_switch_cooldown = 30  # seconds
        self.last_storage_switch = None
        
        # Initialize pygame mixer
        try:
            pygame.mixer.init(frequency=44100, size=-16, channels=2, buffer=1024)
            logger.info("Audio system initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize audio system: {e}")
            raise
        
        # Setup schedule
        self.setup_schedule()
        
        # Start monitoring services
        self.start_monitoring_services()
    
    def start_monitoring_services(self):
        """Start all monitoring services"""
        try:
            self.storage_monitor.start_monitoring()
            self.health_checker.start_continuous_monitoring()
            logger.info("Monitoring services started")
        except Exception as e:
            logger.error(f"Error starting monitoring services: {e}")
    
    def stop_monitoring_services(self):
        """Stop all monitoring services"""
        try:
            self.storage_monitor.stop_monitoring()
            self.health_checker.stop_continuous_monitoring()
            logger.info("Monitoring services stopped")
        except Exception as e:
            logger.error(f"Error stopping monitoring services: {e}")
    
    def setup_schedule(self):
        """Setup the daily schedule"""
        # Schedule Gayatri Mantra
        for time_str in Config.GAYATRI_TIMES:
            schedule.every().day.at(time_str).do(self.play_gayatri_mantra)
        
        # Schedule start and stop times
        schedule.every().day.at(Config.START_TIME).do(self.start_daily_music)
        schedule.every().day.at(Config.END_TIME).do(self.stop_daily_music)
        
        # Schedule periodic health checks
        schedule.every(10).minutes.do(self.perform_health_check)
        
        # Schedule backup sync
        schedule.every().hour.do(self.sync_backup_storage)
        
        logger.info("Schedule setup complete")
    
    def is_active_hours(self):
        """Check if current time is within active hours"""
        now = datetime.now().time()
        start_time = dt_time.fromisoformat(Config.START_TIME)
        end_time = dt_time.fromisoformat(Config.END_TIME)
        return start_time <= now <= end_time
    
    def get_current_storage_path(self):
        """Get current storage path based on storage monitor"""
        return self.storage_monitor.get_current_storage_path()
    
    def check_and_switch_storage(self):
        """Check storage health and switch if necessary"""
        try:
            # Avoid frequent switches
            if (self.last_storage_switch and 
                (datetime.now() - self.last_storage_switch).seconds < self.storage_switch_cooldown):
                return
            
            # Check if automatic switch is needed
            switch_result = self.storage_monitor.auto_switch_storage()
            if switch_result:
                self.current_storage_mode = self.storage_monitor.current_storage
                self.last_storage_switch = datetime.now()
                
                # Update system status
                self.db.update_system_status(storage_mode=self.current_storage_mode)
                
                logger.info(f"Storage switched to {self.current_storage_mode}: {switch_result['message']}")
                
                # Reset playback errors after successful switch
                self.playback_errors = 0
                
        except Exception as e:
            logger.error(f"Error checking storage switch: {e}")
    
    def perform_health_check(self):
        """Perform periodic health check"""
        try:
            # Check current storage health
            health_result = self.health_checker.perform_comprehensive_health_check(self.current_storage_mode)
            
            # Update last health check time
            self.db.update_system_status(last_health_check=datetime.now().isoformat())
            
            # Log critical issues
            if health_result['overall_status'] == 'error':
                logger.warning(f"Critical storage issues detected: {health_result['alerts']}")
            
            self.last_storage_check = datetime.now()
            
        except Exception as e:
            logger.error(f"Error during health check: {e}")
    
    def sync_backup_storage(self):
        """Sync backup storage periodically"""
        try:
            result = self.backup_manager.sync_backup_storage()
            logger.info(f"Backup sync completed: {result}")
        except Exception as e:
            logger.error(f"Error during backup sync: {e}")
    
    def play_gayatri_mantra(self):
        """Play Gayatri Mantra for specified duration"""
        gayatri_path = Config.GAYATRI_MANTRA_PATH
        
        # Try to find Gayatri Mantra in current storage
        if not os.path.exists(gayatri_path):
            # Try fallback location
            fallback_gayatri = os.path.join(Config.BACKUP_FOLDER, 'gayatri_mantra.mp3')
            if os.path.exists(fallback_gayatri):
                gayatri_path = fallback_gayatri
            else:
                logger.error("Gayatri Mantra file not found in any storage location")
                return
        
        logger.info("Playing Gayatri Mantra")
        self.is_special_play = True
        
        try:
            pygame.mixer.music.load(gayatri_path)
            pygame.mixer.music.set_volume(self.volume)
            pygame.mixer.music.play()
            
            # Wait for specified duration
            time.sleep(Config.GAYATRI_DURATION)
            
            if pygame.mixer.music.get_busy():
                pygame.mixer.music.stop()
                
        except Exception as e:
            logger.error(f"Error playing Gayatri Mantra: {e}")
        finally:
            self.is_special_play = False
            logger.info("Gayatri Mantra playback complete")
    
    def play_random_song(self):
        """Play a random song with storage awareness"""
        # Check storage health before playing
        self.check_and_switch_storage()
        
        # Get random song from current storage
        song = self.db.get_random_song(self.current_storage_mode)
        if not song:
            logger.warning(f"No songs available in {self.current_storage_mode} storage")
            
            # Try other storage if current fails
            if self.current_storage_mode == 'primary':
                song = self.db.get_random_song('fallback')
                if song:
                    logger.info("Falling back to backup storage for song selection")
            
            if not song:
                logger.error("No songs available in any storage")
                return False
        
        # Get appropriate file path
        filepath = self.db.get_song_path(song, self.current_storage_mode)
        
        if not os.path.exists(filepath):
            logger.warning(f"Song file not found: {filepath}")
            self.db.mark_song_unavailable(song['id'])
            self.playback_errors += 1
            
            # Try alternative path
            alt_storage = 'fallback' if self.current_storage_mode == 'primary' else 'primary'
            alt_filepath = self.db.get_song_path(song, alt_storage)
            
            if alt_filepath != filepath and os.path.exists(alt_filepath):
                filepath = alt_filepath
                logger.info(f"Using alternative path: {alt_filepath}")
            else:
                return False
        
        try:
            pygame.mixer.music.load(filepath)
            pygame.mixer.music.set_volume(self.volume)
            pygame.mixer.music.play()
            
            self.current_song = dict(song)
            self.current_song['current_filepath'] = filepath
            
            # Determine which storage was actually used
            storage_used = 'primary' if Config.SYNCED_FOLDER in filepath else 'fallback'
            
            self.db.update_system_status(
                is_playing=True,
                current_song_id=song['id'],
                storage_mode=storage_used
            )
            
            # Reset error count on successful playback
            self.playback_errors = 0
            
            logger.info(f"Playing: {song['filename']} from {storage_used} storage")
            return True
            
        except Exception as e:
            logger.error(f"Error playing song {filepath}: {e}")
            self.playback_errors += 1
            
            # Mark song as unavailable if it consistently fails
            if self.playback_errors >= self.max_playback_errors:
                self.db.mark_song_unavailable(song['id'])
                logger.warning(f"Marked song as unavailable after {self.max_playback_errors} failures")
            
            return False
    
    def stop_music(self):
        """Stop current music playback"""
        try:
            pygame.mixer.music.stop()
            self.is_playing = False
            
            if self.current_song:
                # Calculate playback duration
                # Note: This is a simplified approach - in a real implementation,
                # you'd track actual playback time
                duration_played = self.current_song.get('duration', 0)
                storage_used = 'primary' if Config.SYNCED_FOLDER in self.current_song.get('current_filepath', '') else 'fallback'
                
                self.db.update_play_stats(
                    self.current_song['id'], 
                    duration_played, 
                    storage_used
                )
            
            self.db.update_system_status(is_playing=False, current_song_id=None)
            logger.info("Music stopped")
            
        except Exception as e:
            logger.error(f"Error stopping music: {e}")
    
    def set_volume(self, volume):
        """Set playback volume (0.0 to 1.0)"""
        self.volume = max(0.0, min(1.0, volume))
        pygame.mixer.music.set_volume(self.volume)
        self.db.update_system_status(volume=self.volume)
    
    def start_daily_music(self):
        """Start the daily music routine"""
        logger.info("Starting daily music routine")
        self.is_playing = True
        
        # Perform health check at start of day
        self.perform_health_check()
    
    def stop_daily_music(self):
        """Stop the daily music routine"""
        logger.info("Stopping daily music routine")
        self.stop_music()
        self.stop_requested = True
    
    def get_enhanced_status(self):
        """Get enhanced system status including storage information"""
        try:
            base_status = self.db.get_system_status()
            
            # Add storage information
            storage_health = self.storage_monitor.check_storage_health()
            current_alerts = self.health_checker.get_current_alerts()
            
            enhanced_status = dict(base_status)
            enhanced_status.update({
                'current_time': datetime.now().isoformat(),
                'is_active_hours': self.is_active_hours(),
                'storage_health': storage_health['overall_health'],
                'primary_storage_available': storage_health['primary']['is_available'] if storage_health['primary'] else False,
                'fallback_storage_available': storage_health['fallback']['is_available'] if storage_health['fallback'] else False,
                'active_alerts': len([a for a in current_alerts if not a['resolved']]),
                'playback_errors': self.playback_errors,
                'last_storage_check': self.last_storage_check.isoformat() if self.last_storage_check else None
            })
            
            if self.current_song:
                enhanced_status['current_song'] = self.current_song
            
            return enhanced_status
            
        except Exception as e:
            logger.error(f"Error getting enhanced status: {e}")
            return self.db.get_system_status()
    
    def music_loop(self):
        """Main music playback loop with enhanced error handling"""
        while not self.stop_requested:
            try:
                # Check schedule
                schedule.run_pending()
                
                # Skip if special playback is active
                if self.is_special_play:
                    time.sleep(1)
                    continue
                
                # Check if we should be playing
                if not self.is_active_hours() or not self.is_playing:
                    time.sleep(10)
                    continue
                
                # If no music is playing, start a new song
                if not pygame.mixer.music.get_busy():
                    success = self.play_random_song()
                    
                    if not success:
                        # If playback fails, wait longer before retrying
                        wait_time = min(30, 5 * self.playback_errors)
                        logger.warning(f"Playback failed, waiting {wait_time} seconds before retry")
                        time.sleep(wait_time)
                        
                        # Check if we should switch storage due to repeated failures
                        if self.playback_errors >= self.max_playback_errors:
                            logger.warning("Multiple playback failures, checking storage switch")
                            self.check_and_switch_storage()
                            self.playback_errors = 0  # Reset after attempting switch
                
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error in music loop: {e}")
                time.sleep(5)
        
        # Cleanup on exit
        self.stop_monitoring_services()
    
    def force_storage_switch(self, target_storage: str):
        """Force switch to specific storage"""
        try:
            result = self.storage_monitor.switch_storage(target_storage)
            if result['success']:
                self.current_storage_mode = target_storage
                self.last_storage_switch = datetime.now()
                self.db.update_system_status(storage_mode=target_storage)
                logger.info(f"Forced storage switch to {target_storage}")
            return result
        except Exception as e:
            logger.error(f"Error forcing storage switch: {e}")
            return {'success': False, 'error': str(e)}
    
    def get_storage_metrics(self):
        """Get comprehensive storage metrics"""
        try:
            return {
                'storage_monitor': self.storage_monitor.get_storage_metrics(),
                'health_checker': {
                    'primary_health': self.health_checker.perform_comprehensive_health_check('primary'),
                    'fallback_health': self.health_checker.perform_comprehensive_health_check('fallback'),
                    'current_alerts': self.health_checker.get_current_alerts()
                },
                'backup_manager': self.backup_manager.get_backup_status()
            }
        except Exception as e:
            logger.error(f"Error getting storage metrics: {e}")
            return {'error': str(e)}
    
    def cleanup_and_maintenance(self):
        """Perform cleanup and maintenance tasks"""
        try:
            # Cleanup old health data
            self.health_checker.cleanup_old_health_data(days=7)
            
            # Cleanup old storage events
            self.storage_monitor.cleanup_old_events(days=30)
            
            # Verify backup integrity
            self.backup_manager.verify_backup_integrity()
            
            logger.info("Cleanup and maintenance completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup and maintenance: {e}")

# Global instance for Flask integration
enhanced_music_player = None

def get_music_player():
    """Get the global music player instance"""
    global enhanced_music_player
    if enhanced_music_player is None:
        enhanced_music_player = EnhancedMusicPlayer()
    return enhanced_music_player