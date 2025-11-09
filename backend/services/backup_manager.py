import os
import shutil
import hashlib
import sqlite3
import logging
from pathlib import Path
from typing import List, Optional, Tuple
from datetime import datetime

from config import Config

logger = logging.getLogger(__name__)

class BackupManager:
    """Manages backup synchronization between primary (SSD) and fallback (SD) storage"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or Config.DATABASE_PATH
        self.primary_path = Config.SYNCED_FOLDER
        self.fallback_path = Config.BACKUP_FOLDER
        self.max_backup_songs = 100  # Limit backup songs due to SD card space
        
    def get_file_checksum(self, filepath: str) -> str:
        """Calculate MD5 checksum of a file"""
        hash_md5 = hashlib.md5()
        try:
            with open(filepath, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating checksum for {filepath}: {e}")
            return ""
    
    def get_songs_needing_backup(self) -> List[dict]:
        """Get songs that need to be backed up to fallback storage"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM songs 
                WHERE storage_location = 'primary' 
                AND is_backup_synced = FALSE 
                AND is_available = TRUE
                ORDER BY play_count DESC, date_added DESC
                LIMIT ?
            """, (self.max_backup_songs,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_backup_candidates(self) -> List[dict]:
        """Get most played songs that should be prioritized for backup"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM songs 
                WHERE storage_location = 'primary' 
                AND is_available = TRUE
                ORDER BY play_count DESC, last_played DESC
                LIMIT ?
            """, (self.max_backup_songs,))
            return [dict(row) for row in cursor.fetchall()]
    
    def backup_song(self, song: dict) -> bool:
        """Backup a single song to fallback storage"""
        try:
            source_path = song['filepath']
            if not os.path.exists(source_path):
                logger.warning(f"Source file not found: {source_path}")
                return False
            
            # Create fallback path
            filename = os.path.basename(source_path)
            fallback_filepath = os.path.join(self.fallback_path, filename)
            
            # Ensure fallback directory exists
            os.makedirs(self.fallback_path, exist_ok=True)
            
            # Copy file
            shutil.copy2(source_path, fallback_filepath)
            
            # Verify copy with checksum
            source_checksum = self.get_file_checksum(source_path)
            backup_checksum = self.get_file_checksum(fallback_filepath)
            
            if source_checksum != backup_checksum:
                logger.error(f"Checksum mismatch for {filename}")
                os.remove(fallback_filepath)
                return False
            
            # Update database
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    UPDATE songs 
                    SET storage_location = 'both',
                        fallback_path = ?,
                        is_backup_synced = TRUE,
                        backup_date = CURRENT_TIMESTAMP,
                        checksum = ?
                    WHERE id = ?
                """, (fallback_filepath, source_checksum, song['id']))
                
                # Log backup action
                conn.execute("""
                    INSERT INTO backup_sync_log 
                    (song_id, action, source_path, destination_path, file_size, checksum)
                    VALUES (?, 'backup_created', ?, ?, ?, ?)
                """, (song['id'], source_path, fallback_filepath, 
                     os.path.getsize(fallback_filepath), source_checksum))
                
                conn.commit()
            
            logger.info(f"Successfully backed up: {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error backing up song {song.get('filename', 'unknown')}: {e}")
            
            # Log failed backup
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO backup_sync_log 
                    (song_id, action, source_path, error_message)
                    VALUES (?, 'backup_failed', ?, ?)
                """, (song['id'], song['filepath'], str(e)))
                conn.commit()
            
            return False
    
    def sync_backup_storage(self) -> dict:
        """Sync songs to backup storage, prioritizing most played"""
        results = {
            'backed_up': 0,
            'failed': 0,
            'skipped': 0,
            'total_backup_songs': 0
        }
        
        try:
            # Check available space on fallback storage
            fallback_stat = shutil.disk_usage(self.fallback_path)
            available_gb = fallback_stat.free / (1024**3)
            
            if available_gb < 1:  # Less than 1GB free
                logger.warning("Low space on fallback storage, skipping backup")
                return results
            
            # Get current backup count
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT COUNT(*) as count FROM songs 
                    WHERE storage_location IN ('fallback', 'both')
                """)
                results['total_backup_songs'] = cursor.fetchone()[0]
            
            # If we're at max capacity, remove least played songs first
            if results['total_backup_songs'] >= self.max_backup_songs:
                self._cleanup_old_backups()
            
            # Get songs that need backup (prioritize most played)
            backup_candidates = self.get_backup_candidates()
            
            for song in backup_candidates:
                if results['total_backup_songs'] >= self.max_backup_songs:
                    results['skipped'] += 1
                    continue
                
                if song['storage_location'] == 'both':
                    continue  # Already backed up
                
                if self.backup_song(song):
                    results['backed_up'] += 1
                    results['total_backup_songs'] += 1
                else:
                    results['failed'] += 1
            
            logger.info(f"Backup sync completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error during backup sync: {e}")
            return results
    
    def _cleanup_old_backups(self):
        """Remove least played backup songs to make space"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get least played backup songs
                cursor = conn.execute("""
                    SELECT * FROM songs 
                    WHERE storage_location = 'both'
                    ORDER BY play_count ASC, last_played ASC
                    LIMIT 10
                """)
                
                songs_to_remove = cursor.fetchall()
                
                for song in songs_to_remove:
                    fallback_path = song['fallback_path']
                    if fallback_path and os.path.exists(fallback_path):
                        os.remove(fallback_path)
                    
                    # Update database
                    conn.execute("""
                        UPDATE songs 
                        SET storage_location = 'primary',
                            fallback_path = NULL,
                            is_backup_synced = FALSE,
                            backup_date = NULL
                        WHERE id = ?
                    """, (song['id'],))
                    
                    # Log removal
                    conn.execute("""
                        INSERT INTO backup_sync_log 
                        (song_id, action, destination_path)
                        VALUES (?, 'backup_removed', ?)
                    """, (song['id'], fallback_path))
                
                conn.commit()
                logger.info(f"Cleaned up {len(songs_to_remove)} old backup songs")
                
        except Exception as e:
            logger.error(f"Error cleaning up old backups: {e}")
    
    def verify_backup_integrity(self) -> dict:
        """Verify integrity of backup songs"""
        results = {'verified': 0, 'corrupted': 0, 'missing': 0}
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM songs 
                    WHERE storage_location IN ('fallback', 'both')
                    AND fallback_path IS NOT NULL
                """)
                
                backup_songs = cursor.fetchall()
                
                for song in backup_songs:
                    fallback_path = song['fallback_path']
                    
                    if not os.path.exists(fallback_path):
                        results['missing'] += 1
                        # Update database to reflect missing backup
                        conn.execute("""
                            UPDATE songs 
                            SET storage_location = 'primary',
                                is_backup_synced = FALSE,
                                fallback_path = NULL
                            WHERE id = ?
                        """, (song['id'],))
                        continue
                    
                    # Verify checksum if available
                    if song['checksum']:
                        current_checksum = self.get_file_checksum(fallback_path)
                        if current_checksum != song['checksum']:
                            results['corrupted'] += 1
                            logger.warning(f"Corrupted backup detected: {fallback_path}")
                            continue
                    
                    results['verified'] += 1
                
                conn.commit()
            
            logger.info(f"Backup verification completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error verifying backup integrity: {e}")
            return results
    
    def get_backup_status(self) -> dict:
        """Get current backup status and statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT 
                        COUNT(*) as total_songs,
                        SUM(CASE WHEN storage_location = 'primary' THEN 1 ELSE 0 END) as primary_only,
                        SUM(CASE WHEN storage_location = 'fallback' THEN 1 ELSE 0 END) as fallback_only,
                        SUM(CASE WHEN storage_location = 'both' THEN 1 ELSE 0 END) as both_locations
                    FROM songs WHERE is_available = TRUE
                """)
                
                stats = dict(cursor.fetchone())
                
                # Get storage space info
                try:
                    primary_stat = shutil.disk_usage(self.primary_path)
                    fallback_stat = shutil.disk_usage(self.fallback_path)
                    
                    stats['primary_storage'] = {
                        'total_gb': primary_stat.total / (1024**3),
                        'used_gb': (primary_stat.total - primary_stat.free) / (1024**3),
                        'free_gb': primary_stat.free / (1024**3)
                    }
                    
                    stats['fallback_storage'] = {
                        'total_gb': fallback_stat.total / (1024**3),
                        'used_gb': (fallback_stat.total - fallback_stat.free) / (1024**3),
                        'free_gb': fallback_stat.free / (1024**3)
                    }
                except:
                    stats['primary_storage'] = None
                    stats['fallback_storage'] = None
                
                return stats
                
        except Exception as e:
            logger.error(f"Error getting backup status: {e}")
            return {}