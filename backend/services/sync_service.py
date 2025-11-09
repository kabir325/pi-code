import os
import shutil
import sqlite3
import logging
import hashlib
import threading
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from mutagen import File
from mutagen.id3 import ID3NoHeaderError

from config import Config

logger = logging.getLogger(__name__)

class SyncService:
    """Enhanced sync service for processing uploaded files and managing music library"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or Config.DATABASE_PATH
        self.unsynced_folder = Config.UNSYNCED_FOLDER
        self.synced_folder = Config.SYNCED_FOLDER
        self.allowed_extensions = Config.ALLOWED_EXTENSIONS
        
        # Ensure directories exist
        os.makedirs(self.unsynced_folder, exist_ok=True)
        os.makedirs(self.synced_folder, exist_ok=True)
        
        # Initialize database
        self._init_database()
    
    def _init_database(self):
        """Initialize database tables if they don't exist"""
        with sqlite3.connect(self.db_path) as conn:
            # Enhanced songs table
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
            
            # Sync log table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS sync_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    action TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    sync_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    file_size INTEGER,
                    duration INTEGER
                )
            ''')
            
            conn.commit()
    
    def get_song_metadata(self, file_path: str) -> Optional[Dict]:
        """Extract comprehensive metadata from audio file using mutagen"""
        try:
            # Try different approaches for different file types
            audio = None
            
            # First try with easy=True for basic metadata
            try:
                audio = File(file_path, easy=True)
            except ID3NoHeaderError:
                # For files without ID3 headers, try without easy mode
                audio = File(file_path, easy=False)
            except Exception:
                # Last resort - try without easy mode
                try:
                    audio = File(file_path, easy=False)
                except Exception:
                    pass
            
            if audio is None:
                logger.warning(f"Could not read audio file: {file_path}")
                return None
            
            # Extract basic metadata with multiple fallback strategies
            title = self._extract_metadata_field(audio, ['title', 'TIT2'], os.path.splitext(os.path.basename(file_path))[0])
            artist = self._extract_metadata_field(audio, ['artist', 'TPE1'], 'Unknown Artist')
            album = self._extract_metadata_field(audio, ['album', 'TALB'], 'Unknown Album')
            genre = self._extract_metadata_field(audio, ['genre', 'TCON'], 'Unknown')
            year = self._extract_metadata_field(audio, ['date', 'year', 'TDRC', 'TYER'], '')
            
            # Get technical info
            duration = 0
            bitrate = 0
            sample_rate = 0
            channels = 0
            
            if audio.info:
                duration = int(getattr(audio.info, 'length', 0))
                bitrate = getattr(audio.info, 'bitrate', 0)
                sample_rate = getattr(audio.info, 'sample_rate', 0)
                channels = getattr(audio.info, 'channels', 0)
            
            # Get file format and codec info
            file_format = os.path.splitext(file_path)[1][1:].lower()
            codec = self._detect_codec(audio, file_format)
            
            # Calculate audio quality score
            quality_score = self._calculate_quality_score(bitrate, sample_rate, file_format)
            
            return {
                'title': title,
                'artist': artist,
                'album': album,
                'genre': genre,
                'year': year,
                'duration': duration,
                'bitrate': bitrate,
                'sample_rate': sample_rate,
                'channels': channels,
                'format': file_format,
                'codec': codec,
                'quality_score': quality_score
            }
            
        except Exception as e:
            logger.error(f"Error reading metadata for {file_path}: {e}")
            return None
    
    def _extract_metadata_field(self, audio, field_names: List[str], default: str) -> str:
        """Extract metadata field with multiple fallback options"""
        for field_name in field_names:
            try:
                if hasattr(audio, 'get') and audio.get(field_name):
                    value = audio.get(field_name)
                    if isinstance(value, list) and value:
                        return str(value[0]).strip()
                    elif value:
                        return str(value).strip()
                
                # Try direct attribute access for non-easy mode
                if hasattr(audio, field_name):
                    value = getattr(audio, field_name)
                    if value:
                        return str(value).strip()
                
                # Try tags dictionary
                if hasattr(audio, 'tags') and audio.tags:
                    if field_name in audio.tags:
                        value = audio.tags[field_name]
                        if isinstance(value, list) and value:
                            return str(value[0]).strip()
                        elif value:
                            return str(value).strip()
                            
            except Exception:
                continue
        
        return default
    
    def _detect_codec(self, audio, file_format: str) -> str:
        """Detect audio codec"""
        try:
            if hasattr(audio, 'info') and hasattr(audio.info, 'codec'):
                return audio.info.codec
            
            # Fallback based on file format
            codec_map = {
                'mp3': 'MP3',
                'flac': 'FLAC',
                'wav': 'PCM',
                'm4a': 'AAC',
                'aac': 'AAC',
                'ogg': 'Vorbis',
                'wma': 'WMA'
            }
            return codec_map.get(file_format.lower(), file_format.upper())
            
        except Exception:
            return file_format.upper()
    
    def _calculate_quality_score(self, bitrate: int, sample_rate: int, file_format: str) -> int:
        """Calculate audio quality score (0-100)"""
        try:
            score = 0
            
            # Bitrate scoring (40% of total score)
            if bitrate >= 320:
                score += 40
            elif bitrate >= 256:
                score += 35
            elif bitrate >= 192:
                score += 30
            elif bitrate >= 128:
                score += 25
            elif bitrate >= 96:
                score += 15
            else:
                score += 5
            
            # Sample rate scoring (30% of total score)
            if sample_rate >= 48000:
                score += 30
            elif sample_rate >= 44100:
                score += 25
            elif sample_rate >= 22050:
                score += 15
            else:
                score += 5
            
            # Format scoring (30% of total score)
            format_scores = {
                'flac': 30,
                'wav': 30,
                'mp3': 20,
                'm4a': 20,
                'aac': 20,
                'ogg': 15,
                'wma': 10
            }
            score += format_scores.get(file_format.lower(), 5)
            
            return min(100, max(0, score))
            
        except Exception:
            return 50  # Default medium quality
    
    def is_duplicate_song(self, filename: str, file_size: int, checksum: str = None) -> bool:
        """Check if song already exists in database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check by filename first
                cursor = conn.execute('''
                    SELECT id FROM songs WHERE filename = ?
                ''', (filename,))
                
                if cursor.fetchone():
                    return True
                
                # Check by file size and checksum if available
                if checksum:
                    cursor = conn.execute('''
                        SELECT id FROM songs 
                        WHERE file_size = ? AND checksum = ?
                    ''', (file_size, checksum))
                    
                    if cursor.fetchone():
                        return True
                
                return False
                
        except Exception as e:
            logger.error(f"Error checking for duplicate: {e}")
            return False
    
    def sync_new_files(self) -> Dict:
        """Sync all new files from unsynced to synced folder"""
        results = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'duplicates': 0,
            'errors': []
        }
        
        try:
            # Get all files in unsynced folder
            if not os.path.exists(self.unsynced_folder):
                logger.warning(f"Unsynced folder does not exist: {self.unsynced_folder}")
                return results
            
            files = [f for f in os.listdir(self.unsynced_folder) 
                    if os.path.isfile(os.path.join(self.unsynced_folder, f))]
            
            if not files:
                logger.info("No new files to sync")
                return results
            
            logger.info(f"Found {len(files)} files to sync")
            
            for filename in files:
                results['processed'] += 1
                
                try:
                    if self._sync_single_file(filename):
                        results['successful'] += 1
                    else:
                        results['failed'] += 1
                        
                except Exception as e:
                    error_msg = f"Error syncing {filename}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
                    results['failed'] += 1
            
            logger.info(f"Sync completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error during sync process: {e}")
            results['errors'].append(str(e))
            return results
    
    def _sync_single_file(self, filename: str) -> bool:
        """Sync a single file from unsynced to synced folder with enhanced processing"""
        src_path = os.path.join(self.unsynced_folder, filename)
        dest_path = os.path.join(self.synced_folder, filename)
        
        try:
            # Validate file exists and is accessible
            if not os.path.exists(src_path):
                self._log_sync_action(filename, 'skip', 'failed', 'Source file not found')
                return False
            
            # Check if file is valid audio format
            file_ext = os.path.splitext(filename)[1][1:].lower()
            if file_ext not in self.allowed_extensions:
                self._log_sync_action(filename, 'skip', 'failed', 
                                    f'Invalid file format: {file_ext}')
                # Move invalid files to a quarantine folder
                self._quarantine_file(src_path, f'Invalid format: {file_ext}')
                return False
            
            # Get file info
            file_size = os.path.getsize(src_path)
            
            # Validate file size (not empty, not too large)
            if file_size == 0:
                self._log_sync_action(filename, 'skip', 'failed', 'Empty file')
                self._quarantine_file(src_path, 'Empty file')
                return False
            
            if file_size > Config.MAX_UPLOAD_SIZE:
                self._log_sync_action(filename, 'skip', 'failed', 'File too large')
                self._quarantine_file(src_path, 'File too large')
                return False
            
            # Calculate checksum for duplicate detection and integrity
            checksum = self._calculate_checksum(src_path)
            if not checksum:
                self._log_sync_action(filename, 'skip', 'failed', 'Could not calculate checksum')
                return False
            
            # Check for duplicates
            if self.is_duplicate_song(filename, file_size, checksum):
                logger.info(f"Duplicate file skipped: {filename}")
                self._log_sync_action(filename, 'skip', 'duplicate', 
                                    'File already exists in library')
                # Remove the duplicate file
                os.remove(src_path)
                return True
            
            # Extract and validate metadata
            metadata = self.get_song_metadata(src_path)
            if not metadata:
                self._log_sync_action(filename, 'process', 'failed', 
                                    'Could not extract metadata')
                self._quarantine_file(src_path, 'Metadata extraction failed')
                return False
            
            # Validate audio file integrity
            if not self._validate_audio_integrity(src_path, metadata):
                self._log_sync_action(filename, 'process', 'failed', 
                                    'Audio file integrity check failed')
                self._quarantine_file(src_path, 'Integrity check failed')
                return False
            
            # Generate optimized filename if needed
            optimized_filename = self._generate_optimized_filename(filename, metadata)
            final_dest_path = os.path.join(self.synced_folder, optimized_filename)
            
            # Handle filename conflicts
            counter = 1
            base_name, ext = os.path.splitext(final_dest_path)
            while os.path.exists(final_dest_path):
                final_dest_path = f"{base_name}_{counter}{ext}"
                counter += 1
            
            final_filename = os.path.basename(final_dest_path)
            
            # Move file to synced folder
            shutil.move(src_path, final_dest_path)
            
            # Verify file was moved successfully
            if not os.path.exists(final_dest_path):
                self._log_sync_action(filename, 'sync', 'failed', 'File move failed')
                return False
            
            # Verify file integrity after move
            moved_checksum = self._calculate_checksum(final_dest_path)
            if moved_checksum != checksum:
                logger.error(f"Checksum mismatch after move for {filename}")
                os.remove(final_dest_path)  # Remove corrupted file
                self._log_sync_action(filename, 'sync', 'failed', 'Checksum mismatch after move')
                return False
            
            # Add to database with enhanced metadata
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO songs 
                    (filename, filepath, title, artist, album, genre, duration, 
                     file_size, format, bitrate, sample_rate, channels, codec,
                     storage_location, primary_path, checksum, quality_score)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'primary', ?, ?, ?)
                ''', (
                    final_filename, final_dest_path, metadata['title'],
                    metadata['artist'], metadata['album'], metadata.get('genre', ''),
                    metadata['duration'], file_size, metadata['format'], 
                    metadata['bitrate'], metadata.get('sample_rate', 0),
                    metadata.get('channels', 0), metadata.get('codec', ''),
                    final_dest_path, checksum, metadata.get('quality_score', 50)
                ))
                conn.commit()
            
            # Log successful sync with detailed info
            self._log_sync_action(final_filename, 'sync', 'success', 
                                None, file_size, metadata['duration'])
            
            logger.info(f"Successfully synced: {final_filename} "
                       f"({metadata['format']}, {metadata['bitrate']}kbps, "
                       f"{metadata['duration']}s, quality: {metadata.get('quality_score', 50)})")
            
            return True
            
        except Exception as e:
            logger.error(f"Error syncing {filename}: {e}")
            self._log_sync_action(filename, 'sync', 'failed', str(e))
            # Try to quarantine the problematic file
            try:
                self._quarantine_file(src_path, f'Sync error: {str(e)}')
            except:
                pass
            return False
    
    def _validate_audio_integrity(self, file_path: str, metadata: Dict) -> bool:
        """Validate audio file integrity"""
        try:
            # Basic validation - file should have duration
            if metadata.get('duration', 0) <= 0:
                return False
            
            # Try to read a small portion of the file to ensure it's not corrupted
            try:
                audio = File(file_path)
                if audio is None:
                    return False
                
                # Additional format-specific validation
                file_format = metadata.get('format', '').lower()
                if file_format == 'mp3':
                    # For MP3, check if we can read the header
                    if not hasattr(audio.info, 'bitrate') or audio.info.bitrate <= 0:
                        return False
                elif file_format == 'flac':
                    # For FLAC, check if it's a valid FLAC file
                    if not hasattr(audio.info, 'total_samples'):
                        return False
                
                return True
                
            except Exception:
                return False
                
        except Exception as e:
            logger.error(f"Error validating audio integrity: {e}")
            return False
    
    def _generate_optimized_filename(self, original_filename: str, metadata: Dict) -> str:
        """Generate optimized filename based on metadata"""
        try:
            # Keep original filename if it's already well-formatted
            if self._is_filename_well_formatted(original_filename):
                return original_filename
            
            # Generate new filename: Artist - Title.ext
            artist = metadata.get('artist', 'Unknown Artist')
            title = metadata.get('title', 'Unknown Title')
            
            # Clean up artist and title for filename
            artist = self._sanitize_filename_part(artist)
            title = self._sanitize_filename_part(title)
            
            # Get file extension
            _, ext = os.path.splitext(original_filename)
            
            # Generate new filename
            new_filename = f"{artist} - {title}{ext}"
            
            # Ensure filename isn't too long
            if len(new_filename) > 200:
                # Truncate while keeping extension
                max_length = 200 - len(ext)
                truncated = new_filename[:max_length]
                new_filename = truncated + ext
            
            return new_filename
            
        except Exception:
            # If anything goes wrong, return original filename
            return original_filename
    
    def _is_filename_well_formatted(self, filename: str) -> bool:
        """Check if filename is already well-formatted"""
        # Remove extension for checking
        name_without_ext = os.path.splitext(filename)[0]
        
        # Check if it contains artist - title pattern
        if ' - ' in name_without_ext:
            parts = name_without_ext.split(' - ', 1)
            if len(parts) == 2 and len(parts[0].strip()) > 0 and len(parts[1].strip()) > 0:
                return True
        
        return False
    
    def _sanitize_filename_part(self, text: str) -> str:
        """Sanitize text for use in filename"""
        # Remove or replace invalid filename characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            text = text.replace(char, '')
        
        # Replace multiple spaces with single space
        text = ' '.join(text.split())
        
        # Trim and limit length
        text = text.strip()[:50]
        
        return text if text else 'Unknown'
    
    def _quarantine_file(self, file_path: str, reason: str):
        """Move problematic file to quarantine folder"""
        try:
            quarantine_dir = os.path.join(os.path.dirname(self.unsynced_folder), 'quarantine')
            os.makedirs(quarantine_dir, exist_ok=True)
            
            filename = os.path.basename(file_path)
            quarantine_path = os.path.join(quarantine_dir, filename)
            
            # Handle filename conflicts in quarantine
            counter = 1
            base_name, ext = os.path.splitext(quarantine_path)
            while os.path.exists(quarantine_path):
                quarantine_path = f"{base_name}_{counter}{ext}"
                counter += 1
            
            shutil.move(file_path, quarantine_path)
            
            # Log quarantine action
            logger.warning(f"File quarantined: {filename} -> {quarantine_path} (Reason: {reason})")
            
            # Create a reason file
            reason_file = quarantine_path + '.reason'
            with open(reason_file, 'w') as f:
                f.write(f"Quarantined at: {datetime.now().isoformat()}\n")
                f.write(f"Reason: {reason}\n")
                f.write(f"Original path: {file_path}\n")
                
        except Exception as e:
            logger.error(f"Error quarantining file {file_path}: {e}")
            # If quarantine fails, just remove the file
            try:
                os.remove(file_path)
            except:
                pass
    
    def _calculate_checksum(self, file_path: str) -> str:
        """Calculate MD5 checksum of a file"""
        import hashlib
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating checksum: {e}")
            return ""
    
    def _log_sync_action(self, filename: str, action: str, status: str, 
                        error_message: str = None, file_size: int = None, 
                        duration: int = None):
        """Log sync action to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO sync_log 
                    (filename, action, status, error_message, file_size, duration)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (filename, action, status, error_message, file_size, duration))
                conn.commit()
        except Exception as e:
            logger.error(f"Error logging sync action: {e}")
    
    def get_sync_history(self, limit: int = 50) -> List[Dict]:
        """Get recent sync history"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute('''
                    SELECT * FROM sync_log 
                    ORDER BY sync_date DESC 
                    LIMIT ?
                ''', (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting sync history: {e}")
            return []
    
    def get_sync_stats(self) -> Dict:
        """Get sync statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get total counts
                cursor = conn.execute('''
                    SELECT 
                        COUNT(*) as total_syncs,
                        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                        SUM(CASE WHEN status = 'duplicate' THEN 1 ELSE 0 END) as duplicates
                    FROM sync_log
                ''')
                stats = dict(cursor.fetchone())
                
                # Get recent activity (last 24 hours)
                cursor = conn.execute('''
                    SELECT COUNT(*) as recent_syncs
                    FROM sync_log 
                    WHERE sync_date > datetime('now', '-1 day')
                ''')
                stats['recent_syncs'] = cursor.fetchone()[0]
                
                return stats
                
        except Exception as e:
            logger.error(f"Error getting sync stats: {e}")
            return {}
    
    def sync_with_progress_callback(self, progress_callback=None) -> Dict:
        """Sync files with progress callback for real-time updates"""
        results = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'duplicates': 0,
            'quarantined': 0,
            'errors': [],
            'processed_files': []
        }
        
        try:
            # Get all files in unsynced folder
            if not os.path.exists(self.unsynced_folder):
                logger.warning(f"Unsynced folder does not exist: {self.unsynced_folder}")
                return results
            
            files = [f for f in os.listdir(self.unsynced_folder) 
                    if os.path.isfile(os.path.join(self.unsynced_folder, f))]
            
            if not files:
                logger.info("No new files to sync")
                return results
            
            total_files = len(files)
            logger.info(f"Found {total_files} files to sync")
            
            for i, filename in enumerate(files):
                results['processed'] += 1
                
                # Call progress callback if provided
                if progress_callback:
                    progress_callback(i + 1, total_files, filename)
                
                try:
                    sync_result = self._sync_single_file_with_details(filename)
                    results['processed_files'].append(sync_result)
                    
                    if sync_result['status'] == 'success':
                        results['successful'] += 1
                    elif sync_result['status'] == 'duplicate':
                        results['duplicates'] += 1
                    elif sync_result['status'] == 'quarantined':
                        results['quarantined'] += 1
                    else:
                        results['failed'] += 1
                        
                except Exception as e:
                    error_msg = f"Error syncing {filename}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
                    results['failed'] += 1
                    results['processed_files'].append({
                        'filename': filename,
                        'status': 'error',
                        'error': str(e)
                    })
            
            logger.info(f"Sync completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error during sync process: {e}")
            results['errors'].append(str(e))
            return results
    
    def _sync_single_file_with_details(self, filename: str) -> Dict:
        """Sync single file and return detailed results"""
        result = {
            'filename': filename,
            'status': 'failed',
            'message': '',
            'metadata': {},
            'file_size': 0,
            'processing_time': 0
        }
        
        start_time = time.time()
        src_path = os.path.join(self.unsynced_folder, filename)
        
        try:
            # Get file size
            result['file_size'] = os.path.getsize(src_path)
            
            # Check if file is valid audio format
            file_ext = os.path.splitext(filename)[1][1:].lower()
            if file_ext not in self.allowed_extensions:
                result['status'] = 'quarantined'
                result['message'] = f'Invalid file format: {file_ext}'
                self._quarantine_file(src_path, result['message'])
                return result
            
            # Calculate checksum
            checksum = self._calculate_checksum(src_path)
            
            # Check for duplicates
            if self.is_duplicate_song(filename, result['file_size'], checksum):
                result['status'] = 'duplicate'
                result['message'] = 'File already exists in library'
                os.remove(src_path)
                return result
            
            # Extract metadata
            metadata = self.get_song_metadata(src_path)
            if not metadata:
                result['status'] = 'quarantined'
                result['message'] = 'Could not extract metadata'
                self._quarantine_file(src_path, result['message'])
                return result
            
            result['metadata'] = metadata
            
            # Validate audio integrity
            if not self._validate_audio_integrity(src_path, metadata):
                result['status'] = 'quarantined'
                result['message'] = 'Audio file integrity check failed'
                self._quarantine_file(src_path, result['message'])
                return result
            
            # Process the file
            if self._sync_single_file(filename):
                result['status'] = 'success'
                result['message'] = 'Successfully synced'
            else:
                result['status'] = 'failed'
                result['message'] = 'Sync process failed'
            
        except Exception as e:
            result['status'] = 'error'
            result['message'] = str(e)
        finally:
            result['processing_time'] = round(time.time() - start_time, 2)
        
        return result
    
    def get_quarantine_files(self) -> List[Dict]:
        """Get list of quarantined files with reasons"""
        quarantine_files = []
        try:
            quarantine_dir = os.path.join(os.path.dirname(self.unsynced_folder), 'quarantine')
            if not os.path.exists(quarantine_dir):
                return quarantine_files
            
            for filename in os.listdir(quarantine_dir):
                if filename.endswith('.reason'):
                    continue
                
                file_path = os.path.join(quarantine_dir, filename)
                reason_file = file_path + '.reason'
                
                file_info = {
                    'filename': filename,
                    'file_path': file_path,
                    'file_size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                    'quarantined_at': datetime.fromtimestamp(os.path.getctime(file_path)).isoformat(),
                    'reason': 'Unknown reason'
                }
                
                # Read reason if available
                if os.path.exists(reason_file):
                    try:
                        with open(reason_file, 'r') as f:
                            content = f.read()
                            for line in content.split('\n'):
                                if line.startswith('Reason:'):
                                    file_info['reason'] = line.replace('Reason:', '').strip()
                                    break
                    except Exception:
                        pass
                
                quarantine_files.append(file_info)
            
            return sorted(quarantine_files, key=lambda x: x['quarantined_at'], reverse=True)
            
        except Exception as e:
            logger.error(f"Error getting quarantine files: {e}")
            return quarantine_files
    
    def restore_quarantine_file(self, filename: str) -> Dict:
        """Restore a file from quarantine back to unsynced folder"""
        try:
            quarantine_dir = os.path.join(os.path.dirname(self.unsynced_folder), 'quarantine')
            quarantine_path = os.path.join(quarantine_dir, filename)
            unsynced_path = os.path.join(self.unsynced_folder, filename)
            
            if not os.path.exists(quarantine_path):
                return {'success': False, 'error': 'File not found in quarantine'}
            
            # Handle filename conflicts
            counter = 1
            base_name, ext = os.path.splitext(unsynced_path)
            while os.path.exists(unsynced_path):
                unsynced_path = f"{base_name}_{counter}{ext}"
                counter += 1
            
            # Move file back
            shutil.move(quarantine_path, unsynced_path)
            
            # Remove reason file
            reason_file = quarantine_path + '.reason'
            if os.path.exists(reason_file):
                os.remove(reason_file)
            
            logger.info(f"Restored quarantine file: {filename}")
            return {'success': True, 'message': f'File restored to {os.path.basename(unsynced_path)}'}
            
        except Exception as e:
            logger.error(f"Error restoring quarantine file: {e}")
            return {'success': False, 'error': str(e)}
    
    def delete_quarantine_file(self, filename: str) -> Dict:
        """Permanently delete a quarantined file"""
        try:
            quarantine_dir = os.path.join(os.path.dirname(self.unsynced_folder), 'quarantine')
            quarantine_path = os.path.join(quarantine_dir, filename)
            reason_file = quarantine_path + '.reason'
            
            if not os.path.exists(quarantine_path):
                return {'success': False, 'error': 'File not found in quarantine'}
            
            # Delete file and reason file
            os.remove(quarantine_path)
            if os.path.exists(reason_file):
                os.remove(reason_file)
            
            logger.info(f"Deleted quarantine file: {filename}")
            return {'success': True, 'message': 'File permanently deleted'}
            
        except Exception as e:
            logger.error(f"Error deleting quarantine file: {e}")
            return {'success': False, 'error': str(e)}
    
    def cleanup_failed_files(self) -> int:
        """Clean up files that failed to sync"""
        cleaned_count = 0
        try:
            if not os.path.exists(self.unsynced_folder):
                return cleaned_count
            
            files = os.listdir(self.unsynced_folder)
            for filename in files:
                file_path = os.path.join(self.unsynced_folder, filename)
                
                # Check if file has been stuck for more than 1 hour
                file_age = datetime.now().timestamp() - os.path.getmtime(file_path)
                if file_age > 3600:  # 1 hour
                    try:
                        # Move to quarantine instead of deleting
                        self._quarantine_file(file_path, 'Stuck file - cleanup')
                        cleaned_count += 1
                        logger.info(f"Quarantined stuck file: {filename}")
                    except Exception as e:
                        logger.error(f"Error cleaning up {filename}: {e}")
            
            return cleaned_count
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return cleaned_count
    
    def get_detailed_sync_stats(self) -> Dict:
        """Get detailed sync statistics with quality analysis"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                # Basic stats
                cursor = conn.execute('''
                    SELECT 
                        COUNT(*) as total_syncs,
                        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                        SUM(CASE WHEN status = 'duplicate' THEN 1 ELSE 0 END) as duplicates,
                        AVG(file_size) as avg_file_size,
                        AVG(duration) as avg_duration
                    FROM sync_log
                ''')
                basic_stats = dict(cursor.fetchone())
                
                # Format distribution
                cursor = conn.execute('''
                    SELECT format, COUNT(*) as count, AVG(quality_score) as avg_quality
                    FROM songs 
                    GROUP BY format 
                    ORDER BY count DESC
                ''')
                format_stats = [dict(row) for row in cursor.fetchall()]
                
                # Quality distribution
                cursor = conn.execute('''
                    SELECT 
                        CASE 
                            WHEN quality_score >= 80 THEN 'High'
                            WHEN quality_score >= 60 THEN 'Medium'
                            ELSE 'Low'
                        END as quality_tier,
                        COUNT(*) as count
                    FROM songs 
                    GROUP BY quality_tier
                ''')
                quality_stats = [dict(row) for row in cursor.fetchall()]
                
                # Recent activity (last 24 hours)
                cursor = conn.execute('''
                    SELECT COUNT(*) as recent_syncs
                    FROM sync_log 
                    WHERE sync_date > datetime('now', '-1 day')
                ''')
                basic_stats['recent_syncs'] = cursor.fetchone()[0]
                
                # Quarantine stats
                quarantine_files = self.get_quarantine_files()
                quarantine_stats = {
                    'total_quarantined': len(quarantine_files),
                    'quarantine_reasons': {}
                }
                
                for qfile in quarantine_files:
                    reason = qfile['reason']
                    quarantine_stats['quarantine_reasons'][reason] = quarantine_stats['quarantine_reasons'].get(reason, 0) + 1
                
                return {
                    'basic_stats': basic_stats,
                    'format_distribution': format_stats,
                    'quality_distribution': quality_stats,
                    'quarantine_stats': quarantine_stats
                }
                
        except Exception as e:
            logger.error(f"Error getting detailed sync stats: {e}")
            return {}
    
    def optimize_library(self) -> Dict:
        """Optimize music library by identifying and handling issues"""
        results = {
            'scanned_files': 0,
            'missing_files': 0,
            'corrupted_files': 0,
            'low_quality_files': 0,
            'actions_taken': []
        }
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute('SELECT * FROM songs WHERE is_available = TRUE')
                songs = cursor.fetchall()
            
            for song in songs:
                results['scanned_files'] += 1
                song_dict = dict(song)
                
                # Check if file exists
                if not os.path.exists(song_dict['filepath']):
                    results['missing_files'] += 1
                    # Mark as unavailable
                    with sqlite3.connect(self.db_path) as conn:
                        conn.execute('UPDATE songs SET is_available = FALSE WHERE id = ?', (song_dict['id'],))
                        conn.commit()
                    results['actions_taken'].append(f"Marked missing file as unavailable: {song_dict['filename']}")
                    continue
                
                # Check file integrity
                current_checksum = self._calculate_checksum(song_dict['filepath'])
                if current_checksum != song_dict.get('checksum', ''):
                    results['corrupted_files'] += 1
                    results['actions_taken'].append(f"Detected corrupted file: {song_dict['filename']}")
                
                # Check quality
                if song_dict.get('quality_score', 50) < 30:
                    results['low_quality_files'] += 1
                    results['actions_taken'].append(f"Identified low quality file: {song_dict['filename']}")
            
            logger.info(f"Library optimization completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error during library optimization: {e}")
            results['error'] = str(e)
            return results