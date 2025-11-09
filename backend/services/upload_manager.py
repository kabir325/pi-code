import os
import uuid
import hashlib
import sqlite3
import logging
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename

from config import Config

logger = logging.getLogger(__name__)

class UploadManager:
    """Manages file uploads with chunked upload support and progress tracking"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or Config.DATABASE_PATH
        self.unsynced_folder = Config.UNSYNCED_FOLDER
        self.allowed_extensions = Config.ALLOWED_EXTENSIONS
        self.max_file_size = Config.MAX_UPLOAD_SIZE
        self.chunk_size = Config.UPLOAD_CHUNK_SIZE
        
        # Ensure upload directory exists
        os.makedirs(self.unsynced_folder, exist_ok=True)
        
        # Initialize upload sessions table
        self._init_upload_tables()
    
    def _init_upload_tables(self):
        """Initialize upload-related database tables"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS upload_sessions (
                    id TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    original_filename TEXT NOT NULL,
                    file_size INTEGER,
                    bytes_uploaded INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT,
                    checksum TEXT,
                    temp_file_path TEXT
                )
            ''')
            conn.commit()
    
    def is_allowed_file(self, filename: str) -> bool:
        """Check if file extension is allowed"""
        return '.' in filename and \
               filename.rsplit('.', 1)[1].lower() in self.allowed_extensions
    
    def validate_file_size(self, file_size: int) -> bool:
        """Validate file size against maximum allowed"""
        return file_size <= self.max_file_size
    
    def create_upload_session(self, filename: str, file_size: int) -> Dict:
        """Create a new upload session"""
        try:
            # Validate file
            if not self.is_allowed_file(filename):
                return {
                    'success': False,
                    'error': f'File type not allowed. Allowed types: {", ".join(self.allowed_extensions)}'
                }
            
            if not self.validate_file_size(file_size):
                return {
                    'success': False,
                    'error': f'File too large. Maximum size: {self.max_file_size / (1024*1024):.1f}MB'
                }
            
            # Generate session ID and secure filename
            session_id = str(uuid.uuid4())
            secure_name = secure_filename(filename)
            temp_file_path = os.path.join(self.unsynced_folder, f"{session_id}_{secure_name}")
            
            # Create upload session in database
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO upload_sessions 
                    (id, filename, original_filename, file_size, temp_file_path)
                    VALUES (?, ?, ?, ?, ?)
                ''', (session_id, secure_name, filename, file_size, temp_file_path))
                conn.commit()
            
            logger.info(f"Created upload session {session_id} for {filename}")
            
            return {
                'success': True,
                'session_id': session_id,
                'filename': secure_name,
                'chunk_size': self.chunk_size,
                'total_chunks': (file_size + self.chunk_size - 1) // self.chunk_size
            }
            
        except Exception as e:
            logger.error(f"Error creating upload session: {e}")
            return {'success': False, 'error': str(e)}
    
    def upload_chunk(self, session_id: str, chunk_number: int, chunk_data: bytes) -> Dict:
        """Upload a file chunk"""
        try:
            # Get upload session
            session = self.get_upload_session(session_id)
            if not session:
                return {'success': False, 'error': 'Upload session not found'}
            
            if session['status'] not in ['pending', 'uploading']:
                return {'success': False, 'error': f'Upload session is {session["status"]}'}
            
            temp_file_path = session['temp_file_path']
            
            # Write chunk to temporary file
            mode = 'ab' if chunk_number > 0 else 'wb'
            with open(temp_file_path, mode) as f:
                f.write(chunk_data)
            
            # Update progress
            bytes_uploaded = session['bytes_uploaded'] + len(chunk_data)
            progress_percent = (bytes_uploaded / session['file_size']) * 100
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    UPDATE upload_sessions 
                    SET bytes_uploaded = ?, status = 'uploading'
                    WHERE id = ?
                ''', (bytes_uploaded, session_id))
                conn.commit()
            
            logger.debug(f"Uploaded chunk {chunk_number} for session {session_id}")
            
            return {
                'success': True,
                'bytes_uploaded': bytes_uploaded,
                'progress_percent': round(progress_percent, 2),
                'completed': bytes_uploaded >= session['file_size']
            }
            
        except Exception as e:
            logger.error(f"Error uploading chunk: {e}")
            self._mark_upload_failed(session_id, str(e))
            return {'success': False, 'error': str(e)}
    
    def complete_upload(self, session_id: str) -> Dict:
        """Complete the upload and move file to final location"""
        try:
            session = self.get_upload_session(session_id)
            if not session:
                return {'success': False, 'error': 'Upload session not found'}
            
            temp_file_path = session['temp_file_path']
            
            # Verify file size
            if not os.path.exists(temp_file_path):
                return {'success': False, 'error': 'Temporary file not found'}
            
            actual_size = os.path.getsize(temp_file_path)
            if actual_size != session['file_size']:
                return {'success': False, 'error': 'File size mismatch'}
            
            # Calculate checksum
            checksum = self._calculate_checksum(temp_file_path)
            
            # Move to final location
            final_filename = session['filename']
            final_path = os.path.join(self.unsynced_folder, final_filename)
            
            # Handle duplicate filenames
            counter = 1
            base_name, ext = os.path.splitext(final_filename)
            while os.path.exists(final_path):
                final_filename = f"{base_name}_{counter}{ext}"
                final_path = os.path.join(self.unsynced_folder, final_filename)
                counter += 1
            
            os.rename(temp_file_path, final_path)
            
            # Update database
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    UPDATE upload_sessions 
                    SET status = 'completed', 
                        completed_at = CURRENT_TIMESTAMP,
                        checksum = ?,
                        filename = ?
                    WHERE id = ?
                ''', (checksum, final_filename, session_id))
                conn.commit()
            
            logger.info(f"Upload completed: {final_filename}")
            
            return {
                'success': True,
                'filename': final_filename,
                'file_path': final_path,
                'checksum': checksum
            }
            
        except Exception as e:
            logger.error(f"Error completing upload: {e}")
            self._mark_upload_failed(session_id, str(e))
            return {'success': False, 'error': str(e)}
    
    def get_upload_session(self, session_id: str) -> Optional[Dict]:
        """Get upload session details"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute('''
                    SELECT * FROM upload_sessions WHERE id = ?
                ''', (session_id,))
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting upload session: {e}")
            return None
    
    def get_upload_progress(self, session_id: str) -> Dict:
        """Get upload progress information"""
        session = self.get_upload_session(session_id)
        if not session:
            return {'success': False, 'error': 'Upload session not found'}
        
        progress_percent = 0
        if session['file_size'] > 0:
            progress_percent = (session['bytes_uploaded'] / session['file_size']) * 100
        
        # Estimate time remaining
        estimated_time_remaining = None
        if session['status'] == 'uploading' and progress_percent > 0:
            elapsed_time = (datetime.now() - datetime.fromisoformat(session['created_at'])).total_seconds()
            if elapsed_time > 0:
                estimated_total_time = elapsed_time / (progress_percent / 100)
                estimated_time_remaining = max(0, estimated_total_time - elapsed_time)
        
        return {
            'success': True,
            'session_id': session_id,
            'filename': session['filename'],
            'bytes_uploaded': session['bytes_uploaded'],
            'total_bytes': session['file_size'],
            'progress_percent': round(progress_percent, 2),
            'status': session['status'],
            'estimated_time_remaining': estimated_time_remaining,
            'created_at': session['created_at'],
            'completed_at': session['completed_at'],
            'error_message': session['error_message']
        }
    
    def cleanup_expired_sessions(self, hours: int = 24) -> int:
        """Clean up expired upload sessions"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            with sqlite3.connect(self.db_path) as conn:
                # Get expired sessions
                cursor = conn.execute('''
                    SELECT temp_file_path FROM upload_sessions 
                    WHERE created_at < ? AND status IN ('pending', 'uploading', 'failed')
                ''', (cutoff_time.isoformat(),))
                
                expired_files = [row[0] for row in cursor.fetchall()]
                
                # Delete temporary files
                deleted_count = 0
                for file_path in expired_files:
                    if file_path and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                            deleted_count += 1
                        except Exception as e:
                            logger.warning(f"Could not delete expired file {file_path}: {e}")
                
                # Delete database records
                conn.execute('''
                    DELETE FROM upload_sessions 
                    WHERE created_at < ? AND status IN ('pending', 'uploading', 'failed')
                ''', (cutoff_time.isoformat(),))
                
                conn.commit()
                
                logger.info(f"Cleaned up {deleted_count} expired upload sessions")
                return deleted_count
                
        except Exception as e:
            logger.error(f"Error cleaning up expired sessions: {e}")
            return 0
    
    def get_recent_uploads(self, limit: int = 20) -> List[Dict]:
        """Get recent upload sessions"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute('''
                    SELECT * FROM upload_sessions 
                    ORDER BY created_at DESC 
                    LIMIT ?
                ''', (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting recent uploads: {e}")
            return []
    
    def _calculate_checksum(self, file_path: str) -> str:
        """Calculate MD5 checksum of a file"""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating checksum: {e}")
            return ""
    
    def _mark_upload_failed(self, session_id: str, error_message: str):
        """Mark upload session as failed"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    UPDATE upload_sessions 
                    SET status = 'failed', error_message = ?
                    WHERE id = ?
                ''', (error_message, session_id))
                conn.commit()
        except Exception as e:
            logger.error(f"Error marking upload as failed: {e}")
    
    def cancel_upload(self, session_id: str) -> Dict:
        """Cancel an ongoing upload"""
        try:
            session = self.get_upload_session(session_id)
            if not session:
                return {'success': False, 'error': 'Upload session not found'}
            
            # Delete temporary file if exists
            temp_file_path = session['temp_file_path']
            if temp_file_path and os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            
            # Update database
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    UPDATE upload_sessions 
                    SET status = 'cancelled'
                    WHERE id = ?
                ''', (session_id,))
                conn.commit()
            
            logger.info(f"Upload cancelled: {session_id}")
            return {'success': True, 'message': 'Upload cancelled'}
            
        except Exception as e:
            logger.error(f"Error cancelling upload: {e}")
            return {'success': False, 'error': str(e)}