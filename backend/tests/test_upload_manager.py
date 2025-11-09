import pytest
import os
import tempfile
import sqlite3
from unittest.mock import patch, MagicMock

from services.upload_manager import UploadManager
from config import Config

class TestUploadManager:
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary database for testing"""
        db_fd, db_path = tempfile.mkstemp()
        os.close(db_fd)
        yield db_path
        os.unlink(db_path)
    
    @pytest.fixture
    def temp_upload_dir(self):
        """Create a temporary upload directory"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def upload_manager(self, temp_db, temp_upload_dir):
        """Create UploadManager instance with temporary paths"""
        with patch.object(Config, 'DATABASE_PATH', temp_db), \
             patch.object(Config, 'UNSYNCED_FOLDER', temp_upload_dir), \
             patch.object(Config, 'ALLOWED_EXTENSIONS', {'mp3', 'wav', 'flac'}), \
             patch.object(Config, 'MAX_UPLOAD_SIZE', 10 * 1024 * 1024), \
             patch.object(Config, 'UPLOAD_CHUNK_SIZE', 1024):
            
            manager = UploadManager(temp_db)
            yield manager
    
    def test_is_allowed_file(self, upload_manager):
        """Test file extension validation"""
        assert upload_manager.is_allowed_file('song.mp3') == True
        assert upload_manager.is_allowed_file('song.wav') == True
        assert upload_manager.is_allowed_file('song.flac') == True
        assert upload_manager.is_allowed_file('song.txt') == False
        assert upload_manager.is_allowed_file('song') == False
        assert upload_manager.is_allowed_file('song.MP3') == True  # Case insensitive
    
    def test_validate_file_size(self, upload_manager):
        """Test file size validation"""
        assert upload_manager.validate_file_size(1024) == True
        assert upload_manager.validate_file_size(5 * 1024 * 1024) == True
        assert upload_manager.validate_file_size(15 * 1024 * 1024) == False
    
    def test_create_upload_session_success(self, upload_manager):
        """Test successful upload session creation"""
        result = upload_manager.create_upload_session('test.mp3', 1024)
        
        assert result['success'] == True
        assert 'session_id' in result
        assert result['filename'] == 'test.mp3'
        assert result['chunk_size'] == 1024
        assert result['total_chunks'] == 1
    
    def test_create_upload_session_invalid_extension(self, upload_manager):
        """Test upload session creation with invalid file extension"""
        result = upload_manager.create_upload_session('test.txt', 1024)
        
        assert result['success'] == False
        assert 'not allowed' in result['error']
    
    def test_create_upload_session_file_too_large(self, upload_manager):
        """Test upload session creation with file too large"""
        result = upload_manager.create_upload_session('test.mp3', 15 * 1024 * 1024)
        
        assert result['success'] == False
        assert 'too large' in result['error']
    
    def test_upload_chunk_success(self, upload_manager):
        """Test successful chunk upload"""
        # Create upload session
        session_result = upload_manager.create_upload_session('test.mp3', 2048)
        session_id = session_result['session_id']
        
        # Upload first chunk
        chunk_data = b'x' * 1024
        result = upload_manager.upload_chunk(session_id, 0, chunk_data)
        
        assert result['success'] == True
        assert result['bytes_uploaded'] == 1024
        assert result['progress_percent'] == 50.0
        assert result['completed'] == False
        
        # Upload second chunk
        result = upload_manager.upload_chunk(session_id, 1, chunk_data)
        
        assert result['success'] == True
        assert result['bytes_uploaded'] == 2048
        assert result['progress_percent'] == 100.0
        assert result['completed'] == True
    
    def test_upload_chunk_invalid_session(self, upload_manager):
        """Test chunk upload with invalid session ID"""
        result = upload_manager.upload_chunk('invalid-session', 0, b'data')
        
        assert result['success'] == False
        assert 'not found' in result['error']
    
    def test_get_upload_progress(self, upload_manager):
        """Test getting upload progress"""
        # Create session and upload chunk
        session_result = upload_manager.create_upload_session('test.mp3', 2048)
        session_id = session_result['session_id']
        
        upload_manager.upload_chunk(session_id, 0, b'x' * 1024)
        
        # Get progress
        progress = upload_manager.get_upload_progress(session_id)
        
        assert progress['success'] == True
        assert progress['session_id'] == session_id
        assert progress['filename'] == 'test.mp3'
        assert progress['bytes_uploaded'] == 1024
        assert progress['total_bytes'] == 2048
        assert progress['progress_percent'] == 50.0
        assert progress['status'] == 'uploading'
    
    def test_complete_upload_success(self, upload_manager, temp_upload_dir):
        """Test successful upload completion"""
        # Create session
        session_result = upload_manager.create_upload_session('test.mp3', 1024)
        session_id = session_result['session_id']
        
        # Upload chunk
        chunk_data = b'x' * 1024
        upload_manager.upload_chunk(session_id, 0, chunk_data)
        
        # Complete upload
        result = upload_manager.complete_upload(session_id)
        
        assert result['success'] == True
        assert result['filename'] == 'test.mp3'
        assert 'file_path' in result
        assert 'checksum' in result
        
        # Verify file exists
        assert os.path.exists(result['file_path'])
    
    def test_complete_upload_size_mismatch(self, upload_manager):
        """Test upload completion with size mismatch"""
        # Create session for 2048 bytes
        session_result = upload_manager.create_upload_session('test.mp3', 2048)
        session_id = session_result['session_id']
        
        # Upload only 1024 bytes
        upload_manager.upload_chunk(session_id, 0, b'x' * 1024)
        
        # Try to complete upload
        result = upload_manager.complete_upload(session_id)
        
        assert result['success'] == False
        assert 'size mismatch' in result['error']
    
    def test_cancel_upload(self, upload_manager):
        """Test upload cancellation"""
        # Create session and upload chunk
        session_result = upload_manager.create_upload_session('test.mp3', 2048)
        session_id = session_result['session_id']
        
        upload_manager.upload_chunk(session_id, 0, b'x' * 1024)
        
        # Cancel upload
        result = upload_manager.cancel_upload(session_id)
        
        assert result['success'] == True
        assert 'cancelled' in result['message']
        
        # Verify session status
        session = upload_manager.get_upload_session(session_id)
        assert session['status'] == 'cancelled'
    
    def test_cleanup_expired_sessions(self, upload_manager):
        """Test cleanup of expired sessions"""
        # Create a session
        session_result = upload_manager.create_upload_session('test.mp3', 1024)
        session_id = session_result['session_id']
        
        # Mock the session as old
        with sqlite3.connect(upload_manager.db_path) as conn:
            conn.execute('''
                UPDATE upload_sessions 
                SET created_at = datetime('now', '-25 hours')
                WHERE id = ?
            ''', (session_id,))
            conn.commit()
        
        # Run cleanup
        deleted_count = upload_manager.cleanup_expired_sessions(24)
        
        assert deleted_count >= 0  # Should clean up the expired session
        
        # Verify session is gone
        session = upload_manager.get_upload_session(session_id)
        assert session is None
    
    def test_get_recent_uploads(self, upload_manager):
        """Test getting recent uploads"""
        # Create a few upload sessions
        upload_manager.create_upload_session('test1.mp3', 1024)
        upload_manager.create_upload_session('test2.mp3', 2048)
        
        # Get recent uploads
        uploads = upload_manager.get_recent_uploads(10)
        
        assert len(uploads) == 2
        assert uploads[0]['filename'] in ['test1.mp3', 'test2.mp3']
        assert uploads[1]['filename'] in ['test1.mp3', 'test2.mp3']
    
    def test_duplicate_filename_handling(self, upload_manager, temp_upload_dir):
        """Test handling of duplicate filenames"""
        # Create existing file
        existing_file = os.path.join(temp_upload_dir, 'test.mp3')
        with open(existing_file, 'wb') as f:
            f.write(b'existing content')
        
        # Create session and complete upload
        session_result = upload_manager.create_upload_session('test.mp3', 1024)
        session_id = session_result['session_id']
        
        upload_manager.upload_chunk(session_id, 0, b'x' * 1024)
        result = upload_manager.complete_upload(session_id)
        
        assert result['success'] == True
        # Should have renamed the file to avoid conflict
        assert result['filename'] != 'test.mp3' or not os.path.exists(existing_file)

if __name__ == '__main__':
    pytest.main([__file__])