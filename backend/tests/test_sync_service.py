import pytest
import os
import tempfile
import sqlite3
import shutil
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime

from services.sync_service import SyncService
from config import Config

class TestSyncService:
    
    @pytest.fixture
    def temp_dirs(self):
        """Create temporary directories for testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            unsynced_dir = os.path.join(temp_dir, 'unsynced')
            synced_dir = os.path.join(temp_dir, 'synced')
            os.makedirs(unsynced_dir)
            os.makedirs(synced_dir)
            yield temp_dir, unsynced_dir, synced_dir
    
    @pytest.fixture
    def temp_db(self):
        """Create temporary database"""
        db_fd, db_path = tempfile.mkstemp()
        os.close(db_fd)
        yield db_path
        os.unlink(db_path)
    
    @pytest.fixture
    def sync_service(self, temp_dirs, temp_db):
        """Create SyncService instance with temporary paths"""
        temp_dir, unsynced_dir, synced_dir = temp_dirs
        
        with patch.object(Config, 'DATABASE_PATH', temp_db), \
             patch.object(Config, 'UNSYNCED_FOLDER', unsynced_dir), \
             patch.object(Config, 'SYNCED_FOLDER', synced_dir), \
             patch.object(Config, 'ALLOWED_EXTENSIONS', {'mp3', 'wav', 'flac'}), \
             patch.object(Config, 'MAX_UPLOAD_SIZE', 10 * 1024 * 1024):
            
            service = SyncService(temp_db)
            yield service
    
    def test_init_database(self, sync_service):
        """Test database initialization"""
        with sqlite3.connect(sync_service.db_path) as conn:
            # Check if tables exist
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name IN ('songs', 'sync_log')
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            assert 'songs' in tables
            assert 'sync_log' in tables
    
    def test_extract_metadata_field(self, sync_service):
        """Test metadata field extraction"""
        # Mock audio object with different field access methods
        mock_audio = MagicMock()
        mock_audio.get.return_value = ['Test Title']
        
        result = sync_service._extract_metadata_field(mock_audio, ['title'], 'Default')
        assert result == 'Test Title'
        
        # Test with default value
        mock_audio.get.return_value = None
        result = sync_service._extract_metadata_field(mock_audio, ['nonexistent'], 'Default')
        assert result == 'Default'
    
    def test_detect_codec(self, sync_service):
        """Test codec detection"""
        # Mock audio with codec info
        mock_audio = MagicMock()
        mock_audio.info.codec = 'MP3'
        
        result = sync_service._detect_codec(mock_audio, 'mp3')
        assert result == 'MP3'
        
        # Test fallback based on format
        mock_audio.info = None
        result = sync_service._detect_codec(mock_audio, 'flac')
        assert result == 'FLAC'
    
    def test_calculate_quality_score(self, sync_service):
        """Test quality score calculation"""
        # High quality file
        score = sync_service._calculate_quality_score(320, 48000, 'flac')
        assert score >= 90
        
        # Medium quality file
        score = sync_service._calculate_quality_score(192, 44100, 'mp3')
        assert 50 <= score < 90
        
        # Low quality file
        score = sync_service._calculate_quality_score(96, 22050, 'mp3')
        assert score < 50
    
    def test_validate_audio_integrity(self, sync_service, temp_dirs):
        """Test audio file integrity validation"""
        _, unsynced_dir, _ = temp_dirs
        
        # Create test file
        test_file = os.path.join(unsynced_dir, 'test.mp3')
        with open(test_file, 'wb') as f:
            f.write(b'fake audio data')
        
        # Test with valid metadata
        metadata = {'duration': 180, 'format': 'mp3'}
        
        with patch('services.sync_service.File') as mock_file:
            mock_audio = MagicMock()
            mock_audio.info.bitrate = 192
            mock_file.return_value = mock_audio
            
            result = sync_service._validate_audio_integrity(test_file, metadata)
            assert result == True
        
        # Test with invalid metadata (no duration)
        metadata = {'duration': 0, 'format': 'mp3'}
        result = sync_service._validate_audio_integrity(test_file, metadata)
        assert result == False
    
    def test_generate_optimized_filename(self, sync_service):
        """Test optimized filename generation"""
        metadata = {
            'artist': 'Test Artist',
            'title': 'Test Song'
        }
        
        # Test with poorly formatted filename
        result = sync_service._generate_optimized_filename('random123.mp3', metadata)
        assert result == 'Test Artist - Test Song.mp3'
        
        # Test with already well-formatted filename
        result = sync_service._generate_optimized_filename('Artist - Song.mp3', metadata)
        assert result == 'Artist - Song.mp3'
    
    def test_sanitize_filename_part(self, sync_service):
        """Test filename sanitization"""
        # Test with invalid characters
        result = sync_service._sanitize_filename_part('Test<>:"/\\|?*Song')
        assert result == 'TestSong'
        
        # Test with multiple spaces
        result = sync_service._sanitize_filename_part('Test   Multiple   Spaces')
        assert result == 'Test Multiple Spaces'
        
        # Test with long text
        long_text = 'A' * 100
        result = sync_service._sanitize_filename_part(long_text)
        assert len(result) <= 50
    
    def test_quarantine_file(self, sync_service, temp_dirs):
        """Test file quarantine functionality"""
        temp_dir, unsynced_dir, _ = temp_dirs
        
        # Create test file
        test_file = os.path.join(unsynced_dir, 'bad_file.txt')
        with open(test_file, 'w') as f:
            f.write('bad content')
        
        # Quarantine the file
        sync_service._quarantine_file(test_file, 'Test reason')
        
        # Check if file was moved to quarantine
        quarantine_dir = os.path.join(temp_dir, 'quarantine')
        assert os.path.exists(quarantine_dir)
        
        quarantine_file = os.path.join(quarantine_dir, 'bad_file.txt')
        assert os.path.exists(quarantine_file)
        assert not os.path.exists(test_file)
        
        # Check if reason file was created
        reason_file = quarantine_file + '.reason'
        assert os.path.exists(reason_file)
        
        with open(reason_file, 'r') as f:
            content = f.read()
            assert 'Test reason' in content
    
    def test_sync_single_file_success(self, sync_service, temp_dirs):
        """Test successful single file sync"""
        _, unsynced_dir, synced_dir = temp_dirs
        
        # Create test audio file
        test_file = os.path.join(unsynced_dir, 'test.mp3')
        with open(test_file, 'wb') as f:
            f.write(b'fake mp3 data' * 1000)  # Make it reasonably sized
        
        # Mock metadata extraction
        with patch.object(sync_service, 'get_song_metadata') as mock_metadata:
            mock_metadata.return_value = {
                'title': 'Test Song',
                'artist': 'Test Artist',
                'album': 'Test Album',
                'duration': 180,
                'format': 'mp3',
                'bitrate': 192,
                'quality_score': 75
            }
            
            # Mock integrity validation
            with patch.object(sync_service, '_validate_audio_integrity', return_value=True):
                result = sync_service._sync_single_file('test.mp3')
        
        assert result == True
        
        # Check if file was moved
        assert not os.path.exists(test_file)
        synced_file = os.path.join(synced_dir, 'test.mp3')
        assert os.path.exists(synced_file)
        
        # Check database entry
        with sqlite3.connect(sync_service.db_path) as conn:
            cursor = conn.execute('SELECT * FROM songs WHERE filename = ?', ('test.mp3',))
            song = cursor.fetchone()
            assert song is not None
    
    def test_sync_single_file_invalid_format(self, sync_service, temp_dirs):
        """Test sync with invalid file format"""
        _, unsynced_dir, _ = temp_dirs
        
        # Create test file with invalid extension
        test_file = os.path.join(unsynced_dir, 'test.txt')
        with open(test_file, 'w') as f:
            f.write('not audio')
        
        result = sync_service._sync_single_file('test.txt')
        assert result == False
        
        # File should be quarantined
        assert not os.path.exists(test_file)
    
    def test_sync_single_file_duplicate(self, sync_service, temp_dirs):
        """Test sync with duplicate file"""
        _, unsynced_dir, _ = temp_dirs
        
        # Create test file
        test_file = os.path.join(unsynced_dir, 'test.mp3')
        with open(test_file, 'wb') as f:
            f.write(b'fake mp3 data')
        
        # Mock duplicate detection
        with patch.object(sync_service, 'is_duplicate_song', return_value=True):
            result = sync_service._sync_single_file('test.mp3')
        
        assert result == True  # Duplicate handling is considered successful
        assert not os.path.exists(test_file)  # File should be removed
    
    def test_sync_with_progress_callback(self, sync_service, temp_dirs):
        """Test sync with progress callback"""
        _, unsynced_dir, synced_dir = temp_dirs
        
        # Create multiple test files
        for i in range(3):
            test_file = os.path.join(unsynced_dir, f'test{i}.mp3')
            with open(test_file, 'wb') as f:
                f.write(b'fake mp3 data')
        
        # Track progress calls
        progress_calls = []
        def progress_callback(processed, total, filename):
            progress_calls.append((processed, total, filename))
        
        # Mock successful sync for all files
        with patch.object(sync_service, '_sync_single_file_with_details') as mock_sync:
            mock_sync.return_value = {
                'filename': 'test.mp3',
                'status': 'success',
                'message': 'Success',
                'metadata': {},
                'file_size': 100,
                'processing_time': 0.1
            }
            
            results = sync_service.sync_with_progress_callback(progress_callback)
        
        assert results['processed'] == 3
        assert len(progress_calls) == 3
        assert progress_calls[0] == (1, 3, 'test0.mp3')
        assert progress_calls[-1] == (3, 3, 'test2.mp3')
    
    def test_get_quarantine_files(self, sync_service, temp_dirs):
        """Test getting quarantine files list"""
        temp_dir, unsynced_dir, _ = temp_dirs
        
        # Create quarantine directory and files
        quarantine_dir = os.path.join(temp_dir, 'quarantine')
        os.makedirs(quarantine_dir)
        
        # Create quarantined file
        quarantine_file = os.path.join(quarantine_dir, 'bad_file.mp3')
        with open(quarantine_file, 'wb') as f:
            f.write(b'bad data')
        
        # Create reason file
        reason_file = quarantine_file + '.reason'
        with open(reason_file, 'w') as f:
            f.write('Quarantined at: 2023-01-01T00:00:00\n')
            f.write('Reason: Test quarantine\n')
        
        quarantine_files = sync_service.get_quarantine_files()
        
        assert len(quarantine_files) == 1
        assert quarantine_files[0]['filename'] == 'bad_file.mp3'
        assert quarantine_files[0]['reason'] == 'Test quarantine'
    
    def test_restore_quarantine_file(self, sync_service, temp_dirs):
        """Test restoring file from quarantine"""
        temp_dir, unsynced_dir, _ = temp_dirs
        
        # Create quarantine directory and file
        quarantine_dir = os.path.join(temp_dir, 'quarantine')
        os.makedirs(quarantine_dir)
        
        quarantine_file = os.path.join(quarantine_dir, 'restore_test.mp3')
        with open(quarantine_file, 'wb') as f:
            f.write(b'test data')
        
        reason_file = quarantine_file + '.reason'
        with open(reason_file, 'w') as f:
            f.write('Reason: Test\n')
        
        # Restore the file
        result = sync_service.restore_quarantine_file('restore_test.mp3')
        
        assert result['success'] == True
        assert not os.path.exists(quarantine_file)
        assert not os.path.exists(reason_file)
        
        restored_file = os.path.join(unsynced_dir, 'restore_test.mp3')
        assert os.path.exists(restored_file)
    
    def test_delete_quarantine_file(self, sync_service, temp_dirs):
        """Test deleting quarantined file"""
        temp_dir, _, _ = temp_dirs
        
        # Create quarantine directory and file
        quarantine_dir = os.path.join(temp_dir, 'quarantine')
        os.makedirs(quarantine_dir)
        
        quarantine_file = os.path.join(quarantine_dir, 'delete_test.mp3')
        with open(quarantine_file, 'wb') as f:
            f.write(b'test data')
        
        reason_file = quarantine_file + '.reason'
        with open(reason_file, 'w') as f:
            f.write('Reason: Test\n')
        
        # Delete the file
        result = sync_service.delete_quarantine_file('delete_test.mp3')
        
        assert result['success'] == True
        assert not os.path.exists(quarantine_file)
        assert not os.path.exists(reason_file)
    
    def test_get_detailed_sync_stats(self, sync_service):
        """Test getting detailed sync statistics"""
        # Add some test data to database
        with sqlite3.connect(sync_service.db_path) as conn:
            # Add test songs
            conn.execute('''
                INSERT INTO songs (filename, title, format, quality_score)
                VALUES ('test1.mp3', 'Test 1', 'mp3', 80)
            ''')
            conn.execute('''
                INSERT INTO songs (filename, title, format, quality_score)
                VALUES ('test2.flac', 'Test 2', 'flac', 95)
            ''')
            
            # Add test sync log entries
            conn.execute('''
                INSERT INTO sync_log (filename, action, status, file_size, duration)
                VALUES ('test1.mp3', 'sync', 'success', 5000000, 180)
            ''')
            conn.commit()
        
        stats = sync_service.get_detailed_sync_stats()
        
        assert 'basic_stats' in stats
        assert 'format_distribution' in stats
        assert 'quality_distribution' in stats
        assert 'quarantine_stats' in stats
    
    def test_optimize_library(self, sync_service, temp_dirs):
        """Test library optimization"""
        _, _, synced_dir = temp_dirs
        
        # Create test file
        test_file = os.path.join(synced_dir, 'test.mp3')
        with open(test_file, 'wb') as f:
            f.write(b'test data')
        
        # Add to database
        checksum = sync_service._calculate_checksum(test_file)
        with sqlite3.connect(sync_service.db_path) as conn:
            conn.execute('''
                INSERT INTO songs (filename, filepath, title, checksum, is_available)
                VALUES ('test.mp3', ?, 'Test Song', ?, TRUE)
            ''', (test_file, checksum))
            conn.commit()
        
        results = sync_service.optimize_library()
        
        assert 'scanned_files' in results
        assert results['scanned_files'] == 1
        assert results['missing_files'] == 0
        assert results['corrupted_files'] == 0
    
    def test_cleanup_failed_files(self, sync_service, temp_dirs):
        """Test cleanup of failed files"""
        _, unsynced_dir, _ = temp_dirs
        
        # Create old file
        old_file = os.path.join(unsynced_dir, 'old_file.mp3')
        with open(old_file, 'wb') as f:
            f.write(b'old data')
        
        # Mock file age to be old
        old_timestamp = datetime.now().timestamp() - 7200  # 2 hours ago
        os.utime(old_file, (old_timestamp, old_timestamp))
        
        cleaned_count = sync_service.cleanup_failed_files()
        
        assert cleaned_count == 1
        assert not os.path.exists(old_file)  # Should be quarantined

if __name__ == '__main__':
    pytest.main([__file__])