import pytest
import os
import tempfile
import sqlite3
import shutil
from unittest.mock import patch, MagicMock

from services.storage_monitor import StorageMonitor
from config import Config

class TestStorageMonitor:
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary database for testing"""
        db_fd, db_path = tempfile.mkstemp()
        os.close(db_fd)
        yield db_path
        os.unlink(db_path)
    
    @pytest.fixture
    def temp_dirs(self):
        """Create temporary directories for primary and fallback storage"""
        with tempfile.TemporaryDirectory() as primary_dir:
            with tempfile.TemporaryDirectory() as fallback_dir:
                yield primary_dir, fallback_dir
    
    @pytest.fixture
    def storage_monitor(self, temp_db, temp_dirs):
        """Create StorageMonitor instance with temporary paths"""
        primary_dir, fallback_dir = temp_dirs
        
        with patch.object(Config, 'DATABASE_PATH', temp_db), \
             patch.object(Config, 'SYNCED_FOLDER', primary_dir), \
             patch.object(Config, 'BACKUP_FOLDER', fallback_dir), \
             patch.object(Config, 'STORAGE_CHECK_INTERVAL', 1), \
             patch.object(Config, 'STORAGE_WARNING_THRESHOLD', 0.9):
            
            monitor = StorageMonitor(temp_db)
            yield monitor
            monitor.stop_monitoring()
    
    def test_init_database(self, storage_monitor):
        """Test database initialization"""
        with sqlite3.connect(storage_monitor.db_path) as conn:
            # Check if tables exist
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name IN ('storage_status', 'storage_events')
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            assert 'storage_status' in tables
            assert 'storage_events' in tables
            
            # Check if initial records exist
            cursor = conn.execute("SELECT COUNT(*) FROM storage_status")
            count = cursor.fetchone()[0]
            assert count == 2  # primary and fallback records
    
    def test_get_storage_info_success(self, storage_monitor, temp_dirs):
        """Test successful storage info retrieval"""
        primary_dir, _ = temp_dirs
        
        # Create a test file to ensure directory has some content
        test_file = os.path.join(primary_dir, 'test.txt')
        with open(test_file, 'w') as f:
            f.write('test content')
        
        info = storage_monitor.get_storage_info(primary_dir)
        
        assert info is not None
        assert info['path'] == primary_dir
        assert info['is_available'] == True
        assert info['capacity_gb'] > 0
        assert info['free_gb'] >= 0
        assert info['used_gb'] >= 0
        assert info['health_status'] in ['healthy', 'warning', 'error']
    
    def test_get_storage_info_nonexistent_path(self, storage_monitor):
        """Test storage info for non-existent path"""
        info = storage_monitor.get_storage_info('/nonexistent/path')
        
        assert info is not None
        assert info['is_available'] == False
        assert info['health_status'] == 'error'
    
    def test_check_storage_health(self, storage_monitor):
        """Test storage health check"""
        health = storage_monitor.check_storage_health()
        
        assert 'primary' in health
        assert 'fallback' in health
        assert 'current_storage' in health
        assert 'overall_health' in health
        assert health['current_storage'] in ['primary', 'fallback']
        assert health['overall_health'] in ['healthy', 'warning', 'error']
    
    def test_switch_storage_success(self, storage_monitor):
        """Test successful storage switching"""
        # Ensure we start with primary
        storage_monitor.current_storage = 'primary'
        
        result = storage_monitor.switch_storage('fallback')
        
        assert result['success'] == True
        assert storage_monitor.current_storage == 'fallback'
        assert result['old_storage'] == 'primary'
        assert result['new_storage'] == 'fallback'
    
    def test_switch_storage_invalid_type(self, storage_monitor):
        """Test storage switching with invalid type"""
        result = storage_monitor.switch_storage('invalid')
        
        assert result['success'] == False
        assert 'Invalid storage type' in result['error']
    
    def test_switch_storage_unavailable(self, storage_monitor):
        """Test switching to unavailable storage"""
        # Mock storage info to return unavailable
        with patch.object(storage_monitor, 'get_storage_info') as mock_info:
            mock_info.return_value = {'is_available': False}
            
            result = storage_monitor.switch_storage('fallback')
            
            assert result['success'] == False
            assert 'not available' in result['error']
    
    def test_auto_switch_storage(self, storage_monitor):
        """Test automatic storage switching"""
        # Mock health check to indicate switch needed
        with patch.object(storage_monitor, 'check_storage_health') as mock_health:
            mock_health.return_value = {
                'should_switch': True,
                'recommended_storage': 'fallback'
            }
            
            with patch.object(storage_monitor, 'switch_storage') as mock_switch:
                mock_switch.return_value = {'success': True}
                
                result = storage_monitor.auto_switch_storage()
                
                assert result is not None
                mock_switch.assert_called_once_with('fallback')
    
    def test_auto_switch_no_switch_needed(self, storage_monitor):
        """Test auto switch when no switch is needed"""
        with patch.object(storage_monitor, 'check_storage_health') as mock_health:
            mock_health.return_value = {
                'should_switch': False,
                'recommended_storage': 'primary'
            }
            
            result = storage_monitor.auto_switch_storage()
            
            assert result is None
    
    def test_log_storage_event(self, storage_monitor):
        """Test storage event logging"""
        storage_monitor._log_storage_event('test_event', 'primary', 'Test message')
        
        events = storage_monitor.get_storage_events(10)
        
        assert len(events) > 0
        assert events[0]['event_type'] == 'test_event'
        assert events[0]['storage_type'] == 'primary'
        assert events[0]['message'] == 'Test message'
    
    def test_get_storage_events(self, storage_monitor):
        """Test getting storage events"""
        # Add some test events
        storage_monitor._log_storage_event('event1', 'primary', 'Message 1')
        storage_monitor._log_storage_event('event2', 'fallback', 'Message 2')
        
        events = storage_monitor.get_storage_events(10)
        
        assert len(events) >= 2
        # Events should be ordered by most recent first
        assert events[0]['event_type'] == 'event2'
        assert events[1]['event_type'] == 'event1'
    
    def test_get_storage_metrics(self, storage_monitor):
        """Test getting storage metrics"""
        metrics = storage_monitor.get_storage_metrics()
        
        assert 'current_storage' in metrics
        assert 'overall_health' in metrics
        assert 'primary_storage' in metrics
        assert 'fallback_storage' in metrics
        assert 'event_stats' in metrics
        assert 'monitoring_active' in metrics
    
    def test_get_current_storage_path(self, storage_monitor, temp_dirs):
        """Test getting current storage path"""
        primary_dir, fallback_dir = temp_dirs
        
        # Test primary storage
        storage_monitor.current_storage = 'primary'
        assert storage_monitor.get_current_storage_path() == primary_dir
        
        # Test fallback storage
        storage_monitor.current_storage = 'fallback'
        assert storage_monitor.get_current_storage_path() == fallback_dir
    
    def test_is_primary_available(self, storage_monitor):
        """Test checking if primary storage is available"""
        # Mock get_storage_info to return available
        with patch.object(storage_monitor, 'get_storage_info') as mock_info:
            mock_info.return_value = {'is_available': True}
            
            assert storage_monitor.is_primary_available() == True
            
            mock_info.return_value = {'is_available': False}
            assert storage_monitor.is_primary_available() == False
            
            mock_info.return_value = None
            assert storage_monitor.is_primary_available() == False
    
    def test_is_fallback_available(self, storage_monitor):
        """Test checking if fallback storage is available"""
        with patch.object(storage_monitor, 'get_storage_info') as mock_info:
            mock_info.return_value = {'is_available': True}
            
            assert storage_monitor.is_fallback_available() == True
            
            mock_info.return_value = {'is_available': False}
            assert storage_monitor.is_fallback_available() == False
    
    def test_force_storage_check(self, storage_monitor):
        """Test forcing storage check"""
        with patch.object(storage_monitor, '_update_storage_status') as mock_update:
            with patch.object(storage_monitor, 'check_storage_health') as mock_check:
                mock_check.return_value = {'test': 'result'}
                
                result = storage_monitor.force_storage_check()
                
                mock_update.assert_called_once()
                mock_check.assert_called_once()
                assert result == {'test': 'result'}
    
    def test_cleanup_old_events(self, storage_monitor):
        """Test cleaning up old events"""
        # Add some test events
        storage_monitor._log_storage_event('old_event', 'primary', 'Old message')
        
        # Mock the event as old by directly updating database
        with sqlite3.connect(storage_monitor.db_path) as conn:
            conn.execute("""
                UPDATE storage_events 
                SET occurred_at = datetime('now', '-31 days')
                WHERE event_type = 'old_event'
            """)
            conn.commit()
        
        # Clean up events older than 30 days
        deleted_count = storage_monitor.cleanup_old_events(30)
        
        assert deleted_count >= 1
        
        # Verify event was deleted
        events = storage_monitor.get_storage_events(100)
        old_events = [e for e in events if e['event_type'] == 'old_event']
        assert len(old_events) == 0
    
    def test_start_stop_monitoring(self, storage_monitor):
        """Test starting and stopping monitoring"""
        assert storage_monitor.is_monitoring == False
        
        storage_monitor.start_monitoring()
        assert storage_monitor.is_monitoring == True
        assert storage_monitor.monitor_thread is not None
        
        storage_monitor.stop_monitoring()
        assert storage_monitor.is_monitoring == False
    
    @patch('time.sleep')  # Mock sleep to speed up test
    def test_monitor_loop(self, mock_sleep, storage_monitor):
        """Test monitoring loop functionality"""
        with patch.object(storage_monitor, '_update_storage_status') as mock_update:
            with patch.object(storage_monitor, 'auto_switch_storage') as mock_auto:
                mock_auto.return_value = None
                
                # Start monitoring
                storage_monitor.start_monitoring()
                
                # Let it run briefly
                import time
                time.sleep(0.1)
                
                # Stop monitoring
                storage_monitor.stop_monitoring()
                
                # Verify methods were called
                assert mock_update.call_count >= 1

if __name__ == '__main__':
    pytest.main([__file__])