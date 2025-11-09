import pytest
import os
import tempfile
import sqlite3
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from services.auth_manager import AuthManager
from config import Config

class TestAuthManager:
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary database for testing"""
        db_fd, db_path = tempfile.mkstemp()
        os.close(db_fd)
        yield db_path
        os.unlink(db_path)
    
    @pytest.fixture
    def auth_manager(self, temp_db):
        """Create AuthManager instance with temporary database"""
        with patch.object(Config, 'DATABASE_PATH', temp_db), \
             patch.object(Config, 'SECRET_KEY', 'test-secret-key'), \
             patch.object(Config, 'ACCESS_TOKEN_EXPIRE_HOURS', 24), \
             patch.object(Config, 'ADMIN_EMAILS', ['admin@test.com']), \
             patch.object(Config, 'SMTP_HOST', 'smtp.test.com'), \
             patch.object(Config, 'SMTP_PORT', 587), \
             patch.object(Config, 'SMTP_USER', 'test@test.com'), \
             patch.object(Config, 'SMTP_PASS', 'test-password'):
            
            manager = AuthManager(temp_db)
            yield manager
    
    def test_init_database(self, auth_manager):
        """Test database initialization"""
        with sqlite3.connect(auth_manager.db_path) as conn:
            # Check if tables exist
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name IN ('access_requests', 'auth_events')
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            assert 'access_requests' in tables
            assert 'auth_events' in tables
    
    def test_submit_access_request_success(self, auth_manager):
        """Test successful access request submission"""
        with patch.object(auth_manager, '_send_admin_notification', return_value=True):
            result = auth_manager.submit_access_request(
                name='John Doe',
                email='john@test.com',
                reason='Testing access',
                organization='Test Corp',
                phone='123-456-7890'
            )
            
            assert result['success'] == True
            assert 'request_id' in result
            assert 'submitted successfully' in result['message']
    
    def test_submit_access_request_missing_fields(self, auth_manager):
        """Test access request submission with missing required fields"""
        result = auth_manager.submit_access_request(
            name='',
            email='john@test.com',
            reason='Testing access'
        )
        
        assert result['success'] == False
        assert 'required' in result['error']
    
    def test_submit_access_request_duplicate_pending(self, auth_manager):
        """Test duplicate pending request prevention"""
        with patch.object(auth_manager, '_send_admin_notification', return_value=True):
            # Submit first request
            auth_manager.submit_access_request(
                name='John Doe',
                email='john@test.com',
                reason='Testing access'
            )
            
            # Try to submit another request with same email
            result = auth_manager.submit_access_request(
                name='John Doe',
                email='john@test.com',
                reason='Another test'
            )
            
            assert result['success'] == False
            assert 'already have a pending' in result['error']
    
    def test_approve_request_success(self, auth_manager):
        """Test successful request approval"""
        with patch.object(auth_manager, '_send_admin_notification', return_value=True), \
             patch.object(auth_manager, '_send_approval_email', return_value=True):
            
            # Submit request first
            submit_result = auth_manager.submit_access_request(
                name='John Doe',
                email='john@test.com',
                reason='Testing access'
            )
            request_id = submit_result['request_id']
            
            # Approve the request
            result = auth_manager.approve_request(request_id, 'admin@test.com', 48)
            
            assert result['success'] == True
            assert 'approved successfully' in result['message']
            assert result['duration_hours'] == 48
    
    def test_approve_request_not_found(self, auth_manager):
        """Test approving non-existent request"""
        result = auth_manager.approve_request('invalid-id', 'admin@test.com')
        
        assert result['success'] == False
        assert 'not found' in result['error']
    
    def test_approve_request_already_processed(self, auth_manager):
        """Test approving already processed request"""
        with patch.object(auth_manager, '_send_admin_notification', return_value=True), \
             patch.object(auth_manager, '_send_approval_email', return_value=True):
            
            # Submit and approve request
            submit_result = auth_manager.submit_access_request(
                name='John Doe',
                email='john@test.com',
                reason='Testing access'
            )
            request_id = submit_result['request_id']
            auth_manager.approve_request(request_id, 'admin@test.com')
            
            # Try to approve again
            result = auth_manager.approve_request(request_id, 'admin@test.com')
            
            assert result['success'] == False
            assert 'already approved' in result['error']
    
    def test_reject_request_success(self, auth_manager):
        """Test successful request rejection"""
        with patch.object(auth_manager, '_send_admin_notification', return_value=True), \
             patch.object(auth_manager, '_send_rejection_email', return_value=True):
            
            # Submit request first
            submit_result = auth_manager.submit_access_request(
                name='John Doe',
                email='john@test.com',
                reason='Testing access'
            )
            request_id = submit_result['request_id']
            
            # Reject the request
            result = auth_manager.reject_request(request_id, 'admin@test.com', 'Not authorized')
            
            assert result['success'] == True
            assert 'rejected successfully' in result['message']
    
    def test_validate_access_success(self, auth_manager):
        """Test successful access validation"""
        with patch.object(auth_manager, '_send_admin_notification', return_value=True), \
             patch.object(auth_manager, '_send_approval_email', return_value=True):
            
            # Submit and approve request
            submit_result = auth_manager.submit_access_request(
                name='John Doe',
                email='john@test.com',
                reason='Testing access'
            )
            request_id = submit_result['request_id']
            auth_manager.approve_request(request_id, 'admin@test.com')
            
            # Get the session token
            request_data = auth_manager._get_request_by_id(request_id)
            session_token = request_data['session_token']
            
            # Validate access
            result = auth_manager.validate_access('john@test.com', session_token)
            
            assert result['success'] == True
            assert result['email'] == 'john@test.com'
            assert result['name'] == 'John Doe'
            assert 'remaining_hours' in result
    
    def test_validate_access_invalid_credentials(self, auth_manager):
        """Test access validation with invalid credentials"""
        result = auth_manager.validate_access('invalid@test.com', 'invalid-token')
        
        assert result['success'] == False
        assert 'Invalid credentials' in result['error']
    
    def test_validate_access_expired(self, auth_manager):
        """Test access validation with expired token"""
        with patch.object(auth_manager, '_send_admin_notification', return_value=True), \
             patch.object(auth_manager, '_send_approval_email', return_value=True):
            
            # Submit and approve request
            submit_result = auth_manager.submit_access_request(
                name='John Doe',
                email='john@test.com',
                reason='Testing access'
            )
            request_id = submit_result['request_id']
            auth_manager.approve_request(request_id, 'admin@test.com')
            
            # Manually expire the request
            with sqlite3.connect(auth_manager.db_path) as conn:
                expired_time = (datetime.now() - timedelta(hours=1)).isoformat()
                conn.execute('''
                    UPDATE access_requests 
                    SET expires_at = ? 
                    WHERE id = ?
                ''', (expired_time, request_id))
                conn.commit()
            
            # Get the session token
            request_data = auth_manager._get_request_by_id(request_id)
            session_token = request_data['session_token']
            
            # Try to validate access
            result = auth_manager.validate_access('john@test.com', session_token)
            
            assert result['success'] == False
            assert 'expired' in result['error']
    
    def test_get_request_status_no_request(self, auth_manager):
        """Test getting status for email with no request"""
        result = auth_manager.get_request_status('nonexistent@test.com')
        
        assert result['success'] == True
        assert result['has_request'] == False
    
    def test_get_request_status_pending(self, auth_manager):
        """Test getting status for pending request"""
        with patch.object(auth_manager, '_send_admin_notification', return_value=True):
            auth_manager.submit_access_request(
                name='John Doe',
                email='john@test.com',
                reason='Testing access'
            )
            
            result = auth_manager.get_request_status('john@test.com')
            
            assert result['success'] == True
            assert result['has_request'] == True
            assert result['status'] == 'pending'
    
    def test_get_request_status_approved(self, auth_manager):
        """Test getting status for approved request"""
        with patch.object(auth_manager, '_send_admin_notification', return_value=True), \
             patch.object(auth_manager, '_send_approval_email', return_value=True):
            
            # Submit and approve request
            submit_result = auth_manager.submit_access_request(
                name='John Doe',
                email='john@test.com',
                reason='Testing access'
            )
            request_id = submit_result['request_id']
            auth_manager.approve_request(request_id, 'admin@test.com')
            
            result = auth_manager.get_request_status('john@test.com')
            
            assert result['success'] == True
            assert result['has_request'] == True
            assert result['status'] == 'approved'
            assert 'remaining_hours' in result
    
    def test_generate_session_token(self, auth_manager):
        """Test session token generation"""
        token = auth_manager._generate_session_token('test-id', 'test@test.com')
        
        assert isinstance(token, str)
        assert len(token) > 0
    
    def test_get_pending_requests(self, auth_manager):
        """Test getting pending requests"""
        with patch.object(auth_manager, '_send_admin_notification', return_value=True):
            # Submit a few requests
            auth_manager.submit_access_request('John Doe', 'john@test.com', 'Test 1')
            auth_manager.submit_access_request('Jane Doe', 'jane@test.com', 'Test 2')
            
            pending = auth_manager.get_pending_requests()
            
            assert len(pending) == 2
            assert all(req['status'] == 'pending' for req in pending)
    
    def test_get_auth_stats(self, auth_manager):
        """Test getting authentication statistics"""
        with patch.object(auth_manager, '_send_admin_notification', return_value=True), \
             patch.object(auth_manager, '_send_approval_email', return_value=True), \
             patch.object(auth_manager, '_send_rejection_email', return_value=True):
            
            # Submit some requests
            submit1 = auth_manager.submit_access_request('John Doe', 'john@test.com', 'Test 1')
            submit2 = auth_manager.submit_access_request('Jane Doe', 'jane@test.com', 'Test 2')
            
            # Approve one, reject another
            auth_manager.approve_request(submit1['request_id'], 'admin@test.com')
            auth_manager.reject_request(submit2['request_id'], 'admin@test.com')
            
            stats = auth_manager.get_auth_stats()
            
            assert stats['total_requests'] == 2
            assert stats['approved'] == 1
            assert stats['rejected'] == 1
            assert stats['pending'] == 0
    
    def test_cleanup_expired_requests(self, auth_manager):
        """Test cleaning up expired requests"""
        with patch.object(auth_manager, '_send_admin_notification', return_value=True), \
             patch.object(auth_manager, '_send_rejection_email', return_value=True):
            
            # Submit and reject a request
            submit_result = auth_manager.submit_access_request(
                name='John Doe',
                email='john@test.com',
                reason='Testing access'
            )
            request_id = submit_result['request_id']
            auth_manager.reject_request(request_id, 'admin@test.com')
            
            # Make it old
            with sqlite3.connect(auth_manager.db_path) as conn:
                old_date = (datetime.now() - timedelta(days=31)).isoformat()
                conn.execute('''
                    UPDATE access_requests 
                    SET requested_at = ? 
                    WHERE id = ?
                ''', (old_date, request_id))
                conn.commit()
            
            # Clean up
            deleted_count = auth_manager.cleanup_expired_requests(30)
            
            assert deleted_count == 1
            
            # Verify it's gone
            request_data = auth_manager._get_request_by_id(request_id)
            assert request_data is None
    
    @patch('smtplib.SMTP')
    def test_send_email_success(self, mock_smtp, auth_manager):
        """Test successful email sending"""
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server
        
        result = auth_manager._send_email(
            ['test@test.com'], 
            'Test Subject', 
            '<html><body>Test</body></html>'
        )
        
        assert result == True
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once()
        mock_server.send_message.assert_called_once()
    
    def test_send_email_no_credentials(self, auth_manager):
        """Test email sending without SMTP credentials"""
        auth_manager.smtp_config['user'] = None
        
        result = auth_manager._send_email(['test@test.com'], 'Test', 'Test')
        
        assert result == False
    
    def test_log_auth_event(self, auth_manager):
        """Test authentication event logging"""
        auth_manager._log_auth_event('test-id', 'test_event', 'Test details', '127.0.0.1')
        
        # Verify event was logged
        with sqlite3.connect(auth_manager.db_path) as conn:
            cursor = conn.execute('''
                SELECT * FROM auth_events 
                WHERE request_id = 'test-id' AND event_type = 'test_event'
            ''')
            event = cursor.fetchone()
            
            assert event is not None
            assert event[3] == 'Test details'  # details column
            assert event[4] == '127.0.0.1'     # ip_address column

if __name__ == '__main__':
    pytest.main([__file__])