from flask import Blueprint, request, jsonify
import logging

from services.auth_manager import AuthManager

logger = logging.getLogger(__name__)

auth_bp = Blueprint('auth', __name__, url_prefix='/api/auth')

# Initialize auth manager
auth_manager = AuthManager()

@auth_bp.route('/request', methods=['POST'])
def submit_access_request():
    """Submit a new access request"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        # Required fields
        name = data.get('name', '').strip()
        email = data.get('email', '').strip()
        reason = data.get('reason', '').strip()
        
        if not name or not email or not reason:
            return jsonify({
                'success': False, 
                'error': 'Name, email, and reason are required'
            }), 400
        
        # Optional fields
        organization = data.get('organization', '').strip() or None
        phone = data.get('phone_number', '').strip() or None
        
        # Get client info
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
        user_agent = request.headers.get('User-Agent')
        
        result = auth_manager.submit_access_request(
            name=name,
            email=email,
            reason=reason,
            organization=organization,
            phone=phone,
            ip_address=ip_address,
            user_agent=user_agent
        )
        
        if result['success']:
            return jsonify(result), 200
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"Error submitting access request: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@auth_bp.route('/status', methods=['POST'])
def get_request_status():
    """Check status of access request for an email"""
    try:
        data = request.get_json()
        
        if not data or 'email' not in data:
            return jsonify({'success': False, 'error': 'Email is required'}), 400
        
        email = data['email'].strip()
        if not email:
            return jsonify({'success': False, 'error': 'Email is required'}), 400
        
        result = auth_manager.get_request_status(email)
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error getting request status: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@auth_bp.route('/access', methods=['POST'])
def validate_access():
    """Validate user access with email and session token"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        email = data.get('email', '').strip()
        session_token = data.get('session_token', '').strip()
        
        if not email or not session_token:
            return jsonify({
                'success': False, 
                'error': 'Email and session token are required'
            }), 400
        
        # Get client IP
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
        
        result = auth_manager.validate_access(email, session_token, ip_address)
        
        if result['success']:
            return jsonify(result), 200
        else:
            return jsonify(result), 401
            
    except Exception as e:
        logger.error(f"Error validating access: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@auth_bp.route('/approve/<request_id>', methods=['POST'])
def approve_request(request_id):
    """Approve an access request (admin endpoint)"""
    try:
        data = request.get_json() or {}
        
        # In a real implementation, you'd validate admin credentials here
        admin_email = data.get('admin_email', 'admin@resort.com')
        duration_hours = data.get('duration_hours', 24)
        
        result = auth_manager.approve_request(request_id, admin_email, duration_hours)
        
        if result['success']:
            return jsonify(result), 200
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"Error approving request: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@auth_bp.route('/reject/<request_id>', methods=['POST'])
def reject_request(request_id):
    """Reject an access request (admin endpoint)"""
    try:
        data = request.get_json() or {}
        
        # In a real implementation, you'd validate admin credentials here
        admin_email = data.get('admin_email', 'admin@resort.com')
        reason = data.get('reason', '')
        
        result = auth_manager.reject_request(request_id, admin_email, reason)
        
        if result['success']:
            return jsonify(result), 200
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"Error rejecting request: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@auth_bp.route('/pending', methods=['GET'])
def get_pending_requests():
    """Get all pending access requests (admin endpoint)"""
    try:
        # In a real implementation, you'd validate admin credentials here
        requests = auth_manager.get_pending_requests()
        
        return jsonify({
            'success': True,
            'requests': requests
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting pending requests: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@auth_bp.route('/stats', methods=['GET'])
def get_auth_stats():
    """Get authentication statistics (admin endpoint)"""
    try:
        # In a real implementation, you'd validate admin credentials here
        stats = auth_manager.get_auth_stats()
        
        return jsonify({
            'success': True,
            'stats': stats
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting auth stats: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@auth_bp.route('/cleanup', methods=['POST'])
def cleanup_expired_requests():
    """Clean up old expired requests (admin endpoint)"""
    try:
        data = request.get_json() or {}
        days = data.get('days', 30)
        
        # In a real implementation, you'd validate admin credentials here
        deleted_count = auth_manager.cleanup_expired_requests(days)
        
        return jsonify({
            'success': True,
            'message': f'Cleaned up {deleted_count} old requests',
            'deleted_count': deleted_count
        }), 200
        
    except Exception as e:
        logger.error(f"Error cleaning up expired requests: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@auth_bp.route('/logout', methods=['POST'])
def logout():
    """Logout user (invalidate session)"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        email = data.get('email', '').strip()
        
        if not email:
            return jsonify({'success': False, 'error': 'Email is required'}), 400
        
        # In a more sophisticated implementation, you might invalidate the specific session
        # For now, we'll just log the logout event
        logger.info(f"User logged out: {email}")
        
        return jsonify({
            'success': True,
            'message': 'Logged out successfully'
        }), 200
        
    except Exception as e:
        logger.error(f"Error during logout: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# Admin convenience endpoints for email-based approval
@auth_bp.route('/admin/approve/<request_id>/<admin_token>', methods=['GET'])
def admin_approve_via_email(request_id, admin_token):
    """Approve request via email link (GET endpoint for email links)"""
    try:
        # In a real implementation, you'd validate the admin_token
        # For now, we'll use a simple approach
        
        result = auth_manager.approve_request(request_id, 'admin@resort.com', 24)
        
        if result['success']:
            return f"""
            <html>
            <body>
                <h2>Request Approved</h2>
                <p>The access request has been approved successfully.</p>
                <p>The requester will receive an email with access instructions.</p>
            </body>
            </html>
            """, 200
        else:
            return f"""
            <html>
            <body>
                <h2>Error</h2>
                <p>Failed to approve request: {result['error']}</p>
            </body>
            </html>
            """, 400
            
    except Exception as e:
        logger.error(f"Error in admin approval via email: {e}")
        return f"""
        <html>
        <body>
            <h2>Error</h2>
            <p>An error occurred: {str(e)}</p>
        </body>
        </html>
        """, 500

@auth_bp.route('/admin/reject/<request_id>/<admin_token>', methods=['GET'])
def admin_reject_via_email(request_id, admin_token):
    """Reject request via email link (GET endpoint for email links)"""
    try:
        # In a real implementation, you'd validate the admin_token
        
        result = auth_manager.reject_request(request_id, 'admin@resort.com', 'Rejected via email')
        
        if result['success']:
            return f"""
            <html>
            <body>
                <h2>Request Rejected</h2>
                <p>The access request has been rejected.</p>
                <p>The requester will receive an email notification.</p>
            </body>
            </html>
            """, 200
        else:
            return f"""
            <html>
            <body>
                <h2>Error</h2>
                <p>Failed to reject request: {result['error']}</p>
            </body>
            </html>
            """, 400
            
    except Exception as e:
        logger.error(f"Error in admin rejection via email: {e}")
        return f"""
        <html>
        <body>
            <h2>Error</h2>
            <p>An error occurred: {str(e)}</p>
        </body>
        </html>
        """, 500

# Health check endpoint
@auth_bp.route('/health', methods=['GET'])
def auth_health_check():
    """Health check for authentication service"""
    try:
        # Check if we can connect to database
        stats = auth_manager.get_auth_stats()
        
        # Check if SMTP is configured
        smtp_configured = bool(auth_manager.smtp_config['user'] and auth_manager.smtp_config['password'])
        
        # Check if admin emails are configured
        admin_emails_configured = bool(auth_manager.admin_emails and auth_manager.admin_emails != [''])
        
        return jsonify({
            'success': True,
            'status': 'healthy',
            'database_connected': bool(stats),
            'smtp_configured': smtp_configured,
            'admin_emails_configured': admin_emails_configured,
            'total_requests': stats.get('total_requests', 0),
            'pending_requests': stats.get('pending', 0)
        }), 200
        
    except Exception as e:
        logger.error(f"Auth health check failed: {e}")
        return jsonify({
            'success': False,
            'status': 'unhealthy',
            'error': str(e)
        }), 500