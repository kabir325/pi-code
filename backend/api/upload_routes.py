from flask import Blueprint, request, jsonify
from werkzeug.exceptions import RequestEntityTooLarge
import logging

from services.upload_manager import UploadManager
from services.sync_service import SyncService

logger = logging.getLogger(__name__)

upload_bp = Blueprint('upload', __name__, url_prefix='/api/upload')

# Initialize services
upload_manager = UploadManager()
sync_service = SyncService()

@upload_bp.route('/start', methods=['POST'])
def start_upload():
    """Start a new upload session"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        filename = data.get('filename')
        file_size = data.get('file_size')
        
        if not filename or not file_size:
            return jsonify({
                'success': False, 
                'error': 'filename and file_size are required'
            }), 400
        
        result = upload_manager.create_upload_session(filename, file_size)
        
        if result['success']:
            return jsonify(result), 200
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"Error starting upload: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@upload_bp.route('/chunk/<session_id>', methods=['POST'])
def upload_chunk(session_id):
    """Upload a file chunk"""
    try:
        # Get chunk number from form data
        chunk_number = request.form.get('chunk_number', type=int)
        if chunk_number is None:
            return jsonify({'success': False, 'error': 'chunk_number is required'}), 400
        
        # Get chunk data from files
        if 'chunk' not in request.files:
            return jsonify({'success': False, 'error': 'No chunk data provided'}), 400
        
        chunk_file = request.files['chunk']
        chunk_data = chunk_file.read()
        
        if not chunk_data:
            return jsonify({'success': False, 'error': 'Empty chunk data'}), 400
        
        result = upload_manager.upload_chunk(session_id, chunk_number, chunk_data)
        
        if result['success']:
            # If upload is completed, trigger sync
            if result.get('completed'):
                complete_result = upload_manager.complete_upload(session_id)
                if complete_result['success']:
                    # Trigger sync in background
                    try:
                        sync_service.sync_new_files()
                    except Exception as e:
                        logger.warning(f"Sync failed after upload: {e}")
                    
                    result.update(complete_result)
                else:
                    return jsonify(complete_result), 500
            
            return jsonify(result), 200
        else:
            return jsonify(result), 400
            
    except RequestEntityTooLarge:
        return jsonify({
            'success': False, 
            'error': 'Chunk too large'
        }), 413
    except Exception as e:
        logger.error(f"Error uploading chunk: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@upload_bp.route('/status/<session_id>', methods=['GET'])
def get_upload_status(session_id):
    """Get upload progress status"""
    try:
        result = upload_manager.get_upload_progress(session_id)
        
        if result['success']:
            return jsonify(result), 200
        else:
            return jsonify(result), 404
            
    except Exception as e:
        logger.error(f"Error getting upload status: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@upload_bp.route('/cancel/<session_id>', methods=['POST'])
def cancel_upload(session_id):
    """Cancel an ongoing upload"""
    try:
        result = upload_manager.cancel_upload(session_id)
        
        if result['success']:
            return jsonify(result), 200
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"Error cancelling upload: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@upload_bp.route('/recent', methods=['GET'])
def get_recent_uploads():
    """Get recent upload sessions"""
    try:
        limit = request.args.get('limit', 20, type=int)
        uploads = upload_manager.get_recent_uploads(limit)
        
        return jsonify({
            'success': True,
            'uploads': uploads
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting recent uploads: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@upload_bp.route('/cleanup', methods=['POST'])
def cleanup_expired():
    """Clean up expired upload sessions (admin endpoint)"""
    try:
        hours = request.json.get('hours', 24) if request.json else 24
        deleted_count = upload_manager.cleanup_expired_sessions(hours)
        
        return jsonify({
            'success': True,
            'message': f'Cleaned up {deleted_count} expired sessions'
        }), 200
        
    except Exception as e:
        logger.error(f"Error cleaning up expired sessions: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@upload_bp.route('/sync/trigger', methods=['POST'])
def trigger_sync():
    """Manually trigger sync process"""
    try:
        result = sync_service.sync_new_files()
        
        return jsonify({
            'success': True,
            'message': 'Sync completed',
            'result': result
        }), 200
        
    except Exception as e:
        logger.error(f"Error triggering sync: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# Error handlers
@upload_bp.errorhandler(RequestEntityTooLarge)
def handle_file_too_large(e):
    return jsonify({
        'success': False,
        'error': 'File too large'
    }), 413

@upload_bp.errorhandler(413)
def handle_payload_too_large(e):
    return jsonify({
        'success': False,
        'error': 'Request payload too large'
    }), 413