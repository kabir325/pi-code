from flask import Blueprint, request, jsonify
from flask_socketio import emit
import logging
import threading
import time

from services.sync_service import SyncService

logger = logging.getLogger(__name__)

sync_bp = Blueprint('sync', __name__, url_prefix='/api/sync')

# Initialize sync service
sync_service = SyncService()

# Global variable to track sync progress
current_sync_progress = {
    'is_running': False,
    'progress': 0,
    'current_file': '',
    'total_files': 0,
    'processed_files': 0,
    'results': {}
}

@sync_bp.route('/trigger', methods=['POST'])
def trigger_sync():
    """Manually trigger sync process"""
    try:
        if current_sync_progress['is_running']:
            return jsonify({
                'success': False,
                'error': 'Sync is already in progress'
            }), 409
        
        # Start sync in background thread
        sync_thread = threading.Thread(target=_run_sync_with_progress, daemon=True)
        sync_thread.start()
        
        return jsonify({
            'success': True,
            'message': 'Sync process started',
            'sync_id': int(time.time())
        }), 200
        
    except Exception as e:
        logger.error(f"Error triggering sync: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@sync_bp.route('/status', methods=['GET'])
def get_sync_status():
    """Get current sync status and progress"""
    try:
        return jsonify({
            'success': True,
            'sync_status': current_sync_progress
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting sync status: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@sync_bp.route('/history', methods=['GET'])
def get_sync_history():
    """Get sync history"""
    try:
        limit = request.args.get('limit', 50, type=int)
        history = sync_service.get_sync_history(limit)
        
        return jsonify({
            'success': True,
            'history': history
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting sync history: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@sync_bp.route('/stats', methods=['GET'])
def get_sync_stats():
    """Get sync statistics"""
    try:
        basic_stats = sync_service.get_sync_stats()
        detailed_stats = sync_service.get_detailed_sync_stats()
        
        return jsonify({
            'success': True,
            'basic_stats': basic_stats,
            'detailed_stats': detailed_stats
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting sync stats: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@sync_bp.route('/quarantine', methods=['GET'])
def get_quarantine_files():
    """Get list of quarantined files"""
    try:
        quarantine_files = sync_service.get_quarantine_files()
        
        return jsonify({
            'success': True,
            'quarantine_files': quarantine_files
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting quarantine files: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@sync_bp.route('/quarantine/<filename>/restore', methods=['POST'])
def restore_quarantine_file(filename):
    """Restore a file from quarantine"""
    try:
        result = sync_service.restore_quarantine_file(filename)
        
        if result['success']:
            return jsonify(result), 200
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"Error restoring quarantine file: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@sync_bp.route('/quarantine/<filename>/delete', methods=['DELETE'])
def delete_quarantine_file(filename):
    """Permanently delete a quarantined file"""
    try:
        result = sync_service.delete_quarantine_file(filename)
        
        if result['success']:
            return jsonify(result), 200
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"Error deleting quarantine file: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@sync_bp.route('/cleanup', methods=['POST'])
def cleanup_failed_files():
    """Clean up failed/stuck files"""
    try:
        cleaned_count = sync_service.cleanup_failed_files()
        
        return jsonify({
            'success': True,
            'message': f'Cleaned up {cleaned_count} files',
            'cleaned_count': cleaned_count
        }), 200
        
    except Exception as e:
        logger.error(f"Error cleaning up files: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@sync_bp.route('/optimize', methods=['POST'])
def optimize_library():
    """Optimize music library"""
    try:
        if current_sync_progress['is_running']:
            return jsonify({
                'success': False,
                'error': 'Cannot optimize while sync is in progress'
            }), 409
        
        results = sync_service.optimize_library()
        
        return jsonify({
            'success': True,
            'message': 'Library optimization completed',
            'results': results
        }), 200
        
    except Exception as e:
        logger.error(f"Error optimizing library: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@sync_bp.route('/validate', methods=['POST'])
def validate_files():
    """Validate files in unsynced folder before sync"""
    try:
        data = request.get_json() or {}
        filenames = data.get('filenames', [])
        
        validation_results = []
        
        if not filenames:
            # Validate all files in unsynced folder
            import os
            unsynced_folder = sync_service.unsynced_folder
            if os.path.exists(unsynced_folder):
                filenames = [f for f in os.listdir(unsynced_folder) 
                           if os.path.isfile(os.path.join(unsynced_folder, f))]
        
        for filename in filenames:
            file_path = os.path.join(sync_service.unsynced_folder, filename)
            
            validation_result = {
                'filename': filename,
                'valid': False,
                'issues': [],
                'metadata': {},
                'file_size': 0
            }
            
            try:
                if not os.path.exists(file_path):
                    validation_result['issues'].append('File not found')
                    validation_results.append(validation_result)
                    continue
                
                validation_result['file_size'] = os.path.getsize(file_path)
                
                # Check file extension
                file_ext = os.path.splitext(filename)[1][1:].lower()
                if file_ext not in sync_service.allowed_extensions:
                    validation_result['issues'].append(f'Invalid file format: {file_ext}')
                
                # Check file size
                if validation_result['file_size'] == 0:
                    validation_result['issues'].append('Empty file')
                elif validation_result['file_size'] > getattr(sync_service, 'max_file_size', 100 * 1024 * 1024):
                    validation_result['issues'].append('File too large')
                
                # Try to extract metadata
                try:
                    metadata = sync_service.get_song_metadata(file_path)
                    if metadata:
                        validation_result['metadata'] = metadata
                        if metadata.get('duration', 0) <= 0:
                            validation_result['issues'].append('Invalid audio duration')
                    else:
                        validation_result['issues'].append('Could not extract metadata')
                except Exception as e:
                    validation_result['issues'].append(f'Metadata error: {str(e)}')
                
                # Check for duplicates
                if validation_result['file_size'] > 0:
                    checksum = sync_service._calculate_checksum(file_path)
                    if sync_service.is_duplicate_song(filename, validation_result['file_size'], checksum):
                        validation_result['issues'].append('Duplicate file')
                
                # Mark as valid if no issues
                validation_result['valid'] = len(validation_result['issues']) == 0
                
            except Exception as e:
                validation_result['issues'].append(f'Validation error: {str(e)}')
            
            validation_results.append(validation_result)
        
        # Summary
        total_files = len(validation_results)
        valid_files = sum(1 for r in validation_results if r['valid'])
        invalid_files = total_files - valid_files
        
        return jsonify({
            'success': True,
            'summary': {
                'total_files': total_files,
                'valid_files': valid_files,
                'invalid_files': invalid_files
            },
            'validation_results': validation_results
        }), 200
        
    except Exception as e:
        logger.error(f"Error validating files: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@sync_bp.route('/config', methods=['GET'])
def get_sync_config():
    """Get sync service configuration"""
    try:
        config = {
            'unsynced_folder': sync_service.unsynced_folder,
            'synced_folder': sync_service.synced_folder,
            'allowed_extensions': list(sync_service.allowed_extensions),
            'max_file_size': getattr(sync_service, 'max_file_size', 100 * 1024 * 1024),
            'database_path': sync_service.db_path
        }
        
        return jsonify({
            'success': True,
            'config': config
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting sync config: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

def _run_sync_with_progress():
    """Run sync process with progress tracking"""
    global current_sync_progress
    
    try:
        current_sync_progress.update({
            'is_running': True,
            'progress': 0,
            'current_file': '',
            'total_files': 0,
            'processed_files': 0,
            'results': {}
        })
        
        def progress_callback(processed, total, current_file):
            """Progress callback for sync process"""
            current_sync_progress.update({
                'progress': int((processed / total) * 100) if total > 0 else 0,
                'current_file': current_file,
                'total_files': total,
                'processed_files': processed
            })
            
            # Emit progress via WebSocket if available
            try:
                from flask_socketio import emit
                emit('sync_progress', current_sync_progress, broadcast=True)
            except:
                pass  # WebSocket not available
        
        # Run sync with progress callback
        results = sync_service.sync_with_progress_callback(progress_callback)
        
        current_sync_progress.update({
            'is_running': False,
            'progress': 100,
            'current_file': '',
            'results': results
        })
        
        # Emit completion via WebSocket if available
        try:
            from flask_socketio import emit
            emit('sync_completed', current_sync_progress, broadcast=True)
        except:
            pass
        
        logger.info(f"Sync completed with results: {results}")
        
    except Exception as e:
        logger.error(f"Error in sync process: {e}")
        current_sync_progress.update({
            'is_running': False,
            'progress': 0,
            'current_file': '',
            'results': {'error': str(e)}
        })
        
        # Emit error via WebSocket if available
        try:
            from flask_socketio import emit
            emit('sync_error', {'error': str(e)}, broadcast=True)
        except:
            pass

# WebSocket events (if Flask-SocketIO is available)
try:
    from flask_socketio import SocketIO
    
    def register_sync_websocket_events(socketio: SocketIO):
        """Register WebSocket events for sync progress"""
        
        @socketio.on('request_sync_status')
        def handle_sync_status_request():
            """Handle request for current sync status"""
            emit('sync_status', current_sync_progress)
        
        @socketio.on('start_sync')
        def handle_start_sync():
            """Handle WebSocket sync start request"""
            if not current_sync_progress['is_running']:
                sync_thread = threading.Thread(target=_run_sync_with_progress, daemon=True)
                sync_thread.start()
                emit('sync_started', {'message': 'Sync process started'})
            else:
                emit('sync_error', {'error': 'Sync is already in progress'})
    
except ImportError:
    # Flask-SocketIO not available
    def register_sync_websocket_events(socketio):
        pass

# Health check endpoint
@sync_bp.route('/health', methods=['GET'])
def sync_health_check():
    """Health check for sync service"""
    try:
        import os
        
        health_status = {
            'service_status': 'healthy',
            'unsynced_folder_exists': os.path.exists(sync_service.unsynced_folder),
            'synced_folder_exists': os.path.exists(sync_service.synced_folder),
            'database_accessible': False,
            'sync_in_progress': current_sync_progress['is_running']
        }
        
        # Test database connection
        try:
            stats = sync_service.get_sync_stats()
            health_status['database_accessible'] = bool(stats)
        except:
            health_status['service_status'] = 'degraded'
        
        # Check folder permissions
        try:
            if health_status['unsynced_folder_exists']:
                health_status['unsynced_folder_writable'] = os.access(sync_service.unsynced_folder, os.W_OK)
            if health_status['synced_folder_exists']:
                health_status['synced_folder_writable'] = os.access(sync_service.synced_folder, os.W_OK)
        except:
            health_status['service_status'] = 'degraded'
        
        status_code = 200 if health_status['service_status'] == 'healthy' else 503
        
        return jsonify({
            'success': True,
            'health': health_status
        }), status_code
        
    except Exception as e:
        logger.error(f"Sync health check failed: {e}")
        return jsonify({
            'success': False,
            'health': {
                'service_status': 'unhealthy',
                'error': str(e)
            }
        }), 503