from flask import Blueprint, request, jsonify
import logging

from services.storage_monitor import StorageMonitor

logger = logging.getLogger(__name__)

storage_bp = Blueprint('storage', __name__, url_prefix='/api/storage')

# Initialize storage monitor
storage_monitor = StorageMonitor()

@storage_bp.route('/status', methods=['GET'])
def get_storage_status():
    """Get current storage status and health information"""
    try:
        health_check = storage_monitor.check_storage_health()
        
        return jsonify({
            'success': True,
            'current_storage': health_check['current_storage'],
            'overall_health': health_check['overall_health'],
            'primary': health_check['primary'],
            'fallback': health_check['fallback'],
            'should_switch': health_check['should_switch'],
            'recommended_storage': health_check['recommended_storage']
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting storage status: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@storage_bp.route('/metrics', methods=['GET'])
def get_storage_metrics():
    """Get comprehensive storage metrics and statistics"""
    try:
        metrics = storage_monitor.get_storage_metrics()
        
        return jsonify({
            'success': True,
            'metrics': metrics
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting storage metrics: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@storage_bp.route('/switch', methods=['POST'])
def switch_storage():
    """Manually switch between primary and fallback storage"""
    try:
        data = request.get_json()
        if not data or 'storage_type' not in data:
            return jsonify({
                'success': False, 
                'error': 'storage_type is required (primary or fallback)'
            }), 400
        
        storage_type = data['storage_type']
        if storage_type not in ['primary', 'fallback']:
            return jsonify({
                'success': False, 
                'error': 'storage_type must be "primary" or "fallback"'
            }), 400
        
        result = storage_monitor.switch_storage(storage_type)
        
        if result['success']:
            return jsonify(result), 200
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"Error switching storage: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@storage_bp.route('/auto-switch', methods=['POST'])
def auto_switch_storage():
    """Trigger automatic storage switching if needed"""
    try:
        result = storage_monitor.auto_switch_storage()
        
        if result:
            return jsonify({
                'success': True,
                'switched': True,
                'result': result
            }), 200
        else:
            return jsonify({
                'success': True,
                'switched': False,
                'message': 'No storage switch needed'
            }), 200
            
    except Exception as e:
        logger.error(f"Error in auto-switch: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@storage_bp.route('/events', methods=['GET'])
def get_storage_events():
    """Get recent storage events"""
    try:
        limit = request.args.get('limit', 50, type=int)
        events = storage_monitor.get_storage_events(limit)
        
        return jsonify({
            'success': True,
            'events': events
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting storage events: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@storage_bp.route('/check', methods=['POST'])
def force_storage_check():
    """Force an immediate storage health check"""
    try:
        result = storage_monitor.force_storage_check()
        
        return jsonify({
            'success': True,
            'check_result': result
        }), 200
        
    except Exception as e:
        logger.error(f"Error forcing storage check: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@storage_bp.route('/monitoring/start', methods=['POST'])
def start_monitoring():
    """Start continuous storage monitoring"""
    try:
        storage_monitor.start_monitoring()
        
        return jsonify({
            'success': True,
            'message': 'Storage monitoring started'
        }), 200
        
    except Exception as e:
        logger.error(f"Error starting monitoring: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@storage_bp.route('/monitoring/stop', methods=['POST'])
def stop_monitoring():
    """Stop storage monitoring"""
    try:
        storage_monitor.stop_monitoring()
        
        return jsonify({
            'success': True,
            'message': 'Storage monitoring stopped'
        }), 200
        
    except Exception as e:
        logger.error(f"Error stopping monitoring: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@storage_bp.route('/monitoring/status', methods=['GET'])
def get_monitoring_status():
    """Get storage monitoring status"""
    try:
        return jsonify({
            'success': True,
            'monitoring_active': storage_monitor.is_monitoring,
            'current_storage': storage_monitor.current_storage,
            'check_interval': storage_monitor.check_interval
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting monitoring status: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@storage_bp.route('/cleanup', methods=['POST'])
def cleanup_old_events():
    """Clean up old storage events (admin endpoint)"""
    try:
        data = request.get_json() or {}
        days = data.get('days', 30)
        
        deleted_count = storage_monitor.cleanup_old_events(days)
        
        return jsonify({
            'success': True,
            'message': f'Cleaned up {deleted_count} old events',
            'deleted_count': deleted_count
        }), 200
        
    except Exception as e:
        logger.error(f"Error cleaning up events: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@storage_bp.route('/info/<storage_type>', methods=['GET'])
def get_storage_info(storage_type):
    """Get detailed information about specific storage"""
    try:
        if storage_type not in ['primary', 'fallback']:
            return jsonify({
                'success': False, 
                'error': 'storage_type must be "primary" or "fallback"'
            }), 400
        
        if storage_type == 'primary':
            path = storage_monitor.primary_path
        else:
            path = storage_monitor.fallback_path
        
        info = storage_monitor.get_storage_info(path)
        
        return jsonify({
            'success': True,
            'storage_type': storage_type,
            'info': info
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting storage info: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@storage_bp.route('/current-path', methods=['GET'])
def get_current_storage_path():
    """Get the path of currently active storage"""
    try:
        current_path = storage_monitor.get_current_storage_path()
        
        return jsonify({
            'success': True,
            'current_storage': storage_monitor.current_storage,
            'current_path': current_path
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting current storage path: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# Initialize storage monitoring on blueprint registration
@storage_bp.record_once
def on_load(state):
    """Initialize storage monitoring when blueprint is registered"""
    try:
        # Start monitoring automatically
        storage_monitor.start_monitoring()
        logger.info("Storage monitoring initialized and started")
    except Exception as e:
        logger.error(f"Error initializing storage monitoring: {e}")