#!/usr/bin/env python3
"""
Main Flask application for Resort Music Player
Integrates all services and provides unified API
"""

import os
import sys
import logging
import threading
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO

# Add backend directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import Config
from enhanced_music_player import get_music_player
from api.upload_routes import upload_bp
from api.storage_routes import storage_bp
from api.auth_routes import auth_bp
from api.sync_routes import sync_bp, register_sync_websocket_events

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Config.LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = Config.SECRET_KEY
app.config['MAX_CONTENT_LENGTH'] = Config.MAX_UPLOAD_SIZE

# Enable CORS for all routes
CORS(app, origins="*")

# Initialize SocketIO for real-time updates
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Register API blueprints
app.register_blueprint(upload_bp)
app.register_blueprint(storage_bp)
app.register_blueprint(auth_bp)
app.register_blueprint(sync_bp)

# Register WebSocket events for real-time updates
register_sync_websocket_events(socketio)

# Initialize music player (global instance)
music_player = None

def initialize_music_player():
    """Initialize the music player system"""
    global music_player
    try:
        music_player = get_music_player()
        logger.info("Music player initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize music player: {e}")
        return False

# Core API routes
@app.route('/')
def index():
    """API root endpoint"""
    return {
        'message': 'Resort Music Player API',
        'status': 'running',
        'version': '1.0.0',
        'endpoints': {
            'status': '/api/status',
            'control': '/api/control',
            'stats': '/api/stats',
            'upload': '/api/upload/*',
            'storage': '/api/storage/*',
            'auth': '/api/auth/*',
            'sync': '/api/sync/*'
        }
    }

@app.route('/api/status')
def get_status():
    """Get comprehensive system status"""
    try:
        if not music_player:
            return jsonify({
                'success': False,
                'error': 'Music player not initialized'
            }), 503
        
        status = music_player.get_enhanced_status()
        return jsonify({
            'success': True,
            'status': status,
            'timestamp': status.get('current_time')
        })
        
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/control', methods=['POST'])
def control_player():
    """Control music player (play, pause, skip, volume)"""
    try:
        if not music_player:
            return jsonify({
                'success': False,
                'error': 'Music player not initialized'
            }), 503
        
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'error': 'No data provided'
            }), 400
        
        action = data.get('action')
        if not action:
            return jsonify({
                'success': False,
                'error': 'Action is required'
            }), 400
        
        if action == 'play':
            music_player.is_playing = True
            logger.info("Playback started via API")
            return jsonify({
                'success': True,
                'message': 'Playback started'
            })
            
        elif action == 'pause':
            music_player.stop_music()
            logger.info("Playback paused via API")
            return jsonify({
                'success': True,
                'message': 'Playback paused'
            })
            
        elif action == 'skip':
            music_player.stop_music()
            logger.info("Song skipped via API")
            return jsonify({
                'success': True,
                'message': 'Song skipped'
            })
            
        elif action == 'volume':
            volume = data.get('volume')
            if volume is None:
                return jsonify({
                    'success': False,
                    'error': 'Volume value is required'
                }), 400
            
            try:
                volume = float(volume)
                if not 0.0 <= volume <= 1.0:
                    return jsonify({
                        'success': False,
                        'error': 'Volume must be between 0.0 and 1.0'
                    }), 400
                
                music_player.set_volume(volume)
                logger.info(f"Volume set to {volume} via API")
                return jsonify({
                    'success': True,
                    'message': f'Volume set to {volume}'
                })
                
            except ValueError:
                return jsonify({
                    'success': False,
                    'error': 'Invalid volume value'
                }), 400
        
        elif action == 'switch_storage':
            storage_type = data.get('storage_type')
            if storage_type not in ['primary', 'fallback']:
                return jsonify({
                    'success': False,
                    'error': 'storage_type must be "primary" or "fallback"'
                }), 400
            
            result = music_player.force_storage_switch(storage_type)
            return jsonify(result)
        
        else:
            return jsonify({
                'success': False,
                'error': f'Unknown action: {action}'
            }), 400
            
    except Exception as e:
        logger.error(f"Error controlling player: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/stats')
def get_stats():
    """Get music library and system statistics"""
    try:
        if not music_player:
            return jsonify({
                'success': False,
                'error': 'Music player not initialized'
            }), 503
        
        # Get music library stats
        with music_player.db.get_connection() as conn:
            # Total songs
            cursor = conn.execute('SELECT COUNT(*) as total FROM songs WHERE is_available = TRUE')
            total_songs = cursor.fetchone()['total']
            
            # Most played songs
            cursor = conn.execute('''
                SELECT filename, title, artist, play_count 
                FROM songs 
                WHERE is_available = TRUE
                ORDER BY play_count DESC 
                LIMIT 10
            ''')
            top_songs = [dict(row) for row in cursor.fetchall()]
            
            # Recent plays
            cursor = conn.execute('''
                SELECT s.filename, s.title, s.artist, ph.played_at, ph.storage_used
                FROM play_history ph
                JOIN songs s ON ph.song_id = s.id
                ORDER BY ph.played_at DESC
                LIMIT 20
            ''')
            recent_plays = [dict(row) for row in cursor.fetchall()]
            
            # Storage distribution
            cursor = conn.execute('''
                SELECT storage_location, COUNT(*) as count
                FROM songs 
                WHERE is_available = TRUE
                GROUP BY storage_location
            ''')
            storage_distribution = [dict(row) for row in cursor.fetchall()]
            
            # Format distribution
            cursor = conn.execute('''
                SELECT format, COUNT(*) as count, AVG(quality_score) as avg_quality
                FROM songs 
                WHERE is_available = TRUE
                GROUP BY format
                ORDER BY count DESC
            ''')
            format_distribution = [dict(row) for row in cursor.fetchall()]
        
        # Get storage metrics
        storage_metrics = music_player.get_storage_metrics()
        
        return jsonify({
            'success': True,
            'stats': {
                'library': {
                    'total_songs': total_songs,
                    'top_songs': top_songs,
                    'recent_plays': recent_plays,
                    'storage_distribution': storage_distribution,
                    'format_distribution': format_distribution
                },
                'storage': storage_metrics,
                'system': {
                    'current_storage': music_player.current_storage_mode,
                    'playback_errors': music_player.playback_errors,
                    'is_playing': music_player.is_playing,
                    'volume': music_player.volume
                }
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/health')
def health_check():
    """Comprehensive system health check"""
    try:
        health_status = {
            'status': 'healthy',
            'timestamp': Config.DATABASE_PATH,
            'components': {}
        }
        
        # Check music player
        if music_player:
            health_status['components']['music_player'] = {
                'status': 'healthy',
                'is_playing': music_player.is_playing,
                'current_storage': music_player.current_storage_mode
            }
        else:
            health_status['components']['music_player'] = {
                'status': 'unhealthy',
                'error': 'Music player not initialized'
            }
            health_status['status'] = 'degraded'
        
        # Check database
        try:
            with music_player.db.get_connection() as conn:
                cursor = conn.execute('SELECT COUNT(*) FROM songs')
                song_count = cursor.fetchone()[0]
            
            health_status['components']['database'] = {
                'status': 'healthy',
                'song_count': song_count
            }
        except Exception as e:
            health_status['components']['database'] = {
                'status': 'unhealthy',
                'error': str(e)
            }
            health_status['status'] = 'degraded'
        
        # Check storage
        try:
            storage_health = music_player.storage_monitor.check_storage_health()
            health_status['components']['storage'] = {
                'status': storage_health['overall_health'],
                'current_storage': storage_health['current_storage'],
                'primary_available': storage_health['primary']['is_available'] if storage_health['primary'] else False,
                'fallback_available': storage_health['fallback']['is_available'] if storage_health['fallback'] else False
            }
            
            if storage_health['overall_health'] != 'healthy':
                health_status['status'] = 'degraded'
                
        except Exception as e:
            health_status['components']['storage'] = {
                'status': 'unhealthy',
                'error': str(e)
            }
            health_status['status'] = 'degraded'
        
        # Check directories
        directories_to_check = [
            Config.SYNCED_FOLDER,
            Config.UNSYNCED_FOLDER,
            Config.BACKUP_FOLDER
        ]
        
        directory_status = {}
        for directory in directories_to_check:
            directory_status[directory] = {
                'exists': os.path.exists(directory),
                'writable': os.access(directory, os.W_OK) if os.path.exists(directory) else False
            }
        
        health_status['components']['directories'] = directory_status
        
        status_code = 200 if health_status['status'] == 'healthy' else 503
        return jsonify(health_status), status_code
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 503

# WebSocket events for real-time updates
@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    logger.info(f"Client connected: {request.sid}")
    
    # Send current status to newly connected client
    if music_player:
        try:
            status = music_player.get_enhanced_status()
            socketio.emit('status_update', status, room=request.sid)
        except Exception as e:
            logger.error(f"Error sending status to new client: {e}")

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    logger.info(f"Client disconnected: {request.sid}")

@socketio.on('request_status')
def handle_status_request():
    """Handle status request from client"""
    if music_player:
        try:
            status = music_player.get_enhanced_status()
            socketio.emit('status_update', status, room=request.sid)
        except Exception as e:
            logger.error(f"Error sending status update: {e}")
            socketio.emit('error', {'message': str(e)}, room=request.sid)

# Background task to broadcast status updates
def broadcast_status_updates():
    """Broadcast status updates to all connected clients"""
    while True:
        try:
            if music_player:
                status = music_player.get_enhanced_status()
                socketio.emit('status_update', status, broadcast=True)
            socketio.sleep(5)  # Update every 5 seconds
        except Exception as e:
            logger.error(f"Error broadcasting status: {e}")
            socketio.sleep(10)  # Wait longer on error

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'error': 'Endpoint not found'
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'success': False,
        'error': 'Internal server error'
    }), 500

@app.errorhandler(413)
def request_entity_too_large(error):
    return jsonify({
        'success': False,
        'error': 'File too large'
    }), 413

def main():
    """Main application entry point"""
    try:
        # Create necessary directories
        Config.create_directories()
        
        # Validate configuration
        config_errors = Config.validate_config()
        if config_errors:
            logger.warning(f"Configuration issues: {config_errors}")
        
        # Initialize music player
        if not initialize_music_player():
            logger.error("Failed to initialize music player")
            sys.exit(1)
        
        # Start music player in background thread
        music_thread = threading.Thread(target=music_player.music_loop, daemon=True)
        music_thread.start()
        logger.info("Music player thread started")
        
        # Start status broadcast thread
        status_thread = threading.Thread(target=broadcast_status_updates, daemon=True)
        status_thread.start()
        logger.info("Status broadcast thread started")
        
        logger.info(f"Starting Resort Music Player API on {Config.WEB_HOST}:{Config.WEB_PORT}")
        
        # Start the Flask-SocketIO server
        socketio.run(
            app,
            host=Config.WEB_HOST,
            port=Config.WEB_PORT,
            debug=False,
            use_reloader=False
        )
        
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
        if music_player:
            music_player.stop_monitoring_services()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()