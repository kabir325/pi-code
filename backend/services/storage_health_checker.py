import os
import time
import sqlite3
import logging
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from config import Config
from services.storage_monitor import StorageMonitor

logger = logging.getLogger(__name__)

class StorageHealthChecker:
    """Advanced storage health checker with detailed diagnostics and proactive monitoring"""
    
    def __init__(self, storage_monitor: StorageMonitor = None):
        self.storage_monitor = storage_monitor or StorageMonitor()
        self.db_path = Config.DATABASE_PATH
        
        # Health check configuration
        self.io_test_file_size = 1024 * 1024  # 1MB test file
        self.max_io_time = 5.0  # Maximum acceptable I/O time in seconds
        self.health_check_interval = 60  # Check every minute
        
        # Health status tracking
        self.last_health_check = {}
        self.consecutive_failures = {}
        self.is_monitoring = False
        self.monitor_thread = None
        
        # Initialize health tracking
        self._init_health_tracking()
    
    def _init_health_tracking(self):
        """Initialize health tracking database tables"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS storage_health_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    storage_type TEXT NOT NULL,
                    check_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    response_time_ms INTEGER,
                    error_message TEXT,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS storage_alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    storage_type TEXT NOT NULL,
                    alert_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    message TEXT NOT NULL,
                    resolved BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at TIMESTAMP
                )
            ''')
            
            conn.commit()
    
    def perform_comprehensive_health_check(self, storage_type: str) -> Dict:
        """Perform comprehensive health check on storage"""
        if storage_type == 'primary':
            storage_path = Config.SYNCED_FOLDER
        elif storage_type == 'fallback':
            storage_path = Config.BACKUP_FOLDER
        else:
            return {'success': False, 'error': 'Invalid storage type'}
        
        health_results = {
            'storage_type': storage_type,
            'storage_path': storage_path,
            'overall_status': 'healthy',
            'checks': {},
            'alerts': [],
            'recommendations': []
        }
        
        try:
            # 1. Basic availability check
            availability_result = self._check_storage_availability(storage_path)
            health_results['checks']['availability'] = availability_result
            
            if not availability_result['passed']:
                health_results['overall_status'] = 'error'
                health_results['alerts'].append({
                    'type': 'availability',
                    'severity': 'critical',
                    'message': 'Storage is not available'
                })
                return health_results
            
            # 2. I/O performance check
            io_result = self._check_io_performance(storage_path)
            health_results['checks']['io_performance'] = io_result
            
            if not io_result['passed']:
                health_results['overall_status'] = 'warning'
                health_results['alerts'].append({
                    'type': 'performance',
                    'severity': 'warning',
                    'message': f'Slow I/O performance: {io_result["response_time_ms"]}ms'
                })
            
            # 3. Space utilization check
            space_result = self._check_space_utilization(storage_path)
            health_results['checks']['space_utilization'] = space_result
            
            if not space_result['passed']:
                severity = 'critical' if space_result['usage_percent'] > 95 else 'warning'
                health_results['overall_status'] = 'warning' if severity == 'warning' else 'error'
                health_results['alerts'].append({
                    'type': 'space',
                    'severity': severity,
                    'message': f'High disk usage: {space_result["usage_percent"]:.1f}%'
                })
            
            # 4. File system integrity check
            integrity_result = self._check_file_system_integrity(storage_path)
            health_results['checks']['file_system_integrity'] = integrity_result
            
            if not integrity_result['passed']:
                health_results['overall_status'] = 'error'
                health_results['alerts'].append({
                    'type': 'integrity',
                    'severity': 'critical',
                    'message': 'File system integrity issues detected'
                })
            
            # 5. Music file accessibility check
            music_files_result = self._check_music_files_accessibility(storage_path)
            health_results['checks']['music_files'] = music_files_result
            
            if not music_files_result['passed']:
                health_results['overall_status'] = 'warning'
                health_results['alerts'].append({
                    'type': 'music_files',
                    'severity': 'warning',
                    'message': f'{music_files_result["inaccessible_count"]} music files are inaccessible'
                })
            
            # Generate recommendations
            health_results['recommendations'] = self._generate_recommendations(health_results)
            
            # Log health check results
            self._log_health_check_results(storage_type, health_results)
            
            return health_results
            
        except Exception as e:
            logger.error(f"Error during comprehensive health check: {e}")
            health_results['overall_status'] = 'error'
            health_results['error'] = str(e)
            return health_results
    
    def _check_storage_availability(self, storage_path: str) -> Dict:
        """Check if storage is available and accessible"""
        start_time = time.time()
        
        try:
            # Check if path exists
            if not os.path.exists(storage_path):
                return {
                    'passed': False,
                    'response_time_ms': int((time.time() - start_time) * 1000),
                    'error': 'Storage path does not exist'
                }
            
            # Check read access
            if not os.access(storage_path, os.R_OK):
                return {
                    'passed': False,
                    'response_time_ms': int((time.time() - start_time) * 1000),
                    'error': 'No read access to storage'
                }
            
            # Check write access
            if not os.access(storage_path, os.W_OK):
                return {
                    'passed': False,
                    'response_time_ms': int((time.time() - start_time) * 1000),
                    'error': 'No write access to storage'
                }
            
            return {
                'passed': True,
                'response_time_ms': int((time.time() - start_time) * 1000),
                'message': 'Storage is available and accessible'
            }
            
        except Exception as e:
            return {
                'passed': False,
                'response_time_ms': int((time.time() - start_time) * 1000),
                'error': str(e)
            }
    
    def _check_io_performance(self, storage_path: str) -> Dict:
        """Check I/O performance by writing and reading a test file"""
        start_time = time.time()
        test_file_path = os.path.join(storage_path, '.health_check_test')
        
        try:
            # Write test
            write_start = time.time()
            test_data = b'x' * self.io_test_file_size
            with open(test_file_path, 'wb') as f:
                f.write(test_data)
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
            write_time = (time.time() - write_start) * 1000
            
            # Read test
            read_start = time.time()
            with open(test_file_path, 'rb') as f:
                read_data = f.read()
            read_time = (time.time() - read_start) * 1000
            
            # Cleanup
            os.remove(test_file_path)
            
            total_time = int((time.time() - start_time) * 1000)
            
            # Check if performance is acceptable
            passed = total_time < (self.max_io_time * 1000)
            
            return {
                'passed': passed,
                'response_time_ms': total_time,
                'write_time_ms': int(write_time),
                'read_time_ms': int(read_time),
                'throughput_mbps': round((self.io_test_file_size / (1024 * 1024)) / (total_time / 1000), 2),
                'message': f'I/O test completed in {total_time}ms'
            }
            
        except Exception as e:
            # Cleanup on error
            try:
                if os.path.exists(test_file_path):
                    os.remove(test_file_path)
            except:
                pass
            
            return {
                'passed': False,
                'response_time_ms': int((time.time() - start_time) * 1000),
                'error': str(e)
            }
    
    def _check_space_utilization(self, storage_path: str) -> Dict:
        """Check disk space utilization"""
        start_time = time.time()
        
        try:
            import shutil
            usage = shutil.disk_usage(storage_path)
            
            total_gb = usage.total / (1024**3)
            free_gb = usage.free / (1024**3)
            used_gb = (usage.total - usage.free) / (1024**3)
            usage_percent = (used_gb / total_gb) * 100
            
            # Check against warning threshold
            warning_threshold = Config.STORAGE_WARNING_THRESHOLD * 100
            passed = usage_percent < warning_threshold
            
            return {
                'passed': passed,
                'response_time_ms': int((time.time() - start_time) * 1000),
                'total_gb': round(total_gb, 2),
                'used_gb': round(used_gb, 2),
                'free_gb': round(free_gb, 2),
                'usage_percent': round(usage_percent, 1),
                'warning_threshold': warning_threshold,
                'message': f'Disk usage: {usage_percent:.1f}% ({used_gb:.1f}GB / {total_gb:.1f}GB)'
            }
            
        except Exception as e:
            return {
                'passed': False,
                'response_time_ms': int((time.time() - start_time) * 1000),
                'error': str(e)
            }
    
    def _check_file_system_integrity(self, storage_path: str) -> Dict:
        """Check file system integrity by testing directory operations"""
        start_time = time.time()
        test_dir_path = os.path.join(storage_path, '.health_check_dir')
        
        try:
            # Test directory creation
            os.makedirs(test_dir_path, exist_ok=True)
            
            # Test file creation in directory
            test_file_path = os.path.join(test_dir_path, 'test_file.txt')
            with open(test_file_path, 'w') as f:
                f.write('integrity test')
            
            # Test file reading
            with open(test_file_path, 'r') as f:
                content = f.read()
            
            if content != 'integrity test':
                raise Exception("File content mismatch")
            
            # Test file deletion
            os.remove(test_file_path)
            
            # Test directory deletion
            os.rmdir(test_dir_path)
            
            return {
                'passed': True,
                'response_time_ms': int((time.time() - start_time) * 1000),
                'message': 'File system integrity check passed'
            }
            
        except Exception as e:
            # Cleanup on error
            try:
                if os.path.exists(test_file_path):
                    os.remove(test_file_path)
                if os.path.exists(test_dir_path):
                    os.rmdir(test_dir_path)
            except:
                pass
            
            return {
                'passed': False,
                'response_time_ms': int((time.time() - start_time) * 1000),
                'error': str(e)
            }
    
    def _check_music_files_accessibility(self, storage_path: str) -> Dict:
        """Check accessibility of music files in storage"""
        start_time = time.time()
        
        try:
            music_files = []
            inaccessible_files = []
            
            # Get list of music files
            for root, dirs, files in os.walk(storage_path):
                for file in files:
                    if file.lower().endswith(('.mp3', '.wav', '.flac', '.m4a', '.aac', '.ogg')):
                        file_path = os.path.join(root, file)
                        music_files.append(file_path)
                        
                        # Test file accessibility
                        try:
                            # Quick read test
                            with open(file_path, 'rb') as f:
                                f.read(1024)  # Read first 1KB
                        except Exception:
                            inaccessible_files.append(file_path)
            
            total_files = len(music_files)
            inaccessible_count = len(inaccessible_files)
            accessible_count = total_files - inaccessible_count
            
            # Consider passed if less than 5% of files are inaccessible
            passed = (inaccessible_count / max(total_files, 1)) < 0.05
            
            return {
                'passed': passed,
                'response_time_ms': int((time.time() - start_time) * 1000),
                'total_files': total_files,
                'accessible_files': accessible_count,
                'inaccessible_count': inaccessible_count,
                'inaccessible_files': inaccessible_files[:10],  # Limit to first 10
                'message': f'Music files check: {accessible_count}/{total_files} accessible'
            }
            
        except Exception as e:
            return {
                'passed': False,
                'response_time_ms': int((time.time() - start_time) * 1000),
                'error': str(e)
            }
    
    def _generate_recommendations(self, health_results: Dict) -> List[str]:
        """Generate recommendations based on health check results"""
        recommendations = []
        
        # Space recommendations
        space_check = health_results['checks'].get('space_utilization', {})
        if space_check.get('usage_percent', 0) > 80:
            recommendations.append("Consider cleaning up old or unused music files")
            if space_check.get('usage_percent', 0) > 90:
                recommendations.append("Urgent: Free up disk space immediately")
        
        # Performance recommendations
        io_check = health_results['checks'].get('io_performance', {})
        if io_check.get('response_time_ms', 0) > 2000:
            recommendations.append("Storage I/O performance is slow - consider checking disk health")
        
        # Music files recommendations
        music_check = health_results['checks'].get('music_files', {})
        if music_check.get('inaccessible_count', 0) > 0:
            recommendations.append("Some music files are inaccessible - run file system check")
        
        # General recommendations
        if health_results['overall_status'] == 'error':
            recommendations.append("Critical issues detected - immediate attention required")
        elif health_results['overall_status'] == 'warning':
            recommendations.append("Monitor storage closely and address warnings promptly")
        
        return recommendations
    
    def _log_health_check_results(self, storage_type: str, results: Dict):
        """Log health check results to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Log individual check results
                for check_type, check_result in results['checks'].items():
                    status = 'passed' if check_result.get('passed', False) else 'failed'
                    response_time = check_result.get('response_time_ms', 0)
                    error_message = check_result.get('error')
                    
                    conn.execute('''
                        INSERT INTO storage_health_checks 
                        (storage_type, check_type, status, response_time_ms, error_message)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (storage_type, check_type, status, response_time, error_message))
                
                # Log alerts
                for alert in results['alerts']:
                    conn.execute('''
                        INSERT INTO storage_alerts 
                        (storage_type, alert_type, severity, message)
                        VALUES (?, ?, ?, ?)
                    ''', (storage_type, alert['type'], alert['severity'], alert['message']))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error logging health check results: {e}")
    
    def get_health_history(self, storage_type: str, hours: int = 24) -> Dict:
        """Get health check history for a storage type"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                # Get health checks
                cursor = conn.execute('''
                    SELECT * FROM storage_health_checks 
                    WHERE storage_type = ? AND checked_at > ?
                    ORDER BY checked_at DESC
                ''', (storage_type, cutoff_time.isoformat()))
                
                health_checks = [dict(row) for row in cursor.fetchall()]
                
                # Get alerts
                cursor = conn.execute('''
                    SELECT * FROM storage_alerts 
                    WHERE storage_type = ? AND created_at > ?
                    ORDER BY created_at DESC
                ''', (storage_type, cutoff_time.isoformat()))
                
                alerts = [dict(row) for row in cursor.fetchall()]
                
                return {
                    'storage_type': storage_type,
                    'period_hours': hours,
                    'health_checks': health_checks,
                    'alerts': alerts,
                    'total_checks': len(health_checks),
                    'total_alerts': len(alerts)
                }
                
        except Exception as e:
            logger.error(f"Error getting health history: {e}")
            return {}
    
    def get_current_alerts(self, resolved: bool = False) -> List[Dict]:
        """Get current unresolved alerts"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute('''
                    SELECT * FROM storage_alerts 
                    WHERE resolved = ?
                    ORDER BY created_at DESC
                ''', (resolved,))
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting current alerts: {e}")
            return []
    
    def resolve_alert(self, alert_id: int) -> bool:
        """Mark an alert as resolved"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    UPDATE storage_alerts 
                    SET resolved = TRUE, resolved_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (alert_id,))
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error resolving alert: {e}")
            return False
    
    def start_continuous_monitoring(self):
        """Start continuous health monitoring"""
        if self.is_monitoring:
            return
        
        self.is_monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("Storage health monitoring started")
    
    def stop_continuous_monitoring(self):
        """Stop continuous health monitoring"""
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("Storage health monitoring stopped")
    
    def _monitoring_loop(self):
        """Continuous monitoring loop"""
        while self.is_monitoring:
            try:
                # Check both primary and fallback storage
                for storage_type in ['primary', 'fallback']:
                    health_result = self.perform_comprehensive_health_check(storage_type)
                    
                    # Track consecutive failures
                    if health_result['overall_status'] == 'error':
                        self.consecutive_failures[storage_type] = self.consecutive_failures.get(storage_type, 0) + 1
                    else:
                        self.consecutive_failures[storage_type] = 0
                    
                    # Trigger storage switch if primary fails multiple times
                    if (storage_type == 'primary' and 
                        self.consecutive_failures[storage_type] >= 3 and
                        self.storage_monitor.current_storage == 'primary'):
                        
                        logger.warning("Primary storage failing consistently, attempting switch to fallback")
                        self.storage_monitor.auto_switch_storage()
                
                # Sleep until next check
                time.sleep(self.health_check_interval)
                
            except Exception as e:
                logger.error(f"Error in health monitoring loop: {e}")
                time.sleep(self.health_check_interval)
    
    def cleanup_old_health_data(self, days: int = 7) -> Dict:
        """Clean up old health check data"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            with sqlite3.connect(self.db_path) as conn:
                # Clean up old health checks
                cursor = conn.execute('''
                    DELETE FROM storage_health_checks 
                    WHERE checked_at < ?
                ''', (cutoff_date.isoformat(),))
                health_checks_deleted = cursor.rowcount
                
                # Clean up old resolved alerts
                cursor = conn.execute('''
                    DELETE FROM storage_alerts 
                    WHERE resolved = TRUE AND resolved_at < ?
                ''', (cutoff_date.isoformat(),))
                alerts_deleted = cursor.rowcount
                
                conn.commit()
                
                logger.info(f"Cleaned up {health_checks_deleted} health checks and {alerts_deleted} alerts")
                
                return {
                    'health_checks_deleted': health_checks_deleted,
                    'alerts_deleted': alerts_deleted
                }
                
        except Exception as e:
            logger.error(f"Error cleaning up health data: {e}")
            return {'error': str(e)}