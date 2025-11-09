import os
import shutil
import sqlite3
import psutil
import logging
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from config import Config
from .storage_health_checker import StorageHealthChecker

logger = logging.getLogger(__name__)

class StorageMonitor:
    """Monitors storage health and manages failover between primary and fallback storage"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or Config.DATABASE_PATH
        self.primary_path = Config.SYNCED_FOLDER
        self.fallback_path = Config.BACKUP_FOLDER
        self.check_interval = Config.STORAGE_CHECK_INTERVAL
        self.warning_threshold = Config.STORAGE_WARNING_THRESHOLD
        
        self.current_storage = 'primary'
        self.is_monitoring = False
        self.monitor_thread = None
        
        # Initialize health checker
        self.health_checker = StorageHealthChecker(db_path)
        
        # Initialize database
        self._init_database()
        
        # Initial storage check
        self._update_storage_status()
    
    def _init_database(self):
        """Initialize storage monitoring tables"""
        with sqlite3.connect(self.db_path) as conn:
            # Storage status table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS storage_status (
                    id INTEGER PRIMARY KEY,
                    storage_type TEXT NOT NULL,
                    mount_point TEXT,
                    is_available BOOLEAN DEFAULT FALSE,
                    capacity_gb REAL,
                    used_gb REAL,
                    free_gb REAL,
                    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    health_status TEXT DEFAULT 'unknown'
                )
            ''')
            
            # Storage events table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS storage_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    storage_type TEXT NOT NULL,
                    message TEXT,
                    occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Initialize storage status records
            conn.execute('''
                INSERT OR IGNORE INTO storage_status (id, storage_type, mount_point) 
                VALUES (1, 'primary', ?)
            ''', (self.primary_path,))
            
            conn.execute('''
                INSERT OR IGNORE INTO storage_status (id, storage_type, mount_point) 
                VALUES (2, 'fallback', ?)
            ''', (self.fallback_path,))
            
            conn.commit()
    
    def get_storage_info(self, path: str) -> Optional[Dict]:
        """Get storage information for a given path"""
        try:
            if not os.path.exists(path):
                return None
            
            # Get disk usage
            usage = shutil.disk_usage(path)
            total_gb = usage.total / (1024**3)
            free_gb = usage.free / (1024**3)
            used_gb = (usage.total - usage.free) / (1024**3)
            
            # Check if path is mounted (for external drives)
            is_mounted = self._is_path_mounted(path)
            
            # Determine health status
            usage_percent = used_gb / total_gb if total_gb > 0 else 0
            if usage_percent >= self.warning_threshold:
                health_status = 'warning'
            elif not is_mounted:
                health_status = 'error'
            else:
                health_status = 'healthy'
            
            return {
                'path': path,
                'is_available': is_mounted and os.access(path, os.R_OK | os.W_OK),
                'is_mounted': is_mounted,
                'capacity_gb': round(total_gb, 2),
                'used_gb': round(used_gb, 2),
                'free_gb': round(free_gb, 2),
                'usage_percent': round(usage_percent * 100, 1),
                'health_status': health_status
            }
            
        except Exception as e:
            logger.error(f"Error getting storage info for {path}: {e}")
            return {
                'path': path,
                'is_available': False,
                'is_mounted': False,
                'capacity_gb': 0,
                'used_gb': 0,
                'free_gb': 0,
                'usage_percent': 0,
                'health_status': 'error',
                'error': str(e)
            }
    
    def _is_path_mounted(self, path: str) -> bool:
        """Check if a path is on a mounted filesystem"""
        try:
            # Get all mount points
            mount_points = [mount.mountpoint for mount in psutil.disk_partitions()]
            
            # Check if path or any parent is a mount point
            path_obj = Path(path).resolve()
            for parent in [path_obj] + list(path_obj.parents):
                if str(parent) in mount_points:
                    return True
            
            # If not a specific mount point, check if accessible
            return os.path.exists(path) and os.access(path, os.R_OK)
            
        except Exception as e:
            logger.error(f"Error checking mount status for {path}: {e}")
            return False
    
    def check_storage_health(self) -> Dict:
        """Check health of both primary and fallback storage"""
        primary_info = self.get_storage_info(self.primary_path)
        fallback_info = self.get_storage_info(self.fallback_path)
        
        # Determine current best storage option
        if primary_info and primary_info['is_available']:
            recommended_storage = 'primary'
        elif fallback_info and fallback_info['is_available']:
            recommended_storage = 'fallback'
        else:
            recommended_storage = None
        
        # Check if we need to switch storage
        should_switch = False
        if self.current_storage == 'primary' and (not primary_info or not primary_info['is_available']):
            if fallback_info and fallback_info['is_available']:
                should_switch = True
        elif self.current_storage == 'fallback' and primary_info and primary_info['is_available']:
            # Switch back to primary when it becomes available
            should_switch = True
        
        return {
            'primary': primary_info,
            'fallback': fallback_info,
            'current_storage': self.current_storage,
            'recommended_storage': recommended_storage,
            'should_switch': should_switch,
            'overall_health': self._get_overall_health(primary_info, fallback_info)
        }
    
    def _get_overall_health(self, primary_info: Dict, fallback_info: Dict) -> str:
        """Determine overall storage health"""
        if not primary_info or not fallback_info:
            return 'error'
        
        primary_healthy = primary_info['is_available'] and primary_info['health_status'] != 'error'
        fallback_healthy = fallback_info['is_available'] and fallback_info['health_status'] != 'error'
        
        if primary_healthy and fallback_healthy:
            return 'healthy'
        elif primary_healthy or fallback_healthy:
            return 'warning'
        else:
            return 'error'
    
    def switch_storage(self, target_storage: str) -> Dict:
        """Switch to target storage (primary or fallback)"""
        try:
            if target_storage not in ['primary', 'fallback']:
                return {'success': False, 'error': 'Invalid storage type'}
            
            # Check if target storage is available
            health_check = self.check_storage_health()
            target_info = health_check[target_storage]
            
            if not target_info or not target_info['is_available']:
                return {
                    'success': False, 
                    'error': f'{target_storage.title()} storage is not available'
                }
            
            old_storage = self.current_storage
            self.current_storage = target_storage
            
            # Log the switch
            self._log_storage_event('switch', target_storage, 
                                  f'Switched from {old_storage} to {target_storage}')
            
            # Update database
            self._update_storage_status()
            
            logger.info(f"Storage switched from {old_storage} to {target_storage}")
            
            return {
                'success': True,
                'message': f'Switched to {target_storage} storage',
                'old_storage': old_storage,
                'new_storage': target_storage
            }
            
        except Exception as e:
            logger.error(f"Error switching storage: {e}")
            return {'success': False, 'error': str(e)}
    
    def auto_switch_storage(self) -> Optional[Dict]:
        """Automatically switch storage if needed"""
        health_check = self.check_storage_health()
        
        if health_check['should_switch']:
            recommended = health_check['recommended_storage']
            if recommended:
                return self.switch_storage(recommended)
        
        return None
    
    def _update_storage_status(self):
        """Update storage status in database"""
        try:
            health_check = self.check_storage_health()
            
            with sqlite3.connect(self.db_path) as conn:
                # Update primary storage
                if health_check['primary']:
                    primary = health_check['primary']
                    conn.execute('''
                        UPDATE storage_status 
                        SET is_available = ?, capacity_gb = ?, used_gb = ?, 
                            free_gb = ?, health_status = ?, last_checked = CURRENT_TIMESTAMP
                        WHERE storage_type = 'primary'
                    ''', (
                        primary['is_available'], primary['capacity_gb'],
                        primary['used_gb'], primary['free_gb'], primary['health_status']
                    ))
                
                # Update fallback storage
                if health_check['fallback']:
                    fallback = health_check['fallback']
                    conn.execute('''
                        UPDATE storage_status 
                        SET is_available = ?, capacity_gb = ?, used_gb = ?, 
                            free_gb = ?, health_status = ?, last_checked = CURRENT_TIMESTAMP
                        WHERE storage_type = 'fallback'
                    ''', (
                        fallback['is_available'], fallback['capacity_gb'],
                        fallback['used_gb'], fallback['free_gb'], fallback['health_status']
                    ))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error updating storage status: {e}")
    
    def _log_storage_event(self, event_type: str, storage_type: str, message: str):
        """Log storage event to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO storage_events (event_type, storage_type, message)
                    VALUES (?, ?, ?)
                ''', (event_type, storage_type, message))
                conn.commit()
        except Exception as e:
            logger.error(f"Error logging storage event: {e}")
    
    def get_storage_events(self, limit: int = 50) -> List[Dict]:
        """Get recent storage events"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute('''
                    SELECT * FROM storage_events 
                    ORDER BY occurred_at DESC 
                    LIMIT ?
                ''', (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting storage events: {e}")
            return []
    
    def get_storage_metrics(self) -> Dict:
        """Get storage metrics and statistics"""
        try:
            health_check = self.check_storage_health()
            
            # Get event counts
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute('''
                    SELECT 
                        COUNT(*) as total_events,
                        SUM(CASE WHEN event_type = 'switch' THEN 1 ELSE 0 END) as switches,
                        SUM(CASE WHEN event_type = 'mount' THEN 1 ELSE 0 END) as mounts,
                        SUM(CASE WHEN event_type = 'unmount' THEN 1 ELSE 0 END) as unmounts,
                        SUM(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END) as errors
                    FROM storage_events
                    WHERE occurred_at > datetime('now', '-7 days')
                ''')
                event_stats = dict(cursor.fetchone())
            
            return {
                'current_storage': self.current_storage,
                'overall_health': health_check['overall_health'],
                'primary_storage': health_check['primary'],
                'fallback_storage': health_check['fallback'],
                'event_stats': event_stats,
                'monitoring_active': self.is_monitoring
            }
            
        except Exception as e:
            logger.error(f"Error getting storage metrics: {e}")
            return {}
    
    def start_monitoring(self):
        """Start continuous storage monitoring"""
        if self.is_monitoring:
            return
        
        self.is_monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("Storage monitoring started")
    
    def stop_monitoring(self):
        """Stop storage monitoring"""
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("Storage monitoring stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.is_monitoring:
            try:
                # Update storage status
                self._update_storage_status()
                
                # Check for automatic switching
                switch_result = self.auto_switch_storage()
                if switch_result:
                    logger.info(f"Auto-switched storage: {switch_result}")
                
                # Sleep until next check
                time.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"Error in storage monitoring loop: {e}")
                time.sleep(self.check_interval)
    
    def get_current_storage_path(self) -> str:
        """Get the path for current active storage"""
        if self.current_storage == 'primary':
            return self.primary_path
        else:
            return self.fallback_path
    
    def is_primary_available(self) -> bool:
        """Check if primary storage is available"""
        primary_info = self.get_storage_info(self.primary_path)
        return primary_info and primary_info['is_available']
    
    def is_fallback_available(self) -> bool:
        """Check if fallback storage is available"""
        fallback_info = self.get_storage_info(self.fallback_path)
        return fallback_info and fallback_info['is_available']
    
    def force_storage_check(self) -> Dict:
        """Force an immediate storage health check"""
        self._update_storage_status()
        return self.check_storage_health()
    
    def perform_deep_health_check(self, storage_type: str = None) -> Dict:
        """Perform comprehensive health check on storage"""
        try:
            if storage_type == 'primary':
                paths_to_check = [self.primary_path]
            elif storage_type == 'fallback':
                paths_to_check = [self.fallback_path]
            else:
                paths_to_check = [self.primary_path, self.fallback_path]
            
            results = {}
            
            for path in paths_to_check:
                storage_name = 'primary' if path == self.primary_path else 'fallback'
                logger.info(f"Performing deep health check on {storage_name} storage: {path}")
                
                health_result = self.health_checker.perform_comprehensive_health_check(path)
                results[storage_name] = health_result
                
                # Log significant health issues
                if health_result['overall_status'] in ['error', 'warning']:
                    self._log_storage_event(
                        'health_issue',
                        storage_name,
                        f"Health check detected {health_result['overall_status']} status"
                    )
            
            return {
                'success': True,
                'timestamp': datetime.now().isoformat(),
                'results': results
            }
            
        except Exception as e:
            logger.error(f"Error during deep health check: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def get_storage_health_history(self, storage_type: str = None, hours: int = 24) -> Dict:
        """Get health check history for storage"""
        try:
            if storage_type == 'primary':
                path = self.primary_path
            elif storage_type == 'fallback':
                path = self.fallback_path
            else:
                # Get history for both
                return {
                    'primary': self.health_checker.get_health_history(self.primary_path, hours),
                    'fallback': self.health_checker.get_health_history(self.fallback_path, hours)
                }
            
            return self.health_checker.get_health_history(path, hours)
            
        except Exception as e:
            logger.error(f"Error getting health history: {e}")
            return {}
    
    def get_storage_health_summary(self, days: int = 7) -> Dict:
        """Get health summary for both storage types"""
        try:
            primary_summary = self.health_checker.get_health_summary(self.primary_path, days)
            fallback_summary = self.health_checker.get_health_summary(self.fallback_path, days)
            
            return {
                'primary': primary_summary,
                'fallback': fallback_summary,
                'period_days': days,
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting health summary: {e}")
            return {}
    
    def detect_storage_issues(self) -> List[Dict]:
        """Detect current storage issues and return recommendations"""
        issues = []
        
        try:
            # Check both storage types
            for storage_type, path in [('primary', self.primary_path), ('fallback', self.fallback_path)]:
                storage_info = self.get_storage_info(path)
                
                if not storage_info:
                    issues.append({
                        'storage': storage_type,
                        'severity': 'critical',
                        'issue': 'Storage not accessible',
                        'recommendation': f'Check {storage_type} storage connection and mount status'
                    })
                    continue
                
                # Check disk space
                if storage_info['usage_percent'] >= 95:
                    issues.append({
                        'storage': storage_type,
                        'severity': 'critical',
                        'issue': f'Disk space critically low ({storage_info["usage_percent"]:.1f}% used)',
                        'recommendation': 'Free up disk space immediately or add more storage'
                    })
                elif storage_info['usage_percent'] >= 90:
                    issues.append({
                        'storage': storage_type,
                        'severity': 'warning',
                        'issue': f'Disk space low ({storage_info["usage_percent"]:.1f}% used)',
                        'recommendation': 'Consider freeing up disk space or adding more storage'
                    })
                
                # Check availability
                if not storage_info['is_available']:
                    issues.append({
                        'storage': storage_type,
                        'severity': 'critical',
                        'issue': 'Storage not available',
                        'recommendation': f'Check {storage_type} storage connection and permissions'
                    })
                
                # Check health status
                if storage_info['health_status'] == 'error':
                    issues.append({
                        'storage': storage_type,
                        'severity': 'critical',
                        'issue': 'Storage health check failed',
                        'recommendation': f'Run diagnostic on {storage_type} storage device'
                    })
                elif storage_info['health_status'] == 'warning':
                    issues.append({
                        'storage': storage_type,
                        'severity': 'warning',
                        'issue': 'Storage health issues detected',
                        'recommendation': f'Monitor {storage_type} storage closely and consider replacement'
                    })
            
            # Check if both storages have issues
            critical_issues = [issue for issue in issues if issue['severity'] == 'critical']
            if len(critical_issues) >= 2:
                issues.append({
                    'storage': 'system',
                    'severity': 'critical',
                    'issue': 'Both primary and fallback storage have critical issues',
                    'recommendation': 'Immediate attention required - system may fail'
                })
            
            return issues
            
        except Exception as e:
            logger.error(f"Error detecting storage issues: {e}")
            return [{
                'storage': 'system',
                'severity': 'error',
                'issue': f'Error during issue detection: {str(e)}',
                'recommendation': 'Check system logs and storage monitor service'
            }]
    
    def get_storage_recommendations(self) -> List[Dict]:
        """Get storage optimization recommendations"""
        recommendations = []
        
        try:
            # Get current storage status
            health_check = self.check_storage_health()
            
            # Analyze primary storage
            if health_check['primary']:
                primary = health_check['primary']
                
                if primary['usage_percent'] > 80:
                    recommendations.append({
                        'type': 'optimization',
                        'priority': 'medium',
                        'title': 'Primary storage optimization',
                        'description': f'Primary storage is {primary["usage_percent"]:.1f}% full',
                        'actions': [
                            'Clean up old or unused music files',
                            'Move less frequently played songs to archive',
                            'Consider upgrading to larger SSD'
                        ]
                    })
                
                if not primary['is_available'] and health_check['fallback']['is_available']:
                    recommendations.append({
                        'type': 'failover',
                        'priority': 'high',
                        'title': 'Primary storage unavailable',
                        'description': 'System is running on fallback storage',
                        'actions': [
                            'Check SSD connection and power',
                            'Verify SSD mount status',
                            'Consider SSD replacement if hardware failure detected'
                        ]
                    })
            
            # Analyze fallback storage
            if health_check['fallback']:
                fallback = health_check['fallback']
                
                if fallback['usage_percent'] > 90:
                    recommendations.append({
                        'type': 'maintenance',
                        'priority': 'medium',
                        'title': 'Fallback storage cleanup needed',
                        'description': f'Fallback storage is {fallback["usage_percent"]:.1f}% full',
                        'actions': [
                            'Clean up old backup files',
                            'Optimize backup song selection',
                            'Consider larger SD card for fallback storage'
                        ]
                    })
            
            # System-wide recommendations
            if health_check['overall_health'] == 'warning':
                recommendations.append({
                    'type': 'monitoring',
                    'priority': 'medium',
                    'title': 'Increase monitoring frequency',
                    'description': 'Storage health issues detected',
                    'actions': [
                        'Enable more frequent health checks',
                        'Set up automated alerts for storage issues',
                        'Review storage performance metrics'
                    ]
                })
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
            return []
    
    def cleanup_old_events(self, days: int = 30) -> int:
        """Clean up old storage events"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute('''
                    DELETE FROM storage_events 
                    WHERE occurred_at < ?
                ''', (cutoff_date.isoformat(),))
                
                deleted_count = cursor.rowcount
                conn.commit()
                
                logger.info(f"Cleaned up {deleted_count} old storage events")
                return deleted_count
                
        except Exception as e:
            logger.error(f"Error cleaning up old events: {e}")
            return 0
    
    def cleanup_old_health_logs(self, days: int = 30) -> int:
        """Clean up old health check logs"""
        return self.health_checker.cleanup_old_health_logs(days)