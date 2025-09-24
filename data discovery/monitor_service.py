"""
Data Discovery Monitoring Service
Continuous monitoring service for data assets
"""

import asyncio
import signal
import sys
from datetime import datetime
from dynamic_discovery_engine import DynamicDataDiscoveryEngine
class MonitoringService:
    """
    Continuous monitoring service for data discovery
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        self.api = DynamicDataDiscoveryEngine(config_path)
        self.running = False
        self.monitoring_task = None
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.running = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
    
    async def start_monitoring(self, real_time: bool = True):
        """Start the monitoring service"""
        
        self.running = True
        
        try:
            result = await self.api.start_continuous_monitoring(real_time=real_time)
            
            if result['status'] == 'success':
                
                while self.running:
                    await asyncio.sleep(1)
            else:
                return False
                
        except KeyboardInterrupt:
        except Exception as e:
            return False
        finally:
        
        return True
    
    async def run_initial_scan(self):
        """Run an initial scan before starting monitoring"""
        
        try:
            result = await self.api.scan_all_data_sources()
            
            if result['status'] == 'success':
                total_assets = result['summary']['total_assets']
                return True
            else:
                return False
                
        except Exception as e:
            return False
async def main():
    """Main function for the monitoring service"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Data Discovery Monitoring Service')
    parser.add_argument('--config', default='config.yaml', help='Configuration file path')
    parser.add_argument('--no-realtime', action='store_true', help='Disable real-time file watching')
    parser.add_argument('--no-initial-scan', action='store_true', help='Skip initial discovery scan')
    
    args = parser.parse_args()
    
    service = MonitoringService(args.config)
    
    if not args.no_initial_scan:
        initial_success = await service.run_initial_scan()
        if not initial_success:
    
    real_time = not args.no_realtime
    success = await service.start_monitoring(real_time=real_time)
    
    sys.exit(0 if success else 1)
if __name__ == "__main__":
    asyncio.run(main())
