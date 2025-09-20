#!/usr/bin/env python3
"""
Data Discovery Monitoring Service
Continuous monitoring service for data assets
"""

import asyncio
import signal
import sys
from datetime import datetime
from data_discovery_engine import DataDiscoveryAPI


class MonitoringService:
    """
    Continuous monitoring service for data discovery
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        self.api = DataDiscoveryAPI(config_path)
        self.running = False
        self.monitoring_task = None
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        print(f"\nReceived signal {signum}. Shutting down monitoring service...")
        self.running = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
    
    async def start_monitoring(self, real_time: bool = True):
        """Start the monitoring service"""
        print("Starting Data Discovery Monitoring Service")
        print("=" * 50)
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Real-time monitoring: {'Enabled' if real_time else 'Disabled'}")
        print(f"Monitoring paths: {self.api.engine.config.get('discovery', {}).get('file_system', {}).get('scan_paths', [])}")
        print("=" * 50)
        
        self.running = True
        
        try:
            # Start continuous monitoring
            result = await self.api.start_continuous_monitoring(real_time=real_time)
            
            if result['status'] == 'success':
                print("Monitoring service started successfully")
                print("Monitoring data assets continuously...")
                print("Press Ctrl+C to stop monitoring")
                
                # Keep the service running
                while self.running:
                    await asyncio.sleep(1)
            else:
                print(f"Failed to start monitoring: {result.get('error', 'Unknown error')}")
                return False
                
        except KeyboardInterrupt:
            print("\nMonitoring service stopped by user")
        except Exception as e:
            print(f"Error in monitoring service: {e}")
            return False
        finally:
            print("Monitoring service shutdown complete")
        
        return True
    
    async def run_initial_scan(self):
        """Run an initial scan before starting monitoring"""
        print("Running initial data discovery scan...")
        
        try:
            result = await self.api.scan_all_data_sources()
            
            if result['status'] == 'success':
                total_assets = result['summary']['total_assets']
                print(f"Initial scan completed: {total_assets} assets discovered")
                return True
            else:
                print(f"Initial scan failed: {result.get('error', 'Unknown error')}")
                return False
                
        except Exception as e:
            print(f"Error during initial scan: {e}")
            return False


async def main():
    """Main function for the monitoring service"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Data Discovery Monitoring Service')
    parser.add_argument('--config', default='config.yaml', help='Configuration file path')
    parser.add_argument('--no-realtime', action='store_true', help='Disable real-time file watching')
    parser.add_argument('--no-initial-scan', action='store_true', help='Skip initial discovery scan')
    
    args = parser.parse_args()
    
    # Create monitoring service
    service = MonitoringService(args.config)
    
    # Run initial scan if requested
    if not args.no_initial_scan:
        initial_success = await service.run_initial_scan()
        if not initial_success:
            print("Initial scan failed, but continuing with monitoring...")
    
    # Start monitoring
    real_time = not args.no_realtime
    success = await service.start_monitoring(real_time=real_time)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())
