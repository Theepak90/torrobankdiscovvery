"""
SFTP (SSH File Transfer Protocol) Connector

This connector discovers data assets from SFTP servers and SSH-based file systems.
Supports both password and key-based authentication.
"""

import os
import stat
from datetime import datetime
from typing import List, Dict, Any, Optional
import paramiko
from pathlib import Path, PurePosixPath
from .base_connector import BaseConnector

class SFTPConnector(BaseConnector):
    """
    Connector for discovering data assets in SFTP servers
    """
    
    connector_type = "sftp"
    connector_name = "SFTP Server"
    description = "Discover data assets from SFTP servers and SSH-based file systems"
    category = "network_storage"
    supported_services = ["SFTP", "SSH File Transfer", "Secure File Transfer"]
    required_config_fields = ["host", "username", "password"]
    optional_config_fields = ["port", "private_key_path", "scan_paths", "max_depth", "file_extensions"]
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host')
        self.port = config.get('port', 22)
        self.username = config.get('username')
        self.password = config.get('password')
        self.private_key_path = config.get('private_key_path')
        self.scan_paths = config.get('scan_paths', ['/'])
        self.max_depth = config.get('max_depth', 5)
        self.file_extensions = set(config.get('file_extensions', []))
        self.max_file_size = config.get('max_file_size_mb', 1000) * 1024 * 1024
        
        self.ssh_client = None
        self.sftp_client = None
        
    def _initialize_connection(self) -> bool:
        """Initialize SSH and SFTP connections"""
        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            if self.private_key_path and os.path.exists(self.private_key_path):
                private_key = paramiko.RSAKey.from_private_key_file(self.private_key_path)
                self.ssh_client.connect(
                    self.host, 
                    port=self.port, 
                    username=self.username, 
                    pkey=private_key, 
                    timeout=30
                )
            else:
                self.ssh_client.connect(
                    self.host, 
                    port=self.port, 
                    username=self.username, 
                    password=self.password, 
                    timeout=30
                )
            
            self.sftp_client = self.ssh_client.open_sftp()
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize SFTP connection: {e}")
            return False
    
    def _close_connection(self):
        """Close SSH and SFTP connections"""
        try:
            if self.sftp_client:
                self.sftp_client.close()
            if self.ssh_client:
                self.ssh_client.close()
        except Exception as e:
            self.logger.warning(f"Error closing SFTP connection: {e}")
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover assets from SFTP server"""
        assets = []
        
        try:
            if not self._initialize_connection():
                return assets
            
            self.logger.info(f"Connected to SFTP server: {self.host}")
            
            for scan_path in self.scan_paths:
                try:
                    path_assets = self._scan_sftp_directory(scan_path, 0)
                    assets.extend(path_assets)
                except Exception as e:
                    self.logger.error(f"Error scanning SFTP path {scan_path}: {e}")
            
            self.logger.info(f"Discovered {len(assets)} assets from SFTP server")
            return assets
            
        except Exception as e:
            self.logger.error(f"Failed to discover SFTP assets: {e}")
            return assets
        finally:
            self._close_connection()
    
    def _scan_sftp_directory(self, directory: str, depth: int) -> List[Dict[str, Any]]:
        """Recursively scan SFTP directory"""
        assets = []
        
        try:
            if depth > self.max_depth:
                return assets
            
            items = self.sftp_client.listdir_attr(directory)
            
            for item in items:
                item_path = f"{directory.rstrip('/')}/{item.filename}"
                
                try:
                    if stat.S_ISREG(item.st_mode):
                        if item.st_size > self.max_file_size:
                            continue
                        
                        if self.file_extensions and not any(item.filename.lower().endswith(ext) for ext in self.file_extensions):
                            continue
                        
                        asset = self._create_sftp_file_asset(item, item_path)
                        if asset:
                            assets.append(asset)
                    
                    elif stat.S_ISDIR(item.st_mode) and depth < self.max_depth:
                        sub_assets = self._scan_sftp_directory(item_path, depth + 1)
                        assets.extend(sub_assets)
                        
                except Exception as e:
                    self.logger.warning(f"Error processing SFTP item {item_path}: {e}")
                    continue
        
        except Exception as e:
            self.logger.warning(f"Error listing SFTP directory {directory}: {e}")
        
        return assets
    
    def _create_sftp_file_asset(self, file_stat, file_path: str) -> Optional[Dict[str, Any]]:
        """Create asset dictionary for SFTP file"""
        try:
            filename = PurePosixPath(file_path).name
            file_type = self._determine_file_type(filename)
            
            asset = {
                "id": f"sftp_{hash(file_path)}",
                "name": filename,
                "type": file_type,
                "path": file_path,
                "size": file_stat.st_size,
                "created_at": datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                "modified_at": datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                "permissions": oct(file_stat.st_mode)[-3:] if hasattr(file_stat, 'st_mode') else None,
                "source": "SFTP Server",
                "connector_type": self.connector_type,
                "metadata": {
                    "host": self.host,
                    "port": self.port,
                    "username": self.username,
                    "remote_path": file_path,
                    "file_extension": os.path.splitext(filename)[1],
                    "owner_uid": getattr(file_stat, 'st_uid', None),
                    "group_gid": getattr(file_stat, 'st_gid', None),
                    "is_directory": False
                }
            }
            
            return asset
            
        except Exception as e:
            self.logger.error(f"Error creating SFTP asset for {file_path}: {e}")
            return None
    
    def _determine_file_type(self, filename: str) -> str:
        """Determine the type of file based on extension"""
        filename_lower = filename.lower()
        
        file_types = {
            '.csv': 'CSV File',
            '.json': 'JSON File',
            '.xlsx': 'Excel File',
            '.xls': 'Excel File',
            '.parquet': 'Parquet File',
            '.sql': 'SQL File',
            '.txt': 'Text File',
            '.xml': 'XML File',
            '.yaml': 'YAML File',
            '.yml': 'YAML File',
            '.log': 'Log File',
            '.pdf': 'PDF File',
            '.doc': 'Word Document',
            '.docx': 'Word Document',
            '.ppt': 'PowerPoint',
            '.pptx': 'PowerPoint',
            '.zip': 'Archive',
            '.tar': 'Archive',
            '.gz': 'Archive'
        }
        
        for ext, file_type in file_types.items():
            if filename_lower.endswith(ext):
                return file_type
        
        return "Data File"
    
    def test_connection(self) -> bool:
        """Test connection to SFTP server"""
        try:
            if not self._initialize_connection():
                return False
            
            self.sftp_client.listdir('/')
            
            self.logger.info("SFTP connection test successful")
            return True
            
        except Exception as e:
            self.logger.error(f"SFTP connection test failed: {e}")
            return False
        finally:
            self._close_connection()
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information"""
        return {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "private_key_path": self.private_key_path,
            "scan_paths": self.scan_paths,
            "max_depth": self.max_depth,
            "file_extensions": list(self.file_extensions),
            "max_file_size_mb": self.max_file_size // (1024 * 1024)
        }
    
    def get_supported_file_types(self) -> List[str]:
        """Get list of supported file types"""
        return [
            "CSV Files", "JSON Files", "Excel Files", "Parquet Files", 
            "SQL Files", "Text Files", "XML Files", "YAML Files", 
            "Log Files", "PDF Files", "Word Documents", "PowerPoint", 
            "Archives", "Images", "Videos", "Audio"
        ]
    
    def get_server_info(self) -> Dict[str, Any]:
        """Get SFTP server information"""
        try:
            if not self._initialize_connection():
                return {}
            
            stdin, stdout, stderr = self.ssh_client.exec_command('uname -a')
            server_info = stdout.read().decode().strip()
            
            stdin, stdout, stderr = self.ssh_client.exec_command('df -h')
            disk_info = stdout.read().decode().strip()
            
            return {
                "server_info": server_info,
                "disk_usage": disk_info,
                "hostname": self.host,
                "port": self.port
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get server info: {e}")
            return {}
        finally:
            self._close_connection()
