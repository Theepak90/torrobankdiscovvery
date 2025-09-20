"""
NAS (Network Attached Storage) Connector

This connector discovers data assets from NAS drives and network storage systems.
Supports various protocols like SMB/CIFS, NFS, and other network file systems.
"""

import os
import stat
from datetime import datetime
from typing import List, Dict, Any, Optional
import smbclient
from smbclient import path as smb_path
from .base_connector import BaseConnector

class NASConnector(BaseConnector):
    """
    Connector for discovering data assets in NAS drives and network storage systems
    """
    
    connector_type = "nas"
    connector_name = "NAS Drives"
    description = "Discover data assets from NAS drives and network storage systems (SMB/CIFS, NFS)"
    category = "network_storage"
    supported_services = ["SMB/CIFS", "NFS", "Network File Systems"]
    required_config_fields = ["host", "username", "password", "share_name"]
    optional_config_fields = ["domain", "port", "timeout", "max_depth"]
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host')
        self.port = config.get('port', 445)
        self.username = config.get('username')
        self.password = config.get('password')
        self.domain = config.get('domain', '')
        self.share_name = config.get('share_name')
        self.timeout = config.get('timeout', 30)
        self.max_depth = config.get('max_depth', 5)
        
        # Initialize SMB client
        self.smb_client = None
        
    def _initialize_smb_client(self) -> bool:
        """Initialize SMB client connection"""
        try:
            # Configure SMB client
            smbclient.ClientConfig(
                username=self.username,
                password=self.password,
                domain=self.domain,
                port=self.port,
                timeout=self.timeout
            )
            self.smb_client = smbclient
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize SMB client: {e}")
            return False
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover assets from NAS drives"""
        assets = []
        
        try:
            if not self._initialize_smb_client():
                return assets
            
            # Build SMB path
            smb_path_str = f"\\\\{self.host}\\{self.share_name}"
            
            # Discover files and directories
            assets.extend(self._discover_smb_assets(smb_path_str, 0))
            
            self.logger.info(f"Discovered {len(assets)} assets from NAS drive")
            return assets
            
        except Exception as e:
            self.logger.error(f"Failed to discover NAS assets: {e}")
            return assets
    
    def _discover_smb_assets(self, path: str, depth: int) -> List[Dict[str, Any]]:
        """Recursively discover SMB assets"""
        assets = []
        
        try:
            if depth > self.max_depth:
                return assets
            
            # List directory contents
            for item in smb_path.listdir(path):
                item_path = f"{path}\\{item}"
                
                try:
                    # Get file stats
                    stat_info = smb_path.stat(item_path)
                    
                    # Determine asset type
                    if stat.S_ISDIR(stat_info.st_mode):
                        asset_type = "Directory"
                        size = 0
                    else:
                        asset_type = "File"
                        size = stat_info.st_size
                    
                    # Create asset metadata
                    asset = {
                        "id": f"nas_{hash(item_path)}",
                        "name": item,
                        "type": asset_type,
                        "path": item_path,
                        "size": size,
                        "created_at": datetime.fromtimestamp(stat_info.st_ctime).isoformat(),
                        "modified_at": datetime.fromtimestamp(stat_info.st_mtime).isoformat(),
                        "permissions": oct(stat_info.st_mode)[-3:],
                        "source": "NAS Drive",
                        "connector_type": self.connector_type,
                        "metadata": {
                            "host": self.host,
                            "share": self.share_name,
                            "depth": depth,
                            "is_directory": stat.S_ISDIR(stat_info.st_mode),
                            "file_extension": os.path.splitext(item)[1] if not stat.S_ISDIR(stat_info.st_mode) else None
                        }
                    }
                    
                    assets.append(asset)
                    
                    # Recursively discover subdirectories
                    if stat.S_ISDIR(stat_info.st_mode) and depth < self.max_depth:
                        assets.extend(self._discover_smb_assets(item_path, depth + 1))
                        
                except Exception as e:
                    self.logger.warning(f"Failed to process item {item_path}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Failed to list directory {path}: {e}")
        
        return assets
    
    def test_connection(self) -> bool:
        """Test connection to NAS drive"""
        try:
            if not self._initialize_smb_client():
                return False
            
            # Test connection by trying to list the share
            smb_path_str = f"\\\\{self.host}\\{self.share_name}"
            list(smb_path.listdir(smb_path_str))
            
            self.logger.info("NAS connection test successful")
            return True
            
        except Exception as e:
            self.logger.error(f"NAS connection test failed: {e}")
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information"""
        return {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "domain": self.domain,
            "share_name": self.share_name,
            "timeout": self.timeout,
            "max_depth": self.max_depth,
            "protocol": "SMB/CIFS"
        }
    
    def get_supported_file_types(self) -> List[str]:
        """Get list of supported file types"""
        return [
            "Documents", "Spreadsheets", "Presentations", "Images", 
            "Videos", "Audio", "Archives", "Databases", "Logs", "Text Files"
        ]
    
    def get_storage_info(self) -> Dict[str, Any]:
        """Get storage information"""
        try:
            if not self._initialize_smb_client():
                return {}
            
            smb_path_str = f"\\\\{self.host}\\{self.share_name}"
            
            # Calculate storage usage
            total_files = 0
            total_dirs = 0
            total_size = 0
            
            for root, dirs, files in smb_path.walk(smb_path_str):
                total_dirs += len(dirs)
                for file in files:
                    try:
                        file_path = f"{root}\\{file}"
                        stat_info = smb_path.stat(file_path)
                        total_size += stat_info.st_size
                        total_files += 1
                    except:
                        continue
            
            return {
                "total_files": total_files,
                "total_directories": total_dirs,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "total_size_gb": round(total_size / (1024 * 1024 * 1024), 2)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get storage info: {e}")
            return {}
