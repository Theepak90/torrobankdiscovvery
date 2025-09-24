"""
Network Connector - Discovers data assets in NAS and SFTP locations
"""

import os
import paramiko
import stat
from pathlib import Path, PurePosixPath
from datetime import datetime
from typing import List, Dict, Any, Optional
import smbclient
from smbprotocol.exceptions import SMBException
import ftplib
import socket

from .base_connector import BaseConnector
class NetworkConnector(BaseConnector):
    """
    Connector for discovering data assets in network locations (NAS, SFTP, SMB, FTP)
    """
    
    connector_type = "network"
    connector_name = "Network Storage"
    description = "Discover data assets from network storage including SFTP, SMB/CIFS, FTP, and NFS"
    category = "network"
    supported_services = ["SFTP", "SMB/CIFS", "FTP", "NFS"]
    required_config_fields = ["network_sources"]
    optional_config_fields = ["connection_timeout"]
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.network_sources = config.get('network_sources', [])
        self.connection_timeout = config.get('connection_timeout', 30)
        self.file_extensions = set(config.get('file_extensions', []))
        self.max_file_size = config.get('max_file_size_mb', 1000) * 1024 * 1024
        
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover network-based data assets"""
        self.logger.info("Starting network asset discovery")
        
        assets = []
        
        for source_config in self.network_sources:
            source_type = source_config.get('type', '').lower()
            
            try:
                if source_type == 'sftp':
                    assets.extend(self._discover_sftp_assets(source_config))
                elif source_type == 'smb' or source_type == 'nas':
                    assets.extend(self._discover_smb_assets(source_config))
                elif source_type == 'ftp':
                    assets.extend(self._discover_ftp_assets(source_config))
                elif source_type == 'nfs':
                    assets.extend(self._discover_nfs_assets(source_config))
                else:
                    self.logger.warning(f"Unsupported network source type: {source_type}")
                    
            except Exception as e:
                self.logger.error(f"Error discovering assets from {source_type}: {e}")
        
        self.logger.info(f"Discovered {len(assets)} network assets")
        return assets
    
    def _discover_sftp_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover assets via SFTP"""
        assets = []
        
        try:
            hostname = config.get('host')
            port = config.get('port', 22)
            username = config.get('username')
            password = config.get('password')
            private_key_path = config.get('private_key_path')
            scan_paths = config.get('scan_paths', ['/'])
            
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            if private_key_path:
                private_key = paramiko.RSAKey.from_private_key_file(private_key_path)
                ssh_client.connect(hostname, port=port, username=username, pkey=private_key, timeout=self.connection_timeout)
            else:
                ssh_client.connect(hostname, port=port, username=username, password=password, timeout=self.connection_timeout)
            
            sftp_client = ssh_client.open_sftp()
            
            self.logger.info(f"Connected to SFTP server: {hostname}")
            
            for scan_path in scan_paths:
                try:
                    path_assets = self._scan_sftp_directory(sftp_client, scan_path, hostname, config)
                    assets.extend(path_assets)
                except Exception as e:
                    self.logger.error(f"Error scanning SFTP path {scan_path}: {e}")
            
            sftp_client.close()
            ssh_client.close()
            
        except Exception as e:
            self.logger.error(f"Error connecting to SFTP server {config.get('host')}: {e}")
        
        return assets
    
    def _scan_sftp_directory(self, sftp_client, directory: str, hostname: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Recursively scan SFTP directory"""
        assets = []
        
        try:
            items = sftp_client.listdir_attr(directory)
            
            for item in items:
                item_path = f"{directory.rstrip('/')}/{item.filename}"
                
                try:
                    if stat.S_ISREG(item.st_mode):
                        if item.st_size > self.max_file_size:
                            continue
                        
                        if self.file_extensions and not any(item.filename.lower().endswith(ext) for ext in self.file_extensions):
                            continue
                        
                        asset = self._create_sftp_file_asset(item, item_path, hostname, config)
                        if asset:
                            assets.append(asset)
                    
                    elif stat.S_ISDIR(item.st_mode) and directory.count('/') < 10:  # Limit recursion depth
                        sub_assets = self._scan_sftp_directory(sftp_client, item_path, hostname, config)
                        assets.extend(sub_assets)
                        
                except Exception as e:
                    self.logger.warning(f"Error processing SFTP item {item_path}: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error listing SFTP directory {directory}: {e}")
        
        return assets
    
    def _create_sftp_file_asset(self, file_stat, file_path: str, hostname: str, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create asset dictionary for SFTP file"""
        try:
            filename = PurePosixPath(file_path).name
            file_type = self._determine_network_file_type(filename, 'sftp')
            
            asset = {
                'name': filename,
                'type': file_type,
                'source': 'sftp',
                'location': f"sftp://{hostname}{file_path}",
                'size': file_stat.st_size,
                'created_date': datetime.fromtimestamp(file_stat.st_mtime),  # SFTP often only has mtime
                'modified_date': datetime.fromtimestamp(file_stat.st_mtime),
                'schema': {},
                'tags': self._generate_network_file_tags(filename, file_path, 'sftp'),
                'metadata': {
                    'network_type': 'sftp',
                    'hostname': hostname,
                    'remote_path': file_path,
                    'permissions': oct(file_stat.st_mode)[-3:] if hasattr(file_stat, 'st_mode') else None,
                    'owner_uid': getattr(file_stat, 'st_uid', None),
                    'group_gid': getattr(file_stat, 'st_gid', None)
                }
            }
            
            return asset
            
        except Exception as e:
            self.logger.error(f"Error creating SFTP asset for {file_path}: {e}")
            return None
    
    def _discover_smb_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover assets via SMB/CIFS (NAS)"""
        assets = []
        
        try:
            hostname = config.get('host')
            share_name = config.get('share')
            username = config.get('username')
            password = config.get('password')
            domain = config.get('domain', '')
            scan_paths = config.get('scan_paths', ['/'])
            
            self.logger.info(f"Connecting to SMB share: //{hostname}/{share_name}")
            
            smbclient.register_session(hostname, username=username, password=password, domain=domain)
            
            for scan_path in scan_paths:
                try:
                    smb_path = f"//{hostname}/{share_name}{scan_path}"
                    path_assets = self._scan_smb_directory(smb_path, hostname, share_name, config)
                    assets.extend(path_assets)
                except Exception as e:
                    self.logger.error(f"Error scanning SMB path {scan_path}: {e}")
            
        except Exception as e:
            self.logger.error(f"Error connecting to SMB share //{config.get('host')}/{config.get('share')}: {e}")
        
        return assets
    
    def _scan_smb_directory(self, directory: str, hostname: str, share_name: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Recursively scan SMB directory"""
        assets = []
        
        try:
            items = smbclient.listdir(directory)
            
            for item_name in items:
                item_path = f"{directory.rstrip('/')}/{item_name}"
                
                try:
                    item_stat = smbclient.stat(item_path)
                    
                    if smbclient.path.isfile(item_path):
                        if item_stat.st_size > self.max_file_size:
                            continue
                        
                        if self.file_extensions and not any(item_name.lower().endswith(ext) for ext in self.file_extensions):
                            continue
                        
                        asset = self._create_smb_file_asset(item_stat, item_path, item_name, hostname, share_name, config)
                        if asset:
                            assets.append(asset)
                    
                    elif smbclient.path.isdir(item_path) and directory.count('/') < 10:
                        sub_assets = self._scan_smb_directory(item_path, hostname, share_name, config)
                        assets.extend(sub_assets)
                        
                except Exception as e:
                    self.logger.warning(f"Error processing SMB item {item_path}: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error listing SMB directory {directory}: {e}")
        
        return assets
    
    def _create_smb_file_asset(self, file_stat, file_path: str, filename: str, hostname: str, share_name: str, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create asset dictionary for SMB file"""
        try:
            file_type = self._determine_network_file_type(filename, 'smb')
            
            asset = {
                'name': filename,
                'type': file_type,
                'source': 'smb_nas',
                'location': file_path,
                'size': file_stat.st_size,
                'created_date': datetime.fromtimestamp(file_stat.st_ctime) if hasattr(file_stat, 'st_ctime') else datetime.fromtimestamp(file_stat.st_mtime),
                'modified_date': datetime.fromtimestamp(file_stat.st_mtime),
                'schema': {},
                'tags': self._generate_network_file_tags(filename, file_path, 'smb'),
                'metadata': {
                    'network_type': 'smb',
                    'hostname': hostname,
                    'share_name': share_name,
                    'remote_path': file_path,
                    'permissions': oct(file_stat.st_mode)[-3:] if hasattr(file_stat, 'st_mode') else None
                }
            }
            
            return asset
            
        except Exception as e:
            self.logger.error(f"Error creating SMB asset for {file_path}: {e}")
            return None
    
    def _discover_ftp_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover assets via FTP"""
        assets = []
        
        try:
            hostname = config.get('host')
            port = config.get('port', 21)
            username = config.get('username', 'anonymous')
            password = config.get('password', '')
            scan_paths = config.get('scan_paths', ['/'])
            
            ftp = ftplib.FTP()
            ftp.connect(hostname, port, timeout=self.connection_timeout)
            ftp.login(username, password)
            
            self.logger.info(f"Connected to FTP server: {hostname}")
            
            for scan_path in scan_paths:
                try:
                    path_assets = self._scan_ftp_directory(ftp, scan_path, hostname, config)
                    assets.extend(path_assets)
                except Exception as e:
                    self.logger.error(f"Error scanning FTP path {scan_path}: {e}")
            
            ftp.quit()
            
        except Exception as e:
            self.logger.error(f"Error connecting to FTP server {config.get('host')}: {e}")
        
        return assets
    
    def _scan_ftp_directory(self, ftp, directory: str, hostname: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Recursively scan FTP directory"""
        assets = []
        
        try:
            original_dir = ftp.pwd()
            ftp.cwd(directory)
            
            items = []
            ftp.retrlines('LIST', items.append)
            
            for item_line in items:
                try:
                    parts = item_line.split()
                    if len(parts) < 9:
                        continue
                    
                    permissions = parts[0]
                    size = int(parts[4]) if parts[4].isdigit() else 0
                    filename = ' '.join(parts[8:])  # Handle filenames with spaces
                    
                    if filename in ['.', '..']:
                        continue
                    
                    if permissions.startswith('-'):
                        if size > self.max_file_size:
                            continue
                        
                        if self.file_extensions and not any(filename.lower().endswith(ext) for ext in self.file_extensions):
                            continue
                        
                        file_path = f"{directory.rstrip('/')}/{filename}"
                        asset = self._create_ftp_file_asset(size, file_path, filename, hostname, config, permissions)
                        if asset:
                            assets.append(asset)
                    
                    elif permissions.startswith('d') and directory.count('/') < 10:
                        sub_dir = f"{directory.rstrip('/')}/{filename}"
                        sub_assets = self._scan_ftp_directory(ftp, sub_dir, hostname, config)
                        assets.extend(sub_assets)
                        
                except Exception as e:
                    self.logger.warning(f"Error processing FTP item in {directory}: {e}")
                    continue
            
            ftp.cwd(original_dir)
        
        except Exception as e:
            self.logger.error(f"Error listing FTP directory {directory}: {e}")
        
        return assets
    
    def _create_ftp_file_asset(self, size: int, file_path: str, filename: str, hostname: str, config: Dict[str, Any], permissions: str) -> Optional[Dict[str, Any]]:
        """Create asset dictionary for FTP file"""
        try:
            file_type = self._determine_network_file_type(filename, 'ftp')
            
            asset = {
                'name': filename,
                'type': file_type,
                'source': 'ftp',
                'location': f"ftp://{hostname}{file_path}",
                'size': size,
                'created_date': datetime.now(),  # FTP doesn't provide creation time
                'modified_date': datetime.now(),  # FTP LIST doesn't always provide modification time
                'schema': {},
                'tags': self._generate_network_file_tags(filename, file_path, 'ftp'),
                'metadata': {
                    'network_type': 'ftp',
                    'hostname': hostname,
                    'remote_path': file_path,
                    'permissions': permissions
                }
            }
            
            return asset
            
        except Exception as e:
            self.logger.error(f"Error creating FTP asset for {file_path}: {e}")
            return None
    
    def _discover_nfs_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover assets via NFS (Network File System)"""
        assets = []
        
        try:
            mount_point = config.get('mount_point')
            nfs_server = config.get('server')
            nfs_export = config.get('export')
            scan_paths = config.get('scan_paths', ['/'])
            
            if not mount_point or not os.path.exists(mount_point):
                self.logger.warning(f"NFS mount point {mount_point} does not exist or is not mounted")
                return assets
            
            self.logger.info(f"Scanning NFS mount: {mount_point}")
            
            for scan_path in scan_paths:
                full_path = os.path.join(mount_point, scan_path.lstrip('/'))
                if os.path.exists(full_path):
                    path_assets = self._scan_nfs_directory(full_path, mount_point, nfs_server, nfs_export, config)
                    assets.extend(path_assets)
            
        except Exception as e:
            self.logger.error(f"Error discovering NFS assets: {e}")
        
        return assets
    
    def _scan_nfs_directory(self, directory: str, mount_point: str, server: str, export: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Recursively scan NFS directory"""
        assets = []
        
        try:
            for root, dirs, files in os.walk(directory):
                for filename in files:
                    try:
                        file_path = os.path.join(root, filename)
                        
                        stat_info = os.stat(file_path)
                        
                        if stat_info.st_size > self.max_file_size:
                            continue
                        
                        if self.file_extensions and not any(filename.lower().endswith(ext) for ext in self.file_extensions):
                            continue
                        
                        asset = self._create_nfs_file_asset(stat_info, file_path, filename, mount_point, server, export, config)
                        if asset:
                            assets.append(asset)
                            
                    except Exception as e:
                        self.logger.warning(f"Error processing NFS file {filename}: {e}")
                        continue
        
        except Exception as e:
            self.logger.error(f"Error scanning NFS directory {directory}: {e}")
        
        return assets
    
    def _create_nfs_file_asset(self, stat_info, file_path: str, filename: str, mount_point: str, server: str, export: str, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create asset dictionary for NFS file"""
        try:
            file_type = self._determine_network_file_type(filename, 'nfs')
            
            relative_path = os.path.relpath(file_path, mount_point)
            nfs_path = f"nfs://{server}{export}/{relative_path}"
            
            asset = {
                'name': filename,
                'type': file_type,
                'source': 'nfs',
                'location': nfs_path,
                'size': stat_info.st_size,
                'created_date': datetime.fromtimestamp(stat_info.st_ctime),
                'modified_date': datetime.fromtimestamp(stat_info.st_mtime),
                'schema': {},
                'tags': self._generate_network_file_tags(filename, file_path, 'nfs'),
                'metadata': {
                    'network_type': 'nfs',
                    'server': server,
                    'export': export,
                    'mount_point': mount_point,
                    'local_path': file_path,
                    'permissions': oct(stat_info.st_mode)[-3:],
                    'owner_uid': stat_info.st_uid,
                    'group_gid': stat_info.st_gid
                }
            }
            
            return asset
            
        except Exception as e:
            self.logger.error(f"Error creating NFS asset for {file_path}: {e}")
            return None
    
    def _determine_network_file_type(self, filename: str, network_type: str) -> str:
        """Determine the type of network file"""
        filename_lower = filename.lower()
        
        base_types = {
            '.csv': 'csv_file',
            '.json': 'json_file',
            '.xlsx': 'excel_file',
            '.xls': 'excel_file',
            '.parquet': 'parquet_file',
            '.sql': 'sql_file',
            '.txt': 'text_file',
            '.xml': 'xml_file',
            '.yaml': 'yaml_file',
            '.yml': 'yaml_file',
            '.log': 'log_file'
        }
        
        for ext, base_type in base_types.items():
            if filename_lower.endswith(ext):
                return f"{network_type}_{base_type}"
        
        return f"{network_type}_data_file"
    
    def _generate_network_file_tags(self, filename: str, file_path: str, network_type: str) -> List[str]:
        """Generate tags for network file"""
        tags = [network_type, 'network', 'remote']
        
        if '.' in filename:
            ext = filename.split('.')[-1].lower()
            tags.append(f"ext_{ext}")
        
        path_parts = file_path.lower().split('/')
        for part in path_parts:
            if part in ['data', 'backup', 'archive', 'reports', 'logs', 'export']:
                tags.append(f"category_{part}")
        
        try:
            tags.extend(['remote_file', 'network_storage'])
        except:
            pass
        
        return tags
    
    def test_connection(self) -> bool:
        """Test network connections"""
        all_connections_valid = True
        
        for source_config in self.network_sources:
            source_type = source_config.get('type', '').lower()
            hostname = source_config.get('host')
            
            try:
                if source_type == 'sftp':
                    port = source_config.get('port', 22)
                    username = source_config.get('username')
                    password = source_config.get('password')
                    
                    ssh_client = paramiko.SSHClient()
                    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    ssh_client.connect(hostname, port=port, username=username, password=password, timeout=5)
                    ssh_client.close()
                    
                elif source_type == 'smb' or source_type == 'nas':
                    share_name = source_config.get('share')
                    username = source_config.get('username')
                    password = source_config.get('password')
                    domain = source_config.get('domain', '')
                    
                    smbclient.register_session(hostname, username=username, password=password, domain=domain)
                    smbclient.listdir(f"//{hostname}/{share_name}")
                    
                elif source_type == 'ftp':
                    port = source_config.get('port', 21)
                    username = source_config.get('username', 'anonymous')
                    password = source_config.get('password', '')
                    
                    ftp = ftplib.FTP()
                    ftp.connect(hostname, port, timeout=5)
                    ftp.login(username, password)
                    ftp.quit()
                    
                elif source_type == 'nfs':
                    mount_point = source_config.get('mount_point')
                    if not os.path.exists(mount_point):
                        raise Exception(f"NFS mount point {mount_point} does not exist")
                
                self.logger.info(f"Connection test successful for {source_type} at {hostname}")
                
            except Exception as e:
                self.logger.error(f"Connection test failed for {source_type} at {hostname}: {e}")
                all_connections_valid = False
        
        return all_connections_valid
    
    def validate_config(self) -> bool:
        """Validate network connector configuration"""
        if not self.network_sources:
            self.logger.error("No network sources configured")
            return False
        
        for source_config in self.network_sources:
            source_type = source_config.get('type')
            if not source_type:
                self.logger.error("Network source type not specified")
                return False
            
            if not source_config.get('host') and source_type != 'nfs':
                self.logger.error(f"Host not specified for {source_type} source")
                return False
        
        return True
