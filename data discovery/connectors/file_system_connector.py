"""
File System Connector - Discovers data assets in local and network file systems
"""

import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Set
import mimetypes
import sqlite3

from .base_connector import BaseConnector


class FileSystemConnector(BaseConnector):
    """
    Connector for discovering data assets in file systems
    """
    
    # Metadata for dynamic discovery
    connector_type = "file_system"
    connector_name = "File System"
    description = "Discover data assets in local and network file systems"
    category = "file_systems"
    supported_services = ["Local Files", "Network Files", "CSV", "JSON", "Parquet", "Excel"]
    required_config_fields = ["scan_paths"]
    optional_config_fields = ["file_extensions", "max_file_size_mb", "exclude_patterns"]
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.scan_paths = config.get('scan_paths', [])
        self.file_extensions = set(config.get('file_extensions', []))
        self.max_file_size = config.get('max_file_size_mb', 1000) * 1024 * 1024  # Convert to bytes
        
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover file system data assets"""
        self.logger.info("Starting file system asset discovery")
        
        assets = []
        
        for scan_path in self.scan_paths:
            try:
                path_obj = Path(scan_path)
                if not path_obj.exists():
                    self.logger.warning(f"Scan path does not exist: {scan_path}")
                    continue
                
                self.logger.info(f"Scanning path: {scan_path}")
                path_assets = self._scan_directory(path_obj)
                assets.extend(path_assets)
                
            except Exception as e:
                self.logger.error(f"Error scanning path {scan_path}: {e}")
        
        self.logger.info(f"Discovered {len(assets)} file system assets")
        return assets
    
    def _scan_directory(self, directory: Path) -> List[Dict[str, Any]]:
        """Recursively scan directory for data assets"""
        assets = []
        
        try:
            for item in directory.rglob('*'):
                if item.is_file():
                    try:
                        # Check file size limit
                        if item.stat().st_size > self.max_file_size:
                            continue
                        
                        # Check if file extension is in our target list
                        if self.file_extensions and item.suffix.lower() not in self.file_extensions:
                            continue
                        
                        asset = self._create_file_asset(item)
                        if asset:
                            assets.append(asset)
                            
                    except (OSError, PermissionError) as e:
                        self.logger.warning(f"Cannot access file {item}: {e}")
                        continue
                        
        except (OSError, PermissionError) as e:
            self.logger.error(f"Cannot access directory {directory}: {e}")
        
        return assets
    
    def _create_file_asset(self, file_path: Path) -> Dict[str, Any]:
        """Create asset dictionary for a file"""
        try:
            stat_info = file_path.stat()
            file_type = self._determine_file_type(file_path)
            
            asset = {
                'name': file_path.name,
                'type': file_type,
                'source': 'file_system',
                'location': str(file_path.absolute()),
                'size': stat_info.st_size,
                'created_date': datetime.fromtimestamp(stat_info.st_ctime),
                'modified_date': datetime.fromtimestamp(stat_info.st_mtime),
                'extension': file_path.suffix.lower(),
                'directory': str(file_path.parent),
                'schema': {},
                'tags': self._generate_file_tags(file_path),
                'metadata': {
                    'mime_type': mimetypes.guess_type(str(file_path))[0],
                    'permissions': oct(stat_info.st_mode)[-3:],
                    'owner_uid': stat_info.st_uid,
                    'group_gid': stat_info.st_gid
                }
            }
            
            # Try to extract additional metadata based on file type
            self._enrich_file_metadata(asset, file_path)
            
            return asset
            
        except Exception as e:
            self.logger.error(f"Error creating asset for {file_path}: {e}")
            return None
    
    def _determine_file_type(self, file_path: Path) -> str:
        """Determine the type of data file"""
        extension = file_path.suffix.lower()
        
        type_mapping = {
            '.csv': 'csv_file',
            '.json': 'json_file',
            '.xlsx': 'excel_file',
            '.xls': 'excel_file',
            '.parquet': 'parquet_file',
            '.sql': 'sql_file',
            '.db': 'sqlite_database',
            '.sqlite': 'sqlite_database',
            '.sqlite3': 'sqlite_database',
            '.txt': 'text_file',
            '.xml': 'xml_file',
            '.yaml': 'yaml_file',
            '.yml': 'yaml_file'
        }
        
        return type_mapping.get(extension, 'data_file')
    
    def _generate_file_tags(self, file_path: Path) -> List[str]:
        """Generate tags for the file based on location and type"""
        tags = []
        
        # Add extension-based tag
        if file_path.suffix:
            tags.append(f"ext_{file_path.suffix[1:]}")
        
        # Add directory-based tags
        path_parts = file_path.parts
        for part in path_parts:
            if part.lower() in ['data', 'database', 'export', 'backup', 'archive', 'reports']:
                tags.append(f"category_{part.lower()}")
        
        # Add size-based tags
        try:
            size_mb = file_path.stat().st_size / (1024 * 1024)
            if size_mb < 1:
                tags.append('size_small')
            elif size_mb < 100:
                tags.append('size_medium')
            else:
                tags.append('size_large')
        except:
            pass
        
        return tags
    
    def _enrich_file_metadata(self, asset: Dict[str, Any], file_path: Path):
        """Enrich asset metadata based on file type"""
        try:
            file_type = asset['type']
            
            if file_type == 'csv_file':
                self._enrich_csv_metadata(asset, file_path)
            elif file_type == 'json_file':
                self._enrich_json_metadata(asset, file_path)
            elif file_type == 'excel_file':
                self._enrich_excel_metadata(asset, file_path)
            elif file_type == 'sqlite_database':
                self._enrich_sqlite_metadata(asset, file_path)
            elif file_type == 'parquet_file':
                self._enrich_parquet_metadata(asset, file_path)
                
        except Exception as e:
            self.logger.warning(f"Could not enrich metadata for {file_path}: {e}")
    
    def _enrich_csv_metadata(self, asset: Dict[str, Any], file_path: Path):
        """Enrich CSV file metadata"""
        try:
            # Read first few rows to get schema info
            df = pd.read_csv(file_path, nrows=5)
            
            asset['metadata'].update({
                'row_count_sample': len(df),
                'column_count': len(df.columns),
                'columns': df.columns.tolist(),
                'data_types': df.dtypes.astype(str).to_dict()
            })
            
            # Try to get full row count (for smaller files)
            if asset['size'] < 50 * 1024 * 1024:  # 50MB limit
                full_df = pd.read_csv(file_path)
                asset['metadata']['row_count'] = len(full_df)
                
        except Exception as e:
            self.logger.warning(f"Could not read CSV metadata for {file_path}: {e}")
    
    def _enrich_json_metadata(self, asset: Dict[str, Any], file_path: Path):
        """Enrich JSON file metadata"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            asset['metadata'].update({
                'json_type': type(data).__name__,
                'is_array': isinstance(data, list),
                'keys': list(data.keys()) if isinstance(data, dict) else None,
                'array_length': len(data) if isinstance(data, list) else None
            })
            
        except Exception as e:
            self.logger.warning(f"Could not read JSON metadata for {file_path}: {e}")
    
    def _enrich_excel_metadata(self, asset: Dict[str, Any], file_path: Path):
        """Enrich Excel file metadata"""
        try:
            # Get sheet names
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names
            
            asset['metadata'].update({
                'sheet_count': len(sheet_names),
                'sheet_names': sheet_names
            })
            
            # Get info about first sheet
            if sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet_names[0], nrows=5)
                asset['metadata'].update({
                    'first_sheet_columns': df.columns.tolist(),
                    'first_sheet_column_count': len(df.columns)
                })
                
        except Exception as e:
            self.logger.warning(f"Could not read Excel metadata for {file_path}: {e}")
    
    def _enrich_sqlite_metadata(self, asset: Dict[str, Any], file_path: Path):
        """Enrich SQLite database metadata"""
        try:
            conn = sqlite3.connect(file_path)
            cursor = conn.cursor()
            
            # Get table names
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            asset['metadata'].update({
                'table_count': len(tables),
                'table_names': tables
            })
            
            conn.close()
            
        except Exception as e:
            self.logger.warning(f"Could not read SQLite metadata for {file_path}: {e}")
    
    def _enrich_parquet_metadata(self, asset: Dict[str, Any], file_path: Path):
        """Enrich Parquet file metadata"""
        try:
            df = pd.read_parquet(file_path)
            
            asset['metadata'].update({
                'row_count': len(df),
                'column_count': len(df.columns),
                'columns': df.columns.tolist(),
                'data_types': df.dtypes.astype(str).to_dict()
            })
            
        except Exception as e:
            self.logger.warning(f"Could not read Parquet metadata for {file_path}: {e}")
    
    def test_connection(self) -> bool:
        """Test file system access"""
        try:
            for scan_path in self.scan_paths:
                path_obj = Path(scan_path)
                if not path_obj.exists():
                    self.logger.error(f"Scan path does not exist: {scan_path}")
                    return False
                if not os.access(scan_path, os.R_OK):
                    self.logger.error(f"No read access to scan path: {scan_path}")
                    return False
            return True
        except Exception as e:
            self.logger.error(f"File system connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate file system connector configuration"""
        if not self.scan_paths:
            self.logger.error("No scan paths configured")
            return False
        
        return True
