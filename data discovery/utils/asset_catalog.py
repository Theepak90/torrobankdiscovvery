"""
Asset Catalog - Manages and stores discovered data assets
"""

import json
import sqlite3
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
import logging
class AssetCatalog:
    """
    Manages the catalog of discovered data assets
    """
    
    def __init__(self, catalog_db_path: str = "asset_catalog.db"):
        self.catalog_db_path = catalog_db_path
        self.logger = logging.getLogger(self.__class__.__name__)
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize the SQLite database for asset catalog"""
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    source TEXT NOT NULL,
                    location TEXT NOT NULL,
                    size INTEGER DEFAULT 0,
                    created_date TEXT,
                    modified_date TEXT,
                    discovered_date TEXT NOT NULL,
                    last_scanned TEXT NOT NULL,
                    schema_json TEXT,
                    tags_json TEXT,
                    metadata_json TEXT,
                    fingerprint TEXT UNIQUE,
                    is_active BOOLEAN DEFAULT 1
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_name ON assets(name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_type ON assets(type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_source ON assets(source)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_fingerprint ON assets(fingerprint)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_active ON assets(is_active)')
            
            # Create asset_history table for tracking changes
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS asset_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_fingerprint TEXT NOT NULL,
                    change_type TEXT NOT NULL,
                    change_date TEXT NOT NULL,
                    old_values_json TEXT,
                    new_values_json TEXT,
                    FOREIGN KEY (asset_fingerprint) REFERENCES assets(fingerprint)
                )
            ''')
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Asset catalog database initialized at {self.catalog_db_path}")
            
        except Exception as e:
            self.logger.error(f"Error initializing asset catalog database: {e}")
    
    async def add_or_update_asset(self, asset: Dict[str, Any]) -> bool:
        """
        Add a new asset or update an existing one
        
        Args:
            asset: Asset dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Generate fingerprint if not present
            fingerprint = asset.get('metadata', {}).get('asset_fingerprint')
            if not fingerprint:
                # Generate fingerprint from location and name
                import hashlib
                fingerprint_data = f"{asset.get('source', '')}-{asset.get('location', '')}-{asset.get('name', '')}"
                fingerprint = hashlib.md5(fingerprint_data.encode()).hexdigest()
                
                if 'metadata' not in asset:
                    asset['metadata'] = {}
                asset['metadata']['asset_fingerprint'] = fingerprint
            
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            # Check if asset already exists
            cursor.execute('SELECT * FROM assets WHERE fingerprint = ?', (fingerprint,))
            existing_asset = cursor.fetchone()
            
            current_time = datetime.now().isoformat()
            
            if existing_asset:
                self._update_existing_asset(cursor, asset, fingerprint, current_time)
            else:
                self._insert_new_asset(cursor, asset, fingerprint, current_time)
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding/updating asset {asset.get('name', 'unknown')}: {e}")
            return False
    
    def add_asset(self, asset: Dict[str, Any]) -> bool:
        """
        Synchronous version of add_or_update_asset for use by discovery engine
        
        Args:
            asset: Asset dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Generate fingerprint if not present
            fingerprint = asset.get('metadata', {}).get('asset_fingerprint')
            if not fingerprint:
                # Generate fingerprint from location and name
                import hashlib
                fingerprint_data = f"{asset.get('source', '')}-{asset.get('location', '')}-{asset.get('name', '')}"
                fingerprint = hashlib.md5(fingerprint_data.encode()).hexdigest()
                
                if 'metadata' not in asset:
                    asset['metadata'] = {}
                asset['metadata']['asset_fingerprint'] = fingerprint
            
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            # Check if asset already exists
            cursor.execute('SELECT * FROM assets WHERE fingerprint = ?', (fingerprint,))
            existing_asset = cursor.fetchone()
            
            current_time = datetime.now().isoformat()
            
            if existing_asset:
                self._update_existing_asset(cursor, asset, fingerprint, current_time)
            else:
                self._insert_new_asset(cursor, asset, fingerprint, current_time)
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding asset {asset.get('name', 'unknown')}: {e}")
            return False
    
    def _insert_new_asset(self, cursor, asset: Dict[str, Any], fingerprint: str, current_time: str):
        """Insert a new asset into the catalog"""
        cursor.execute('''
            INSERT INTO assets (
                name, type, source, location, size, created_date, modified_date,
                discovered_date, last_scanned, schema_json, tags_json, metadata_json, fingerprint
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            asset.get('name', ''),
            asset.get('type', ''),
            asset.get('source', ''),
            asset.get('location', ''),
            asset.get('size', 0),
            asset.get('created_date') if isinstance(asset.get('created_date'), str) else (asset.get('created_date').isoformat() if asset.get('created_date') else None),
            asset.get('modified_date') if isinstance(asset.get('modified_date'), str) else (asset.get('modified_date').isoformat() if asset.get('modified_date') else None),
            current_time,
            current_time,
            json.dumps(asset.get('schema', {})),
            json.dumps(asset.get('tags', [])),
            json.dumps(asset.get('metadata', {})),
            fingerprint
        ))
        
        cursor.execute('''
            INSERT INTO asset_history (asset_fingerprint, change_type, change_date, new_values_json)
            VALUES (?, ?, ?, ?)
        ''', (fingerprint, 'CREATED', current_time, json.dumps(asset)))
        
        self.logger.info(f"Added new asset: {asset.get('name')}")
    
    def _update_existing_asset(self, cursor, asset: Dict[str, Any], fingerprint: str, current_time: str):
        """Update an existing asset in the catalog"""
        # Get current values for history tracking
        cursor.execute('SELECT metadata_json FROM assets WHERE fingerprint = ?', (fingerprint,))
        old_metadata = cursor.fetchone()[0]
        
        cursor.execute('''
            UPDATE assets SET
                name = ?, type = ?, source = ?, location = ?, size = ?,
                modified_date = ?, last_scanned = ?, schema_json = ?,
                tags_json = ?, metadata_json = ?, is_active = 1
            WHERE fingerprint = ?
        ''', (
            asset.get('name', ''),
            asset.get('type', ''),
            asset.get('source', ''),
            asset.get('location', ''),
            asset.get('size', 0),
            asset.get('modified_date') if isinstance(asset.get('modified_date'), str) else (asset.get('modified_date').isoformat() if asset.get('modified_date') else None),
            current_time,
            json.dumps(asset.get('schema', {})),
            json.dumps(asset.get('tags', [])),
            json.dumps(asset.get('metadata', {})),
            fingerprint
        ))
        
        cursor.execute('''
            INSERT INTO asset_history (asset_fingerprint, change_type, change_date, old_values_json, new_values_json)
            VALUES (?, ?, ?, ?, ?)
        ''', (fingerprint, 'UPDATED', current_time, old_metadata, json.dumps(asset.get('metadata', {}))))
        
        self.logger.debug(f"Updated existing asset: {asset.get('name')}")
    
    def get_asset_by_fingerprint(self, fingerprint: str) -> Optional[Dict[str, Any]]:
        """Get an asset by its fingerprint"""
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM assets WHERE fingerprint = ? AND is_active = 1', (fingerprint,))
            row = cursor.fetchone()
            
            conn.close()
            
            if row:
                return self._row_to_asset_dict(row)
            return None
            
        except Exception as e:
            self.logger.error(f"Error retrieving asset by fingerprint {fingerprint}: {e}")
            return None
    
    def search_assets(self, query: str, asset_type: str = None, source: str = None, 
                     limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search assets in the catalog
        
        Args:
            query: Search query string
            asset_type: Filter by asset type
            source: Filter by source
            limit: Maximum number of results
            
        Returns:
            List of matching assets
        """
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            where_conditions = ['is_active = 1']
            params = []
            
            if query:
                where_conditions.append('(name LIKE ? OR location LIKE ?)')
                params.extend([f'%{query}%', f'%{query}%'])
            
            if asset_type:
                where_conditions.append('type = ?')
                params.append(asset_type)
            
            if source:
                where_conditions.append('source = ?')
                params.append(source)
            
            sql = f'''
                SELECT * FROM assets 
                WHERE {' AND '.join(where_conditions)}
                ORDER BY last_scanned DESC
                LIMIT ?
            '''
            params.append(limit)
            
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            
            conn.close()
            
            return [self._row_to_asset_dict(row) for row in rows]
            
        except Exception as e:
            self.logger.error(f"Error searching assets: {e}")
            return []
    
    def get_assets_by_type(self, asset_type: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all assets of a specific type"""
        return self.search_assets('', asset_type=asset_type, limit=limit)
    
    def get_assets_by_source(self, source: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all assets from a specific source"""
        return self.search_assets('', source=source, limit=limit)
    
    def get_catalog_statistics(self) -> Dict[str, Any]:
        """Get catalog statistics"""
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) FROM assets WHERE is_active = 1')
            total_assets = cursor.fetchone()[0]
            
            cursor.execute('''
                SELECT type, COUNT(*) as count 
                FROM assets 
                WHERE is_active = 1 
                GROUP BY type 
                ORDER BY count DESC
            ''')
            assets_by_type = dict(cursor.fetchall())
            
            cursor.execute('''
                SELECT source, COUNT(*) as count 
                FROM assets 
                WHERE is_active = 1 
                GROUP BY source 
                ORDER BY count DESC
            ''')
            assets_by_source = dict(cursor.fetchall())
            
            cursor.execute('''
                SELECT COUNT(*) 
                FROM assets 
                WHERE is_active = 1 
                AND discovered_date > datetime('now', '-7 days')
            ''')
            recent_discoveries = cursor.fetchone()[0]
            
            cursor.execute('SELECT SUM(size) FROM assets WHERE is_active = 1')
            total_size = cursor.fetchone()[0] or 0
            
            conn.close()
            
            return {
                'total_assets': total_assets,
                'assets_by_type': assets_by_type,
                'assets_by_source': assets_by_source,
                'recent_discoveries': recent_discoveries,
                'total_size_bytes': total_size,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting catalog statistics: {e}")
            return {}
    
    def mark_assets_inactive(self, fingerprints: List[str]) -> bool:
        """Mark assets as inactive (soft delete)"""
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            current_time = datetime.now().isoformat()
            
            for fingerprint in fingerprints:
                cursor.execute('''
                    UPDATE assets SET is_active = 0 WHERE fingerprint = ?
                ''', (fingerprint,))
                
                cursor.execute('''
                    INSERT INTO asset_history (asset_fingerprint, change_type, change_date)
                    VALUES (?, ?, ?)
                ''', (fingerprint, 'DEACTIVATED', current_time))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Marked {len(fingerprints)} assets as inactive")
            return True
            
        except Exception as e:
            self.logger.error(f"Error marking assets inactive: {e}")
            return False
    
    def get_asset_history(self, fingerprint: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get change history for an asset"""
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM asset_history 
                WHERE asset_fingerprint = ? 
                ORDER BY change_date DESC 
                LIMIT ?
            ''', (fingerprint, limit))
            
            rows = cursor.fetchall()
            conn.close()
            
            history = []
            for row in rows:
                history_entry = {
                    'change_type': row['change_type'],
                    'change_date': row['change_date'],
                    'old_values': json.loads(row['old_values_json']) if row['old_values_json'] else None,
                    'new_values': json.loads(row['new_values_json']) if row['new_values_json'] else None
                }
                history.append(history_entry)
            
            return history
            
        except Exception as e:
            self.logger.error(f"Error getting asset history for {fingerprint}: {e}")
            return []
    
    def _row_to_asset_dict(self, row) -> Dict[str, Any]:
        """Convert database row to asset dictionary"""
        return {
            'id': row['id'],
            'name': row['name'],
            'type': row['type'],
            'source': row['source'],
            'location': row['location'],
            'size': row['size'],
            'created_date': row['created_date'],
            'modified_date': row['modified_date'],
            'discovered_date': row['discovered_date'],
            'last_scanned': row['last_scanned'],
            'schema': json.loads(row['schema_json']) if row['schema_json'] else {},
            'tags': json.loads(row['tags_json']) if row['tags_json'] else [],
            'metadata': json.loads(row['metadata_json']) if row['metadata_json'] else {},
            'fingerprint': row['fingerprint'],
            'is_active': bool(row['is_active'])
        }
    
    def export_catalog(self, output_file: str, format: str = 'json') -> bool:
        """
        Export the entire catalog to a file
        
        Args:
            output_file: Output file path
            format: Export format ('json' or 'csv')
            
        Returns:
            True if successful, False otherwise
        """
        try:
            assets = self.search_assets('', limit=10000)  # Get all assets
            
            if format.lower() == 'json':
                with open(output_file, 'w') as f:
                    json.dump({
                        'export_date': datetime.now().isoformat(),
                        'total_assets': len(assets),
                        'assets': assets
                    }, f, indent=2, default=str)
            
            elif format.lower() == 'csv':
                import csv
                
                with open(output_file, 'w', newline='') as f:
                    if assets:
                        writer = csv.DictWriter(f, fieldnames=assets[0].keys())
                        writer.writeheader()
                        for asset in assets:
                            # Convert complex fields to JSON strings for CSV
                            row = asset.copy()
                            row['schema'] = json.dumps(row['schema'])
                            row['tags'] = json.dumps(row['tags'])
                            row['metadata'] = json.dumps(row['metadata'])
                            writer.writerow(row)
            
            self.logger.info(f"Exported {len(assets)} assets to {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting catalog: {e}")
            return False
    
    def cleanup_old_history(self, days_to_keep: int = 90) -> bool:
        """Clean up old history entries"""
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                DELETE FROM asset_history 
                WHERE change_date < datetime('now', '-{} days')
            '''.format(days_to_keep))
            
            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()
            
            self.logger.info(f"Cleaned up {deleted_count} old history entries")
            return True
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old history: {e}")
            return False
    
    async def add_custom_relationship(self, relationship_data: dict) -> bool:
        """Add a custom lineage relationship"""
        try:
            # Initialize relationships table if it doesn't exist
            self._initialize_relationships_table()
            
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO custom_relationships 
                (id, source_asset, target_asset, relationship_type, confidence, description, created_by, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                relationship_data['id'],
                relationship_data['source'],
                relationship_data['target'],
                relationship_data['relationship'],
                relationship_data['confidence'],
                relationship_data.get('description', ''),
                relationship_data.get('created_by', 'user'),
                relationship_data['created_at']
            ))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Added custom relationship: {relationship_data['id']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding custom relationship: {e}")
            return False
    
    async def get_custom_relationships(self) -> list:
        """Get all custom lineage relationships"""
        try:
            self._initialize_relationships_table()
            
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id, source_asset, target_asset, relationship_type, confidence, 
                       description, created_by, created_at
                FROM custom_relationships
                ORDER BY created_at DESC
            ''')
            
            relationships = []
            for row in cursor.fetchall():
                relationships.append({
                    'id': row[0],
                    'source': row[1],
                    'target': row[2],
                    'relationship': row[3],
                    'confidence': row[4],
                    'description': row[5],
                    'created_by': row[6],
                    'created_at': row[7]
                })
            
            conn.close()
            return relationships
            
        except Exception as e:
            self.logger.error(f"Error getting custom relationships: {e}")
            return []
    
    async def delete_custom_relationship(self, relationship_id: str) -> bool:
        """Delete a custom lineage relationship"""
        try:
            self._initialize_relationships_table()
            
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            cursor.execute('DELETE FROM custom_relationships WHERE id = ?', (relationship_id,))
            deleted_count = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            if deleted_count > 0:
                self.logger.info(f"Deleted custom relationship: {relationship_id}")
                return True
            else:
                self.logger.warning(f"Relationship not found: {relationship_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error deleting custom relationship: {e}")
            return False
    
    async def get_asset_relationships(self, asset_id: str) -> list:
        """Get all relationships for a specific asset"""
        try:
            self._initialize_relationships_table()
            
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id, source_asset, target_asset, relationship_type, confidence, 
                       description, created_by, created_at
                FROM custom_relationships
                WHERE source_asset = ? OR target_asset = ?
                ORDER BY created_at DESC
            ''', (asset_id, asset_id))
            
            relationships = []
            for row in cursor.fetchall():
                relationships.append({
                    'id': row[0],
                    'source': row[1],
                    'target': row[2],
                    'relationship': row[3],
                    'confidence': row[4],
                    'description': row[5],
                    'created_by': row[6],
                    'created_at': row[7],
                    'direction': 'upstream' if row[2] == asset_id else 'downstream'
                })
            
            conn.close()
            return relationships
            
        except Exception as e:
            self.logger.error(f"Error getting asset relationships: {e}")
            return []
    
    def _initialize_relationships_table(self):
        """Initialize the custom relationships table"""
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS custom_relationships (
                    id TEXT PRIMARY KEY,
                    source_asset TEXT NOT NULL,
                    target_asset TEXT NOT NULL,
                    relationship_type TEXT NOT NULL,
                    confidence REAL DEFAULT 1.0,
                    description TEXT,
                    created_by TEXT DEFAULT 'user',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (source_asset) REFERENCES assets(fingerprint),
                    FOREIGN KEY (target_asset) REFERENCES assets(fingerprint)
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_source_asset ON custom_relationships(source_asset)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_target_asset ON custom_relationships(target_asset)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_relationship_type ON custom_relationships(relationship_type)')
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error initializing relationships table: {e}")