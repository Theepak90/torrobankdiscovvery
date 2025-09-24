"""
Metadata Extractor - Extracts and enriches metadata for discovered data assets
"""

import re
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional
import logging
class MetadataExtractor:
    """
    Extracts and enriches metadata for data assets
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.extract_schema = config.get('extract_schema', True)
        self.extract_samples = config.get('extract_samples', True)
        self.sample_size = config.get('sample_size', 100)
        self.detect_pii = config.get('detect_pii', True)
        
        self.pii_patterns = {
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'phone': r'(\+\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
            'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
            'credit_card': r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b',
            'ip_address': r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b'
        }
        
        self.sensitive_column_patterns = [
            r'.*password.*', r'.*pwd.*', r'.*pass.*',
            r'.*ssn.*', r'.*social.*security.*',
            r'.*credit.*card.*', r'.*cc.*number.*',
            r'.*account.*number.*', r'.*routing.*number.*',
            r'.*tax.*id.*', r'.*ein.*',
            r'.*api.*key.*', r'.*secret.*', r'.*token.*'
        ]
    
    def extract_metadata(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract comprehensive metadata for a data asset
        
        Args:
            asset: Asset dictionary
            
        Returns:
            Dictionary with extracted metadata
        """
        metadata = {}
        
        try:
            metadata.update(self._extract_basic_metadata(asset))
            
            metadata.update(self._extract_quality_metrics(asset))
            
            if self.detect_pii:
                metadata.update(self._detect_pii_data(asset))
            
            metadata.update(self._extract_business_context(asset))
            
            metadata.update(self._extract_technical_metadata(asset))
            
            metadata.update(self._extract_lineage_hints(asset))
            
        except Exception as e:
            self.logger.error(f"Error extracting metadata for asset {asset.get('name', 'unknown')}: {e}")
        
        return metadata
    
    def _extract_basic_metadata(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """Extract basic metadata"""
        metadata = {
            'discovery_timestamp': datetime.now().isoformat(),
            'data_classification': self._classify_data_type(asset),
            'estimated_value': self._estimate_data_value(asset)
        }
        
        if 'file' in asset.get('type', '').lower():
            metadata.update({
                'file_extension': asset.get('extension', ''),
                'file_directory': asset.get('directory', ''),
                'file_permissions': asset.get('metadata', {}).get('permissions', '')
            })
        
        return metadata
    
    def _extract_quality_metrics(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data quality metrics"""
        quality_metrics = {
            'completeness_score': 0.0,
            'consistency_score': 0.0,
            'freshness_score': 0.0,
            'accuracy_score': 0.0
        }
        
        try:
            if asset.get('schema', {}).get('columns'):
                quality_metrics['completeness_score'] = 0.8  # Default high score if schema exists
            
            if asset.get('modified_date'):
                days_old = (datetime.now() - asset['modified_date']).days
                if days_old < 1:
                    quality_metrics['freshness_score'] = 1.0
                elif days_old < 7:
                    quality_metrics['freshness_score'] = 0.9
                elif days_old < 30:
                    quality_metrics['freshness_score'] = 0.7
                elif days_old < 90:
                    quality_metrics['freshness_score'] = 0.5
                else:
                    quality_metrics['freshness_score'] = 0.3
            
            name = asset.get('name', '')
            if re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', name):
                quality_metrics['consistency_score'] = 0.8
            else:
                quality_metrics['consistency_score'] = 0.6
            
            quality_metrics['overall_quality_score'] = sum(quality_metrics.values()) / len(quality_metrics)
            
        except Exception as e:
            self.logger.warning(f"Error calculating quality metrics: {e}")
        
        return {'quality_metrics': quality_metrics}
    
    def _detect_pii_data(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """Detect potential PII in the asset"""
        pii_detection = {
            'contains_pii': False,
            'pii_types': [],
            'sensitive_columns': [],
            'privacy_risk_level': 'low'
        }
        
        try:
            columns = asset.get('schema', {}).get('columns', [])
            if isinstance(columns, list):
                for column in columns:
                    column_name = column.get('name', '') if isinstance(column, dict) else str(column)
                    
                    for pattern in self.sensitive_column_patterns:
                        if re.match(pattern, column_name.lower()):
                            pii_detection['sensitive_columns'].append(column_name)
                            pii_detection['contains_pii'] = True
            
            asset_text = f"{asset.get('name', '')} {asset.get('location', '')}"
            for pii_type, pattern in self.pii_patterns.items():
                if re.search(pattern, asset_text, re.IGNORECASE):
                    pii_detection['pii_types'].append(pii_type)
                    pii_detection['contains_pii'] = True
            
            if pii_detection['contains_pii']:
                if len(pii_detection['pii_types']) > 2 or len(pii_detection['sensitive_columns']) > 3:
                    pii_detection['privacy_risk_level'] = 'high'
                elif len(pii_detection['pii_types']) > 0 or len(pii_detection['sensitive_columns']) > 1:
                    pii_detection['privacy_risk_level'] = 'medium'
                else:
                    pii_detection['privacy_risk_level'] = 'low'
            
        except Exception as e:
            self.logger.warning(f"Error in PII detection: {e}")
        
        return {'pii_detection': pii_detection}
    
    def _extract_business_context(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """Extract business context from asset metadata"""
        business_context = {
            'domain': 'unknown',
            'business_importance': 'medium',
            'usage_frequency': 'unknown',
            'stakeholders': []
        }
        
        try:
            name_location = f"{asset.get('name', '')} {asset.get('location', '')}".lower()
            
            domain_keywords = {
                'finance': ['finance', 'financial', 'payment', 'transaction', 'invoice', 'billing', 'accounting'],
                'hr': ['hr', 'human', 'employee', 'staff', 'payroll', 'personnel'],
                'sales': ['sales', 'customer', 'crm', 'lead', 'opportunity', 'revenue'],
                'marketing': ['marketing', 'campaign', 'promotion', 'advertisement', 'social'],
                'operations': ['operations', 'inventory', 'supply', 'logistics', 'warehouse'],
                'analytics': ['analytics', 'report', 'dashboard', 'metric', 'kpi', 'analysis']
            }
            
            for domain, keywords in domain_keywords.items():
                if any(keyword in name_location for keyword in keywords):
                    business_context['domain'] = domain
                    break
            
            size = asset.get('size', 0)
            if business_context['domain'] in ['finance', 'hr'] or size > 1000000:  # 1MB+
                business_context['business_importance'] = 'high'
            elif size > 100000:  # 100KB+
                business_context['business_importance'] = 'medium'
            else:
                business_context['business_importance'] = 'low'
            
        except Exception as e:
            self.logger.warning(f"Error extracting business context: {e}")
        
        return {'business_context': business_context}
    
    def _extract_technical_metadata(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """Extract technical metadata"""
        technical_metadata = {
            'data_format': self._determine_data_format(asset),
            'encoding': 'utf-8',  # Default assumption
            'compression': None,
            'partitioning': None,
            'indexing': None
        }
        
        try:
            asset_name = asset.get('name', '').lower()
            if any(ext in asset_name for ext in ['.gz', '.zip', '.bz2', '.lz4']):
                technical_metadata['compression'] = 'compressed'
            
            location = asset.get('location', '').lower()
            if any(pattern in location for pattern in ['year=', 'month=', 'day=', 'partition']):
                technical_metadata['partitioning'] = 'partitioned'
            
            if 'database' in asset.get('source', ''):
                technical_metadata.update({
                    'row_count': asset.get('metadata', {}).get('row_count', 0),
                    'column_count': asset.get('metadata', {}).get('column_count', 0),
                    'has_primary_key': self._has_primary_key(asset),
                    'has_foreign_keys': self._has_foreign_keys(asset)
                })
            
        except Exception as e:
            self.logger.warning(f"Error extracting technical metadata: {e}")
        
        return {'technical_metadata': technical_metadata}
    
    def _extract_lineage_hints(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data lineage hints"""
        lineage_hints = {
            'potential_sources': [],
            'potential_targets': [],
            'transformation_hints': [],
            'update_frequency': 'unknown'
        }
        
        try:
            asset_name = asset.get('name', '').lower()
            location = asset.get('location', '').lower()
            
            etl_patterns = ['etl', 'transform', 'process', 'clean', 'stage', 'raw', 'curated']
            for pattern in etl_patterns:
                if pattern in asset_name or pattern in location:
                    lineage_hints['transformation_hints'].append(pattern)
            
            if any(pattern in asset_name for pattern in ['daily', 'hourly', 'weekly', 'monthly']):
                for pattern in ['daily', 'hourly', 'weekly', 'monthly']:
                    if pattern in asset_name:
                        lineage_hints['update_frequency'] = pattern
                        break
            
            if any(pattern in location for pattern in ['backup', 'archive', 'historical']):
                lineage_hints['transformation_hints'].append('archived')
            
        except Exception as e:
            self.logger.warning(f"Error extracting lineage hints: {e}")
        
        return {'lineage_hints': lineage_hints}
    
        """Generate a unique fingerprint for the asset"""
        fingerprint_data = f"{asset.get('name', '')}{asset.get('location', '')}{asset.get('size', 0)}"
        return hashlib.md5(fingerprint_data.encode()).hexdigest()
    
    def _classify_data_type(self, asset: Dict[str, Any]) -> str:
        """Classify the type of data"""
        asset_type = asset.get('type', '').lower()
        
        if 'csv' in asset_type or 'excel' in asset_type:
            return 'structured'
        elif 'json' in asset_type or 'xml' in asset_type:
            return 'semi-structured'
        elif 'table' in asset_type or 'database' in asset_type:
            return 'structured'
        elif 'text' in asset_type or 'log' in asset_type:
            return 'unstructured'
        else:
            return 'unknown'
    
    def _estimate_data_value(self, asset: Dict[str, Any]) -> str:
        """Estimate the business value of the data"""
        size = asset.get('size', 0)
        asset_type = asset.get('type', '').lower()
        
        high_value_indicators = ['customer', 'transaction', 'financial', 'revenue', 'sales']
        medium_value_indicators = ['user', 'product', 'inventory', 'log', 'event']
        
        asset_text = f"{asset.get('name', '')} {asset.get('location', '')}".lower()
        
        if any(indicator in asset_text for indicator in high_value_indicators):
            return 'high'
        elif any(indicator in asset_text for indicator in medium_value_indicators):
            return 'medium'
        elif size > 1000000:  # Large datasets might be valuable
            return 'medium'
        else:
            return 'low'
    
    def _determine_data_format(self, asset: Dict[str, Any]) -> str:
        """Determine the data format"""
        asset_type = asset.get('type', '').lower()
        
        format_mapping = {
            'csv': 'csv',
            'json': 'json',
            'parquet': 'parquet',
            'avro': 'avro',
            'orc': 'orc',
            'excel': 'excel',
            'xml': 'xml',
            'yaml': 'yaml',
            'table': 'sql_table',
            'view': 'sql_view'
        }
        
        for key, format_type in format_mapping.items():
            if key in asset_type:
                return format_type
        
        return 'unknown'
    
    def _has_primary_key(self, asset: Dict[str, Any]) -> bool:
        """Check if database table has a primary key"""
        columns = asset.get('schema', {}).get('columns', [])
        if isinstance(columns, list):
            for column in columns:
                if isinstance(column, dict) and column.get('primary_key'):
                    return True
        return False
    
    def _has_foreign_keys(self, asset: Dict[str, Any]) -> bool:
        """Check if database table has foreign keys"""
        columns = asset.get('schema', {}).get('columns', [])
        if isinstance(columns, list):
            for column in columns:
                column_name = column.get('name', '') if isinstance(column, dict) else str(column)
                if column_name.lower().endswith('_id') and column_name.lower() != 'id':
                    return True
        return False
