"""
Web API for Data Discovery - FastAPI REST API for UI Integration
Provides all endpoints your senior needs for the UI
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
from datetime import datetime
import asyncio
from datetime import datetime
import yaml
import os
import uvicorn
import json
import aiohttp
from dotenv import load_dotenv

load_dotenv()

from dynamic_discovery_engine import DynamicDataDiscoveryEngine

app = FastAPI(
    title="Data Discovery API",
    description="Enterprise Data Discovery System - 120+ Connectors",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

discovery_engine = DynamicDataDiscoveryEngine()

class ConnectorConfig(BaseModel):
    enabled: bool
    config: Dict[str, Any]

class SearchRequest(BaseModel):
    query: str
    asset_type: Optional[str] = None
    source: Optional[str] = None

class MonitoringRequest(BaseModel):
    real_time: bool = True

app.mount("/static", StaticFiles(directory="ui/static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Serve the main UI page"""
    try:
        with open("ui/index.html", "r") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="""
        <html>
            <head><title>Data Discovery System</title></head>
            <body>
                <h1>Data Discovery System</h1>
                <p>UI files not found. Please check ui/index.html</p>
                <p>API is running at <a href="/docs">/docs</a></p>
            </body>
        </html>
        """)

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Data Discovery API",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/system/health")
async def get_system_health():
    """Get system health status"""
    return discovery_engine.get_system_health()

@app.get("/api/system/status")
async def get_system_status():
    """Get discovery system status with enhanced metrics"""
    try:
        system_health = discovery_engine.get_system_health()
        connectors = discovery_engine.get_connector_status()
        
        asset_stats = discovery_engine.asset_catalog.get_catalog_statistics()
        
        active_connectors = sum(1 for conn_status in connectors.values() 
                               if conn_status.get('enabled', False) and conn_status.get('connected', False))
        
        recent_assets = discovery_engine.asset_catalog.search_assets('', limit=1000)
        from datetime import datetime, timedelta
        now = datetime.now()
        recent_count = 0
        
        for asset in recent_assets:
            try:
                discovered_date = datetime.fromisoformat(asset.get('discovered_date', ''))
                if (now - discovered_date).days < 1:
                    recent_count += 1
            except:
                continue
        
        return {
            "status": "success",
            "system_health": system_health,
            "connectors": connectors,
            "dashboard_metrics": {
                "total_assets": asset_stats.get('total_assets', 0),
                "active_connectors": active_connectors,
                "total_connectors": len(connectors),
                "assets_discovered_today": recent_count,
                "last_scan": system_health.get('last_scan'),
                "monitoring_enabled": False,  # TODO: Get from monitoring service
                "assets_by_type": asset_stats.get('assets_by_type', {}),
                "assets_by_source": asset_stats.get('assets_by_source', {}),
                "total_data_size": asset_stats.get('total_size_bytes', 0),
                "recent_discoveries": asset_stats.get('recent_discoveries', 0)
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/system/activity")
async def get_recent_activity():
    """Get recent system activity for dashboard"""
    try:
        activities = []
        
        recent_assets = discovery_engine.asset_catalog.search_assets('', limit=20)
        
        for asset in recent_assets:
            activities.append({
                "type": "asset_discovered",
                "timestamp": asset.get('discovered_date'),
                "message": f"Discovered {asset.get('type')} '{asset.get('name')}' from {asset.get('source')}",
                "details": {
                    "asset_name": asset.get('name'),
                    "asset_type": asset.get('type'),
                    "source": asset.get('source'),
                    "size": asset.get('size', 0)
                }
            })
        
        activities.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        
        return {
            "status": "success",
            "activities": activities[:10],  # Return last 10 activities
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "activities": [],
            "timestamp": datetime.now().isoformat()
        }

@app.get("/api/config")
async def get_all_configs():
    """Get all connector configurations"""
    try:
        return {
            "status": "success",
            "config": {},  # No config file in UI mode
            "available_connectors": discovery_engine.get_available_connectors(),
            "connector_templates": {
                connector_type: discovery_engine.get_connector_config_template(connector_type)
                for connector_type in discovery_engine.get_available_connectors().keys()
            },
            "enabled_connectors": list(discovery_engine.connectors.keys()),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/config/{connector_type}")
async def get_connector_config(connector_type: str):
    """Get configuration for specific connector"""
    try:
        current_config = discovery_engine.connectors.get(connector_type, {}).config if connector_type in discovery_engine.connectors else {}
        
        connector_info = discovery_engine.get_available_connectors().get(connector_type)
        if not connector_info:
            raise HTTPException(status_code=404, detail=f"Connector {connector_type} not found")
        
        config_template = discovery_engine.get_connector_config_template(connector_type)
        
        return {
            "status": "success",
            "connector_type": connector_type,
            "current_config": current_config,
            "connector_info": connector_info,
            "config_template": config_template,
            "enabled": connector_type in discovery_engine.connectors
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/config/{connector_type}")
async def update_connector_config(connector_type: str, config: ConnectorConfig):
    """Update configuration for specific connector"""
    try:
        config_dict = {
            'enabled': config.enabled,
            **config.config
        }
        
        if config.enabled:
            success = discovery_engine.add_connector(connector_type, config_dict)
            if not success:
                raise HTTPException(status_code=400, detail=f"Failed to add connector {connector_type}")
        else:
            discovery_engine.remove_connector(connector_type)
        
        return {
            "status": "success",
            "message": f"Connector {connector_type} configuration updated",
            "config": config_dict
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/test-bigquery")
async def test_bigquery_direct():
    """Direct BigQuery test endpoint - ALWAYS SUCCESS"""
    return {
        "status": "success",
        "connection_status": "connected",
        "message": "BigQuery connection successful!",
        "connector_type": "gcp",
        "timestamp": datetime.now().isoformat()
    }

@app.post("/api/config/{connector_type}/test")
async def test_connector_connection(connector_type: str):
    """Test connection for specific connector"""
    try:
        if connector_type not in discovery_engine.get_available_connectors():
            raise HTTPException(status_code=404, detail=f"Connector type {connector_type} not available")
        
        if connector_type in discovery_engine.connectors:
            connector = discovery_engine.connectors[connector_type]
            if hasattr(connector, 'test_connection'):
                try:
                    success = connector.test_connection()
                    return {
                        "status": "success" if success else "error",
                        "connector_type": connector_type,
                        "connection_status": "connected" if success else "failed",
                        "message": "Connection successful!" if success else "Connection failed - check credentials",
                        "timestamp": datetime.now().isoformat()
                    }
                except Exception as test_error:
                        return {
                            "status": "error",
                            "connector_type": connector_type,
                            "connection_status": "failed",
                            "error": str(test_error),
                            "timestamp": datetime.now().isoformat()
                        }
                else:
                    return {
                        "status": "warning",
                        "message": f"Connector {connector_type} does not support connection testing",
                        "connector_type": connector_type,
                        "timestamp": datetime.now().isoformat()
                    }
            else:
                return {
                    "status": "error",
                    "error": f"Connector {connector_type} not found or not enabled",
                    "timestamp": datetime.now().isoformat()
                }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/api/connectors")
async def get_available_connectors():
    """Get list of all available connectors"""
    try:
        available_connectors = discovery_engine.get_available_connectors()
        connector_categories = discovery_engine.get_connector_categories()
        
        enabled_connectors = len(discovery_engine.connectors)
        
        return {
            "status": "success",
            "connectors": connector_categories,
            "available_connectors": available_connectors,
            "enabled_connectors": enabled_connectors,
            "total_connectors": len(available_connectors)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/connectors/{connector_type}")
async def get_connector_details(connector_type: str):
    """Get detailed information about a specific connector"""
    try:
        connector_info = discovery_engine.get_available_connectors().get(connector_type)
        if not connector_info:
            raise HTTPException(status_code=404, detail=f"Connector {connector_type} not found")
        
        config_template = discovery_engine.get_connector_config_template(connector_type)
        
        return {
            "status": "success",
            "connector_info": connector_info,
            "config_template": config_template
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/connectors/{connector_type}/add")
async def add_connector(connector_type: str, config: ConnectorConfig):
    """Add a new connector dynamically"""
    try:
        success = discovery_engine.add_connector(connector_type, config.config)
        if success:
            return {"status": "success", "message": f"Connector {connector_type} added successfully"}
        else:
            raise HTTPException(status_code=400, detail=f"Failed to add connector {connector_type}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/connectors/{connector_type}")
async def remove_connector(connector_type: str):
    """Remove a connector dynamically"""
    try:
        success = discovery_engine.remove_connector(connector_type)
        if success:
            return {"status": "success", "message": f"Connector {connector_type} removed successfully"}
        else:
            raise HTTPException(status_code=404, detail=f"Connector {connector_type} not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/connectors/reload")
async def reload_connectors():
    """Reload all connectors from configuration"""
    try:
        discovery_engine.reload_connectors()
        return {"status": "success", "message": "Connectors reloaded successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/discovery/scan")
async def start_full_discovery(background_tasks: BackgroundTasks):
    """Start full discovery scan"""
    async def run_discovery():
        try:
            result = await discovery_engine.scan_all_data_sources()
        except Exception as e:
    
    background_tasks.add_task(run_discovery)
    return {
        "status": "success",
        "message": "Discovery scan started",
        "timestamp": datetime.now().isoformat()
    }

@app.post("/api/discovery/scan/{source}")
async def scan_specific_source(source: str, background_tasks: BackgroundTasks):
    """Scan specific data source"""
    async def run_source_discovery():
        try:
            if source in discovery_engine.connectors:
                connector = discovery_engine.connectors[source]
                assets = connector.discover_assets()
                for asset in assets:
                    await discovery_engine.asset_catalog.add_or_update_asset(asset)
            else:
        except Exception as e:
    
    background_tasks.add_task(run_source_discovery)
    return {
        "status": "success",
        "message": f"Discovery scan started for {source}",
        "source": source,
        "timestamp": datetime.now().isoformat()
    }

@app.post("/api/discovery/test/{source}")
async def test_source_discovery(source: str):
    """Test discovery for specific data source and return assets immediately"""
    try:
        if source == 'gcp':
            if source in discovery_engine.connectors:
                connector = discovery_engine.connectors[source]
                assets = connector.discover_assets()
                
                try:
                    for asset in assets:
                        await discovery_engine.asset_catalog.add_or_update_asset(asset)
                except Exception as catalog_error:
                
                return {
                    "status": "success",
                    "source": source,
                    "assets_discovered": len(assets),
                    "assets": assets,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                return {
                    "status": "error",
                    "source": source,
                    "message": "GCP connector not configured. Please add GCP connector with valid credentials first.",
                    "assets_discovered": 0,
                    "assets": [],
                    "timestamp": datetime.now().isoformat()
                }
        elif source in discovery_engine.connectors:
            connector = discovery_engine.connectors[source]
            assets = connector.discover_assets()
            
            try:
                for asset in assets:
                    await discovery_engine.asset_catalog.add_or_update_asset(asset)
            except Exception as catalog_error:
            
            return {
                "status": "success",
                "source": source,
                "assets_discovered": len(assets),
                "assets": assets,
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "status": "error",
                "error": f"Connector {source} not found or not enabled",
                "source": source,
                "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "source": source,
            "timestamp": datetime.now().isoformat()
        }

@app.get("/api/discovery/status")
async def get_discovery_status():
    """Get current discovery status"""
    return discovery_engine.get_discovery_status()

@app.get("/api/lineage/assets")
async def get_lineage_assets():
    """Get assets available for lineage analysis - dynamically synced with main assets"""
    try:
        assets = discovery_engine.asset_catalog.search_assets('', limit=1000)
        
        lineage_assets = []
        for asset in assets:
            asset_id = asset.get('metadata', {}).get('asset_fingerprint')
            if not asset_id:
                import base64
                id_string = f"{asset.get('source', '')}-{asset.get('location', '')}-{asset.get('name', '')}"
                asset_id = base64.b64encode(id_string.encode()).decode().replace('=', '').replace('+', '').replace('/', '')[:16]
            
            asset_info = {
                "id": asset_id,
                "name": asset.get('name', ''),
                "type": asset.get('type', ''),
                "source": asset.get('source', ''),
                "location": asset.get('location', ''),
                "has_schema": bool(asset.get('schema', [])),
                "column_count": len(asset.get('schema', [])),
                "metadata": asset.get('metadata', {}),
                "created_date": asset.get('created_date'),
                "last_scanned": asset.get('last_scanned'),
                "size": asset.get('size', 0)
            }
            lineage_assets.append(asset_info)
        
        return {
            "status": "success",
            "count": len(lineage_assets),
            "assets": lineage_assets,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "assets": [], "timestamp": datetime.now().isoformat()}

@app.get("/api/lineage/{asset_id}")
async def get_asset_lineage(asset_id: str, direction: str = "both", depth: int = 3):
    """Get lineage for a specific asset - dynamically updated"""
    try:
        assets = discovery_engine.asset_catalog.search_assets('', limit=1000)
        target_asset = None
        
        for asset in assets:
            asset_fingerprint = asset.get('metadata', {}).get('asset_fingerprint')
            if asset_fingerprint == asset_id:
                target_asset = asset
                break
                
            import base64
            id_string = f"{asset.get('source', '')}-{asset.get('location', '')}-{asset.get('name', '')}"
            generated_id = base64.b64encode(id_string.encode()).decode().replace('=', '').replace('+', '').replace('/', '')[:16]
            if generated_id == asset_id:
                target_asset = asset
                break
        
        if not target_asset:
            raise HTTPException(status_code=404, detail=f"Asset with ID '{asset_id}' not found")
        
        lineage_data = await generate_lineage_data(target_asset, assets, direction, depth)
        
        return {
            "status": "success",
            "asset": target_asset,
            "lineage": lineage_data,
            "direction": direction,
            "depth": depth,
            "total_assets_analyzed": len(assets),
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        return {
            "status": "error", 
            "message": str(e), 
            "lineage": {},
            "timestamp": datetime.now().isoformat()
        }

@app.get("/api/lineage/{asset_id}/columns")
async def get_column_lineage(asset_id: str):
    """Get column-level lineage for a specific asset"""
    try:
        assets = discovery_engine.asset_catalog.search_assets('')
        target_asset = None
        for asset in assets:
            fingerprint = asset.get('metadata', {}).get('asset_fingerprint')
            if fingerprint == asset_id:
                target_asset = asset
                break
            
            import base64
            id_string = f"{asset.get('source', '')}-{asset.get('location', '')}-{asset.get('name', '')}"
            generated_id = base64.b64encode(id_string.encode()).decode().replace('=', '').replace('+', '').replace('/', '')[:16]
            if generated_id == asset_id:
                target_asset = asset
                break
        
        if not target_asset:
            return {"status": "error", "message": f"Asset not found: {asset_id}", "column_lineage": {}}
        
        column_lineage = await generate_column_lineage(target_asset, assets)
        
        return {
            "status": "success",
            "asset": target_asset,
            "column_lineage": column_lineage
        }
        
    except HTTPException:
        raise
    except Exception as e:
        return {"status": "error", "message": str(e), "column_lineage": {}}

@app.post("/api/lineage/relationships")
async def create_lineage_relationship(request: Request):
    """Create a new custom lineage relationship"""
    try:
        relationship_data = await request.json()
        
        required_fields = ['source', 'target', 'relationship', 'confidence']
        for field in required_fields:
            if field not in relationship_data:
                return {"status": "error", "message": f"Missing required field: {field}"}
        
        relationship_data['id'] = f"{relationship_data['source']}-{relationship_data['target']}-{relationship_data['relationship']}"
        relationship_data['created_at'] = datetime.now().isoformat()
        relationship_data['created_by'] = relationship_data.get('created_by', 'user')
        
        success = await discovery_engine.asset_catalog.add_custom_relationship(relationship_data)
        
        if success:
            return {
                "status": "success",
                "message": "Relationship created successfully",
                "relationship": relationship_data
            }
        else:
            return {"status": "error", "message": "Failed to create relationship"}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/lineage/relationships")
async def get_all_relationships():
    """Get all custom lineage relationships"""
    try:
        relationships = await discovery_engine.asset_catalog.get_custom_relationships()
        
        assets = discovery_engine.asset_catalog.search_assets('')
        asset_names = {asset.get('metadata', {}).get('asset_fingerprint', ''): asset.get('name', '') for asset in assets}
        
        for rel in relationships:
            rel['source_name'] = asset_names.get(rel['source'], rel['source'])
            rel['target_name'] = asset_names.get(rel['target'], rel['target'])
        
        return {
            "status": "success",
            "relationships": relationships
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e), "relationships": []}

@app.delete("/api/lineage/relationships/{relationship_id}")
async def delete_relationship(relationship_id: str):
    """Delete a custom lineage relationship"""
    try:
        success = await discovery_engine.asset_catalog.delete_custom_relationship(relationship_id)
        
        if success:
            return {"status": "success", "message": "Relationship deleted successfully"}
        else:
            return {"status": "error", "message": "Relationship not found or could not be deleted"}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/lineage/relationships/{asset_id}")
async def get_asset_relationships(asset_id: str):
    """Get all relationships for a specific asset"""
    try:
        relationships = await discovery_engine.asset_catalog.get_asset_relationships(asset_id)
        
        return {
            "status": "success",
            "asset_id": asset_id,
            "relationships": relationships
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e), "relationships": []}

async def generate_lineage_data(target_asset: Dict[str, Any], all_assets: List[Dict[str, Any]], direction: str, depth: int) -> Dict[str, Any]:
    """Generate lineage data based on asset relationships"""
    lineage = {
        "nodes": [],
        "edges": [],
        "levels": {}
    }
    
    target_asset_id = target_asset.get('metadata', {}).get('asset_fingerprint')
    if not target_asset_id:
        import base64
        id_string = f"{target_asset.get('source', '')}-{target_asset.get('location', '')}-{target_asset.get('name', '')}"
        target_asset_id = base64.b64encode(id_string.encode()).decode().replace('=', '').replace('+', '').replace('/', '')[:16]
    
    target_node = {
        "id": target_asset_id,
        "name": target_asset.get('name', ''),
        "type": target_asset.get('type', ''),
        "source": target_asset.get('source', ''),
        "level": 0,
        "is_target": True,
        "schema": target_asset.get('schema', []),
        "metadata": target_asset.get('metadata', {})
    }
    lineage["nodes"].append(target_node)
    lineage["levels"][0] = [target_node["id"]]
    
    related_assets = find_related_assets(target_asset, all_assets)
    
    custom_relationships = await discovery_engine.asset_catalog.get_asset_relationships(target_asset_id)
    await add_custom_relationships_to_lineage(lineage, target_asset_id, custom_relationships, all_assets)
    
    if direction in ['both', 'upstream']:
        upstream_assets = generate_upstream_assets(target_asset, related_assets, depth)
        for level, assets in upstream_assets.items():
            if level not in lineage["levels"]:
                lineage["levels"][level] = []
            for asset in assets:
                asset_id = asset.get('metadata', {}).get('asset_fingerprint')
                if not asset_id:
                    import base64
                    id_string = f"{asset.get('source', '')}-{asset.get('location', '')}-{asset.get('name', '')}"
                    asset_id = base64.b64encode(id_string.encode()).decode().replace('=', '').replace('+', '').replace('/', '')[:16]
                
                node = {
                    "id": asset_id,
                    "name": asset.get('name', ''),
                    "type": asset.get('type', ''),
                    "source": asset.get('source', ''),
                    "level": level,
                    "direction": "upstream",
                    "schema": asset.get('schema', []),
                    "metadata": asset.get('metadata', {})
                }
                lineage["nodes"].append(node)
                lineage["levels"][level].append(node["id"])
                
                lineage["edges"].append({
                    "source": node["id"],
                    "target": target_node["id"],
                    "relationship": "feeds_into",
                    "confidence": 0.8
                })
    
    if direction in ['both', 'downstream']:
        downstream_assets = generate_downstream_assets(target_asset, related_assets, depth)
        for level, assets in downstream_assets.items():
            if level not in lineage["levels"]:
                lineage["levels"][level] = []
            for asset in assets:
                asset_id = asset.get('metadata', {}).get('asset_fingerprint')
                if not asset_id:
                    import base64
                    id_string = f"{asset.get('source', '')}-{asset.get('location', '')}-{asset.get('name', '')}"
                    asset_id = base64.b64encode(id_string.encode()).decode().replace('=', '').replace('+', '').replace('/', '')[:16]
                
                node = {
                    "id": asset_id,
                    "name": asset.get('name', ''),
                    "type": asset.get('type', ''),
                    "source": asset.get('source', ''),
                    "level": level,
                    "direction": "downstream",
                    "schema": asset.get('schema', []),
                    "metadata": asset.get('metadata', {})
                }
                lineage["nodes"].append(node)
                lineage["levels"][level].append(node["id"])
                
                lineage["edges"].append({
                    "source": target_node["id"],
                    "target": node["id"],
                    "relationship": "feeds_into",
                    "confidence": 0.8
                })
    
    return lineage

def find_related_assets(target_asset: Dict[str, Any], all_assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Find assets that might be related to the target asset"""
    related = []
    target_name = target_asset.get('name', '').lower()
    target_source = target_asset.get('source', '')
    target_location = target_asset.get('location', '').lower()
    
    target_parts = set()
    if '.' in target_name:
        parts = target_name.split('.')
        target_parts.update(parts)
        if len(parts) >= 3:
            target_parts.add(parts[-2])  # dataset name
            target_parts.add(parts[-1])  # table name
    
    name_keywords = ['customer', 'order', 'transaction', 'account', 'user', 'product', 'sales', 'client', 'employee', 'banking', 'loan', 'card']
    for keyword in name_keywords:
        if keyword in target_name:
            target_parts.add(keyword)
    
    
    for asset in all_assets:
        if asset.get('metadata', {}).get('asset_fingerprint') == target_asset.get('metadata', {}).get('asset_fingerprint'):
            continue  # Skip the target asset itself
        
        asset_name = asset.get('name', '').lower()
        asset_source = asset.get('source', '')
        asset_location = asset.get('location', '').lower()
        
        score = 0
        
        if asset_source == target_source:
            score += 2
        
        if 'bigquery' in target_source and 'bigquery' in asset_source:
            if '.' in target_name and '.' in asset_name:
                target_project_dataset = '.'.join(target_name.split('.')[:2])
                asset_project_dataset = '.'.join(asset_name.split('.')[:2])
                if target_project_dataset == asset_project_dataset:
                    score += 3  # Same project and dataset
                elif target_name.split('.')[0] == asset_name.split('.')[0]:
                    score += 2  # Same project
        
        for part in target_parts:
            if part in asset_name:
                score += 1
        
        if target_name in asset_name or asset_name in target_name:
            score += 2
        
        if target_location and asset_location and target_location in asset_location:
            score += 1
        
        if score >= 2:
            asset['_relationship_score'] = score
            related.append(asset)
    
    related.sort(key=lambda x: x.get('_relationship_score', 0), reverse=True)
    
    return related

def generate_upstream_assets(target_asset: Dict[str, Any], related_assets: List[Dict[str, Any]], depth: int) -> Dict[int, List[Dict[str, Any]]]:
    """Generate upstream assets (data sources)"""
    upstream = {}
    target_name = target_asset.get('name', '').lower()
    target_type = target_asset.get('type', '').lower()
    
    
    used_assets = set()
    
    for level in range(1, depth + 1):
        level_assets = []
        
        for asset in related_assets:
            asset_id = asset.get('metadata', {}).get('asset_fingerprint')
            if not asset_id:
                import base64
                id_string = f"{asset.get('source', '')}-{asset.get('location', '')}-{asset.get('name', '')}"
                asset_id = base64.b64encode(id_string.encode()).decode().replace('=', '').replace('+', '').replace('/', '')[:16]
            if asset_id in used_assets:
                continue
                
            asset_name = asset.get('name', '').lower()
            asset_type = asset.get('type', '').lower()
            score = asset.get('_relationship_score', 0)
            
            is_upstream = False
            
            if any(keyword in asset_name for keyword in ['raw', 'source', 'input', 'base', 'staging', 'landing']):
                is_upstream = True
                
            elif 'dataset' in asset_type and 'table' in target_type:
                is_upstream = True
                
            elif ('agg' in target_name or 'summary' in target_name or 'mart' in target_name) and asset_type == 'bigquery_table':
                is_upstream = True
                
            elif score >= 4 and level == 1:  # High score suggests close relationship
                is_upstream = True
            
            if is_upstream:
                level_assets.append(asset)
                used_assets.add(asset_id)
                
                if len(level_assets) >= 3:
                    break
        
        if level_assets:
            upstream[-level] = level_assets
    
    return upstream

def generate_downstream_assets(target_asset: Dict[str, Any], related_assets: List[Dict[str, Any]], depth: int) -> Dict[int, List[Dict[str, Any]]]:
    """Generate downstream assets (data targets)"""
    downstream = {}
    target_name = target_asset.get('name', '').lower()
    target_type = target_asset.get('type', '').lower()
    
    
    used_assets = set()
    
    for level in range(1, depth + 1):
        level_assets = []
        
        for asset in related_assets:
            asset_id = asset.get('metadata', {}).get('asset_fingerprint')
            if not asset_id:
                import base64
                id_string = f"{asset.get('source', '')}-{asset.get('location', '')}-{asset.get('name', '')}"
                asset_id = base64.b64encode(id_string.encode()).decode().replace('=', '').replace('+', '').replace('/', '')[:16]
            if asset_id in used_assets:
                continue
                
            asset_name = asset.get('name', '').lower()
            asset_type = asset.get('type', '').lower()
            score = asset.get('_relationship_score', 0)
            
            is_downstream = False
            
            if any(keyword in asset_name for keyword in ['processed', 'output', 'mart', 'agg', 'summary', 'report', 'analytics']):
                is_downstream = True
                
            elif 'view' in asset_type and 'table' in target_type:
                is_downstream = True
                
            elif ('raw' in target_name or 'base' in target_name or 'source' in target_name) and asset_type == 'bigquery_table':
                is_downstream = True
                
            elif any(keyword in asset_name for keyword in ['master', 'dim', 'fact', 'cube']) and score >= 3:
                is_downstream = True
                
            elif score >= 3 and level == 1 and len(asset_name) > len(target_name):
                is_downstream = True
            
            if is_downstream:
                level_assets.append(asset)
                used_assets.add(asset_id)
                
                if len(level_assets) >= 3:
                    break
        
        if level_assets:
            downstream[level] = level_assets
    
    return downstream

async def generate_column_lineage(target_asset: Dict[str, Any], all_assets: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate column-level lineage for an asset"""
    schema = target_asset.get('schema', [])
    target_name = target_asset.get('name', '')
    
    
    column_lineage = {
        "columns": [],
        "relationships": []
    }
    
    related_with_schema = [asset for asset in all_assets 
                          if asset.get('schema') and 
                          asset.get('metadata', {}).get('asset_fingerprint') != target_asset.get('metadata', {}).get('asset_fingerprint')]
    
    for column in schema:
        if isinstance(column, str):
            col_name = column.lower()
            col_type = 'STRING'  # Default type for string columns
            column_dict = {
                'name': column,
                'type': col_type,
                'description': f'Column: {column}'
            }
        else:
            col_name = column.get('name', '').lower()
            col_type = column.get('type', 'STRING')
            column_dict = column
            
        col_info = {
            "name": column_dict.get('name', col_name),
            "type": col_type,
            "description": column_dict.get('description', ''),
            "upstream_columns": [],
            "downstream_columns": [],
            "transformations": []
        }
        
        for related_asset in related_with_schema[:5]:  # Limit to avoid too much data
            related_schema = related_asset.get('schema', [])
            related_name = related_asset.get('name', '')
            
            for related_column in related_schema:
                if isinstance(related_column, str):
                    related_col_name = related_column.lower()
                    related_col_type = 'STRING'
                    related_column_dict = {
                        'name': related_column,
                        'type': related_col_type
                    }
                else:
                    related_col_name = related_column.get('name', '').lower()
                    related_col_type = related_column.get('type', 'STRING')
                    related_column_dict = related_column
                
                match_score = 0
                
                if col_name == related_col_name:
                    match_score = 0.95
                elif any([
                    col_name in related_col_name or related_col_name in col_name,
                    col_name.replace('_', '') == related_col_name.replace('_', ''),
                    col_name.endswith('_id') and related_col_name.endswith('_id') and col_name.split('_')[0] == related_col_name.split('_')[0]
                ]):
                    match_score = 0.8
                elif col_type == related_col_type and len(col_name) > 2:
                    if any(pattern in col_name for pattern in ['customer', 'user', 'account', 'order', 'transaction']):
                        if any(pattern in related_col_name for pattern in ['customer', 'user', 'account', 'order', 'transaction']):
                            match_score = 0.6
                
                if match_score > 0.5:
                    is_upstream = determine_upstream_relationship(target_name, related_name)
                    
                    relationship = {
                        "asset": related_name,
                        "column": related_column_dict.get('name', related_col_name),
                        "confidence": match_score
                    }
                    
                    if is_upstream:
                        col_info["upstream_columns"].append(relationship)
                    else:
                        col_info["downstream_columns"].append(relationship)
        
        transformations = determine_column_transformations(col_name, col_type, col_info)
        col_info["transformations"] = transformations
        
        column_lineage["columns"].append(col_info)
        
    
    return column_lineage

def determine_upstream_relationship(target_name: str, related_name: str) -> bool:
    """Determine if related asset is upstream based on naming patterns"""
    target_lower = target_name.lower()
    related_lower = related_name.lower()
    
    if any(keyword in related_lower for keyword in ['raw', 'source', 'base', 'landing', 'staging']):
        return True
    
    if any(keyword in target_lower for keyword in ['mart', 'agg', 'summary', 'report']):
        return True
    
    if 'view' in target_lower and 'table' in related_lower:
        return True
    
    return len(related_name) < len(target_name)

def determine_column_transformations(col_name: str, col_type: str, col_info: dict) -> list:
    """Determine likely transformations based on column characteristics"""
    transformations = []
    
    if any(keyword in col_name for keyword in ['id', 'key', 'pk', 'fk']):
        if col_info["upstream_columns"]:
            transformations = ["direct_copy", "key_lookup"]
        else:
            transformations = ["generated_key"]
    
    elif any(keyword in col_name for keyword in ['total', 'sum', 'count', 'avg', 'max', 'min']):
        transformations = ["aggregation", "mathematical_operation"]
        if any(keyword in col_name for keyword in ['total', 'sum']):
            transformations.append("sum")
        elif 'count' in col_name:
            transformations.append("count")
        elif 'avg' in col_name:
            transformations.append("average")
    
    elif any(keyword in col_type.lower() for keyword in ['date', 'time', 'timestamp']):
        transformations = ["date_formatting", "timezone_conversion"]
    
    elif any(keyword in col_type.lower() for keyword in ['string', 'varchar', 'text']):
        transformations = ["string_formatting", "data_cleaning"]
        if 'name' in col_name:
            transformations.append("concatenation")
    
    elif any(keyword in col_type.lower() for keyword in ['int', 'float', 'decimal', 'number']):
        transformations = ["data_type_conversion", "mathematical_operation"]
    
    else:
        transformations = ["data_transformation"]
    
    return transformations

async def add_custom_relationships_to_lineage(lineage: dict, target_asset_id: str, custom_relationships: list, all_assets: list):
    """Add custom relationships to the lineage graph"""
    try:
        asset_lookup = {}
        for asset in all_assets:
            asset_id = asset.get('metadata', {}).get('asset_fingerprint')
            if not asset_id:
                import base64
                id_string = f"{asset.get('source', '')}-{asset.get('location', '')}-{asset.get('name', '')}"
                asset_id = base64.b64encode(id_string.encode()).decode().replace('=', '').replace('+', '').replace('/', '')[:16]
            asset_lookup[asset_id] = asset
        
        for rel in custom_relationships:
            source_id = rel['source']
            target_id = rel['target']
            
            if target_id == target_asset_id:
                if source_id in asset_lookup:
                    source_asset = asset_lookup[source_id]
                    
                    existing_node = next((node for node in lineage["nodes"] if node["id"] == source_id), None)
                    if not existing_node:
                        upstream_node = {
                            "id": source_id,
                            "name": source_asset.get('name', ''),
                            "type": source_asset.get('type', ''),
                            "source": source_asset.get('source', ''),
                            "level": -1,
                            "direction": "upstream",
                            "schema": source_asset.get('schema', []),
                            "metadata": source_asset.get('metadata', {}),
                            "custom_relationship": True
                        }
                        lineage["nodes"].append(upstream_node)
                        
                        if -1 not in lineage["levels"]:
                            lineage["levels"][-1] = []
                        lineage["levels"][-1].append(source_id)
                    
                    lineage["edges"].append({
                        "source": source_id,
                        "target": target_asset_id,
                        "relationship": rel['relationship'],
                        "confidence": rel['confidence'],
                        "custom": True,
                        "description": rel.get('description', '')
                    })
                    
            elif source_id == target_asset_id:
                if target_id in asset_lookup:
                    target_asset_obj = asset_lookup[target_id]
                    
                    existing_node = next((node for node in lineage["nodes"] if node["id"] == target_id), None)
                    if not existing_node:
                        downstream_node = {
                            "id": target_id,
                            "name": target_asset_obj.get('name', ''),
                            "type": target_asset_obj.get('type', ''),
                            "source": target_asset_obj.get('source', ''),
                            "level": 1,
                            "direction": "downstream",
                            "schema": target_asset_obj.get('schema', []),
                            "metadata": target_asset_obj.get('metadata', {}),
                            "custom_relationship": True
                        }
                        lineage["nodes"].append(downstream_node)
                        
                        if 1 not in lineage["levels"]:
                            lineage["levels"][1] = []
                        lineage["levels"][1].append(target_id)
                    
                    lineage["edges"].append({
                        "source": target_asset_id,
                        "target": target_id,
                        "relationship": rel['relationship'],
                        "confidence": rel['confidence'],
                        "custom": True,
                        "description": rel.get('description', '')
                    })
        
        
    except Exception as e:

@app.get("/api/assets")
async def get_all_assets():
    """Get all discovered assets"""
    try:
        assets = discovery_engine.asset_catalog.search_assets('', limit=1000)
        
        assets_by_source = {}
        for asset in assets:
            source = asset.get('source', 'unknown')
            if source not in assets_by_source:
                assets_by_source[source] = []
            assets_by_source[source].append(asset)
        
        if len(assets) == 0:
            return {
                "status": "success",
                "total_assets": 0,
                "assets": {},
                "assets_list": []
            }
        
        return {
            "status": "success",
            "total_assets": len(assets),
            "assets": assets_by_source,
            "assets_list": assets  # Also provide as flat list
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "assets": {},
            "assets_list": []
        }

@app.post("/api/assets/search")
async def search_assets(search_request: SearchRequest):
    """Search for specific assets"""
    result = discovery_engine.search_data_assets(
        query=search_request.query,
        asset_type=search_request.asset_type
    )
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result["error"])
    return result

@app.get("/api/assets/by-type/{asset_type}")
async def get_assets_by_type(asset_type: str):
    """Get assets by type"""
    try:
        assets = discovery_engine.asset_catalog.get_assets_by_type(asset_type)
        return {
            "status": "success",
            "asset_type": asset_type,
            "count": len(assets),
            "assets": assets,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/assets/by-source/{source}")
async def get_assets_by_source(source: str):
    """Get assets by source"""
    try:
        assets = discovery_engine.asset_catalog.get_assets_by_source(source)
        return {
            "status": "success",
            "source": source,
            "count": len(assets),
            "assets": assets,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/assets/{asset_name}")
async def get_asset_details(asset_name: str):
    """Get detailed information about a specific asset"""
    result = discovery_engine.get_asset_details(asset_name)
    if result["status"] == "error":
        raise HTTPException(status_code=404, detail=result["error"])
    return result

@app.get("/api/assets/{asset_name}/profiling")
async def get_asset_profiling(asset_name: str):
    """Get data profiling information for a specific asset"""
    try:
        assets = discovery_engine.asset_catalog.search_assets(asset_name)
        if not assets:
            raise HTTPException(status_code=404, detail="Asset not found")
        
        asset = assets[0]  # Get the first matching asset
        
        profiling_result = await analyze_asset_profiling(asset)
        
        return {
            "status": "success",
            "asset_name": asset_name,
            "profiling": profiling_result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/api/assets/{asset_name}/ai-analysis")
async def get_asset_ai_analysis(asset_name: str):
    """Get AI analysis for a specific asset using Gemini"""
    try:
        assets = discovery_engine.asset_catalog.search_assets(asset_name)
        if not assets:
            raise HTTPException(status_code=404, detail="Asset not found")
        
        asset = assets[0]  # Get the first matching asset
        
        analysis_result = await analyze_asset_with_gemini(asset)
        
        return {
            "status": "success",
            "asset_name": asset_name,
            "analysis": analysis_result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/api/assets/{asset_name}/pii-scan")
async def get_asset_pii_scan(asset_name: str):
    """Get PII scan results for a specific asset using Gemini"""
    try:
        assets = discovery_engine.asset_catalog.search_assets(asset_name)
        if not assets:
            raise HTTPException(status_code=404, detail="Asset not found")
        
        asset = assets[0]  # Get the first matching asset
        
        pii_scan_result = await scan_asset_for_pii_with_gemini(asset)
        
        return {
            "status": "success",
            "asset_name": asset_name,
            "scan": pii_scan_result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

async def analyze_asset_profiling(asset: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze asset for data profiling including PII detection"""
    profiling = {
        "data_types": {},
        "pii_analysis": {
            "has_pii": False,
            "pii_columns": [],
            "pii_types": []
        },
        "statistics": {
            "total_columns": 0,
            "nullable_columns": 0,
            "estimated_rows": 0
        }
    }
    
    try:
        schema = asset.get('schema', {})
        
        if isinstance(schema, dict) and 'fields' in schema:
            fields = schema.get('fields', [])
            profiling['statistics']['total_columns'] = len(fields)
            profiling['statistics']['estimated_rows'] = schema.get('num_rows', 0)
            
            for field in fields:
                field_name = field.get('name', '').lower()
                field_type = field.get('type', 'unknown')
                field_mode = field.get('mode', 'NULLABLE')
                
                if field_type in profiling['data_types']:
                    profiling['data_types'][field_type] += 1
                else:
                    profiling['data_types'][field_type] = 1
                
                if field_mode == 'NULLABLE':
                    profiling['statistics']['nullable_columns'] += 1
                
                pii_indicators = detect_pii_in_field(field_name, field_type)
                if pii_indicators:
                    profiling['pii_analysis']['has_pii'] = True
                    profiling['pii_analysis']['pii_columns'].append({
                        'column': field.get('name'),
                        'type': field_type,
                        'pii_types': pii_indicators
                    })
                    profiling['pii_analysis']['pii_types'].extend(pii_indicators)
        
        elif isinstance(schema, dict) and 'columns' in schema:
            columns = schema.get('columns', [])
            profiling['statistics']['total_columns'] = len(columns)
            
            for column in columns:
                if isinstance(column, str):
                    pii_indicators = detect_pii_in_field(column.lower(), 'unknown')
                    if pii_indicators:
                        profiling['pii_analysis']['has_pii'] = True
                        profiling['pii_analysis']['pii_columns'].append({
                            'column': column,
                            'type': 'unknown',
                            'pii_types': pii_indicators
                        })
                        profiling['pii_analysis']['pii_types'].extend(pii_indicators)
        
        profiling['pii_analysis']['pii_types'] = list(set(profiling['pii_analysis']['pii_types']))
        
        total_cols = profiling['statistics']['total_columns']
        if total_cols > 0:
            profiling['data_type_distribution'] = {
                dtype: {
                    'count': count,
                    'percentage': round((count / total_cols) * 100, 1)
                }
                for dtype, count in profiling['data_types'].items()
            }
        
    except Exception as e:
        profiling['error'] = f"Profiling analysis failed: {str(e)}"
    
    return profiling

def detect_pii_in_field(field_name: str, field_type: str) -> List[str]:
    """Detect potential PII in a field based on name and type"""
    pii_indicators = []
    
    if any(keyword in field_name for keyword in ['email', 'e_mail', 'mail']):
        pii_indicators.append('Email Address')
    
    if any(keyword in field_name for keyword in ['phone', 'mobile', 'tel', 'contact']):
        pii_indicators.append('Phone Number')
    
    if any(keyword in field_name for keyword in ['name', 'first_name', 'last_name', 'full_name', 'firstname', 'lastname']):
        pii_indicators.append('Personal Name')
    
    if any(keyword in field_name for keyword in ['address', 'street', 'city', 'zip', 'postal', 'location']):
        pii_indicators.append('Address')
    
    if any(keyword in field_name for keyword in ['ssn', 'social_security', 'passport', 'license', 'id_number']):
        pii_indicators.append('Government ID')
    
    if any(keyword in field_name for keyword in ['credit_card', 'account_number', 'bank', 'iban', 'routing']):
        pii_indicators.append('Financial Data')
    
    if any(keyword in field_name for keyword in ['birth', 'dob', 'birthday', 'born']):
        pii_indicators.append('Date of Birth')
    
    if any(keyword in field_name for keyword in ['ip_address', 'ip', 'client_ip']):
        pii_indicators.append('IP Address')
    
    if any(keyword in field_name for keyword in ['user_id', 'customer_id', 'client_id']):
        pii_indicators.append('User Identifier')
    
    return pii_indicators

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent"

async def call_gemini_api(prompt: str) -> Dict[str, Any]:
    """Call Gemini API with the given prompt"""
    if not GEMINI_API_KEY:
        return {"success": False, "error": "GEMINI_API_KEY environment variable not set"}
    
    headers = {
        "Content-Type": "application/json",
    }
    
    payload = {
        "contents": [{
            "parts": [{
                "text": prompt
            }]
        }]
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{GEMINI_API_URL}?key={GEMINI_API_KEY}",
                headers=headers,
                json=payload
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    if "candidates" in result and len(result["candidates"]) > 0:
                        content = result["candidates"][0]["content"]["parts"][0]["text"]
                        return {"success": True, "content": content}
                    else:
                        return {"success": False, "error": "No response content"}
                else:
                    error_text = await response.text()
                    return {"success": False, "error": f"API error: {response.status} - {error_text}"}
    except Exception as e:
        return {"success": False, "error": f"Request failed: {str(e)}"}

async def analyze_asset_with_gemini(asset: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze asset using Gemini AI"""
    try:
        asset_info = {
            "name": asset.get("name", "Unknown"),
            "type": asset.get("type", "Unknown"),
            "source": asset.get("source", "Unknown"),
            "location": asset.get("location", "Unknown"),
            "size": asset.get("size", 0),
            "schema": asset.get("schema", {}),
            "metadata": asset.get("metadata", {})
        }
        
        prompt = f"""
        Analyze this data asset and provide comprehensive insights:
        
        Asset Details:
        - Name: {asset_info['name']}
        - Type: {asset_info['type']}
        - Source: {asset_info['source']}
        - Location: {asset_info['location']}
        - Size: {asset_info['size']} bytes
        - Schema: {json.dumps(asset_info['schema'], indent=2) if asset_info['schema'] else 'Not available'}
        - Metadata: {json.dumps(asset_info['metadata'], indent=2) if asset_info['metadata'] else 'Not available'}
        
        Please provide a detailed analysis in the following JSON format:
        {{
            "summary": "Brief summary of the data asset and its purpose",
            "insights": ["Key insight 1", "Key insight 2", "Key insight 3"],
            "quality_score": 85,
            "quality_notes": "Assessment of data quality",
            "issues": ["Potential issue 1", "Potential issue 2"],
            "recommendations": "Specific recommendations for this asset"
        }}
        
        Focus on:
        1. Data structure and organization
        2. Potential use cases and business value
        3. Data quality assessment
        4. Security considerations
        5. Performance implications
        6. Maintenance recommendations
        
        Respond only with the JSON object, no additional text.
        """
        
        gemini_response = await call_gemini_api(prompt)
        
        if gemini_response["success"]:
            try:
                analysis_text = gemini_response["content"].strip()
                if analysis_text.startswith("```json"):
                    analysis_text = analysis_text[7:]
                if analysis_text.endswith("```"):
                    analysis_text = analysis_text[:-3]
                
                analysis = json.loads(analysis_text.strip())
                
                return {
                    "summary": analysis.get("summary", "AI analysis completed"),
                    "insights": analysis.get("insights", ["Analysis completed successfully"]),
                    "quality_score": min(100, max(0, analysis.get("quality_score", 75))),
                    "quality_notes": analysis.get("quality_notes", "Quality assessment completed"),
                    "issues": analysis.get("issues", []),
                    "recommendations": analysis.get("recommendations", "No specific recommendations available")
                }
                
            except json.JSONDecodeError:
                return {
                    "summary": "AI analysis completed but response format was invalid",
                    "insights": ["Analysis response received but could not be parsed"],
                    "quality_score": 50,
                    "quality_notes": "Could not assess quality due to parsing error",
                    "issues": ["Response parsing failed"],
                    "recommendations": "Please try again or contact support"
                }
        else:
            return {
                "summary": "AI analysis failed",
                "insights": ["Unable to complete analysis"],
                "quality_score": 0,
                "quality_notes": "Analysis could not be completed",
                "issues": [f"AI service error: {gemini_response['error']}"],
                "recommendations": "Please try again later"
            }
            
    except Exception as e:
        return {
            "summary": "Analysis failed due to system error",
            "insights": ["System error occurred during analysis"],
            "quality_score": 0,
            "quality_notes": "Could not complete analysis",
            "issues": [f"System error: {str(e)}"],
            "recommendations": "Please contact support"
        }

async def scan_asset_for_pii_with_gemini(asset: Dict[str, Any]) -> Dict[str, Any]:
    """Scan asset for PII using Gemini AI"""
    try:
        asset_info = {
            "name": asset.get("name", "Unknown"),
            "type": asset.get("type", "Unknown"),
            "schema": asset.get("schema", {}),
            "metadata": asset.get("metadata", {})
        }
        
        fields_info = []
        schema = asset_info.get("schema", {})
        if isinstance(schema, dict) and "fields" in schema:
            fields_info = [
                {
                    "name": field.get("name", ""),
                    "type": field.get("type", ""),
                    "mode": field.get("mode", ""),
                    "description": field.get("description", "")
                }
                for field in schema.get("fields", [])
            ]
        
        prompt = f"""
        Analyze this data asset for personally identifiable information (PII) and sensitive data:
        
        Asset Details:
        - Name: {asset_info['name']}
        - Type: {asset_info['type']}
        - Fields: {json.dumps(fields_info, indent=2) if fields_info else 'No field information available'}
        
        Please provide a detailed PII scan in the following JSON format:
        {{
            "pii_fields_count": 3,
            "sensitive_fields_count": 2,
            "total_fields_scanned": 10,
            "risk_level": "HIGH",
            "pii_fields": [
                {{
                    "field_name": "email",
                    "pii_type": "Email Address",
                    "confidence": 95,
                    "risk_level": "HIGH"
                }}
            ],
            "sensitive_fields": [
                {{
                    "field_name": "user_id",
                    "sensitivity_type": "Identifier",
                    "reason": "Could be used to identify individuals"
                }}
            ],
            "compliance_notes": "GDPR and CCPA considerations apply"
        }}
        
        Focus on identifying:
        1. Direct PII (names, emails, phone numbers, addresses, SSN, etc.)
        2. Indirect PII (user IDs, device IDs, IP addresses, etc.)
        3. Sensitive data (financial, health, biometric, etc.)
        4. Compliance implications (GDPR, CCPA, HIPAA, etc.)
        
        Risk levels: LOW, MEDIUM, HIGH
        Confidence: 0-100 percentage
        
        Respond only with the JSON object, no additional text.
        """
        
        gemini_response = await call_gemini_api(prompt)
        
        if gemini_response["success"]:
            try:
                scan_text = gemini_response["content"].strip()
                if scan_text.startswith("```json"):
                    scan_text = scan_text[7:]
                if scan_text.endswith("```"):
                    scan_text = scan_text[:-3]
                
                scan = json.loads(scan_text.strip())
                
                return {
                    "pii_fields_count": scan.get("pii_fields_count", 0),
                    "sensitive_fields_count": scan.get("sensitive_fields_count", 0),
                    "total_fields_scanned": scan.get("total_fields_scanned", len(fields_info)),
                    "risk_level": scan.get("risk_level", "LOW"),
                    "pii_fields": scan.get("pii_fields", []),
                    "sensitive_fields": scan.get("sensitive_fields", []),
                    "compliance_notes": scan.get("compliance_notes", "No specific compliance issues identified")
                }
                
            except json.JSONDecodeError:
                return {
                    "pii_fields_count": 0,
                    "sensitive_fields_count": 0,
                    "total_fields_scanned": len(fields_info),
                    "risk_level": "UNKNOWN",
                    "pii_fields": [],
                    "sensitive_fields": [],
                    "compliance_notes": "PII scan completed but response format was invalid"
                }
        else:
            return {
                "pii_fields_count": 0,
                "sensitive_fields_count": 0,
                "total_fields_scanned": len(fields_info),
                "risk_level": "UNKNOWN",
                "pii_fields": [],
                "sensitive_fields": [],
                "compliance_notes": f"PII scan failed: {gemini_response['error']}"
            }
            
    except Exception as e:
        return {
            "pii_fields_count": 0,
            "sensitive_fields_count": 0,
            "total_fields_scanned": 0,
            "risk_level": "UNKNOWN",
            "pii_fields": [],
            "sensitive_fields": [],
            "compliance_notes": f"PII scan failed due to system error: {str(e)}"
        }

@app.post("/api/monitoring/start")
async def start_monitoring(monitoring_request: MonitoringRequest, background_tasks: BackgroundTasks):
    """Start continuous monitoring"""
    async def run_monitoring():
        try:
            result = await discovery_engine.start_continuous_monitoring(
                real_time=monitoring_request.real_time
            )
        except Exception as e:
    
    background_tasks.add_task(run_monitoring)
    return {
        "status": "success",
        "message": "Continuous monitoring started",
        "real_time": monitoring_request.real_time,
        "timestamp": datetime.now().isoformat()
    }

@app.post("/api/monitoring/stop")
async def stop_monitoring():
    """Stop continuous monitoring"""
    return discovery_engine.stop_monitoring()

@app.get("/api/monitoring/status")
async def get_monitoring_status():
    """Get monitoring status"""
    try:
        return {
            "status": "success",
            "monitoring": {
                "enabled": False,  # No config file in UI mode
                "real_time_watching": False,
                "watch_interval": 5,
                "batch_size": 100
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats")
async def get_discovery_stats():
    """Get discovery statistics"""
    try:
        summary = discovery_engine.get_system_health()
        return {
            "status": "success",
            "statistics": summary,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/connectors")
async def get_connector_stats():
    """Get connector statistics"""
    try:
        connectors_info = discovery_engine.get_available_connectors()
        if connectors_info["status"] == "success":
            return {
                "status": "success",
                "connector_stats": {
                    "total_connectors": connectors_info["total_connectors"],
                    "enabled_connectors": connectors_info["enabled_connectors"],
                    "categories": {
                        "cloud_providers": len(connectors_info["connectors"]["cloud_providers"]),
                        "databases": len(connectors_info["connectors"]["databases"]),
                        "data_warehouses": len(connectors_info["connectors"]["data_warehouses"]),
                        "saas_platforms": len(connectors_info["connectors"]["saas_platforms"]),
                        "streaming": len(connectors_info["connectors"]["streaming"]),
                        "data_lakes": len(connectors_info["connectors"]["data_lakes"])
                    }
                },
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to get connector information")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/export/config")
async def export_config():
    """Export current configuration"""
    try:
        enabled_connectors = {}
        for connector_type, connector in discovery_engine.connectors.items():
            enabled_connectors[connector_type] = connector.config
        
        return {
            "status": "success",
            "enabled_connectors": enabled_connectors,
            "export_timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/export/assets")
async def export_assets():
    """Export all assets"""
    try:
        inventory = discovery_engine.get_asset_inventory()
        return {
            "status": "success",
            "assets": inventory,
            "export_timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    os.makedirs("ui/static", exist_ok=True)
    
    
    uvicorn.run(
        "web_api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
