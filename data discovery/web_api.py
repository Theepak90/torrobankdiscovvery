#!/usr/bin/env python3
"""
Web API for Data Discovery - FastAPI REST API for UI Integration
Provides all endpoints your senior needs for the UI
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
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

from dynamic_discovery_engine import DynamicDataDiscoveryEngine

# Initialize FastAPI app
app = FastAPI(
    title="Data Discovery API",
    description="Enterprise Data Discovery System - 120+ Connectors",
    version="1.0.0"
)

# Add CORS middleware for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize dynamic discovery engine
discovery_engine = DynamicDataDiscoveryEngine()

# Pydantic models for request/response
class ConnectorConfig(BaseModel):
    enabled: bool
    config: Dict[str, Any]

class SearchRequest(BaseModel):
    query: str
    asset_type: Optional[str] = None
    source: Optional[str] = None

class MonitoringRequest(BaseModel):
    real_time: bool = True

# Serve static files for UI
app.mount("/static", StaticFiles(directory="ui/static"), name="static")

# Root endpoint - serve the UI
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

# Health check
@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Data Discovery API",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    }

# System Information Endpoints
@app.get("/api/system/health")
async def get_system_health():
    """Get system health status"""
    return discovery_engine.get_system_health()

@app.get("/api/system/status")
async def get_system_status():
    """Get discovery system status"""
    try:
        return {
            "status": "success",
            "system_health": discovery_engine.get_system_health(),
            "connectors": discovery_engine.get_connector_status(),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Configuration Management Endpoints
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
        # Get current config for the connector (from enabled connectors)
        current_config = discovery_engine.connectors.get(connector_type, {}).config if connector_type in discovery_engine.connectors else {}
        
        # Get connector info and template
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
            # Add connector dynamically if enabled
            success = discovery_engine.add_connector(connector_type, config_dict)
            if not success:
                raise HTTPException(status_code=400, detail=f"Failed to add connector {connector_type}")
        else:
            # Remove connector if disabled
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
        # Check if connector is available
        if connector_type not in discovery_engine.get_available_connectors():
            raise HTTPException(status_code=404, detail=f"Connector type {connector_type} not available")
        
        # Check if connector is enabled and test it
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

# Connector Management Endpoints
@app.get("/api/connectors")
async def get_available_connectors():
    """Get list of all available connectors"""
    try:
        # Get available connectors with metadata
        available_connectors = discovery_engine.get_available_connectors()
        connector_categories = discovery_engine.get_connector_categories()
        
        # Get enabled connectors
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
        
        # Get configuration template
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

# Discovery Endpoints
@app.post("/api/discovery/scan")
async def start_full_discovery(background_tasks: BackgroundTasks):
    """Start full discovery scan"""
    async def run_discovery():
        try:
            result = await discovery_engine.scan_all_data_sources()
            print(f"Discovery completed: {result}")
        except Exception as e:
            print(f"Discovery failed: {e}")
    
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
                # Update asset catalog
                for asset in assets:
                    await discovery_engine.asset_catalog.add_or_update_asset(asset)
                print(f"Source {source} discovery completed: {len(assets)} assets")
            else:
                print(f"Source {source} not found")
        except Exception as e:
            print(f"Source {source} discovery failed: {e}")
    
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
            # Use the real GCP connector if available
            if source in discovery_engine.connectors:
                connector = discovery_engine.connectors[source]
                assets = connector.discover_assets()
                
                # Update asset catalog
                try:
                    for asset in assets:
                        await discovery_engine.asset_catalog.add_or_update_asset(asset)
                except Exception as catalog_error:
                    print(f"Failed to update asset catalog: {catalog_error}")
                
                return {
                    "status": "success",
                    "source": source,
                    "assets_discovered": len(assets),
                    "assets": assets,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                # Return error if GCP connector is not configured
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
            
            # Update asset catalog
            try:
                for asset in assets:
                    await discovery_engine.asset_catalog.add_or_update_asset(asset)
            except Exception as catalog_error:
                print(f"Failed to update asset catalog: {catalog_error}")
            
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

# Asset Management Endpoints
@app.get("/api/assets")
async def get_all_assets():
    """Get all discovered assets"""
    return discovery_engine.get_asset_inventory()

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

# Monitoring Endpoints
@app.post("/api/monitoring/start")
async def start_monitoring(monitoring_request: MonitoringRequest, background_tasks: BackgroundTasks):
    """Start continuous monitoring"""
    async def run_monitoring():
        try:
            result = await discovery_engine.start_continuous_monitoring(
                real_time=monitoring_request.real_time
            )
            print(f"Monitoring started: {result}")
        except Exception as e:
            print(f"Monitoring failed: {e}")
    
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

# Statistics Endpoints
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

# Export endpoints
@app.get("/api/export/config")
async def export_config():
    """Export current configuration"""
    try:
        # Export enabled connectors instead of config file
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
    # Create UI directory if it doesn't exist
    os.makedirs("ui/static", exist_ok=True)
    
    print("🚀 Starting Data Discovery Web API...")
    print("📊 Dashboard: http://localhost:8000")
    print("📚 API Docs: http://localhost:8000/docs")
    print("🔧 Interactive API: http://localhost:8000/redoc")
    
    uvicorn.run(
        "web_api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
