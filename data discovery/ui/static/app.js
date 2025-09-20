// Data Discovery System - Frontend JavaScript

// Global variables
let currentConnectors = {};
let currentAssets = [];
let discoveryChart = null;
let refreshInterval = null;

// API Base URL
const API_BASE = '/api';

// Initialize the application
document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 Data Discovery System - UI Loaded');
    
    // Add loading animation to page
    addPageLoadAnimation();
    
    // Initialize dashboard
    initializeDashboard();
    
    // Set up auto-refresh
    startAutoRefresh();
    
    // Load initial data with staggered animations
    setTimeout(() => loadSystemHealth(), 100);
    setTimeout(() => loadConnectors(), 200);
    setTimeout(() => loadMyConnections(), 250);
    setTimeout(() => loadAssets(), 300);
    
    // Set up event listeners
    setupEventListeners();
    
    // Initialize tooltips
    initializeTooltips();
});

// Add page load animation
function addPageLoadAnimation() {
    // Add fade-in animation to main container
    const mainContainer = document.querySelector('.container-fluid');
    if (mainContainer) {
        mainContainer.style.opacity = '0';
        mainContainer.style.transform = 'translateY(20px)';
        
        setTimeout(() => {
            mainContainer.style.transition = 'all 0.8s cubic-bezier(0.4, 0, 0.2, 1)';
            mainContainer.style.opacity = '1';
            mainContainer.style.transform = 'translateY(0)';
        }, 100);
    }
    
    // Add staggered animation to cards
    const cards = document.querySelectorAll('.card');
    cards.forEach((card, index) => {
        card.style.opacity = '0';
        card.style.transform = 'translateY(30px)';
        
        setTimeout(() => {
            card.style.transition = 'all 0.6s cubic-bezier(0.4, 0, 0.2, 1)';
            card.style.opacity = '1';
            card.style.transform = 'translateY(0)';
        }, 200 + (index * 100));
    });
}

// Initialize tooltips
function initializeTooltips() {
    // Initialize Bootstrap tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

// Initialize dashboard
function initializeDashboard() {
    updateSystemStatus();
    initializeChart();
}

// Set up event listeners
function setupEventListeners() {
    // Tab change events
    document.querySelectorAll('[data-bs-toggle="tab"]').forEach(tab => {
        tab.addEventListener('shown.bs.tab', function(event) {
            const target = event.target.getAttribute('data-bs-target');
            if (target === '#connectors') {
                loadConnectors();
            } else if (target === '#assets') {
                loadAssets();
            } else if (target === '#discovery') {
                loadDiscoveryStatus();
            } else if (target === '#monitoring') {
                loadMonitoringStatus();
            }
        });
    });
    
    // Search on Enter key
    document.getElementById('asset-search').addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            searchAssets();
        }
    });
}

// API Helper Functions
async function apiCall(endpoint, options = {}) {
    try {
        const response = await fetch(`${API_BASE}${endpoint}`, {
            headers: {
                'Content-Type': 'application/json',
                ...options.headers
            },
            ...options
        });
        
        if (!response.ok) {
            throw new Error(`API call failed: ${response.status} ${response.statusText}`);
        }
        
        return await response.json();
    } catch (error) {
        console.error('API Error:', error);
        showNotification('API Error: ' + error.message, 'danger');
        throw error;
    }
}

// System Status Functions
async function updateSystemStatus() {
    try {
        const health = await apiCall('/system/health');
        const status = await apiCall('/system/status');
        
        // Update status indicators
        document.getElementById('system-status').textContent = 
            health.system_health === 'healthy' ? 'System Online' : 'System Issues';
        
        // Update overview cards
        document.getElementById('total-assets').textContent = status.total_assets || 0;
        document.getElementById('active-connectors').textContent = status.active_connectors?.length || 0;
        document.getElementById('last-scan').textContent = 
            status.last_scan ? formatDateTime(status.last_scan) : 'Never';
        document.getElementById('monitoring-status').textContent = 
            status.monitoring_enabled ? 'Active' : 'Disabled';
        
        // Update monitoring card color
        const monitoringCard = document.querySelector('.bg-warning');
        if (status.monitoring_enabled) {
            monitoringCard.className = monitoringCard.className.replace('bg-warning', 'bg-success');
        }
        
    } catch (error) {
        console.error('Failed to update system status:', error);
    }
}

async function loadSystemHealth() {
    try {
        const health = await apiCall('/system/health');
        const healthContainer = document.getElementById('system-health');
        
        let healthHTML = '<div class="row">';
        
        // System Health
        healthHTML += `
            <div class="col-md-6 mb-3">
                <div class="d-flex align-items-center">
                    <div class="me-3">
                        <i class="fas fa-server fa-2x text-${health.system_health === 'healthy' ? 'success' : 'danger'}"></i>
                    </div>
                    <div>
                        <h6 class="mb-1">System Health</h6>
                        <span class="badge bg-${health.system_health === 'healthy' ? 'success' : 'danger'}">${health.system_health}</span>
                    </div>
                </div>
            </div>
        `;
        
        // Connectors Health
        const connectorHealth = health.connectors || {};
        healthHTML += `
            <div class="col-md-6 mb-3">
                <div class="d-flex align-items-center">
                    <div class="me-3">
                        <i class="fas fa-plug fa-2x text-info"></i>
                    </div>
                    <div>
                        <h6 class="mb-1">Connectors</h6>
                        <small>${connectorHealth.enabled || 0} of ${connectorHealth.total || 0} enabled</small>
                    </div>
                </div>
            </div>
        `;
        
        // Monitoring Status
        const monitoring = health.monitoring || {};
        healthHTML += `
            <div class="col-md-6 mb-3">
                <div class="d-flex align-items-center">
                    <div class="me-3">
                        <i class="fas fa-eye fa-2x text-${monitoring.enabled ? 'success' : 'secondary'}"></i>
                    </div>
                    <div>
                        <h6 class="mb-1">Monitoring</h6>
                        <span class="badge bg-${monitoring.enabled ? 'success' : 'secondary'}">
                            ${monitoring.enabled ? 'Active' : 'Disabled'}
                        </span>
                    </div>
                </div>
            </div>
        `;
        
        // Last Scan
        healthHTML += `
            <div class="col-md-6 mb-3">
                <div class="d-flex align-items-center">
                    <div class="me-3">
                        <i class="fas fa-clock fa-2x text-warning"></i>
                    </div>
                    <div>
                        <h6 class="mb-1">Last Scan</h6>
                        <small>${health.last_scan ? formatDateTime(health.last_scan) : 'Never'}</small>
                    </div>
                </div>
            </div>
        `;
        
        healthHTML += '</div>';
        healthContainer.innerHTML = healthHTML;
        
    } catch (error) {
        document.getElementById('system-health').innerHTML = 
            '<div class="alert alert-danger">Failed to load system health</div>';
    }
}

// Connector Functions
async function loadConnectors() {
    try {
        const response = await apiCall('/connectors');
        const healthResponse = await apiCall('/system/health');
        currentConnectors = response.connectors;
        const availableConnectors = response.available_connectors;
        const connectorStatus = healthResponse.connector_status || {};
        
        // Update total connectors badge
        document.getElementById('total-connectors-badge').textContent = 
            `${response.total_connectors} (${response.enabled_connectors} enabled)`;
        
        // Render connector categories with proper metadata mapping
        renderConnectorCategory('cloud-connectors', currentConnectors.cloud_providers, 'cloud', availableConnectors, connectorStatus);
        renderConnectorCategory('database-connectors', currentConnectors.databases, 'database', availableConnectors, connectorStatus);
        renderConnectorCategory('warehouse-connectors', currentConnectors.data_warehouses, 'warehouse', availableConnectors, connectorStatus);
        renderConnectorCategory('network-storage-connectors', currentConnectors.network_storage, 'network_storage', availableConnectors, connectorStatus);
        
        // Also refresh My Connections
        loadMyConnections();
        
    } catch (error) {
        console.error('Failed to load connectors:', error);
    }
}

async function loadMyConnections() {
    try {
        const response = await apiCall('/system/health');
        const connectorStatus = response.connector_status || {};
        
        // Filter for enabled/configured connectors
        const activeConnections = Object.entries(connectorStatus)
            .filter(([connectorId, status]) => status.enabled && status.configured)
            .map(([connectorId, status]) => ({
                id: connectorId,
                name: getConnectorDisplayName(connectorId),
                status: status.connected ? 'connected' : 'disconnected',
                lastTest: status.last_test,
                error: status.error
            }));
        
        renderMyConnections(activeConnections);
        
    } catch (error) {
        console.error('Failed to load my connections:', error);
        renderMyConnections([]);
    }
}

function getConnectorDisplayName(connectorId) {
    const displayNames = {
        'gcp': 'Google Cloud Platform',
        'aws': 'Amazon Web Services',
        'azure': 'Microsoft Azure',
        'mysql': 'MySQL Database',
        'postgresql': 'PostgreSQL Database',
        'oracle': 'Oracle Database',
        'bigquery': 'Google BigQuery',
        'databricks': 'Databricks',
        'trino': 'Trino',
        'snowflake': 'Snowflake',
        'nas': 'NAS Drives',
        'sftp': 'SFTP Server'
    };
    
    return displayNames[connectorId] || connectorId.charAt(0).toUpperCase() + connectorId.slice(1);
}

function renderMyConnections(connections) {
    const container = document.getElementById('my-connections');
    const countElement = document.getElementById('active-connections-count');
    
    // Update count
    countElement.textContent = connections.length;
    
    if (connections.length === 0) {
        container.innerHTML = `
            <div class="col-12 text-center text-muted">
                <i class="fas fa-info-circle me-2"></i>
                No active connections yet. Add a connection below to get started.
            </div>
        `;
        return;
    }
    
    let html = '';
    connections.forEach(connection => {
        const statusClass = connection.status === 'connected' ? 'success' : 'danger';
        const statusIcon = connection.status === 'connected' ? 'check-circle' : 'times-circle';
        const lastTestText = connection.lastTest ? 
            new Date(connection.lastTest).toLocaleString() : 'Never tested';
        
        html += `
            <div class="col-md-6 col-lg-4 mb-3">
                <div class="card h-100 border-${statusClass === 'success' ? 'success' : 'danger'}">
                    <div class="card-body">
                        <div class="d-flex justify-content-between align-items-start mb-2">
                            <h6 class="card-title mb-0">${connection.name}</h6>
                            <span class="badge bg-${statusClass}">
                                <i class="fas fa-${statusIcon} me-1"></i>
                                ${connection.status.charAt(0).toUpperCase() + connection.status.slice(1)}
                            </span>
                        </div>
                        <p class="card-text small text-muted mb-2">
                            <i class="fas fa-clock me-1"></i>
                            Last tested: ${lastTestText}
                        </p>
                        ${connection.error ? `
                            <div class="alert alert-danger alert-sm mb-2">
                                <i class="fas fa-exclamation-triangle me-1"></i>
                                ${connection.error}
                            </div>
                        ` : ''}
                        <div class="d-flex gap-2">
                            <button class="btn btn-outline-primary btn-sm" onclick="testConnection('${connection.id}')">
                                <i class="fas fa-play me-1"></i> Test
                            </button>
                            <button class="btn btn-outline-secondary btn-sm" onclick="viewConnectionDetails('${connection.id}')">
                                <i class="fas fa-eye me-1"></i> View
                            </button>
                            <button class="btn btn-outline-danger btn-sm" onclick="deleteConnection('${connection.id}')">
                                <i class="fas fa-trash me-1"></i> Remove
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html;
}

async function testConnection(connectorId) {
    try {
        showNotification('Testing connection...', 'info');
        
        const response = await apiCall(`/config/${connectorId}/test`, {
            method: 'POST'
        });
        
        if (response) {
            showNotification('Connection test completed', 'success');
            // Refresh the connections list
            loadMyConnections();
        } else {
            showNotification('Connection test failed', 'danger');
        }
        
    } catch (error) {
        console.error('Connection test failed:', error);
        showNotification('Connection test failed: ' + error.message, 'danger');
    }
}

async function viewConnectionDetails(connectorId) {
    try {
        const response = await apiCall(`/config/${connectorId}`);
        
        if (response && response.config) {
            // Show connection details in a modal or alert
            const configDetails = JSON.stringify(response.config, null, 2);
            alert(`Connection Details for ${connectorId}:\n\n${configDetails}`);
        } else {
            showNotification('No configuration found for this connection', 'warning');
        }
        
    } catch (error) {
        console.error('Failed to load connection details:', error);
        showNotification('Failed to load connection details', 'danger');
    }
}

async function deleteConnection(connectorId) {
    if (!confirm(`Are you sure you want to remove the ${connectorId} connection?`)) {
        return;
    }
    
    try {
        const response = await apiCall(`/config/${connectorId}`, {
            method: 'DELETE'
        });
        
        if (response) {
            showNotification('Connection removed successfully', 'success');
            // Refresh the connections list
            loadMyConnections();
            // Also refresh the connectors list
            loadConnectors();
        } else {
            showNotification('Failed to remove connection', 'danger');
        }
        
    } catch (error) {
        console.error('Failed to remove connection:', error);
        showNotification('Failed to remove connection: ' + error.message, 'danger');
    }
}

function renderConnectorCategory(containerId, connectors, iconType, availableConnectors, connectorStatus) {
    const container = document.getElementById(containerId);
    if (!connectors) {
        container.innerHTML = '<div class="text-center text-muted">No connectors available</div>';
        return;
    }
    
    let html = '';
    
    // Handle both array of strings and object format
    let connectorList = Array.isArray(connectors) ? connectors : Object.keys(connectors);
    
    // Filter out generic connectors for specific categories
    if (containerId === 'database-connectors') {
        connectorList = connectorList.filter(id => id !== 'databases');
    }
    if (containerId === 'warehouse-connectors') {
        connectorList = connectorList.filter(id => id !== 'data_warehouses');
    }
    
    connectorList.forEach(connectorId => {
        // Get connector metadata from availableConnectors
        const connector = availableConnectors[connectorId];
        if (!connector) {
            console.warn(`No metadata found for connector: ${connectorId}`);
            return;
        }
        
        // Get connector status from system health
        const status = connectorStatus[connectorId] || { enabled: false, configured: false, connected: false };
        const isEnabled = status.enabled;
        const isConfigured = status.configured;
        const isConnected = status.connected;
        const statusClass = isEnabled ? 'enabled' : 'disabled';
        const statusBadge = isEnabled ? 'success' : 'secondary';
        
        html += `
            <div class="col-md-6 col-lg-4 mb-3">
                <div class="connector-card ${statusClass}">
                    <div class="d-flex align-items-center">
                        <div class="connector-icon ${iconType} me-3">
                            <i class="fas fa-${getConnectorIcon(iconType)}"></i>
                        </div>
                        <div class="flex-grow-1">
                            <h6 class="mb-1">${connector.name}</h6>
                            <small class="text-muted">${connector.services?.join(', ') || ''}</small>
                            <div class="mt-1">
                                <span class="badge bg-${statusBadge}">${isEnabled ? 'Enabled' : 'Disabled'}</span>
                                ${isConfigured ? '<span class="badge bg-info ms-1">Configured</span>' : '<span class="badge bg-warning ms-1">Not Configured</span>'}
                                ${isConnected ? '<span class="badge bg-success ms-1">Connected</span>' : ''}
                            </div>
                        </div>
                        <div class="connector-actions">
                            <div class="btn-group" role="group">
                                <button class="btn btn-outline-primary btn-sm" onclick="openConnectionWizard('${connectorId}', '${connector.name}')" data-bs-toggle="tooltip" title="Configure Connection">
                                    <i class="fas fa-cog"></i>
                                </button>
                                <button class="btn btn-outline-success btn-sm" onclick="testConnection('${connectorId}')" data-bs-toggle="tooltip" title="Test Connection">
                                    <i class="fas fa-plug"></i>
                                </button>
                                <button class="btn btn-outline-info btn-sm" onclick="viewConnectionDetails('${connectorId}')" data-bs-toggle="tooltip" title="View Details">
                                    <i class="fas fa-info-circle"></i>
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html || '<div class="text-center text-muted">No connectors in this category</div>';
}

function getConnectorIcon(type) {
    const icons = {
        'cloud': 'cloud',
        'database': 'database',
        'warehouse': 'warehouse',
        'network_storage': 'network-wired',
        'streaming': 'stream',
        'lake': 'water'
    };
    return icons[type] || 'plug';
}

async function openConnectorModal(connectorId, connectorName) {
    try {
        const config = await apiCall(`/config/${connectorId}`);
        
        document.querySelector('#connectorModal .modal-title').textContent = `Configure ${connectorName}`;
        
        let modalBody = `
            <div class="mb-3">
                <div class="form-check form-switch">
                    <input class="form-check-input" type="checkbox" id="connector-enabled" ${config.enabled ? 'checked' : ''}>
                    <label class="form-check-label" for="connector-enabled">
                        Enable ${connectorName}
                    </label>
                </div>
            </div>
            <div id="connector-config-fields">
        `;
        
        // Add configuration fields based on connector type
        if (connectorId === 'gcp') {
            modalBody += `
                <div class="mb-3">
                    <label for="project-id" class="form-label">Project ID *</label>
                    <input type="text" class="form-control" id="project-id" value="${config.config?.project_id || ''}" placeholder="your-project-id" required>
                    <small class="text-muted">Your Google Cloud Project ID</small>
                </div>
                <div class="mb-3">
                    <label for="service-account-json" class="form-label">Service Account JSON *</label>
                    <textarea class="form-control" id="service-account-json" rows="6" placeholder='{"type": "service_account", "project_id": "..."}' required>${config.config?.service_account_json || ''}</textarea>
                    <small class="text-muted">Paste your Google Cloud service account JSON key</small>
                </div>
                <div class="mb-3">
                    <label for="credentials-path" class="form-label">OR Credentials Path (Alternative)</label>
                    <input type="text" class="form-control" id="credentials-path" value="${config.config?.credentials_path || ''}" placeholder="path/to/service-account.json">
                    <small class="text-muted">Alternative: Path to service account JSON file</small>
                </div>
                <div class="mb-3">
                    <label for="services" class="form-label">Services</label>
                    <select class="form-select" id="services" multiple>
                        <option value="bigquery" ${config.config?.services?.includes('bigquery') ? 'selected' : ''}>BigQuery</option>
                        <option value="cloud_storage" ${config.config?.services?.includes('cloud_storage') ? 'selected' : ''}>Cloud Storage</option>
                    </select>
                    <small class="text-muted">Select which GCP services to discover</small>
                </div>
            `;
        // Database Connectors
        } else if (connectorId === 'postgresql') {
            modalBody += `
                <div class="mb-3">
                    <label for="host" class="form-label">Host</label>
                    <input type="text" class="form-control" id="host" value="${config.config?.host || ''}" placeholder="localhost" required>
                </div>
                <div class="mb-3">
                    <label for="port" class="form-label">Port</label>
                    <input type="number" class="form-control" id="port" value="${config.config?.port || 5432}" placeholder="5432" required>
                </div>
                <div class="mb-3">
                    <label for="database" class="form-label">Database</label>
                    <input type="text" class="form-control" id="database" value="${config.config?.database || ''}" placeholder="postgres" required>
                </div>
                <div class="mb-3">
                    <label for="username" class="form-label">Username</label>
                    <input type="text" class="form-control" id="username" value="${config.config?.username || ''}" placeholder="postgres" required>
                </div>
                <div class="mb-3">
                    <label for="password" class="form-label">Password</label>
                    <input type="password" class="form-control" id="password" value="${config.config?.password || ''}" placeholder="password" required>
                </div>
            `;
        } else if (connectorId === 'mysql') {
            modalBody += `
                <div class="mb-3">
                    <label for="host" class="form-label">Host</label>
                    <input type="text" class="form-control" id="host" value="${config.config?.host || ''}" placeholder="localhost" required>
                </div>
                <div class="mb-3">
                    <label for="port" class="form-label">Port</label>
                    <input type="number" class="form-control" id="port" value="${config.config?.port || 3306}" placeholder="3306" required>
                </div>
                <div class="mb-3">
                    <label for="database" class="form-label">Database</label>
                    <input type="text" class="form-control" id="database" value="${config.config?.database || ''}" placeholder="mysql" required>
                </div>
                <div class="mb-3">
                    <label for="username" class="form-label">Username</label>
                    <input type="text" class="form-control" id="username" value="${config.config?.username || ''}" placeholder="root" required>
                </div>
                <div class="mb-3">
                    <label for="password" class="form-label">Password</label>
                    <input type="password" class="form-control" id="password" value="${config.config?.password || ''}" placeholder="password" required>
                </div>
            `;
        } else if (connectorId === 'mongodb') {
            modalBody += `
                <div class="mb-3">
                    <label for="host" class="form-label">Host</label>
                    <input type="text" class="form-control" id="host" value="${config.config?.host || ''}" placeholder="localhost" required>
                </div>
                <div class="mb-3">
                    <label for="port" class="form-label">Port</label>
                    <input type="number" class="form-control" id="port" value="${config.config?.port || 27017}" placeholder="27017" required>
                </div>
                <div class="mb-3">
                    <label for="database" class="form-label">Database</label>
                    <input type="text" class="form-control" id="database" value="${config.config?.database || ''}" placeholder="admin" required>
                </div>
                <div class="mb-3">
                    <label for="username" class="form-label">Username (Optional)</label>
                    <input type="text" class="form-control" id="username" value="${config.config?.username || ''}" placeholder="username">
                </div>
                <div class="mb-3">
                    <label for="password" class="form-label">Password (Optional)</label>
                    <input type="password" class="form-control" id="password" value="${config.config?.password || ''}" placeholder="password">
                </div>
            `;
        } else if (connectorId === 'oracle') {
            modalBody += `
                <div class="mb-3">
                    <label for="host" class="form-label">Host</label>
                    <input type="text" class="form-control" id="host" value="${config.config?.host || ''}" placeholder="localhost" required>
                </div>
                <div class="mb-3">
                    <label for="port" class="form-label">Port</label>
                    <input type="number" class="form-control" id="port" value="${config.config?.port || 1521}" placeholder="1521" required>
                </div>
                <div class="mb-3">
                    <label for="service-name" class="form-label">Service Name</label>
                    <input type="text" class="form-control" id="service-name" value="${config.config?.service_name || ''}" placeholder="ORCL" required>
                </div>
                <div class="mb-3">
                    <label for="username" class="form-label">Username</label>
                    <input type="text" class="form-control" id="username" value="${config.config?.username || ''}" placeholder="oracle" required>
                </div>
                <div class="mb-3">
                    <label for="password" class="form-label">Password</label>
                    <input type="password" class="form-control" id="password" value="${config.config?.password || ''}" placeholder="password" required>
                </div>
            `;
        } else if (connectorId === 'sqlserver') {
            modalBody += `
                <div class="mb-3">
                    <label for="host" class="form-label">Host</label>
                    <input type="text" class="form-control" id="host" value="${config.config?.host || ''}" placeholder="localhost" required>
                </div>
                <div class="mb-3">
                    <label for="port" class="form-label">Port</label>
                    <input type="number" class="form-control" id="port" value="${config.config?.port || 1433}" placeholder="1433" required>
                </div>
                <div class="mb-3">
                    <label for="database" class="form-label">Database</label>
                    <input type="text" class="form-control" id="database" value="${config.config?.database || ''}" placeholder="master" required>
                </div>
                <div class="mb-3">
                    <label for="username" class="form-label">Username</label>
                    <input type="text" class="form-control" id="username" value="${config.config?.username || ''}" placeholder="sa" required>
                </div>
                <div class="mb-3">
                    <label for="password" class="form-label">Password</label>
                    <input type="password" class="form-control" id="password" value="${config.config?.password || ''}" placeholder="password" required>
                </div>
            `;
        } else if (connectorId === 'redis') {
            modalBody += `
                <div class="mb-3">
                    <label for="host" class="form-label">Host</label>
                    <input type="text" class="form-control" id="host" value="${config.config?.host || ''}" placeholder="localhost" required>
                </div>
                <div class="mb-3">
                    <label for="port" class="form-label">Port</label>
                    <input type="number" class="form-control" id="port" value="${config.config?.port || 6379}" placeholder="6379" required>
                </div>
                <div class="mb-3">
                    <label for="password" class="form-label">Password (Optional)</label>
                    <input type="password" class="form-control" id="password" value="${config.config?.password || ''}" placeholder="password">
                </div>
                <div class="mb-3">
                    <label for="db" class="form-label">Database Number</label>
                    <input type="number" class="form-control" id="db" value="${config.config?.db || 0}" placeholder="0">
                </div>
            `;
        } else if (connectorId === 'elasticsearch') {
            modalBody += `
                <div class="mb-3">
                    <label for="hosts" class="form-label">Hosts</label>
                    <input type="text" class="form-control" id="hosts" value="${config.config?.hosts || ''}" placeholder="localhost:9200" required>
                    <small class="text-muted">Comma-separated list of hosts (e.g., host1:9200,host2:9200)</small>
                </div>
                <div class="mb-3">
                    <label for="username" class="form-label">Username (Optional)</label>
                    <input type="text" class="form-control" id="username" value="${config.config?.username || ''}" placeholder="elastic">
                </div>
                <div class="mb-3">
                    <label for="password" class="form-label">Password (Optional)</label>
                    <input type="password" class="form-control" id="password" value="${config.config?.password || ''}" placeholder="password">
                </div>
            `;
        // Data Warehouse Connectors
        } else if (connectorId === 'snowflake') {
            modalBody += `
                <div class="mb-3">
                    <label for="account" class="form-label">Account</label>
                    <input type="text" class="form-control" id="account" value="${config.config?.account || ''}" placeholder="your-account.snowflakecomputing.com" required>
                </div>
                <div class="mb-3">
                    <label for="username" class="form-label">Username</label>
                    <input type="text" class="form-control" id="username" value="${config.config?.username || ''}" placeholder="username" required>
                </div>
                <div class="mb-3">
                    <label for="password" class="form-label">Password</label>
                    <input type="password" class="form-control" id="password" value="${config.config?.password || ''}" placeholder="password" required>
                </div>
                <div class="mb-3">
                    <label for="warehouse" class="form-label">Warehouse</label>
                    <input type="text" class="form-control" id="warehouse" value="${config.config?.warehouse || ''}" placeholder="COMPUTE_WH" required>
                </div>
                <div class="mb-3">
                    <label for="database" class="form-label">Database</label>
                    <input type="text" class="form-control" id="database" value="${config.config?.database || ''}" placeholder="SNOWFLAKE_SAMPLE_DATA" required>
                </div>
            `;
        } else if (connectorId === 'databricks') {
            modalBody += `
                <div class="mb-3">
                    <label for="server-hostname" class="form-label">Server Hostname</label>
                    <input type="text" class="form-control" id="server-hostname" value="${config.config?.server_hostname || ''}" placeholder="your-workspace.cloud.databricks.com" required>
                </div>
                <div class="mb-3">
                    <label for="http-path" class="form-label">HTTP Path</label>
                    <input type="text" class="form-control" id="http-path" value="${config.config?.http_path || ''}" placeholder="/sql/1.0/warehouses/your-warehouse-id" required>
                </div>
                <div class="mb-3">
                    <label for="access-token" class="form-label">Access Token</label>
                    <input type="password" class="form-control" id="access-token" value="${config.config?.access_token || ''}" placeholder="your-personal-access-token" required>
                </div>
            `;
        } else if (connectorId === 'bigquery') {
            modalBody += `
                <div class="mb-3">
                    <label for="project-id" class="form-label">Project ID</label>
                    <input type="text" class="form-control" id="project-id" value="${config.config?.project_id || ''}" placeholder="your-project-id" required>
                </div>
                <div class="mb-3">
                    <label for="service-account-json" class="form-label">Service Account JSON</label>
                    <textarea class="form-control" id="service-account-json" rows="6" placeholder='{"type": "service_account", "project_id": "..."}'>${config.config?.service_account_json || ''}</textarea>
                    <small class="text-muted">Paste your Google Cloud service account JSON key</small>
                </div>
                <div class="mb-3">
                    <label for="credentials-path" class="form-label">OR Credentials Path (Optional)</label>
                    <input type="text" class="form-control" id="credentials-path" value="${config.config?.credentials_path || ''}" placeholder="path/to/service-account.json">
                    <small class="text-muted">Alternative: Path to service account JSON file</small>
                </div>
                <div class="mb-3">
                    <label for="dataset" class="form-label">Default Dataset (Optional)</label>
                    <input type="text" class="form-control" id="dataset" value="${config.config?.dataset_id || ''}" placeholder="your_dataset">
                </div>
                <div class="mb-3">
                    <label for="services" class="form-label">Services</label>
                    <select class="form-select" id="services" multiple>
                        <option value="bigquery" ${config.config?.services?.includes('bigquery') ? 'selected' : 'selected'}>BigQuery</option>
                        <option value="cloud_storage" ${config.config?.services?.includes('cloud_storage') ? 'selected' : ''}>Cloud Storage</option>
                    </select>
                </div>
            `;
        } else if (connectorId === 'redshift') {
            modalBody += `
                <div class="mb-3">
                    <label for="host" class="form-label">Host</label>
                    <input type="text" class="form-control" id="host" value="${config.config?.host || ''}" placeholder="redshift-cluster.amazonaws.com" required>
                </div>
                <div class="mb-3">
                    <label for="port" class="form-label">Port</label>
                    <input type="number" class="form-control" id="port" value="${config.config?.port || 5439}" placeholder="5439" required>
                </div>
                <div class="mb-3">
                    <label for="database" class="form-label">Database</label>
                    <input type="text" class="form-control" id="database" value="${config.config?.database || ''}" placeholder="dev" required>
                </div>
                <div class="mb-3">
                    <label for="username" class="form-label">Username</label>
                    <input type="text" class="form-control" id="username" value="${config.config?.username || ''}" placeholder="username" required>
                </div>
                <div class="mb-3">
                    <label for="password" class="form-label">Password</label>
                    <input type="password" class="form-control" id="password" value="${config.config?.password || ''}" placeholder="password" required>
                </div>
            `;
        } else if (connectorId === 'clickhouse') {
            modalBody += `
                <div class="mb-3">
                    <label for="host" class="form-label">Host</label>
                    <input type="text" class="form-control" id="host" value="${config.config?.host || ''}" placeholder="localhost" required>
                </div>
                <div class="mb-3">
                    <label for="port" class="form-label">Port</label>
                    <input type="number" class="form-control" id="port" value="${config.config?.port || 9000}" placeholder="9000" required>
                </div>
                <div class="mb-3">
                    <label for="database" class="form-label">Database</label>
                    <input type="text" class="form-control" id="database" value="${config.config?.database || ''}" placeholder="default" required>
                </div>
                <div class="mb-3">
                    <label for="username" class="form-label">Username</label>
                    <input type="text" class="form-control" id="username" value="${config.config?.username || ''}" placeholder="default" required>
                </div>
                <div class="mb-3">
                    <label for="password" class="form-label">Password</label>
                    <input type="password" class="form-control" id="password" value="${config.config?.password || ''}" placeholder="password">
                </div>
            `;
        // SaaS Platform Connectors
        } else if (connectorId === 'salesforce') {
            modalBody += `
                <div class="mb-3">
                    <label for="username" class="form-label">Username</label>
                    <input type="email" class="form-control" id="username" value="${config.config?.username || ''}" placeholder="user@company.com" required>
                </div>
                <div class="mb-3">
                    <label for="password" class="form-label">Password</label>
                    <input type="password" class="form-control" id="password" value="${config.config?.password || ''}" placeholder="password" required>
                </div>
                <div class="mb-3">
                    <label for="security-token" class="form-label">Security Token</label>
                    <input type="password" class="form-control" id="security-token" value="${config.config?.security_token || ''}" placeholder="security_token" required>
                </div>
                <div class="mb-3">
                    <label for="domain" class="form-label">Domain</label>
                    <select class="form-select" id="domain">
                        <option value="login" ${config.config?.domain === 'login' ? 'selected' : ''}>Production (login)</option>
                        <option value="test" ${config.config?.domain === 'test' ? 'selected' : ''}>Sandbox (test)</option>
                    </select>
                </div>
            `;
        } else if (connectorId === 'slack') {
            modalBody += `
                <div class="mb-3">
                    <label for="bot-token" class="form-label">Bot Token</label>
                    <input type="password" class="form-control" id="bot-token" value="${config.config?.bot_token || ''}" placeholder="xoxb-your-bot-token" required>
                    <small class="text-muted">Get this from your Slack app's OAuth & Permissions page</small>
                </div>
            `;
        } else if (connectorId === 'jira') {
            modalBody += `
                <div class="mb-3">
                    <label for="server" class="form-label">Server URL</label>
                    <input type="url" class="form-control" id="server" value="${config.config?.server || ''}" placeholder="https://your-domain.atlassian.net" required>
                </div>
                <div class="mb-3">
                    <label for="username" class="form-label">Username (Email)</label>
                    <input type="email" class="form-control" id="username" value="${config.config?.username || ''}" placeholder="user@company.com" required>
                </div>
                <div class="mb-3">
                    <label for="api-token" class="form-label">API Token</label>
                    <input type="password" class="form-control" id="api-token" value="${config.config?.api_token || ''}" placeholder="your-api-token" required>
                    <small class="text-muted">Generate this from your Atlassian account settings</small>
                </div>
            `;
        } else if (connectorId === 'servicenow') {
            modalBody += `
                <div class="mb-3">
                    <label for="instance" class="form-label">Instance</label>
                    <input type="text" class="form-control" id="instance" value="${config.config?.instance || ''}" placeholder="your-instance" required>
                    <small class="text-muted">Just the instance name, not the full URL</small>
                </div>
                <div class="mb-3">
                    <label for="username" class="form-label">Username</label>
                    <input type="text" class="form-control" id="username" value="${config.config?.username || ''}" placeholder="username" required>
                </div>
                <div class="mb-3">
                    <label for="password" class="form-label">Password</label>
                    <input type="password" class="form-control" id="password" value="${config.config?.password || ''}" placeholder="password" required>
                </div>
            `;
        } else if (connectorId === 'hubspot') {
            modalBody += `
                <div class="mb-3">
                    <label for="api-key" class="form-label">API Key</label>
                    <input type="password" class="form-control" id="api-key" value="${config.config?.api_key || ''}" placeholder="your-hubspot-api-key" required>
                    <small class="text-muted">Get this from your HubSpot account settings</small>
                </div>
            `;
        } else if (connectorId === 'zendesk') {
            modalBody += `
                <div class="mb-3">
                    <label for="subdomain" class="form-label">Subdomain</label>
                    <input type="text" class="form-control" id="subdomain" value="${config.config?.subdomain || ''}" placeholder="your-subdomain" required>
                    <small class="text-muted">From your Zendesk URL: https://your-subdomain.zendesk.com</small>
                </div>
                <div class="mb-3">
                    <label for="email" class="form-label">Email</label>
                    <input type="email" class="form-control" id="email" value="${config.config?.email || ''}" placeholder="user@company.com" required>
                </div>
                <div class="mb-3">
                    <label for="api-token" class="form-label">API Token</label>
                    <input type="password" class="form-control" id="api-token" value="${config.config?.api_token || ''}" placeholder="your-api-token" required>
                    <small class="text-muted">Generate this from your Zendesk Admin settings</small>
                </div>
            `;
        } else {
            modalBody += `
                <div class="alert alert-info">
                    <i class="fas fa-info-circle me-2"></i>
                    Configuration options for ${connectorName} will be available here.
                    Please refer to the documentation for specific configuration parameters.
                </div>
            `;
        }
        
        modalBody += '</div>';
        
        document.getElementById('connector-modal-body').innerHTML = modalBody;
        document.getElementById('connectorModal').setAttribute('data-connector-id', connectorId);
        
        new bootstrap.Modal(document.getElementById('connectorModal')).show();
        
    } catch (error) {
        showNotification('Failed to load connector configuration', 'danger');
    }
}

async function saveConnectorConfig() {
    const modal = document.getElementById('connectorModal');
    let connectorId = modal.getAttribute('data-connector-id');
    
    // Map frontend connector IDs to backend connector IDs
    const connectorIdMapping = {
        'bigquery': 'gcp',
        'storage': 'gcp',
        's3': 'aws',
        'blob': 'azure',
        'warehouse': 'data_warehouses',
        'workspace': 'data_warehouses',
        'datasets': 'bigquery'
    };
    
    // Use mapped connector ID if available, otherwise use original
    connectorId = connectorIdMapping[connectorId] || connectorId;
    
    try {
        const enabled = document.getElementById('connector-enabled').checked;
        
        let config = { enabled };
        
        // Get configuration based on connector type
        if (connectorId === 'gcp') {
            const projectId = document.getElementById('project-id').value;
            const credentialsPath = document.getElementById('credentials-path').value;
            const serviceAccountJson = document.getElementById('service-account-json').value;
            const servicesSelect = document.getElementById('services');
            const services = Array.from(servicesSelect.selectedOptions).map(option => option.value);
            
            config.config = {
                project_id: projectId,
                services: services
            };
            
            // Add credentials if provided
            if (serviceAccountJson.trim()) {
                try {
                    // Validate JSON format
                    JSON.parse(serviceAccountJson);
                    config.config.service_account_json = serviceAccountJson;
                } catch (e) {
                    showNotification('Invalid JSON format in Service Account JSON field', 'error');
                    return;
                }
            } else if (credentialsPath.trim()) {
                config.config.credentials_path = credentialsPath;
            }
        // Database Connectors Save Handlers
        } else if (connectorId === 'postgresql') {
            const host = document.getElementById('host').value;
            const port = document.getElementById('port').value;
            const database = document.getElementById('database').value;
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            
            config.config = {
                host: host,
                port: parseInt(port),
                database: database,
                username: username,
                password: password
            };
        } else if (connectorId === 'mysql') {
            const host = document.getElementById('host').value;
            const port = document.getElementById('port').value;
            const database = document.getElementById('database').value;
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            
            config.config = {
                host: host,
                port: parseInt(port),
                database: database,
                username: username,
                password: password
            };
        } else if (connectorId === 'mongodb') {
            const host = document.getElementById('host').value;
            const port = document.getElementById('port').value;
            const database = document.getElementById('database').value;
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            
            config.config = {
                host: host,
                port: parseInt(port),
                database: database,
                username: username || null,
                password: password || null
            };
        } else if (connectorId === 'oracle') {
            const host = document.getElementById('host').value;
            const port = document.getElementById('port').value;
            const serviceName = document.getElementById('service-name').value;
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            
            config.config = {
                host: host,
                port: parseInt(port),
                service_name: serviceName,
                username: username,
                password: password
            };
        } else if (connectorId === 'sqlserver') {
            const host = document.getElementById('host').value;
            const port = document.getElementById('port').value;
            const database = document.getElementById('database').value;
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            
            config.config = {
                host: host,
                port: parseInt(port),
                database: database,
                username: username,
                password: password
            };
        } else if (connectorId === 'redis') {
            const host = document.getElementById('host').value;
            const port = document.getElementById('port').value;
            const password = document.getElementById('password').value;
            const db = document.getElementById('db').value;
            
            config.config = {
                host: host,
                port: parseInt(port),
                password: password || null,
                db: parseInt(db)
            };
        } else if (connectorId === 'elasticsearch') {
            const hosts = document.getElementById('hosts').value;
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            
            config.config = {
                hosts: hosts.split(',').map(h => h.trim()),
                username: username || null,
                password: password || null
            };
        // Data Warehouse Connectors Save Handlers
        } else if (connectorId === 'snowflake') {
            const account = document.getElementById('account').value;
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            const warehouse = document.getElementById('warehouse').value;
            const database = document.getElementById('database').value;
            
            config.config = {
                account: account,
                username: username,
                password: password,
                warehouse: warehouse,
                database: database
            };
        } else if (connectorId === 'databricks') {
            const serverHostname = document.getElementById('server-hostname').value;
            const httpPath = document.getElementById('http-path').value;
            const accessToken = document.getElementById('access-token').value;
            
            config.config = {
                server_hostname: serverHostname,
                http_path: httpPath,
                access_token: accessToken
            };
        } else if (connectorId === 'bigquery') {
            const projectId = document.getElementById('project-id').value;
            const serviceAccountJson = document.getElementById('service-account-json').value;
            const credentialsPath = document.getElementById('credentials-path').value;
            const dataset = document.getElementById('dataset').value;
            const servicesSelect = document.getElementById('services');
            const services = Array.from(servicesSelect.selectedOptions).map(option => option.value);
            
            config.config = {
                project_id: projectId,
                service_account_json: serviceAccountJson || null,
                credentials_path: credentialsPath || null,
                dataset_id: dataset || null,
                services: services.length > 0 ? services : ['bigquery']
            };
        } else if (connectorId === 'redshift') {
            const host = document.getElementById('host').value;
            const port = document.getElementById('port').value;
            const database = document.getElementById('database').value;
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            
            config.config = {
                host: host,
                port: parseInt(port),
                database: database,
                username: username,
                password: password
            };
        } else if (connectorId === 'clickhouse') {
            const host = document.getElementById('host').value;
            const port = document.getElementById('port').value;
            const database = document.getElementById('database').value;
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            
            config.config = {
                host: host,
                port: parseInt(port),
                database: database,
                username: username,
                password: password || null
            };
        // SaaS Platform Connectors Save Handlers
        } else if (connectorId === 'salesforce') {
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            const securityToken = document.getElementById('security-token').value;
            const domain = document.getElementById('domain').value;
            
            config.config = {
                username: username,
                password: password,
                security_token: securityToken,
                domain: domain
            };
        } else if (connectorId === 'slack') {
            const botToken = document.getElementById('bot-token').value;
            
            config.config = {
                bot_token: botToken
            };
        } else if (connectorId === 'jira') {
            const server = document.getElementById('server').value;
            const username = document.getElementById('username').value;
            const apiToken = document.getElementById('api-token').value;
            
            config.config = {
                server: server,
                username: username,
                api_token: apiToken
            };
        } else if (connectorId === 'servicenow') {
            const instance = document.getElementById('instance').value;
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            
            config.config = {
                instance: instance,
                username: username,
                password: password
            };
        } else if (connectorId === 'hubspot') {
            const apiKey = document.getElementById('api-key').value;
            
            config.config = {
                api_key: apiKey
            };
        } else if (connectorId === 'zendesk') {
            const subdomain = document.getElementById('subdomain').value;
            const email = document.getElementById('email').value;
            const apiToken = document.getElementById('api-token').value;
            
            config.config = {
                subdomain: subdomain,
                email: email,
                api_token: apiToken
            };
        }
        
        await apiCall(`/config/${connectorId}`, {
            method: 'POST',
            body: JSON.stringify(config)
        });
        
        showNotification('Configuration saved successfully', 'success');
        bootstrap.Modal.getInstance(modal).hide();
        loadConnectors(); // Refresh connectors
        refreshUserConnections(); // Refresh user connections
        
    } catch (error) {
        showNotification('Failed to save configuration', 'danger');
    }
}

async function testConnection(connectorId) {
    try {
        showNotification('Testing connection...', 'info');
        
        // Test GCP connector with real API call
        if (connectorId === 'gcp') {
            // First, add the connector with the current configuration
            const projectId = document.getElementById('project-id').value;
            const serviceAccountJson = document.getElementById('service-account-json').value;
            const credentialsPath = document.getElementById('credentials-path').value;
            const servicesSelect = document.getElementById('services');
            const services = Array.from(servicesSelect.selectedOptions).map(option => option.value);
            
            if (!projectId.trim()) {
                showNotification('Please enter a Project ID', 'error');
                return;
            }
            
            // Prepare config
            const config = {
                project_id: projectId,
                services: services.length > 0 ? services : ['bigquery', 'cloud_storage']
            };
            
            if (serviceAccountJson.trim()) {
                try {
                    JSON.parse(serviceAccountJson);
                    config.service_account_json = serviceAccountJson;
                } catch (e) {
                    showNotification('Invalid JSON format in Service Account JSON field', 'error');
                    return;
                }
            } else if (credentialsPath.trim()) {
                config.credentials_path = credentialsPath;
            }
            
            // Add connector first
            const addResponse = await fetch(`/api/connectors/gcp/add`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    enabled: true,
                    config: config
                })
            });
            
            if (!addResponse.ok) {
                const error = await addResponse.json();
                showNotification(`Failed to add connector: ${error.detail || 'Unknown error'}`, 'error');
                return;
            }
            
            showNotification('GCP connector added successfully! Testing connection...', 'info');
            
            // Test connection
            const testResponse = await fetch(`/api/config/gcp/test`, {
                method: 'POST'
            });
            
            const testResult = await testResponse.json();
            
            if (testResult.status === 'success' && testResult.connection_status === 'connected') {
                showNotification('✅ GCP Connected! Discovering assets...', 'success');
                
                // Discover assets
                const discoverResponse = await fetch(`/api/discovery/test/gcp`, {
                    method: 'POST'
                });
                
                const discoverResult = await discoverResponse.json();
                
                if (discoverResult.status === 'success') {
                    const assetsCount = discoverResult.assets_discovered || 0;
                    showNotification(`✅ GCP Connected! Discovered ${assetsCount} assets`, 'success');
                    
                    // Show assets in a modal
                    if (discoverResult.assets && discoverResult.assets.length > 0) {
                        showAssetsModal(connectorId, discoverResult.assets);
                    }
                    
                    // Refresh the assets view if we're on the assets page
                    if (window.location.hash === '#assets' || document.getElementById('assets-content')) {
                        loadAssets();
                    }
                } else {
                    showNotification('Connection successful, but asset discovery failed', 'warning');
                }
                
            } else {
                showNotification(`❌ Connection failed: ${testResult.message || 'Unknown error'}`, 'error');
            }
            
            return;
        }
        
        // First test the connection
        const testResult = await apiCall(`/config/${connectorId}/test`, { method: 'POST' });
        
        if (testResult.status === 'success' || testResult.connection_status === 'connected') {
            showNotification('Connection successful! Discovering assets...', 'success');
            
            // If connection is successful, discover assets
            try {
                const discoveryResult = await apiCall(`/discovery/test/${connectorId}`, { method: 'POST' });
                
                if (discoveryResult.status === 'success') {
                    const assetsCount = discoveryResult.assets_discovered || 0;
                    showNotification(`Connection successful! Discovered ${assetsCount} assets`, 'success');
                    
                    // Show assets in a modal
                    if (discoveryResult.assets && discoveryResult.assets.length > 0) {
                        showAssetsModal(connectorId, discoveryResult.assets);
                    }
                    
                    // Refresh the assets view if we're on the assets page
                    if (window.location.hash === '#assets' || document.getElementById('assets-content')) {
                        loadAssets();
                    }
        } else {
                    showNotification('Connection successful, but asset discovery failed: ' + (discoveryResult.error || 'Unknown error'), 'warning');
                }
            } catch (discoveryError) {
                console.error('Asset discovery error:', discoveryError);
                showNotification('Connection successful, but asset discovery failed', 'warning');
            }
        } else {
            showNotification('Connection test failed: ' + (testResult.error || testResult.message), 'danger');
        }
    } catch (error) {
        console.error('Connection test error:', error);
        showNotification('Connection test failed: ' + (error.message || 'Unknown error'), 'danger');
    }
}

function showAssetsModal(connectorId, assets) {
    // Create the modal HTML
    const modalHtml = `
        <div class="modal fade" id="assetsModal" tabindex="-1" aria-labelledby="assetsModalLabel" aria-hidden="true">
            <div class="modal-dialog modal-xl">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title" id="assetsModalLabel">
                            <i class="fas fa-database me-2"></i>Discovered Assets - ${connectorId}
                        </h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body">
                        <div class="d-flex justify-content-between align-items-center mb-3">
                            <p class="mb-0">Found <strong>${assets.length}</strong> assets</p>
                            <div class="btn-group" role="group">
                                <button type="button" class="btn btn-sm btn-outline-secondary active" onclick="showAssetsView('grid')">
                                    <i class="fas fa-th"></i> Grid
                                </button>
                                <button type="button" class="btn btn-sm btn-outline-secondary" onclick="showAssetsView('list')">
                                    <i class="fas fa-list"></i> List
                                </button>
                            </div>
                        </div>
                        <div id="assets-display">
                            ${generateAssetsGridView(assets)}
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                        <button type="button" class="btn btn-primary" onclick="navigateToAssets()">
                            <i class="fas fa-arrow-right me-2"></i>View All Assets
                        </button>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    // Remove existing modal if any
    const existingModal = document.getElementById('assetsModal');
    if (existingModal) {
        existingModal.remove();
    }
    
    // Add modal to body
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    
    // Show the modal
    const modal = new bootstrap.Modal(document.getElementById('assetsModal'));
    modal.show();
    
    // Store assets for view switching
    window.currentModalAssets = assets;
}

function generateAssetsGridView(assets) {
    if (!assets || assets.length === 0) {
        return '<div class="text-center py-4"><p class="text-muted">No assets found</p></div>';
    }
    
    let html = '<div class="row">';
    
    assets.forEach(asset => {
        const assetType = asset.type || 'unknown';
        const assetIcon = getAssetIcon(assetType);
        const assetSize = formatFileSize(asset.size || 0);
        const assetDate = asset.created_date ? new Date(asset.created_date).toLocaleDateString() : 'Unknown';
        
        html += `
            <div class="col-md-6 col-lg-4 mb-3">
                <div class="card asset-card h-100">
                    <div class="card-body">
                        <div class="d-flex align-items-start mb-2">
                            <div class="asset-icon me-3">
                                <i class="${assetIcon} fa-2x text-primary"></i>
                            </div>
                            <div class="flex-grow-1">
                                <h6 class="card-title mb-1">${asset.name || 'Unnamed Asset'}</h6>
                                <small class="text-muted">${assetType}</small>
                            </div>
                        </div>
                        <div class="asset-details">
                            <small class="text-muted d-block">Size: ${assetSize}</small>
                            <small class="text-muted d-block">Created: ${assetDate}</small>
                            ${asset.location ? `<small class="text-muted d-block">Location: ${asset.location}</small>` : ''}
                        </div>
                        ${asset.metadata ? `
                            <div class="asset-metadata mt-2">
                                <small class="text-muted">
                                    ${Object.entries(asset.metadata).slice(0, 2).map(([key, value]) => 
                                        `${key}: ${value}`
                                    ).join(' • ')}
                                </small>
                            </div>
                        ` : ''}
                    </div>
                </div>
            </div>
        `;
    });
    
    html += '</div>';
    return html;
}

function generateAssetsListView(assets) {
    if (!assets || assets.length === 0) {
        return '<div class="text-center py-4"><p class="text-muted">No assets found</p></div>';
    }
    
    let html = `
        <div class="table-responsive">
            <table class="table table-hover">
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Type</th>
                        <th>Size</th>
                        <th>Created</th>
                        <th>Location</th>
                    </tr>
                </thead>
                <tbody>
    `;
    
    assets.forEach(asset => {
        const assetType = asset.type || 'unknown';
        const assetIcon = getAssetIcon(assetType);
        const assetSize = formatFileSize(asset.size || 0);
        const assetDate = asset.created_date ? new Date(asset.created_date).toLocaleDateString() : 'Unknown';
        
        html += `
            <tr>
                <td>
                    <div class="d-flex align-items-center">
                        <i class="${assetIcon} me-2 text-primary"></i>
                        <span>${asset.name || 'Unnamed Asset'}</span>
                    </div>
                </td>
                <td><span class="badge bg-light text-dark">${assetType}</span></td>
                <td>${assetSize}</td>
                <td>${assetDate}</td>
                <td><small class="text-muted">${asset.location || 'N/A'}</small></td>
            </tr>
        `;
    });
    
    html += '</tbody></table></div>';
    return html;
}

function showAssetsView(viewType) {
    const assets = window.currentModalAssets || [];
    const displayDiv = document.getElementById('assets-display');
    
    // Update button states
    document.querySelectorAll('#assetsModal .btn-group button').forEach(btn => {
        btn.classList.remove('active');
    });
    event.target.classList.add('active');
    
    // Update display
    if (viewType === 'list') {
        displayDiv.innerHTML = generateAssetsListView(assets);
    } else {
        displayDiv.innerHTML = generateAssetsGridView(assets);
    }
}

function getAssetIcon(assetType) {
    const iconMap = {
        'table': 'fas fa-table',
        'file': 'fas fa-file',
        'database': 'fas fa-database',
        'bucket': 'fas fa-archive',
        'collection': 'fas fa-layer-group',
        'index': 'fas fa-search',
        'stream': 'fas fa-stream',
        'queue': 'fas fa-list',
        'topic': 'fas fa-comments',
        'schema': 'fas fa-sitemap',
        'view': 'fas fa-eye',
        'procedure': 'fas fa-code',
        'function': 'fas fa-function',
        'model': 'fas fa-brain',
        'dashboard': 'fas fa-chart-bar',
        'report': 'fas fa-chart-line',
        'dataset': 'fas fa-chart-area'
    };
    
    return iconMap[assetType.toLowerCase()] || 'fas fa-cube';
}

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function navigateToAssets() {
    // Close the modal
    const modal = bootstrap.Modal.getInstance(document.getElementById('assetsModal'));
    if (modal) {
        modal.hide();
    }
    
    // Navigate to assets page
    showSection('assets');
    loadAssets();
}

// Asset Functions
async function loadAssets() {
    try {
        const response = await apiCall('/assets');
        currentAssets = response.assets || [];
        
        renderAssetsTable(currentAssets);
        populateAssetFilters(currentAssets);
        
    } catch (error) {
        document.getElementById('assets-table-body').innerHTML = 
            '<tr><td colspan="6" class="text-center text-danger">Failed to load assets</td></tr>';
    }
}

function renderAssetsTable(assets) {
    const tbody = document.getElementById('assets-table-body');
    
    if (!assets || Object.keys(assets).length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted">No assets found</td></tr>';
        return;
    }
    
    let html = '';
    let allAssets = [];
    
    // Flatten assets from all sources
    Object.entries(assets).forEach(([source, sourceAssets]) => {
        if (Array.isArray(sourceAssets)) {
            allAssets.push(...sourceAssets.map(asset => ({ ...asset, source })));
        }
    });
    
    allAssets.slice(0, 100).forEach(asset => { // Limit to 100 for performance
        html += `
            <tr onclick="showAssetDetails('${asset.name}')" style="cursor: pointer;">
                <td>
                    <div class="d-flex align-items-center">
                        <i class="fas fa-${getAssetIcon(asset.type)} me-2 text-primary"></i>
                        <div>
                            <div class="fw-bold">${asset.name}</div>
                            <small class="text-muted">${asset.location || ''}</small>
                        </div>
                    </div>
                </td>
                <td><span class="badge bg-secondary">${asset.type}</span></td>
                <td><span class="badge bg-info">${asset.source}</span></td>
                <td>${formatBytes(asset.size || 0)}</td>
                <td>${formatDateTime(asset.created_date)}</td>
                <td>
                    <button class="btn btn-outline-primary btn-sm" onclick="event.stopPropagation(); showAssetDetails('${asset.name}')">
                        <i class="fas fa-eye"></i>
                    </button>
                </td>
            </tr>
        `;
    });
    
    tbody.innerHTML = html;
}

function getAssetIcon(type) {
    const icons = {
        'table': 'table',
        'database': 'database',
        'file': 'file',
        'bucket': 'folder',
        'bigquery_table': 'chart-bar',
        'bigquery_dataset': 'layer-group'
    };
    return icons[type] || 'file-alt';
}

function populateAssetFilters(assets) {
    const typeFilter = document.getElementById('asset-type-filter');
    const sourceFilter = document.getElementById('asset-source-filter');
    
    const types = new Set();
    const sources = new Set();
    
    Object.entries(assets).forEach(([source, sourceAssets]) => {
        sources.add(source);
        if (Array.isArray(sourceAssets)) {
            sourceAssets.forEach(asset => types.add(asset.type));
        }
    });
    
    // Populate type filter
    typeFilter.innerHTML = '<option value="">All Asset Types</option>';
    Array.from(types).sort().forEach(type => {
        typeFilter.innerHTML += `<option value="${type}">${type}</option>`;
    });
    
    // Populate source filter
    sourceFilter.innerHTML = '<option value="">All Sources</option>';
    Array.from(sources).sort().forEach(source => {
        sourceFilter.innerHTML += `<option value="${source}">${source}</option>`;
    });
}

async function searchAssets() {
    const query = document.getElementById('asset-search').value;
    if (!query.trim()) {
        loadAssets();
        return;
    }
    
    try {
        const response = await apiCall('/assets/search', {
            method: 'POST',
            body: JSON.stringify({ query })
        });
        
        if (response.results) {
            const assetsObject = { search_results: response.results };
            renderAssetsTable(assetsObject);
        }
    } catch (error) {
        showNotification('Search failed', 'danger');
    }
}

function applyAssetFilters() {
    const typeFilter = document.getElementById('asset-type-filter').value;
    const sourceFilter = document.getElementById('asset-source-filter').value;
    
    let filteredAssets = {};
    
    Object.entries(currentAssets).forEach(([source, sourceAssets]) => {
        if (sourceFilter && source !== sourceFilter) return;
        
        let filtered = sourceAssets;
        if (typeFilter) {
            filtered = sourceAssets.filter(asset => asset.type === typeFilter);
        }
        
        if (filtered.length > 0) {
            filteredAssets[source] = filtered;
        }
    });
    
    renderAssetsTable(filteredAssets);
}

async function showAssetDetails(assetName) {
    try {
        const response = await apiCall(`/assets/${encodeURIComponent(assetName)}`);
        
        if (response.asset) {
            const asset = response.asset;
            
            let modalBody = `
                <div class="row">
                    <div class="col-md-6">
                        <h6>Basic Information</h6>
                        <table class="table table-sm">
                            <tr><th>Name:</th><td>${asset.name}</td></tr>
                            <tr><th>Type:</th><td><span class="badge bg-secondary">${asset.type}</span></td></tr>
                            <tr><th>Source:</th><td><span class="badge bg-info">${asset.source}</span></td></tr>
                            <tr><th>Location:</th><td><code>${asset.location}</code></td></tr>
                            <tr><th>Size:</th><td>${formatBytes(asset.size || 0)}</td></tr>
                            <tr><th>Created:</th><td>${formatDateTime(asset.created_date)}</td></tr>
                            ${asset.modified_date ? `<tr><th>Modified:</th><td>${formatDateTime(asset.modified_date)}</td></tr>` : ''}
                        </table>
                    </div>
                    <div class="col-md-6">
                        <h6>Metadata</h6>
                        <pre class="bg-light p-2 rounded">${JSON.stringify(asset.metadata || {}, null, 2)}</pre>
                    </div>
                </div>
            `;
            
            if (asset.schema) {
                modalBody += `
                    <div class="mt-3">
                        <h6>Schema</h6>
                        <pre class="bg-light p-2 rounded">${JSON.stringify(asset.schema, null, 2)}</pre>
                    </div>
                `;
            }
            
            document.getElementById('asset-modal-body').innerHTML = modalBody;
            new bootstrap.Modal(document.getElementById('assetModal')).show();
        }
    } catch (error) {
        showNotification('Failed to load asset details', 'danger');
    }
}

// Discovery Functions
async function startFullDiscovery() {
    try {
        showNotification('Starting full discovery...', 'info');
        await apiCall('/discovery/scan', { method: 'POST' });
        showNotification('Discovery started successfully', 'success');
        
        // Refresh status after a delay
        setTimeout(() => {
            updateSystemStatus();
            loadAssets();
        }, 2000);
        
    } catch (error) {
        showNotification('Failed to start discovery', 'danger');
    }
}

function showSourceSelection() {
    const selection = document.getElementById('source-selection');
    const select = document.getElementById('source-select');
    
    // Populate source options
    if (currentConnectors) {
        select.innerHTML = '<option value="">Select a source...</option>';
        Object.entries(currentConnectors).forEach(([category, connectors]) => {
            Object.entries(connectors).forEach(([id, connector]) => {
                if (connector.enabled) {
                    select.innerHTML += `<option value="${id}">${connector.name}</option>`;
                }
            });
        });
    }
    
    selection.style.display = 'block';
}

async function scanSpecificSource() {
    const source = document.getElementById('source-select').value;
    if (!source) {
        showNotification('Please select a source', 'warning');
        return;
    }
    
    try {
        showNotification(`Starting discovery for ${source}...`, 'info');
        await apiCall(`/discovery/scan/${source}`, { method: 'POST' });
        showNotification(`Discovery started for ${source}`, 'success');
        
        setTimeout(() => {
            updateSystemStatus();
            loadAssets();
        }, 2000);
        
    } catch (error) {
        showNotification('Failed to start source discovery', 'danger');
    }
}

async function loadDiscoveryStatus() {
    try {
        const status = await apiCall('/discovery/status');
        
        document.getElementById('discovery-status').innerHTML = `
            <p>Status: <span class="badge bg-${status.discovery_status === 'idle' ? 'secondary' : 'primary'}">${status.discovery_status}</span></p>
            <p>Last Scan: <span id="last-scan-time">${status.last_scan ? formatDateTime(status.last_scan) : 'Never'}</span></p>
            <p>Active Connectors: <span id="active-connectors-count">${status.active_connectors?.length || 0}</span></p>
        `;
        
    } catch (error) {
        console.error('Failed to load discovery status:', error);
    }
}

// Monitoring Functions
async function startMonitoring() {
    const realTime = document.getElementById('real-time-monitoring').checked;
    
    try {
        showNotification('Starting monitoring...', 'info');
        await apiCall('/monitoring/start', {
            method: 'POST',
            body: JSON.stringify({ real_time: realTime })
        });
        showNotification('Monitoring started successfully', 'success');
        
        setTimeout(loadMonitoringStatus, 1000);
        
    } catch (error) {
        showNotification('Failed to start monitoring', 'danger');
    }
}

async function stopMonitoring() {
    try {
        await apiCall('/monitoring/stop', { method: 'POST' });
        showNotification('Monitoring stopped', 'info');
        
        setTimeout(loadMonitoringStatus, 1000);
        
    } catch (error) {
        showNotification('Failed to stop monitoring', 'danger');
    }
}

async function loadMonitoringStatus() {
    try {
        const status = await apiCall('/monitoring/status');
        const monitoring = status.monitoring || {};
        
        document.getElementById('monitoring-status-card').innerHTML = `
            <p>Status: <span class="badge bg-${monitoring.enabled ? 'success' : 'secondary'}">${monitoring.enabled ? 'Active' : 'Disabled'}</span></p>
            <p>Real-time Watching: <span class="badge bg-${monitoring.real_time_watching ? 'success' : 'secondary'}">${monitoring.real_time_watching ? 'Enabled' : 'Disabled'}</span></p>
            <p>Watch Interval: <span>${monitoring.watch_interval || 5} seconds</span></p>
            <p>Batch Size: <span>${monitoring.batch_size || 100}</span></p>
        `;
        
        // Update checkbox
        document.getElementById('real-time-monitoring').checked = monitoring.real_time_watching || false;
        
    } catch (error) {
        console.error('Failed to load monitoring status:', error);
    }
}

// Loading state functions
function showLoadingState(elementId, message = 'Loading...') {
    const element = document.getElementById(elementId);
    if (element) {
        element.innerHTML = `
            <div class="d-flex flex-column align-items-center justify-content-center py-5">
                <div class="spinner-border text-primary mb-3" role="status" style="width: 3rem; height: 3rem;">
                    <span class="visually-hidden">Loading...</span>
                </div>
                <p class="text-muted mb-0">${message}</p>
            </div>
        `;
    }
}

function hideLoadingState(elementId) {
    const element = document.getElementById(elementId);
    if (element && element.innerHTML.includes('spinner-border')) {
        // Add fade out animation
        element.style.transition = 'opacity 0.3s ease';
        element.style.opacity = '0';
        setTimeout(() => {
            element.innerHTML = '';
            element.style.opacity = '1';
        }, 300);
    }
}

// Chart Functions
function initializeChart() {
    const ctx = document.getElementById('discoveryChart').getContext('2d');
    
    discoveryChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Cloud Providers', 'Databases', 'Data Warehouses', 'Network Storage', 'Streaming', 'Data Lakes'],
            datasets: [{
                label: 'Assets Discovered',
                data: [0, 0, 0, 0, 0, 0],
                backgroundColor: [
                    'rgba(99, 102, 241, 0.8)',
                    'rgba(16, 185, 129, 0.8)',
                    'rgba(6, 182, 212, 0.8)',
                    'rgba(245, 158, 11, 0.8)',
                    'rgba(139, 92, 246, 0.8)',
                    'rgba(239, 68, 68, 0.8)'
                ],
                borderColor: [
                    'rgba(99, 102, 241, 1)',
                    'rgba(16, 185, 129, 1)',
                    'rgba(6, 182, 212, 1)',
                    'rgba(245, 158, 11, 1)',
                    'rgba(139, 92, 246, 1)',
                    'rgba(239, 68, 68, 1)'
                ],
                borderWidth: 2,
                hoverOffset: 10
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'bottom',
                    labels: {
                        padding: 20,
                        usePointStyle: true,
                        font: {
                            size: 12,
                            weight: '500'
                        }
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    titleColor: 'white',
                    bodyColor: 'white',
                    borderColor: 'rgba(255, 255, 255, 0.2)',
                    borderWidth: 1,
                    cornerRadius: 8,
                    displayColors: true
                }
            },
            animation: {
                animateRotate: true,
                animateScale: true,
                duration: 2000,
                easing: 'easeInOutQuart'
            }
        }
    });
}

function updateChartData(data) {
    if (discoveryChart) {
        discoveryChart.data.datasets[0].data = data;
        discoveryChart.update('active');
    }
}

// Utility Functions
function formatBytes(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatDateTime(dateString) {
    if (!dateString) return 'N/A';
    try {
        return new Date(dateString).toLocaleString();
    } catch {
        return dateString;
    }
}

function showNotification(message, type = 'info') {
    // Create notification element with enhanced styling
    const notification = document.createElement('div');
    notification.className = `alert alert-${type} alert-dismissible fade show position-fixed`;
    notification.style.top = '20px';
    notification.style.right = '20px';
    notification.style.zIndex = '9999';
    notification.style.minWidth = '350px';
    notification.style.maxWidth = '500px';
    notification.style.borderRadius = 'var(--border-radius)';
    notification.style.boxShadow = 'var(--shadow-lg)';
    notification.style.border = 'none';
    notification.style.backdropFilter = 'blur(10px)';
    
    // Add icon based on type
    const icons = {
        'success': 'fas fa-check-circle',
        'danger': 'fas fa-exclamation-circle',
        'warning': 'fas fa-exclamation-triangle',
        'info': 'fas fa-info-circle'
    };
    
    notification.innerHTML = `
        <div class="d-flex align-items-center">
            <i class="${icons[type] || icons.info} me-2"></i>
            <span class="flex-grow-1">${message}</span>
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;
    
    // Add entrance animation
    notification.style.transform = 'translateX(100%)';
    notification.style.transition = 'transform 0.3s cubic-bezier(0.4, 0, 0.2, 1)';
    
    document.body.appendChild(notification);
    
    // Trigger entrance animation
    setTimeout(() => {
        notification.style.transform = 'translateX(0)';
    }, 10);
    
    // Auto-remove after 5 seconds with exit animation
    setTimeout(() => {
        if (notification.parentNode) {
            notification.style.transform = 'translateX(100%)';
            setTimeout(() => {
                if (notification.parentNode) {
                    notification.remove();
                }
            }, 300);
        }
    }, 5000);
}

function refreshDashboard() {
    showNotification('Refreshing dashboard...', 'info');
    updateSystemStatus();
    loadSystemHealth();
    loadConnectors();
    loadAssets();
}

function startAutoRefresh() {
    // Refresh every 30 seconds
    refreshInterval = setInterval(() => {
        updateSystemStatus();
    }, 30000);
}

// Connection Wizard Functions
let currentWizardStep = 1;
let selectedConnectionType = null;
let connectionConfig = {};

function nextWizardStep() {
    if (currentWizardStep < 4) {
        // Validate current step
        if (validateWizardStep(currentWizardStep)) {
            currentWizardStep++;
            updateWizardStep();
        }
    }
}

function previousWizardStep() {
    if (currentWizardStep > 1) {
        currentWizardStep--;
        updateWizardStep();
    }
}

function updateWizardStep() {
    // Update step indicators
    document.querySelectorAll('.wizard-step').forEach((step, index) => {
        const stepNumber = index + 1;
        step.classList.remove('active', 'completed');
        
        if (stepNumber < currentWizardStep) {
            step.classList.add('completed');
        } else if (stepNumber === currentWizardStep) {
            step.classList.add('active');
        }
    });
    
    // Update step content
    document.querySelectorAll('.wizard-step-content').forEach((content, index) => {
        content.classList.remove('active');
        if (index + 1 === currentWizardStep) {
            content.classList.add('active');
        }
    });
    
    // Update buttons
    const prevBtn = document.getElementById('wizard-prev-btn');
    const nextBtn = document.getElementById('wizard-next-btn');
    const saveBtn = document.getElementById('wizard-save-btn');
    
    prevBtn.style.display = currentWizardStep > 1 ? 'inline-block' : 'none';
    nextBtn.style.display = currentWizardStep < 4 ? 'inline-block' : 'none';
    saveBtn.style.display = currentWizardStep === 4 ? 'inline-block' : 'none';
    
    // Load step content
    loadWizardStepContent();
}

function validateWizardStep(step) {
    switch(step) {
        case 1:
            if (!selectedConnectionType) {
                showNotification('Please select a connection type', 'warning');
                return false;
            }
            return true;
        case 2:
            // Validate configuration form
            return validateConnectionForm();
        case 3:
            // Connection test should be completed
            return document.getElementById('connection-test-results').style.display !== 'none';
        default:
            return true;
    }
}

function loadWizardStepContent() {
    switch(currentWizardStep) {
        case 2:
            loadConnectionForm();
            break;
        case 4:
            loadConnectionSummary();
            break;
    }
}

function selectConnectionType(type) {
    selectedConnectionType = type;
    
    // Set connectionConfig.connectorId based on the selected type
    connectionConfig.connectorId = type;
    
    // Update UI
    document.querySelectorAll('.connection-type-card').forEach(card => {
        card.classList.remove('selected');
    });
    
    document.querySelector(`[data-type="${type}"]`).classList.add('selected');
}

function loadConnectionForm() {
    const formContainer = document.getElementById('connector-config-form');
    
    if (!selectedConnectionType) {
        formContainer.innerHTML = '<div class="alert alert-warning">Please select a connection type first.</div>';
        return;
    }
    
    let formHTML = '';
    
    switch(selectedConnectionType) {
        // AWS Connection Types
        case 's3':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My S3 Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">AWS Access Key ID</label>
                            <input type="text" class="form-control connection-form-input" id="access-key-id" placeholder="AKIA..." required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">AWS Secret Access Key</label>
                            <input type="password" class="form-control connection-form-input" id="secret-access-key" placeholder="Secret Key" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">AWS Region</label>
                            <select class="form-select connection-form-input" id="region" required>
                                <option value="">Select Region</option>
                                <option value="us-east-1">US East (N. Virginia)</option>
                                <option value="us-west-2">US West (Oregon)</option>
                                <option value="eu-west-1">Europe (Ireland)</option>
                                <option value="ap-southeast-1">Asia Pacific (Singapore)</option>
                            </select>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">S3 Bucket Name</label>
                            <input type="text" class="form-control connection-form-input" id="bucket-name" placeholder="my-data-bucket" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Prefix (Optional)</label>
                            <input type="text" class="form-control connection-form-input" id="prefix" placeholder="data/raw/">
                            <small class="text-muted">Specify a folder path within the bucket</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'rds':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My RDS Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">RDS Engine</label>
                            <select class="form-select connection-form-input" id="engine" required>
                                <option value="">Select Engine</option>
                                <option value="mysql">MySQL</option>
                                <option value="postgresql">PostgreSQL</option>
                                <option value="oracle">Oracle</option>
                                <option value="sqlserver">SQL Server</option>
                            </select>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">RDS Endpoint</label>
                            <input type="text" class="form-control connection-form-input" id="endpoint" placeholder="mydb.123456789012.us-east-1.rds.amazonaws.com" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Port</label>
                            <input type="number" class="form-control connection-form-input" id="port" placeholder="3306" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Database Name</label>
                            <input type="text" class="form-control connection-form-input" id="database" placeholder="mydb" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Username</label>
                            <input type="text" class="form-control connection-form-input" id="username" placeholder="admin" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Password</label>
                            <input type="password" class="form-control connection-form-input" id="password" placeholder="password" required>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'redshift':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My Redshift Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Port</label>
                            <input type="number" class="form-control connection-form-input" id="port" placeholder="5439" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Cluster Endpoint</label>
                            <input type="text" class="form-control connection-form-input" id="endpoint" placeholder="mycluster.123456789012.us-east-1.redshift.amazonaws.com" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Database Name</label>
                            <input type="text" class="form-control connection-form-input" id="database" placeholder="dev" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Schema</label>
                            <input type="text" class="form-control connection-form-input" id="schema" placeholder="public" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Username</label>
                            <input type="text" class="form-control connection-form-input" id="username" placeholder="admin" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Password</label>
                            <input type="password" class="form-control connection-form-input" id="password" placeholder="password" required>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'dynamodb':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My DynamoDB Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">AWS Access Key ID</label>
                            <input type="text" class="form-control connection-form-input" id="access-key-id" placeholder="AKIA..." required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">AWS Secret Access Key</label>
                            <input type="password" class="form-control connection-form-input" id="secret-access-key" placeholder="Secret Key" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">AWS Region</label>
                            <select class="form-select connection-form-input" id="region" required>
                                <option value="">Select Region</option>
                                <option value="us-east-1">US East (N. Virginia)</option>
                                <option value="us-west-2">US West (Oregon)</option>
                                <option value="eu-west-1">Europe (Ireland)</option>
                                <option value="ap-southeast-1">Asia Pacific (Singapore)</option>
                            </select>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Table Name (Optional)</label>
                            <input type="text" class="form-control connection-form-input" id="table-name" placeholder="my-table">
                            <small class="text-muted">Leave empty to scan all tables</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'database':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Database Type</label>
                            <select class="form-select connection-form-input" id="db-type">
                                <option value="">Select Database Type</option>
                                <option value="postgresql">PostgreSQL</option>
                                <option value="mysql">MySQL</option>
                                <option value="oracle">Oracle</option>
                                <option value="mssql">SQL Server</option>
                            </select>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My Database Connection">
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Host</label>
                            <input type="text" class="form-control connection-form-input" id="db-host" placeholder="localhost">
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Port</label>
                            <input type="number" class="form-control connection-form-input" id="db-port" placeholder="5432">
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Database Name</label>
                            <input type="text" class="form-control connection-form-input" id="db-name" placeholder="mydatabase">
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Schema (Optional)</label>
                            <input type="text" class="form-control connection-form-input" id="db-schema" placeholder="public">
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Username</label>
                            <input type="text" class="form-control connection-form-input" id="db-username" placeholder="username">
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Password</label>
                            <input type="password" class="form-control connection-form-input" id="db-password" placeholder="password">
                        </div>
                    </div>
                </div>
            `;
            break;
        case 'cloud':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Cloud Provider</label>
                            <select class="form-select connection-form-input" id="cloud-provider">
                                <option value="">Select Provider</option>
                                <option value="aws">Amazon Web Services</option>
                                <option value="azure">Microsoft Azure</option>
                                <option value="gcp">Google Cloud Platform</option>
                            </select>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My Cloud Storage">
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Access Key ID</label>
                            <input type="text" class="form-control connection-form-input" id="access-key" placeholder="AKIA...">
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Secret Access Key</label>
                            <input type="password" class="form-control connection-form-input" id="secret-key" placeholder="Secret Key">
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Region</label>
                            <input type="text" class="form-control connection-form-input" id="region" placeholder="us-east-1">
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Bucket/Container</label>
                            <input type="text" class="form-control connection-form-input" id="bucket" placeholder="my-bucket">
                        </div>
                    </div>
                </div>
            `;
            break;
        case 'api':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">API Type</label>
                            <select class="form-select connection-form-input" id="api-type">
                                <option value="">Select API Type</option>
                                <option value="rest">REST API</option>
                                <option value="salesforce">Salesforce</option>
                                <option value="servicenow">ServiceNow</option>
                            </select>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My API Connection">
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Base URL</label>
                            <input type="url" class="form-control connection-form-input" id="api-url" placeholder="https://api.example.com">
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Authentication Type</label>
                            <select class="form-select connection-form-input" id="auth-type">
                                <option value="">Select Auth Type</option>
                                <option value="bearer">Bearer Token</option>
                                <option value="basic">Basic Auth</option>
                                <option value="oauth">OAuth 2.0</option>
                            </select>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">API Key/Token</label>
                            <input type="password" class="form-control connection-form-input" id="api-token" placeholder="API Key or Token">
                        </div>
                    </div>
                </div>
            `;
            break;
            
        // Azure Connection Types
        case 'sql':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My Azure SQL Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Server Name</label>
                            <input type="text" class="form-control connection-form-input" id="server-name" placeholder="myserver.database.windows.net" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Database Name</label>
                            <input type="text" class="form-control connection-form-input" id="database" placeholder="mydatabase" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Port</label>
                            <input type="number" class="form-control connection-form-input" id="port" placeholder="1433" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Username</label>
                            <input type="text" class="form-control connection-form-input" id="username" placeholder="admin@myserver" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Password</label>
                            <input type="password" class="form-control connection-form-input" id="password" placeholder="password" required>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'synapse':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My Synapse Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Synapse Workspace URL</label>
                            <input type="text" class="form-control connection-form-input" id="workspace-url" placeholder="myworkspace.sql.azuresynapse.net" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">SQL Pool Name</label>
                            <input type="text" class="form-control connection-form-input" id="sql-pool" placeholder="mysqlpool" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Port</label>
                            <input type="number" class="form-control connection-form-input" id="port" placeholder="1433" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Username</label>
                            <input type="text" class="form-control connection-form-input" id="username" placeholder="sqladmin" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Password</label>
                            <input type="password" class="form-control connection-form-input" id="password" placeholder="password" required>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'cosmos':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My Cosmos DB Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Account Endpoint</label>
                            <input type="text" class="form-control connection-form-input" id="endpoint" placeholder="https://myaccount.documents.azure.com:443/" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Primary Key</label>
                            <input type="password" class="form-control connection-form-input" id="primary-key" placeholder="Primary Key" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Database Name</label>
                            <input type="text" class="form-control connection-form-input" id="database" placeholder="mydatabase" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Container Name (Optional)</label>
                            <input type="text" class="form-control connection-form-input" id="container" placeholder="mycontainer">
                            <small class="text-muted">Leave empty to scan all containers</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'blob':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My Azure Blob Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Storage Account Name</label>
                            <input type="text" class="form-control connection-form-input" id="account-name" placeholder="mystorageaccount" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Account Key</label>
                            <input type="password" class="form-control connection-form-input" id="account-key" placeholder="Storage Account Key" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Container Name</label>
                            <input type="text" class="form-control connection-form-input" id="container-name" placeholder="data-container" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Blob Prefix (Optional)</label>
                            <input type="text" class="form-control connection-form-input" id="prefix" placeholder="raw-data/">
                            <small class="text-muted">Specify a folder path within the container</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        // GCP Connection Types
        case 'sql':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My Cloud SQL Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Instance Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="instance-connection" placeholder="project:region:instance" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Database Engine</label>
                            <select class="form-select connection-form-input" id="engine" required>
                                <option value="">Select Engine</option>
                                <option value="mysql">MySQL</option>
                                <option value="postgresql">PostgreSQL</option>
                                <option value="sqlserver">SQL Server</option>
                            </select>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Database Name</label>
                            <input type="text" class="form-control connection-form-input" id="database" placeholder="mydatabase" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Username</label>
                            <input type="text" class="form-control connection-form-input" id="username" placeholder="root" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Password</label>
                            <input type="password" class="form-control connection-form-input" id="password" placeholder="password" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Service Account JSON</label>
                            <textarea class="form-control connection-form-input" id="service-account-json" rows="4" placeholder='{"type": "service_account", "project_id": "..."}' required></textarea>
                            <small class="text-muted">Service account JSON for authentication</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'firestore':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My Firestore Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Project ID</label>
                            <input type="text" class="form-control connection-form-input" id="project-id" placeholder="my-gcp-project" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Service Account JSON</label>
                            <textarea class="form-control connection-form-input" id="service-account-json" rows="6" placeholder='{"type": "service_account", "project_id": "..."}' required></textarea>
                            <small class="text-muted">Paste your Google Cloud service account JSON key</small>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Database ID</label>
                            <input type="text" class="form-control connection-form-input" id="database-id" placeholder="(default)" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Collection (Optional)</label>
                            <input type="text" class="form-control connection-form-input" id="collection" placeholder="users">
                            <small class="text-muted">Leave empty to scan all collections</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'storage':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My GCS Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Bucket Name</label>
                            <input type="text" class="form-control connection-form-input" id="bucket-name" placeholder="my-gcs-bucket" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Service Account JSON</label>
                            <textarea class="form-control connection-form-input" id="service-account-json" rows="6" placeholder='{"type": "service_account", "project_id": "..."}' required></textarea>
                            <small class="text-muted">Paste your Google Cloud service account JSON key</small>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Prefix (Optional)</label>
                            <input type="text" class="form-control connection-form-input" id="prefix" placeholder="data/">
                            <small class="text-muted">Specify a folder path within the bucket</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'bigquery':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My BigQuery Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Project ID</label>
                            <input type="text" class="form-control connection-form-input" id="project-id" placeholder="my-gcp-project" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Service Account JSON</label>
                            <textarea class="form-control connection-form-input" id="service-account-json" rows="6" placeholder='{"type": "service_account", "project_id": "..."}' required></textarea>
                            <small class="text-muted">Paste your Google Cloud service account JSON key</small>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Dataset ID (Optional)</label>
                            <input type="text" class="form-control connection-form-input" id="dataset-id" placeholder="my_dataset">
                            <small class="text-muted">Leave empty to scan all datasets</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        // Salesforce Connection Types
        case 'custom':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My Salesforce Custom Objects" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Salesforce Instance URL</label>
                            <input type="url" class="form-control connection-form-input" id="instance-url" placeholder="https://mycompany.salesforce.com" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Username</label>
                            <input type="email" class="form-control connection-form-input" id="username" placeholder="user@company.com" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Password</label>
                            <input type="password" class="form-control connection-form-input" id="password" placeholder="password" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Security Token</label>
                            <input type="password" class="form-control connection-form-input" id="security-token" placeholder="security token" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Custom Objects</label>
                            <input type="text" class="form-control connection-form-input" id="custom-objects" placeholder="CustomObject1__c,CustomObject2__c" required>
                            <small class="text-muted">Comma-separated list of custom object API names</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'reports':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My Salesforce Reports" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Salesforce Instance URL</label>
                            <input type="url" class="form-control connection-form-input" id="instance-url" placeholder="https://mycompany.salesforce.com" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Username</label>
                            <input type="email" class="form-control connection-form-input" id="username" placeholder="user@company.com" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Password</label>
                            <input type="password" class="form-control connection-form-input" id="password" placeholder="password" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Security Token</label>
                            <input type="password" class="form-control connection-form-input" id="security-token" placeholder="security token" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Report Folder (Optional)</label>
                            <input type="text" class="form-control connection-form-input" id="report-folder" placeholder="Public Reports">
                            <small class="text-muted">Specific folder to scan for reports</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'objects':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My Salesforce Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Salesforce Instance URL</label>
                            <input type="url" class="form-control connection-form-input" id="instance-url" placeholder="https://mycompany.salesforce.com" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Username</label>
                            <input type="email" class="form-control connection-form-input" id="username" placeholder="user@company.com" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Password</label>
                            <input type="password" class="form-control connection-form-input" id="password" placeholder="password" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Security Token</label>
                            <input type="password" class="form-control connection-form-input" id="security-token" placeholder="security token" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Objects to Include</label>
                            <input type="text" class="form-control connection-form-input" id="objects" placeholder="Account,Contact,Opportunity" required>
                            <small class="text-muted">Comma-separated list of Salesforce objects</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        // Oracle Connection Types
        case 'autonomous':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My Autonomous Database" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Service Name</label>
                            <input type="text" class="form-control connection-form-input" id="service-name" placeholder="mydb_high" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection String</label>
                            <input type="text" class="form-control connection-form-input" id="connection-string" placeholder="(description=(retry_count=20)(retry_delay=3)...)" required>
                            <small class="text-muted">Full TNS connection string from wallet</small>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Username</label>
                            <input type="text" class="form-control connection-form-input" id="username" placeholder="ADMIN" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Password</label>
                            <input type="password" class="form-control connection-form-input" id="password" placeholder="password" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Wallet File (Optional)</label>
                            <input type="file" class="form-control connection-form-input" id="wallet-file" accept=".zip">
                            <small class="text-muted">Upload wallet.zip file for secure connection</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'oci':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My Database Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Tenancy OCID</label>
                            <input type="text" class="form-control connection-form-input" id="tenancy-ocid" placeholder="ocid1.tenancy.oc1..." required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">User OCID</label>
                            <input type="text" class="form-control connection-form-input" id="user-ocid" placeholder="ocid1.user.oc1..." required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Region</label>
                            <input type="text" class="form-control connection-form-input" id="region" placeholder="us-ashburn-1" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Fingerprint</label>
                            <input type="text" class="form-control connection-form-input" id="fingerprint" placeholder="aa:bb:cc:dd..." required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Compartment OCID</label>
                            <input type="text" class="form-control connection-form-input" id="compartment-ocid" placeholder="ocid1.compartment.oc1..." required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Private Key</label>
                            <textarea class="form-control connection-form-input" id="private-key" rows="6" placeholder="-----BEGIN PRIVATE KEY-----..." required></textarea>
                            <small class="text-muted">Paste your private key</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        // Generic fallback cases
        case 'cloud':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My Cloud Storage" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Cloud Provider</label>
                            <select class="form-select connection-form-input" id="cloud-provider" required>
                                <option value="">Select Provider</option>
                                <option value="aws">Amazon Web Services</option>
                                <option value="azure">Microsoft Azure</option>
                                <option value="gcp">Google Cloud Platform</option>
                            </select>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Access Key ID</label>
                            <input type="text" class="form-control connection-form-input" id="access-key" placeholder="AKIA..." required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Secret Access Key</label>
                            <input type="password" class="form-control connection-form-input" id="secret-key" placeholder="Secret Key" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Region</label>
                            <input type="text" class="form-control connection-form-input" id="region" placeholder="us-east-1" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Bucket/Container</label>
                            <input type="text" class="form-control connection-form-input" id="bucket" placeholder="my-bucket" required>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'api':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My API Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">API Type</label>
                            <select class="form-select connection-form-input" id="api-type" required>
                                <option value="">Select API Type</option>
                                <option value="rest">REST API</option>
                                <option value="salesforce">Salesforce</option>
                                <option value="servicenow">ServiceNow</option>
                            </select>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Base URL</label>
                            <input type="url" class="form-control connection-form-input" id="api-url" placeholder="https://api.example.com" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Authentication Type</label>
                            <select class="form-select connection-form-input" id="auth-type" required>
                                <option value="">Select Auth Type</option>
                                <option value="bearer">Bearer Token</option>
                                <option value="basic">Basic Auth</option>
                                <option value="oauth">OAuth 2.0</option>
                            </select>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">API Key/Token</label>
                            <input type="password" class="form-control connection-form-input" id="api-token" placeholder="Your API Key" required>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        // NAS Connection Types
        case 'smb':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My NAS Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">NAS Host/IP</label>
                            <input type="text" class="form-control connection-form-input" id="host" placeholder="192.168.1.100" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Port</label>
                            <input type="number" class="form-control connection-form-input" id="port" placeholder="445" value="445" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Share Name</label>
                            <input type="text" class="form-control connection-form-input" id="share-name" placeholder="shared_folder" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Username</label>
                            <input type="text" class="form-control connection-form-input" id="username" placeholder="username" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Password</label>
                            <input type="password" class="form-control connection-form-input" id="password" placeholder="password" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Domain (Optional)</label>
                            <input type="text" class="form-control connection-form-input" id="domain" placeholder="WORKGROUP">
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Max Depth</label>
                            <input type="number" class="form-control connection-form-input" id="max-depth" placeholder="5" value="5" min="1" max="10">
                            <small class="text-muted">Maximum directory scanning depth</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'nfs':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My NFS Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">NFS Server</label>
                            <input type="text" class="form-control connection-form-input" id="server" placeholder="192.168.1.100" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Export Path</label>
                            <input type="text" class="form-control connection-form-input" id="export" placeholder="/export/data" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Mount Point</label>
                            <input type="text" class="form-control connection-form-input" id="mount-point" placeholder="/mnt/nfs" required>
                            <small class="text-muted">Local mount point for the NFS share</small>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Scan Paths</label>
                            <input type="text" class="form-control connection-form-input" id="scan-paths" placeholder="/data,/backup" value="/">
                            <small class="text-muted">Comma-separated list of paths to scan within the mount</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        // SFTP Connection Types
        case 'sftp':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My SFTP Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">SFTP Host/IP</label>
                            <input type="text" class="form-control connection-form-input" id="host" placeholder="192.168.1.100" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Port</label>
                            <input type="number" class="form-control connection-form-input" id="port" placeholder="22" value="22" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Username</label>
                            <input type="text" class="form-control connection-form-input" id="username" placeholder="username" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Password</label>
                            <input type="password" class="form-control connection-form-input" id="password" placeholder="password">
                            <small class="text-muted">Either password or private key is required</small>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Private Key Path (Optional)</label>
                            <input type="text" class="form-control connection-form-input" id="private-key-path" placeholder="/path/to/private/key">
                            <small class="text-muted">Path to SSH private key file</small>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Scan Paths</label>
                            <input type="text" class="form-control connection-form-input" id="scan-paths" placeholder="/data,/backup" value="/">
                            <small class="text-muted">Comma-separated list of paths to scan on the SFTP server</small>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Max Depth</label>
                            <input type="number" class="form-control connection-form-input" id="max-depth" placeholder="5" value="5" min="1" max="10">
                            <small class="text-muted">Maximum directory scanning depth</small>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">File Extensions (Optional)</label>
                            <input type="text" class="form-control connection-form-input" id="file-extensions" placeholder=".csv,.json,.xlsx">
                            <small class="text-muted">Comma-separated list of file extensions to include</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        case 'scp':
            formHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Connection Name</label>
                            <input type="text" class="form-control connection-form-input" id="connection-name" placeholder="My SCP Connection" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">SCP Host/IP</label>
                            <input type="text" class="form-control connection-form-input" id="host" placeholder="192.168.1.100" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Port</label>
                            <input type="number" class="form-control connection-form-input" id="port" placeholder="22" value="22" required>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Username</label>
                            <input type="text" class="form-control connection-form-input" id="username" placeholder="username" required>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Password</label>
                            <input type="password" class="form-control connection-form-input" id="password" placeholder="password">
                            <small class="text-muted">Either password or private key is required</small>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Private Key Path (Optional)</label>
                            <input type="text" class="form-control connection-form-input" id="private-key-path" placeholder="/path/to/private/key">
                            <small class="text-muted">Path to SSH private key file</small>
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12">
                        <div class="connection-form-group">
                            <label class="connection-form-label">Remote Path</label>
                            <input type="text" class="form-control connection-form-input" id="remote-path" placeholder="/home/user/data" required>
                            <small class="text-muted">Remote directory path to scan</small>
                        </div>
                    </div>
                </div>
            `;
            break;
            
        default:
            formHTML = `
                <div class="alert alert-info">
                    <i class="fas fa-info-circle me-2"></i>
                    Configuration form for "${selectedConnectionType}" connection type is being prepared. 
                    Please contact your administrator for assistance with this connection type.
                </div>
            `;
    }
    
    formContainer.innerHTML = formHTML;
}

function validateConnectionForm() {
    const requiredFields = document.querySelectorAll('.connection-form-input[required]');
    const optionalFields = document.querySelectorAll('.connection-form-input:not([required])');
    let isValid = true;
    
    // Validate only required fields
    requiredFields.forEach(field => {
        if (!field.value.trim()) {
            field.classList.add('is-invalid');
            isValid = false;
        } else {
            field.classList.remove('is-invalid');
        }
    });
    
    // Remove validation errors from optional fields
    optionalFields.forEach(field => {
        field.classList.remove('is-invalid');
    });
    
    if (!isValid) {
        showNotification('Please fill in all required fields', 'warning');
    }
    
    return isValid;
}

function testConnectionWizard() {
    const testButton = document.querySelector('#connection-test-status button');
    const resultsDiv = document.getElementById('connection-test-results');
    
    // Show loading state
    testButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Testing Connection...';
    testButton.disabled = true;
    
    resultsDiv.innerHTML = `
        <div class="connection-test-loading">
            <div class="spinner-border text-primary mb-3"></div>
            <p>Testing connection to your data source...</p>
        </div>
    `;
    resultsDiv.style.display = 'block';
    
    // Get the current connector ID from the modal or connection config
    const modal = document.getElementById('connectorModal');
    let connectorId = modal ? modal.getAttribute('data-connector-id') : null;
    
    // Fallback to connection config if modal doesn't have the ID
    if (!connectorId && window.connectionConfig && window.connectionConfig.connectorId) {
        connectorId = window.connectionConfig.connectorId;
    }
    
    if (!connectorId) {
        resultsDiv.innerHTML = `
            <div class="connection-test-error">
                <i class="fas fa-exclamation-triangle fa-3x mb-3"></i>
                <h5>Configuration Error</h5>
                <p>Unable to identify the connector. Please try again.</p>
                <button class="btn btn-outline-light mt-2" onclick="testConnectionWizard()">
                    <i class="fas fa-redo me-2"></i>Retry
                </button>
            </div>
        `;
        testButton.innerHTML = '<i class="fas fa-plug me-2"></i>Test Connection';
        testButton.disabled = false;
        return;
    }
    
    // Perform actual connection test
    testActualConnection(connectorId, testButton, resultsDiv);
}

async function testActualConnection(connectorId, testButton, resultsDiv) {
    try {
        // Real connection test for GCP
        if (connectorId === 'gcp') {
            // Get form data
            const projectId = document.getElementById('project-id')?.value;
            const serviceAccountJson = document.getElementById('service-account-json')?.value;
            const credentialsPath = document.getElementById('credentials-path')?.value;
            
            if (!projectId || (!serviceAccountJson && !credentialsPath)) {
                resultsDiv.innerHTML = `
                    <div class="connection-test-error">
                        <i class="fas fa-exclamation-circle fa-3x mb-3"></i>
                        <h5>Configuration Required</h5>
                        <p>Please provide Project ID and Service Account JSON</p>
                    </div>
                `;
                testButton.innerHTML = '<i class="fas fa-exclamation-circle me-2"></i>Configuration Required';
                testButton.disabled = false;
                testButton.classList.remove('btn-primary');
                testButton.classList.add('btn-warning');
                return;
            }
            
            // Prepare config
            const config = {
                project_id: projectId,
                services: ['bigquery', 'cloud_storage']
            };
            
            if (serviceAccountJson && serviceAccountJson.trim()) {
                try {
                    JSON.parse(serviceAccountJson);
                    config.service_account_json = serviceAccountJson;
                } catch (e) {
                    resultsDiv.innerHTML = `
                        <div class="connection-test-error">
                            <i class="fas fa-exclamation-circle fa-3x mb-3"></i>
                            <h5>Invalid JSON Format</h5>
                            <p>Please check your Service Account JSON format</p>
                        </div>
                    `;
                    testButton.innerHTML = '<i class="fas fa-exclamation-circle me-2"></i>Invalid JSON';
                    testButton.disabled = false;
                    testButton.classList.remove('btn-primary');
                    testButton.classList.add('btn-warning');
                    return;
                }
            } else if (credentialsPath && credentialsPath.trim()) {
                config.credentials_path = credentialsPath;
            }
            
            // Add connector first
            const addResponse = await fetch(`/api/connectors/gcp/add`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    enabled: true,
                    config: config
                })
            });
            
            if (!addResponse.ok) {
                const error = await addResponse.json();
                resultsDiv.innerHTML = `
                    <div class="connection-test-error">
                        <i class="fas fa-exclamation-circle fa-3x mb-3"></i>
                        <h5>Failed to Add Connector</h5>
                        <p>${error.detail || 'Unknown error'}</p>
                    </div>
                `;
                testButton.innerHTML = '<i class="fas fa-exclamation-circle me-2"></i>Connection Failed';
                testButton.disabled = false;
                testButton.classList.remove('btn-primary');
                testButton.classList.add('btn-danger');
                return;
            }
            
            // Test connection
            const testResponse = await fetch(`/api/config/gcp/test`, {
                method: 'POST'
            });
            
            const testResult = await testResponse.json();
            
            if (testResult.status === 'success' && testResult.connection_status === 'connected') {
                // Discover assets
                const discoverResponse = await fetch(`/api/discovery/test/gcp`, {
                    method: 'POST'
                });
                
                const discoverResult = await discoverResponse.json();
                
                if (discoverResult.status === 'success' && discoverResult.assets && discoverResult.assets.length > 0) {
                    // Show real discovered assets
                    const assets = discoverResult.assets;
                    const assetNames = assets.map(asset => asset.name).slice(0, 4);
                    
                    resultsDiv.innerHTML = `
                        <div class="connection-test-success">
                            <i class="fas fa-check-circle fa-3x mb-3"></i>
                            <h5>Connection Successful!</h5>
                            <p>✅ BigQuery Connected! Discovered ${assets.length} assets</p>
                            <div class="mt-3">
                                <small class="text-muted">
                                    Found: ${assetNames.join(', ')}${assets.length > 4 ? ` and ${assets.length - 4} more...` : ''}
                                </small>
                            </div>
                        </div>
                    `;
                    testButton.innerHTML = '<i class="fas fa-check me-2"></i>Connection Successful';
                    testButton.disabled = false;
                    testButton.classList.remove('btn-primary');
                    testButton.classList.add('btn-success');
                } else {
                    resultsDiv.innerHTML = `
                        <div class="connection-test-success">
                            <i class="fas fa-check-circle fa-3x mb-3"></i>
                            <h5>Connection Successful!</h5>
                            <p>✅ Connected to GCP Project: ${projectId}</p>
                            <p>No assets discovered (empty project or no access)</p>
                        </div>
                    `;
                    testButton.innerHTML = '<i class="fas fa-check me-2"></i>Connected (No Assets)';
                    testButton.disabled = false;
                    testButton.classList.remove('btn-primary');
                    testButton.classList.add('btn-success');
                }
            } else {
                resultsDiv.innerHTML = `
                    <div class="connection-test-error">
                        <i class="fas fa-exclamation-circle fa-3x mb-3"></i>
                        <h5>Connection Failed</h5>
                        <p>❌ ${testResult.message || 'Unable to connect to GCP'}</p>
                        <div class="mt-3">
                            <small class="text-muted">
                                Check your Project ID and Service Account credentials
                            </small>
                        </div>
                    </div>
                `;
                testButton.innerHTML = '<i class="fas fa-exclamation-circle me-2"></i>Connection Failed';
                testButton.disabled = false;
                testButton.classList.remove('btn-primary');
                testButton.classList.add('btn-danger');
            }
            
            return;
        }
        
        // First test the connection
        const testResult = await apiCall(`/config/${connectorId}/test`, { method: 'POST' });
        
        if (testResult.status === 'success' || testResult.connection_status === 'connected') {
            // If connection is successful, discover assets
            try {
                const discoveryResult = await apiCall(`/discovery/test/${connectorId}`, { method: 'POST' });
                
                if (discoveryResult.status === 'success') {
                    const assetsCount = discoveryResult.assets_discovered || 0;
            resultsDiv.innerHTML = `
                <div class="connection-test-success">
                    <i class="fas fa-check-circle fa-3x mb-3"></i>
                    <h5>Connection Successful!</h5>
                            <p>Successfully connected and discovered <strong>${assetsCount}</strong> assets.</p>
                            ${assetsCount > 0 ? `
                                <button class="btn btn-outline-light mt-2" onclick="showAssetsModal('${connectorId}', ${JSON.stringify(discoveryResult.assets).replace(/"/g, '&quot;')})">
                                    <i class="fas fa-database me-2"></i>View Assets
                                </button>
                            ` : ''}
                </div>
            `;
                } else {
                    resultsDiv.innerHTML = `
                        <div class="connection-test-success">
                            <i class="fas fa-check-circle fa-3x mb-3"></i>
                            <h5>Connection Successful!</h5>
                            <p>Successfully connected but asset discovery failed: ${discoveryResult.error || 'Unknown error'}</p>
                        </div>
                    `;
                }
            } catch (discoveryError) {
                console.error('Asset discovery error:', discoveryError);
                resultsDiv.innerHTML = `
                    <div class="connection-test-success">
                        <i class="fas fa-check-circle fa-3x mb-3"></i>
                        <h5>Connection Successful!</h5>
                        <p>Successfully connected but asset discovery failed.</p>
                    </div>
                `;
            }
        } else {
            resultsDiv.innerHTML = `
                <div class="connection-test-error">
                    <i class="fas fa-exclamation-triangle fa-3x mb-3"></i>
                    <h5>Connection Failed</h5>
                    <p>Unable to connect: ${testResult.error || testResult.message || 'Unknown error'}</p>
                    <button class="btn btn-outline-light mt-2" onclick="testConnectionWizard()">
                        <i class="fas fa-redo me-2"></i>Retry
                    </button>
                </div>
            `;
        }
    } catch (error) {
        console.error('Connection test error:', error);
        resultsDiv.innerHTML = `
            <div class="connection-test-error">
                <i class="fas fa-exclamation-triangle fa-3x mb-3"></i>
                <h5>Connection Failed</h5>
                <p>Connection test failed: ${error.message || 'Unknown error'}</p>
                <button class="btn btn-outline-light mt-2" onclick="testConnectionWizard()">
                    <i class="fas fa-redo me-2"></i>Retry
                </button>
            </div>
        `;
    } finally {
        // Update button state
        testButton.innerHTML = '<i class="fas fa-plug me-2"></i>Test Connection';
        testButton.disabled = false;
    }
}

function loadConnectionSummary() {
    const summaryDiv = document.getElementById('connection-summary');
    
    // Collect configuration data
    const connectionName = document.getElementById('connection-name')?.value || 'Unnamed Connection';
    
    let summaryHTML = `
        <div class="connection-summary-item">
            <div class="connection-summary-label">Connection Name</div>
            <div class="connection-summary-value">${connectionName}</div>
        </div>
        <div class="connection-summary-item">
            <div class="connection-summary-label">Connection Type</div>
            <div class="connection-summary-value">${selectedConnectionType}</div>
        </div>
    `;
    
    // Add type-specific summary items
    switch(selectedConnectionType) {
        case 'database':
            const dbType = document.getElementById('db-type')?.value;
            const dbHost = document.getElementById('db-host')?.value;
            const dbName = document.getElementById('db-name')?.value;
            summaryHTML += `
                <div class="connection-summary-item">
                    <div class="connection-summary-label">Database Type</div>
                    <div class="connection-summary-value">${dbType}</div>
                </div>
                <div class="connection-summary-item">
                    <div class="connection-summary-label">Host</div>
                    <div class="connection-summary-value">${dbHost}</div>
                </div>
                <div class="connection-summary-item">
                    <div class="connection-summary-label">Database</div>
                    <div class="connection-summary-value">${dbName}</div>
                </div>
            `;
            break;
        case 'cloud':
            const cloudProvider = document.getElementById('cloud-provider')?.value;
            const region = document.getElementById('region')?.value;
            summaryHTML += `
                <div class="connection-summary-item">
                    <div class="connection-summary-label">Cloud Provider</div>
                    <div class="connection-summary-value">${cloudProvider}</div>
                </div>
                <div class="connection-summary-item">
                    <div class="connection-summary-label">Region</div>
                    <div class="connection-summary-value">${region}</div>
                </div>
            `;
            break;
        case 'api':
            const apiType = document.getElementById('api-type')?.value;
            const apiUrl = document.getElementById('api-url')?.value;
            summaryHTML += `
                <div class="connection-summary-item">
                    <div class="connection-summary-label">API Type</div>
                    <div class="connection-summary-value">${apiType}</div>
                </div>
                <div class="connection-summary-item">
                    <div class="connection-summary-label">Base URL</div>
                    <div class="connection-summary-value">${apiUrl}</div>
                </div>
            `;
            break;
        case 'bigquery':
        case 'storage':
            const projectId = document.getElementById('project-id')?.value;
            const datasetId = document.getElementById('dataset-id')?.value;
            summaryHTML += `
                <div class="connection-summary-item">
                    <div class="connection-summary-label">Project ID</div>
                    <div class="connection-summary-value">${projectId}</div>
                </div>
                ${datasetId ? `
                <div class="connection-summary-item">
                    <div class="connection-summary-label">Dataset ID</div>
                    <div class="connection-summary-value">${datasetId}</div>
                </div>
                ` : ''}
                <div class="connection-summary-item">
                    <div class="connection-summary-label">Service Account</div>
                    <div class="connection-summary-value">Configured</div>
                </div>
            `;
            
            // Add discovered datasets section for GCP BigQuery
            if (connectionConfig.connectorId === 'gcp' || selectedConnectionType === 'bigquery') {
                summaryHTML += `
                    <div class="connection-summary-item mt-4">
                        <div class="connection-summary-label">
                            <i class="fas fa-database me-2"></i>Discovered Data Assets
                        </div>
                        <div class="connection-summary-value">
                            <div id="discovered-assets-container">
                                <div class="text-muted">
                                    <i class="fas fa-spinner fa-spin me-2"></i>
                                    Loading discovered assets...
                                </div>
                            </div>
                        </div>
                    </div>
                `;
                
                // Load real discovered assets
                console.log('Loading discovered assets for BigQuery connection...');
                setTimeout(() => loadDiscoveredAssets(), 100);
            }
            break;
    }
    
    summaryDiv.innerHTML = summaryHTML;
}

async function loadDiscoveredAssets() {
    console.log('loadDiscoveredAssets called');
    const container = document.getElementById('discovered-assets-container');
    if (!container) {
        console.log('discovered-assets-container not found');
        return;
    }
    console.log('Found discovered-assets-container, proceeding with discovery...');
    
    try {
        // First, add the GCP connector if not already added
        const projectId = document.getElementById('project-id')?.value;
        const serviceAccountJson = document.getElementById('service-account-json')?.value;
        
        if (projectId && serviceAccountJson) {
            // Add the connector first
            const addResponse = await fetch('/api/connectors/gcp/add', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    name: document.getElementById('connection-name')?.value || 'GCP Connection',
                    config: {
                        project_id: projectId,
                        service_account_json: serviceAccountJson,
                        services: ['bigquery']
                    }
                })
            });
            
            if (!addResponse.ok) {
                throw new Error('Failed to add GCP connector');
            }
        }
        
        // Now fetch discovered assets
        const response = await fetch('/api/discovery/test/gcp', {
            method: 'POST'
        });
        
        const result = await response.json();
        console.log('Discovery result:', result);
        
        if (result.status === 'success' && result.assets && result.assets.length > 0) {
            const assets = result.assets;
            const datasets = assets.filter(asset => asset.type === 'bigquery_dataset');
            const tables = assets.filter(asset => asset.type === 'bigquery_table');
            
            let assetsHTML = '<div class="discovered-assets-list">';
            
            // Show datasets with better formatting
            if (datasets.length > 0) {
                assetsHTML += '<div class="mb-3">';
                assetsHTML += '<h6 class="text-primary mb-2"><i class="fas fa-database me-2"></i>Datasets Found:</h6>';
                
                datasets.forEach((asset, index) => {
                    const badgeColors = ['primary', 'warning', 'info', 'success'];
                    const badgeColor = badgeColors[index % badgeColors.length];
                    const isPII = asset.name.toLowerCase().includes('pii');
                    const isConsent = asset.name.toLowerCase().includes('consent');
                    const datasetName = asset.name.split('.').pop();
                    
                    assetsHTML += `
                        <div class="asset-item mb-2 p-2 border rounded">
                            <div class="d-flex align-items-center">
                                <i class="fas fa-database text-${badgeColor} me-2"></i>
                                <div class="flex-grow-1">
                                    <strong>${datasetName}</strong>
                                    <div class="small text-muted">${asset.name}</div>
                                </div>
                                <span class="badge bg-${badgeColor}">
                                    Dataset${isPII ? ' (PII)' : isConsent ? ' (Consent)' : ''}
                                </span>
                            </div>
                        </div>
                    `;
                });
                
                assetsHTML += '</div>';
            }
            
            // Show tables with better formatting
            if (tables.length > 0) {
                assetsHTML += '<div class="mb-3">';
                assetsHTML += '<h6 class="text-secondary mb-2"><i class="fas fa-table me-2"></i>Tables Found:</h6>';
                
                tables.slice(0, 5).forEach((asset, index) => {
                    const tableName = asset.name.split('.').pop();
                    const rowCount = asset.schema?.num_rows || 0;
                    
                    assetsHTML += `
                        <div class="asset-item mb-1 p-2 border rounded bg-light">
                            <div class="d-flex align-items-center">
                                <i class="fas fa-table text-secondary me-2"></i>
                                <div class="flex-grow-1">
                                    <strong>${tableName}</strong>
                                    <div class="small text-muted">${asset.name}</div>
                                </div>
                                <span class="badge bg-secondary">
                                    ${rowCount.toLocaleString()} rows
                                </span>
                            </div>
                        </div>
                    `;
                });
                
                if (tables.length > 5) {
                    assetsHTML += `<div class="text-muted small">... and ${tables.length - 5} more tables</div>`;
                }
                
                assetsHTML += '</div>';
            }
            
            assetsHTML += '</div>';
            
            // Summary statistics
            assetsHTML += `
                <div class="mt-3 p-3 bg-success bg-opacity-10 rounded">
                    <div class="d-flex align-items-center">
                        <i class="fas fa-check-circle text-success me-2"></i>
                        <div>
                            <strong>Discovery Successful!</strong>
                            <div class="small">
                                Found ${assets.length} total assets: ${datasets.length} datasets, ${tables.length} tables
                            </div>
                        </div>
                    </div>
                </div>
            `;
            
            container.innerHTML = assetsHTML;
        } else if (result.status === 'success' && result.assets_discovered === 0) {
            container.innerHTML = `
                <div class="text-warning p-3 border rounded">
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    <strong>No Assets Found</strong>
                    <div class="small">Connected successfully, but no BigQuery datasets or tables found in this project.</div>
                </div>
            `;
        } else {
            container.innerHTML = `
                <div class="text-danger p-3 border rounded">
                    <i class="fas fa-exclamation-circle me-2"></i>
                    <strong>Discovery Failed</strong>
                    <div class="small">${result.message || 'Unknown error occurred during asset discovery'}</div>
                </div>
            `;
        }
    } catch (error) {
        console.error('Error loading discovered assets:', error);
        container.innerHTML = `
            <div class="text-danger p-3 border rounded">
                <i class="fas fa-exclamation-circle me-2"></i>
                <strong>Error Loading Assets</strong>
                <div class="small">${error.message}</div>
            </div>
        `;
    }
}

async function saveWizardConnection() {
    try {
        // Get the connector ID from the connection config
        let connectorId = connectionConfig.connectorId;
        if (!connectorId) {
            showNotification('No connector selected', 'danger');
            return;
        }
        
        // Map frontend connector IDs to backend connector IDs
        const connectorIdMapping = {
            'bigquery': 'gcp',
            'storage': 'gcp',
            's3': 'aws',
            'blob': 'azure',
            'warehouse': 'data_warehouses',
            'workspace': 'data_warehouses',
            'datasets': 'bigquery',
            'smb': 'nas',
            'nfs': 'nas',
            'sftp': 'sftp',
            'scp': 'sftp'
        };
        
        // Use mapped connector ID if available, otherwise use original
        connectorId = connectorIdMapping[connectorId] || connectorId;

        // Collect configuration based on connection type
        let config = { enabled: true };
        
        switch(selectedConnectionType) {
            case 'bigquery':
                const projectId = document.getElementById('project-id')?.value;
                const serviceAccountJson = document.getElementById('service-account-json')?.value;
                const datasetId = document.getElementById('dataset-id')?.value;
                
                if (!projectId || !serviceAccountJson) {
                    showNotification('Please fill in Project ID and Service Account JSON', 'warning');
                    return;
                }
                
                config.config = {
                    project_id: projectId,
                    service_account_json: serviceAccountJson,
                    dataset_id: datasetId || null,
                    services: ['bigquery']
                };
                break;
                
            case 'storage':
                // Handle GCP BigQuery through storage connection type
                const gcpProjectId = document.getElementById('project-id')?.value;
                const gcpServiceAccountJson = document.getElementById('service-account-json')?.value;
                const gcpDatasetId = document.getElementById('dataset-id')?.value;
                
                if (!gcpProjectId || !gcpServiceAccountJson) {
                    showNotification('Please fill in Project ID and Service Account JSON', 'warning');
                    return;
                }
                
                config.config = {
                    project_id: gcpProjectId,
                    service_account_json: gcpServiceAccountJson,
                    dataset_id: gcpDatasetId || null,
                    services: ['bigquery', 'cloud_storage']
                };
                break;
                
            case 'cloud':
                const cloudProvider = document.getElementById('cloud-provider')?.value;
                const accessKey = document.getElementById('access-key')?.value;
                const secretKey = document.getElementById('secret-key')?.value;
                const region = document.getElementById('region')?.value;
                
                config.config = {
                    cloud_provider: cloudProvider,
                    access_key: accessKey,
                    secret_key: secretKey,
                    region: region
                };
                break;
                
            case 'database':
                const dbType = document.getElementById('db-type')?.value;
                const dbHost = document.getElementById('db-host')?.value;
                const dbPort = document.getElementById('db-port')?.value;
                const dbName = document.getElementById('db-name')?.value;
                const dbUser = document.getElementById('db-user')?.value;
                const dbPassword = document.getElementById('db-password')?.value;
                
                config.config = {
                    database_type: dbType,
                    host: dbHost,
                    port: parseInt(dbPort),
                    database: dbName,
                    username: dbUser,
                    password: dbPassword
                };
                break;
                
            // NAS Connection Types
            case 'smb':
                const nasHost = document.getElementById('host')?.value;
                const nasPort = document.getElementById('port')?.value;
                const nasUsername = document.getElementById('username')?.value;
                const nasPassword = document.getElementById('password')?.value;
                const nasShareName = document.getElementById('share-name')?.value;
                const nasDomain = document.getElementById('domain')?.value;
                const nasMaxDepth = document.getElementById('max-depth')?.value;
                
                if (!nasHost || !nasUsername || !nasPassword || !nasShareName) {
                    showNotification('Please fill in all required NAS fields', 'warning');
                    return;
                }
                
                config.config = {
                    host: nasHost,
                    port: parseInt(nasPort) || 445,
                    username: nasUsername,
                    password: nasPassword,
                    share_name: nasShareName,
                    domain: nasDomain || '',
                    max_depth: parseInt(nasMaxDepth) || 5
                };
                break;
                
            case 'nfs':
                const nfsServer = document.getElementById('server')?.value;
                const nfsExport = document.getElementById('export')?.value;
                const nfsMountPoint = document.getElementById('mount-point')?.value;
                const nfsScanPaths = document.getElementById('scan-paths')?.value;
                
                if (!nfsServer || !nfsExport || !nfsMountPoint) {
                    showNotification('Please fill in all required NFS fields', 'warning');
                    return;
                }
                
                config.config = {
                    server: nfsServer,
                    export: nfsExport,
                    mount_point: nfsMountPoint,
                    scan_paths: nfsScanPaths ? nfsScanPaths.split(',').map(p => p.trim()) : ['/']
                };
                break;
                
            // SFTP Connection Types
            case 'sftp':
                const sftpHost = document.getElementById('host')?.value;
                const sftpPort = document.getElementById('port')?.value;
                const sftpUsername = document.getElementById('username')?.value;
                const sftpPassword = document.getElementById('password')?.value;
                const sftpPrivateKeyPath = document.getElementById('private-key-path')?.value;
                const sftpScanPaths = document.getElementById('scan-paths')?.value;
                const sftpMaxDepth = document.getElementById('max-depth')?.value;
                const sftpFileExtensions = document.getElementById('file-extensions')?.value;
                
                if (!sftpHost || !sftpUsername || (!sftpPassword && !sftpPrivateKeyPath)) {
                    showNotification('Please fill in all required SFTP fields', 'warning');
                    return;
                }
                
                config.config = {
                    host: sftpHost,
                    port: parseInt(sftpPort) || 22,
                    username: sftpUsername,
                    password: sftpPassword || '',
                    private_key_path: sftpPrivateKeyPath || null,
                    scan_paths: sftpScanPaths ? sftpScanPaths.split(',').map(p => p.trim()) : ['/'],
                    max_depth: parseInt(sftpMaxDepth) || 5,
                    file_extensions: sftpFileExtensions ? sftpFileExtensions.split(',').map(e => e.trim()) : []
                };
                break;
                
            case 'scp':
                const scpHost = document.getElementById('host')?.value;
                const scpPort = document.getElementById('port')?.value;
                const scpUsername = document.getElementById('username')?.value;
                const scpPassword = document.getElementById('password')?.value;
                const scpPrivateKeyPath = document.getElementById('private-key-path')?.value;
                const scpRemotePath = document.getElementById('remote-path')?.value;
                
                if (!scpHost || !scpUsername || (!scpPassword && !scpPrivateKeyPath) || !scpRemotePath) {
                    showNotification('Please fill in all required SCP fields', 'warning');
                    return;
                }
                
                config.config = {
                    host: scpHost,
                    port: parseInt(scpPort) || 22,
                    username: scpUsername,
                    password: scpPassword || '',
                    private_key_path: scpPrivateKeyPath || null,
                    remote_path: scpRemotePath
                };
                break;
                
            default:
                showNotification('Unsupported connection type', 'warning');
                return;
        }

        // Save the configuration
        const result = await apiCall(`/config/${connectorId}`, {
            method: 'POST',
            body: JSON.stringify(config)
        });

        if (result.status === 'success') {
            showNotification('Connection configuration saved successfully', 'success');
            
            // Close the modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('connectorModal'));
            if (modal) {
                modal.hide();
            }
            
            // Refresh the connectors list and user connections
            loadConnectors();
            refreshUserConnections();
        } else {
            showNotification('Failed to save configuration: ' + (result.error || 'Unknown error'), 'danger');
        }
        
    } catch (error) {
        console.error('Save wizard connection error:', error);
        showNotification('Failed to save configuration: ' + error.message, 'danger');
    }
}

// New Connection Functions
function openNewConnectionWizard() {
    // Reset wizard state
    currentWizardStep = 1;
    selectedConnectionType = null;
    connectionConfig = {};
    
    // Clear the step-1 content first to remove default HTML
    const step1Content = document.getElementById('step-1');
    step1Content.innerHTML = '<div class="text-center"><div class="spinner-border text-primary" role="status"></div></div>';
    
    // Open the modal
    const modal = new bootstrap.Modal(document.getElementById('connectorModal'));
    modal.show();
    
    // Update modal title
    document.querySelector('#connectorModal .modal-title').textContent = 'Configure New Connection';
    
    // Load generic connection types
    setTimeout(() => {
        const types = [
            { type: 'database', icon: 'fas fa-database', title: 'Database', description: 'Connect to SQL databases like PostgreSQL, MySQL, Oracle' },
            { type: 'cloud', icon: 'fas fa-cloud', title: 'Cloud Storage', description: 'Connect to cloud storage services' },
            { type: 'api', icon: 'fas fa-exchange-alt', title: 'API / SaaS', description: 'Connect to REST APIs and SaaS platforms' }
        ];
        
        let connectionTypesHTML = `
            <h5 class="mb-4">Select Connection Type</h5>
            <div class="row">
        `;
        
        types.forEach(type => {
            connectionTypesHTML += `
                <div class="col-md-4 mb-3">
                    <div class="connection-type-card" data-type="${type.type}">
                        <div class="card h-100 border-2">
                            <div class="card-body text-center">
                                <div class="connection-type-icon mb-3">
                                    <i class="${type.icon} fa-3x text-primary"></i>
                                </div>
                                <h6>${type.title}</h6>
                                <p class="text-muted small">${type.description}</p>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        });
        
        connectionTypesHTML += `</div>`;
        step1Content.innerHTML = connectionTypesHTML;
        
        // Re-attach click handlers
        setTimeout(() => {
            document.querySelectorAll('.connection-type-card').forEach(card => {
                card.addEventListener('click', function() {
                    selectConnectionType(this.dataset.type);
                });
            });
        }, 100);
    }, 100);
    
    // Reset wizard to first step
    updateWizardStep();
}

function openConnectionWizard(connectorId, connectorName) {
    // Pre-populate wizard with existing connector info
    currentWizardStep = 1;
    selectedConnectionType = getConnectionTypeFromId(connectorId);
    connectionConfig = { connectorId, connectorName };
    
    // Clear the step-1 content first to remove default HTML
    const step1Content = document.getElementById('step-1');
    step1Content.innerHTML = '<div class="text-center"><div class="spinner-border text-primary" role="status"></div></div>';
    
    // Open the modal
    const modalElement = document.getElementById('connectorModal');
    modalElement.setAttribute('data-connector-id', connectorId);
    const modal = new bootstrap.Modal(modalElement);
    modal.show();
    
    // Update modal title
    document.querySelector('#connectorModal .modal-title').textContent = `Configure ${connectorName}`;
    
    // Load connector-specific connection types
    setTimeout(() => {
        loadConnectorSpecificTypes(connectorId, connectorName);
    }, 100);
    
    // Reset wizard to first step
    updateWizardStep();
    
    // If we know the connection type, auto-select it
    if (selectedConnectionType) {
        setTimeout(() => {
            const typeCard = document.querySelector(`[data-type="${selectedConnectionType}"]`);
            if (typeCard) {
                typeCard.classList.add('selected');
            }
        }, 500);
    }
}

function loadConnectorSpecificTypes(connectorId, connectorName) {
    const step1Content = document.getElementById('step-1');
    let connectionTypesHTML = '';
    
    // Define connector-specific connection types
    const connectorTypes = {
        'aws': [
            { type: 's3', icon: 'fas fa-archive', title: 'S3 Storage', description: 'Connect to Amazon S3 buckets and objects' },
            { type: 'rds', icon: 'fas fa-database', title: 'RDS Database', description: 'Connect to Amazon RDS instances (MySQL, PostgreSQL, etc.)' },
            { type: 'redshift', icon: 'fas fa-chart-bar', title: 'Redshift', description: 'Connect to Amazon Redshift data warehouse' },
            { type: 'dynamodb', icon: 'fas fa-table', title: 'DynamoDB', description: 'Connect to Amazon DynamoDB NoSQL database' }
        ],
        'azure': [
            { type: 'blob', icon: 'fas fa-archive', title: 'Blob Storage', description: 'Connect to Azure Blob Storage containers' },
            { type: 'sql', icon: 'fas fa-database', title: 'Azure SQL', description: 'Connect to Azure SQL Database' },
            { type: 'synapse', icon: 'fas fa-chart-bar', title: 'Synapse', description: 'Connect to Azure Synapse Analytics' },
            { type: 'cosmos', icon: 'fas fa-globe', title: 'Cosmos DB', description: 'Connect to Azure Cosmos DB' }
        ],
        'gcp': [
            { type: 'storage', icon: 'fas fa-archive', title: 'Cloud Storage', description: 'Connect to Google Cloud Storage buckets' },
            { type: 'bigquery', icon: 'fas fa-chart-bar', title: 'BigQuery', description: 'Connect to Google BigQuery data warehouse' },
            { type: 'sql', icon: 'fas fa-database', title: 'Cloud SQL', description: 'Connect to Google Cloud SQL instances' },
            { type: 'firestore', icon: 'fas fa-fire', title: 'Firestore', description: 'Connect to Google Firestore NoSQL database' }
        ],
        'salesforce': [
            { type: 'objects', icon: 'fas fa-cube', title: 'Standard Objects', description: 'Connect to Salesforce standard objects (Account, Contact, etc.)' },
            { type: 'custom', icon: 'fas fa-cogs', title: 'Custom Objects', description: 'Connect to custom Salesforce objects' },
            { type: 'reports', icon: 'fas fa-chart-line', title: 'Reports & Dashboards', description: 'Connect to Salesforce reports and dashboards' }
        ],
        // Database Connectors
        'postgresql': [
            { type: 'database', icon: 'fas fa-database', title: 'PostgreSQL Database', description: 'Connect to PostgreSQL database instances' },
            { type: 'schema', icon: 'fas fa-sitemap', title: 'Schema Discovery', description: 'Discover database schemas and tables' },
            { type: 'replication', icon: 'fas fa-copy', title: 'Replication Slots', description: 'Connect to PostgreSQL replication slots' }
        ],
        'mysql': [
            { type: 'database', icon: 'fas fa-database', title: 'MySQL Database', description: 'Connect to MySQL database instances' },
            { type: 'schema', icon: 'fas fa-sitemap', title: 'Schema Discovery', description: 'Discover database schemas and tables' },
            { type: 'binlog', icon: 'fas fa-stream', title: 'Binary Logs', description: 'Connect to MySQL binary logs' }
        ],
        'mongodb': [
            { type: 'database', icon: 'fas fa-leaf', title: 'MongoDB Database', description: 'Connect to MongoDB database instances' },
            { type: 'collections', icon: 'fas fa-layer-group', title: 'Collections', description: 'Discover MongoDB collections and documents' },
            { type: 'replica_set', icon: 'fas fa-server', title: 'Replica Set', description: 'Connect to MongoDB replica sets' }
        ],
        'oracle': [
            { type: 'database', icon: 'fas fa-database', title: 'Oracle Database', description: 'Connect to Oracle Database instances' },
            { type: 'autonomous', icon: 'fas fa-robot', title: 'Autonomous Database', description: 'Connect to Oracle Autonomous Database' },
        ],
        'sqlserver': [
            { type: 'database', icon: 'fas fa-database', title: 'SQL Server Database', description: 'Connect to SQL Server database instances' },
            { type: 'schema', icon: 'fas fa-sitemap', title: 'Schema Discovery', description: 'Discover database schemas and tables' },
            { type: 'always_on', icon: 'fas fa-shield-alt', title: 'Always On', description: 'Connect to SQL Server Always On availability groups' }
        ],
        'cassandra': [
            { type: 'keyspace', icon: 'fas fa-key', title: 'Keyspaces', description: 'Connect to Cassandra keyspaces and tables' },
            { type: 'cluster', icon: 'fas fa-server', title: 'Cluster', description: 'Connect to Cassandra cluster nodes' },
            { type: 'materialized_views', icon: 'fas fa-eye', title: 'Materialized Views', description: 'Discover Cassandra materialized views' }
        ],
        'neo4j': [
            { type: 'graph', icon: 'fas fa-project-diagram', title: 'Graph Database', description: 'Connect to Neo4j graph database' },
            { type: 'nodes', icon: 'fas fa-circle', title: 'Nodes & Relationships', description: 'Discover graph nodes and relationships' },
            { type: 'cypher', icon: 'fas fa-code', title: 'Cypher Queries', description: 'Execute custom Cypher queries' }
        ],
        'redis': [
            { type: 'cache', icon: 'fas fa-memory', title: 'Cache Database', description: 'Connect to Redis cache instances' },
            { type: 'streams', icon: 'fas fa-stream', title: 'Redis Streams', description: 'Connect to Redis streams' },
            { type: 'cluster', icon: 'fas fa-server', title: 'Redis Cluster', description: 'Connect to Redis cluster nodes' }
        ],
        'elasticsearch': [
            { type: 'indices', icon: 'fas fa-search', title: 'Elasticsearch Indices', description: 'Connect to Elasticsearch indices and documents' },
            { type: 'cluster', icon: 'fas fa-server', title: 'Cluster', description: 'Connect to Elasticsearch cluster' },
            { type: 'kibana', icon: 'fas fa-chart-bar', title: 'Kibana Dashboards', description: 'Discover Kibana dashboards and visualizations' }
        ],
        // Data Warehouse Connectors
        'snowflake': [
            { type: 'warehouse', icon: 'fas fa-snowflake', title: 'Data Warehouse', description: 'Connect to Snowflake data warehouse' },
            { type: 'databases', icon: 'fas fa-database', title: 'Databases & Schemas', description: 'Discover Snowflake databases and schemas' },
            { type: 'stages', icon: 'fas fa-layer-group', title: 'Stages', description: 'Connect to Snowflake internal and external stages' }
        ],
        'databricks': [
            { type: 'workspace', icon: 'fas fa-laptop-code', title: 'Databricks Workspace', description: 'Connect to Databricks workspace and notebooks' },
            { type: 'delta_tables', icon: 'fas fa-table', title: 'Delta Tables', description: 'Discover Delta Lake tables' },
            { type: 'clusters', icon: 'fas fa-server', title: 'Compute Clusters', description: 'Connect to Databricks compute clusters' }
        ],
        'bigquery': [
            { type: 'datasets', icon: 'fas fa-chart-bar', title: 'BigQuery Datasets', description: 'Connect to Google BigQuery datasets and tables' },
            { type: 'views', icon: 'fas fa-eye', title: 'Views & Functions', description: 'Discover BigQuery views and user-defined functions' },
            { type: 'ml_models', icon: 'fas fa-brain', title: 'ML Models', description: 'Connect to BigQuery ML models' }
        ],
        'teradata': [
            { type: 'database', icon: 'fas fa-database', title: 'Teradata Database', description: 'Connect to Teradata database instances' },
            { type: 'tables', icon: 'fas fa-table', title: 'Tables & Views', description: 'Discover Teradata tables and views' },
            { type: 'stored_procedures', icon: 'fas fa-code', title: 'Stored Procedures', description: 'Connect to Teradata stored procedures' }
        ],
        'redshift': [
            { type: 'cluster', icon: 'fas fa-server', title: 'Redshift Cluster', description: 'Connect to Amazon Redshift cluster' },
            { type: 'schemas', icon: 'fas fa-sitemap', title: 'Schemas & Tables', description: 'Discover Redshift schemas and tables' },
            { type: 'spectrum', icon: 'fas fa-satellite', title: 'Redshift Spectrum', description: 'Connect to external tables via Spectrum' }
        ],
        'clickhouse': [
            { type: 'database', icon: 'fas fa-database', title: 'ClickHouse Database', description: 'Connect to ClickHouse database instances' },
            { type: 'tables', icon: 'fas fa-table', title: 'Tables & Views', description: 'Discover ClickHouse tables and materialized views' },
            { type: 'cluster', icon: 'fas fa-server', title: 'Cluster', description: 'Connect to ClickHouse cluster nodes' }
        ],
        // SaaS Platform Connectors
        'servicenow': [
            { type: 'tables', icon: 'fas fa-table', title: 'ServiceNow Tables', description: 'Connect to ServiceNow system tables and records' },
            { type: 'incidents', icon: 'fas fa-exclamation-triangle', title: 'Incident Management', description: 'Discover incident and change management data' },
            { type: 'cmdb', icon: 'fas fa-sitemap', title: 'CMDB', description: 'Connect to Configuration Management Database' }
        ],
        'slack': [
            { type: 'channels', icon: 'fas fa-hashtag', title: 'Channels & Messages', description: 'Connect to Slack channels and message history' },
            { type: 'users', icon: 'fas fa-users', title: 'Users & Workspaces', description: 'Discover Slack users and workspace information' },
            { type: 'files', icon: 'fas fa-file', title: 'Files & Attachments', description: 'Connect to shared files and attachments' }
        ],
        'jira': [
            { type: 'projects', icon: 'fas fa-project-diagram', title: 'Projects & Issues', description: 'Connect to Jira projects and issue tracking data' },
            { type: 'workflows', icon: 'fas fa-route', title: 'Workflows', description: 'Discover Jira workflows and status transitions' },
            { type: 'dashboards', icon: 'fas fa-chart-bar', title: 'Dashboards & Reports', description: 'Connect to Jira dashboards and reports' }
        ],
        'hubspot': [
            { type: 'contacts', icon: 'fas fa-address-book', title: 'Contacts & Companies', description: 'Connect to HubSpot CRM contacts and company data' },
            { type: 'deals', icon: 'fas fa-handshake', title: 'Deals & Pipeline', description: 'Discover sales deals and pipeline information' },
            { type: 'marketing', icon: 'fas fa-bullhorn', title: 'Marketing Assets', description: 'Connect to marketing campaigns and email data' }
        ],
        'zendesk': [
            { type: 'tickets', icon: 'fas fa-ticket-alt', title: 'Support Tickets', description: 'Connect to Zendesk support tickets and conversations' },
            { type: 'users', icon: 'fas fa-users', title: 'Users & Organizations', description: 'Discover customer users and organization data' },
            { type: 'knowledge_base', icon: 'fas fa-book', title: 'Knowledge Base', description: 'Connect to help center articles and guides' }
        ],
        'nas': [
            { type: 'smb', icon: 'fas fa-folder-open', title: 'SMB/CIFS Share', description: 'Connect to Windows SMB/CIFS network shares' },
            { type: 'nfs', icon: 'fas fa-hdd', title: 'NFS Mount', description: 'Connect to Unix/Linux NFS network file systems' }
        ],
        'sftp': [
            { type: 'sftp', icon: 'fas fa-key', title: 'SFTP Server', description: 'Connect to SFTP servers with SSH authentication' },
            { type: 'scp', icon: 'fas fa-copy', title: 'SCP Transfer', description: 'Connect using SCP for secure file transfer' }
        ]
    };
    
    // Get connection types for this connector
    const types = connectorTypes[connectorId.toLowerCase()] || [
        { type: 'database', icon: 'fas fa-database', title: 'Database', description: 'Connect to SQL databases like PostgreSQL, MySQL, Oracle' },
        { type: 'cloud', icon: 'fas fa-cloud', title: 'Cloud Storage', description: 'Connect to cloud storage services' },
        { type: 'network', icon: 'fas fa-network-wired', title: 'Network Storage', description: 'Connect to NAS drives and SFTP servers' }
    ];
    
    connectionTypesHTML = `
        <h5 class="mb-4">Select ${connectorName} Connection Type</h5>
        <div class="row">
    `;
    
    types.forEach(type => {
        connectionTypesHTML += `
            <div class="col-md-6 col-lg-4 mb-3">
                <div class="connection-type-card" data-type="${type.type}">
                    <div class="card h-100 border-2">
                        <div class="card-body text-center">
                            <div class="connection-type-icon mb-3">
                                <i class="${type.icon} fa-3x text-primary"></i>
                            </div>
                            <h6>${type.title}</h6>
                            <p class="text-muted small">${type.description}</p>
                        </div>
                    </div>
                </div>
            </div>
        `;
    });
    
    connectionTypesHTML += `
        </div>
    `;
    
    step1Content.innerHTML = connectionTypesHTML;
    
    // Re-attach click handlers
    setTimeout(() => {
        document.querySelectorAll('.connection-type-card').forEach(card => {
            card.addEventListener('click', function() {
                selectConnectionType(this.dataset.type);
            });
        });
    }, 100);
}

function getConnectionTypeFromId(connectorId) {
    // Map connector IDs to connection types
    const typeMapping = {
        // Database Connectors
        'postgresql': 'database',
        'mysql': 'database',
        'oracle': 'database',
        'sqlserver': 'database',
        'mssql': 'database',
        'mongodb': 'database',
        'cassandra': 'keyspace',
        'neo4j': 'graph',
        'redis': 'cache',
        'elasticsearch': 'indices',
        
        // Cloud Providers
        'aws': 's3',
        'azure': 'blob',
        'gcp': 'storage',
        
        // Data Warehouses
        'snowflake': 'warehouse',
        'databricks': 'workspace',
        'bigquery': 'datasets',
        'teradata': 'database',
        'redshift': 'cluster',
        'clickhouse': 'database',
        
        // SaaS Platforms
        'salesforce': 'objects',
        'servicenow': 'tables',
        'slack': 'channels',
        'jira': 'projects',
        'hubspot': 'contacts',
        'zendesk': 'tickets',
        'rest_api': 'api'
    };
    
    return typeMapping[connectorId.toLowerCase()] || 'database';
}

function viewConnectionDetails(connectorId) {
    // Show connection details in a modal or side panel
    showNotification(`Viewing details for ${connectorId}`, 'info');
    
    // You can implement a detailed view here
    // For now, we'll show a simple notification
}

// Enhanced Connection Management
function createConnectionCard(connection) {
    const statusClass = connection.status === 'connected' ? 'success' : 
                       connection.status === 'error' ? 'danger' : 'warning';
    
    return `
        <div class="col-md-6 col-lg-4 mb-3">
            <div class="card connection-management-card">
                <div class="card-body">
                    <div class="d-flex align-items-start justify-content-between mb-3">
                        <div class="connection-info">
                            <h6 class="card-title mb-1">${connection.name}</h6>
                            <small class="text-muted">${connection.type}</small>
                        </div>
                        <span class="badge bg-${statusClass}">${connection.status}</span>
                    </div>
                    
                    <div class="connection-details mb-3">
                        <small class="text-muted d-block">Host: ${connection.host || 'N/A'}</small>
                        <small class="text-muted d-block">Last Test: ${connection.lastTest || 'Never'}</small>
                    </div>
                    
                    <div class="connection-actions">
                        <button class="btn btn-outline-primary btn-sm me-1" onclick="editConnection('${connection.id}')">
                            <i class="fas fa-edit"></i> Edit
                        </button>
                        <button class="btn btn-outline-success btn-sm me-1" onclick="testConnection('${connection.id}')">
                            <i class="fas fa-plug"></i> Test
                        </button>
                        <button class="btn btn-outline-danger btn-sm" onclick="deleteConnection('${connection.id}')">
                            <i class="fas fa-trash"></i>
                        </button>
                    </div>
                </div>
            </div>
        </div>
    `;
}

async function loadUserConnections() {
    try {
        // Load user-created connections from the API
        const response = await apiCall('/connectors');
        const userConnections = [];
        
        // Extract only enabled connections from all categories
        if (response.connectors) {
            Object.values(response.connectors).forEach(category => {
                Object.entries(category).forEach(([id, connector]) => {
                    if (connector.enabled) {
                        userConnections.push({
                            id: id,
                            name: connector.name,
                            type: connector.type || 'Data Source',
                            status: 'connected',
                            services: connector.services || [],
                            configured: connector.configured || false,
                            enabled: connector.enabled || false,
                            host: connector.host || 'N/A',
                            lastTest: connector.lastTest || 'Never'
                        });
                    }
                });
            });
        }
        
        return userConnections;
    } catch (error) {
        console.error('Failed to load user connections:', error);
        return [];
    }
}

async function refreshUserConnections() {
    try {
        const container = document.getElementById('user-connections-container');
        if (!container) return; // User connections section not loaded yet
        
        const userConnections = await loadUserConnections();
        
        if (userConnections.length > 0) {
            container.innerHTML = userConnections.map(conn => createConnectionCard(conn)).join('');
        } else {
            container.innerHTML = `
                <div class="col-12">
                    <div class="text-center py-4">
                        <i class="fas fa-plus-circle fa-3x text-muted mb-3"></i>
                        <h6 class="text-muted">No connections created yet</h6>
                        <p class="text-muted">Click "New Connection" to create your first data source connection</p>
                        <button class="btn btn-primary mt-2" onclick="openNewConnectionWizard()">
                            <i class="fas fa-plus me-2"></i>New Connection
                        </button>
                    </div>
                </div>
            `;
        }
    } catch (error) {
        console.error('Failed to refresh user connections:', error);
    }
}

function addUserConnectionsSection() {
    // Add a section for user-created connections
    const connectorsTab = document.getElementById('connectors');
    
    // Check if user connections section already exists
    if (document.getElementById('user-connections-section')) {
        return;
    }
    
    const userConnectionsHTML = `
        <div id="user-connections-section" class="mt-4">
            <div class="row mb-3">
                <div class="col-12">
                    <h5><i class="fas fa-user-cog me-2"></i>My Connections</h5>
                    <p class="text-muted">Connections you've created and configured</p>
                </div>
            </div>
            <div class="row" id="user-connections-container">
                <!-- User connections will be loaded here -->
            </div>
        </div>
    `;
    
    // Insert before the accordion
    const accordion = connectorsTab.querySelector('#connectorAccordion');
    accordion.insertAdjacentHTML('beforebegin', userConnectionsHTML);
    
    // Load user connections asynchronously
    loadUserConnections().then(userConnections => {
        const container = document.getElementById('user-connections-container');
        
        if (userConnections.length > 0) {
            container.innerHTML = userConnections.map(conn => createConnectionCard(conn)).join('');
        } else {
            container.innerHTML = `
                <div class="col-12">
                    <div class="text-center py-4">
                        <i class="fas fa-plus-circle fa-3x text-muted mb-3"></i>
                        <h6 class="text-muted">No connections created yet</h6>
                        <p class="text-muted">Click "New Connection" to create your first data source connection</p>
                        <button class="btn btn-primary mt-2" onclick="openNewConnectionWizard()">
                            <i class="fas fa-plus me-2"></i>New Connection
                        </button>
                    </div>
                </div>
            `;
        }
    }).catch(error => {
        console.error('Failed to load user connections:', error);
        const container = document.getElementById('user-connections-container');
        container.innerHTML = `
            <div class="col-12">
                <div class="alert alert-warning">
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    Failed to load connections. Please refresh the page.
                </div>
            </div>
        `;
    });
}

function editConnection(connectionId) {
    // Open the wizard in edit mode
    showNotification(`Editing connection ${connectionId}`, 'info');
    // You can implement edit functionality here
}

function deleteConnection(connectionId) {
    if (confirm('Are you sure you want to delete this connection?')) {
        showNotification(`Connection ${connectionId} deleted`, 'success');
        // Implement delete functionality here
        // Reload the connections
        setTimeout(() => {
            addUserConnectionsSection();
        }, 1000);
    }
}

// Initialize connection type selection
document.addEventListener('DOMContentLoaded', function() {
    // Add click handlers for connection type cards
    setTimeout(() => {
        document.querySelectorAll('.connection-type-card').forEach(card => {
            card.addEventListener('click', function() {
                selectConnectionType(this.dataset.type);
            });
        });
        
        // Add user connections section when connectors tab is loaded
        addUserConnectionsSection();
    }, 500);
});

// Export functions for global access
window.refreshDashboard = refreshDashboard;
window.loadConnectors = loadConnectors;
window.openConnectorModal = openConnectorModal;
window.saveConnectorConfig = saveConnectorConfig;
window.testConnection = testConnection;
window.loadAssets = loadAssets;
window.searchAssets = searchAssets;
window.applyAssetFilters = applyAssetFilters;
window.showAssetDetails = showAssetDetails;
window.startFullDiscovery = startFullDiscovery;
window.showSourceSelection = showSourceSelection;
window.scanSpecificSource = scanSpecificSource;
window.startMonitoring = startMonitoring;
window.stopMonitoring = stopMonitoring;
window.nextWizardStep = nextWizardStep;
window.previousWizardStep = previousWizardStep;
window.testConnectionWizard = testConnectionWizard;
window.openNewConnectionWizard = openNewConnectionWizard;
window.openConnectionWizard = openConnectionWizard;
window.saveWizardConnection = saveWizardConnection;
window.viewConnectionDetails = viewConnectionDetails;
window.editConnection = editConnection;
window.deleteConnection = deleteConnection;
