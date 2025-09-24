let currentConnectors = {};
let currentAssets = [];
let discoveryChart = null;
let refreshInterval = null;

const API_BASE = '/api';
function ensureIconsLoaded() {
    const checkFontAwesome = () => {
        const testIcon = document.createElement('i');
        testIcon.className = 'fas fa-check';
        testIcon.style.position = 'absolute';
        testIcon.style.left = '-9999px';
        document.body.appendChild(testIcon);
        
        const computedStyle = window.getComputedStyle(testIcon, ':before');
        const fontFamily = computedStyle.getPropertyValue('font-family');
        
        document.body.removeChild(testIcon);
        
        return fontFamily.includes('Font Awesome');
    };
    
    if (!checkFontAwesome()) {
        
        const link = document.createElement('link');
        link.rel = 'stylesheet';
        link.href = 'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css';
        link.onload = () => {
            document.querySelectorAll('.fas, .far, .fab, .fa').forEach(icon => {
                icon.style.display = 'none';
                icon.offsetHeight;
                icon.style.display = '';
            });
        };
        link.onerror = () => {
        };
        
        document.head.appendChild(link);
    } else {
    }
    
    document.querySelectorAll('.fas, .far, .fab, .fa').forEach(icon => {
        if (!icon.style.fontFamily.includes('Font Awesome')) {
            icon.style.fontFamily = '"Font Awesome 6 Free"';
            icon.style.fontWeight = '900';
            icon.style.webkitFontSmoothing = 'antialiased';
            icon.style.mozOsxFontSmoothing = 'grayscale';
            icon.style.textRendering = 'auto';
            icon.style.display = 'inline-block';
            icon.style.fontStyle = 'normal';
            icon.style.fontVariant = 'normal';
            icon.style.lineHeight = '1';
        }
    });
}

document.addEventListener('DOMContentLoaded', function() {
    
    ensureIconsLoaded();
    
    addPageLoadAnimation();
    
    initializeDashboard();
    
    startAutoRefresh();
    
    setTimeout(() => loadSystemHealth(), 100);
    setTimeout(() => loadConnectors(), 200);
    setTimeout(() => loadMyConnections(), 250);
    setTimeout(() => loadAssets(), 300);
    setTimeout(() => loadRecentActivity(), 400);
    
    setupEventListeners();
    
    initializeTooltips();
    
    startDynamicDashboard();
    
    startBackgroundMonitoring();
});

function addPageLoadAnimation() {
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

function initializeTooltips() {
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

function initializeDashboard() {
    updateSystemStatus();
    initializeChart();
}

function setupEventListeners() {
    document.querySelectorAll('[data-bs-toggle="tab"]').forEach(tab => {
        tab.addEventListener('shown.bs.tab', function(event) {
            const target = event.target.getAttribute('data-bs-target');
            if (target === '#connectors') {
                loadConnectors();
            } else if (target === '#assets') {
                loadAssets();
            } else if (target === '#discovery') {
                loadDiscoveryStatus();
            } else if (target === '#lineage') {
                loadLineageAssets();
            }
        });
    });
    
    document.getElementById('asset-search').addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            searchAssets();
        }
    });
}

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
        showNotification('API Error: ' + error.message, 'danger');
        throw error;
    }
}

async function updateSystemStatus() {
    try {
        const response = await apiCall('/system/status');
        
        if (response.status === 'success') {
            const metrics = response.dashboard_metrics;
            const health = response.system_health;
            
            document.getElementById('system-status').textContent = 
                health.status === 'healthy' ? 'System Online' : 'System Issues';
            
            animateCounter('total-assets', metrics.total_assets || 0);
            animateCounter('active-connectors', metrics.active_connectors || 0);
            
            animateTextUpdate('last-scan', 
                metrics.last_scan ? formatDateTime(metrics.last_scan) : 'Never');
            animateTextUpdate('monitoring-status', 
                metrics.monitoring_enabled ? 'Active' : 'Disabled');
            
            const monitoringCard = document.querySelector('.bg-warning');
            if (metrics.monitoring_enabled) {
                monitoringCard.className = monitoringCard.className.replace('bg-warning', 'bg-success');
            } else {
                monitoringCard.className = monitoringCard.className.replace('bg-success', 'bg-warning');
            }
            
            updateDiscoveryChart(metrics);
        }
        
    } catch (error) {
    }
}

function animateCounter(elementId, targetValue) {
    const element = document.getElementById(elementId);
    if (!element) return;
    
    const currentValue = parseInt(element.textContent) || 0;
    if (currentValue === targetValue) return;
    
    element.style.transition = 'none';
    
    const increment = targetValue > currentValue ? 1 : -1;
    const duration = Math.min(Math.abs(targetValue - currentValue) * 30, 800);
    const stepTime = duration / Math.abs(targetValue - currentValue);
    
    let current = currentValue;
    const timer = setInterval(() => {
        current += increment;
        element.textContent = current;
        
        if (current === targetValue) {
            clearInterval(timer);
            element.style.textShadow = 'none';
        }
    }, stepTime);
}

function animateTextUpdate(elementId, newText) {
    const element = document.getElementById(elementId);
    if (!element) return;
    
    if (element.textContent === newText) return;
    
    element.style.transition = 'opacity 0.3s ease';
    element.style.opacity = '0.7';
    
    setTimeout(() => {
        element.textContent = newText;
        element.style.opacity = '1';
        element.style.textShadow = 'none';
    }, 150);
}

async function loadSystemHealth() {
    try {
        const health = await apiCall('/system/health');
        const healthContainer = document.getElementById('system-health');
        
        let healthHTML = '<div class="row">';
        
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
        
        const connectorHealth = health.connectors || {};
        healthHTML += `
            <div class="col-md-6 mb-3">
                <div class="d-flex align-items-center">
                    <div class="me-3">
                        <i class="fas fa-link fa-2x text-info"></i>
                    </div>
                    <div>
                        <h6 class="mb-1">Connectors</h6>
                        <small>${connectorHealth.enabled || 0} of ${connectorHealth.total || 0} enabled</small>
                    </div>
                </div>
            </div>
        `;
        
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
        
        healthHTML += `
            <div class="col-md-6 mb-3">
                <div class="d-flex align-items-center">
                    <div class="me-3">
                        <i class="fas fa-stopwatch fa-2x text-warning"></i>
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

async function loadConnectors() {
    try {
        const response = await apiCall('/connectors');
        const healthResponse = await apiCall('/system/health');
        currentConnectors = response.connectors;
        const availableConnectors = response.available_connectors;
        const connectorStatus = healthResponse.connector_status || {};
        
        document.getElementById('total-connectors-badge').textContent = 
            `${response.total_connectors} (${response.enabled_connectors} enabled)`;
        
        renderConnectorCategory('cloud-connectors', currentConnectors.cloud_providers, 'cloud', availableConnectors, connectorStatus);
        renderConnectorCategory('database-connectors', currentConnectors.databases, 'database', availableConnectors, connectorStatus);
        renderConnectorCategory('warehouse-connectors', currentConnectors.data_warehouses, 'warehouse', availableConnectors, connectorStatus);
        renderConnectorCategory('network-storage-connectors', currentConnectors.network_storage, 'network_storage', availableConnectors, connectorStatus);
        
        loadMyConnections();
        
    } catch (error) {
    }
}

async function loadMyConnections() {
    try {
        const response = await apiCall('/system/health');
        const connectorStatus = response.connector_status || {};
        
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
        renderMyConnections([]);
    }
}

function getConnectorDisplayName(connectorId) {
    const displayNames = {
        'gcp': 'Google Cloud Platform',
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
                            <button class="btn btn-outline-primary btn-sm" onclick="testConnectorConnection('${connection.id}')">
                                <i class="fas fa-play me-1"></i> Test
                            </button>
                            <button class="btn btn-outline-secondary btn-sm" onclick="viewConnectorConnectionDetails('${connection.id}')">
                                <i class="fas fa-eye me-1"></i> View
                            </button>
                            <button class="btn btn-outline-danger btn-sm" onclick="deleteConnectorConnection('${connection.id}')">
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

async function testConnectorConnection(connectorId) {
    try {
        showNotification('Testing connection...', 'info');
        
        const response = await apiCall(`/config/${connectorId}/test`, {
            method: 'POST'
        });
        
        if (response) {
            showNotification('Connection test completed', 'success');
            loadMyConnections();
            
            await discoverAssetsForConnection(connectorId, `${connectorId} Connection`);
        } else {
            showNotification('Connection test failed', 'danger');
        }
        
    } catch (error) {
        showNotification('Connection test failed: ' + error.message, 'danger');
    }
}

async function viewConnectorConnectionDetails(connectorId) {
    try {
        const response = await apiCall(`/config/${connectorId}`);
        
        if (response && response.config) {
            const configDetails = JSON.stringify(response.config, null, 2);
            alert(`Connection Details for ${connectorId}:\n\n${configDetails}`);
        } else {
            showNotification('No configuration found for this connection', 'warning');
        }
        
    } catch (error) {
        showNotification('Failed to load connection details', 'danger');
    }
}

async function deleteConnectorConnection(connectorId) {
    if (!confirm(`Are you sure you want to remove the ${connectorId} connection?`)) {
        return;
    }
    
    try {
        const response = await apiCall(`/connectors/${connectorId}`, {
            method: 'DELETE'
        });
        
        if (response) {
            showNotification('Connection removed successfully', 'success');
            loadMyConnections();
            loadConnectors();
        } else {
            showNotification('Failed to remove connection', 'danger');
        }
        
    } catch (error) {
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
    
    let connectorList = Array.isArray(connectors) ? connectors : Object.keys(connectors);
    
    if (containerId === 'database-connectors') {
        connectorList = connectorList.filter(id => id !== 'databases');
    }
    if (containerId === 'warehouse-connectors') {
        connectorList = connectorList.filter(id => id !== 'data_warehouses');
    }
    
    connectorList.forEach(connectorId => {
        const connector = availableConnectors[connectorId];
        if (!connector) {
            return;
        }
        
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
                                <button class="btn btn-outline-success btn-sm" onclick="testConnectorConnection('${connectorId}')" data-bs-toggle="tooltip" title="Test Connection">
                                    <i class="fas fa-link"></i>
                                </button>
                                <button class="btn btn-outline-info btn-sm" onclick="viewConnectorConnectionDetails('${connectorId}')" data-bs-toggle="tooltip" title="View Details">
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
        
        if (connectorId === 'gcp') {
            modalBody += `
                <div class="mb-3">
                    <label for="project-id" class="form-label">Project ID *</label>
                    <input type="text" class="form-control" id="project-id" value="${config.config?.project_id || ''}" placeholder="Enter project ID" required>
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
        } else if (connectorId === 'postgresql') {
            modalBody += `
                <div class="mb-3">
                    <label for="host" class="form-label">Host</label>
                    <input type="text" class="form-control" id="host" value="${config.config?.host || ''}" placeholder="Enter hostname" required>
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
                    <input type="text" class="form-control" id="host" value="${config.config?.host || ''}" placeholder="Enter hostname" required>
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
                    <input type="text" class="form-control" id="host" value="${config.config?.host || ''}" placeholder="Enter hostname" required>
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
                    <input type="text" class="form-control" id="host" value="${config.config?.host || ''}" placeholder="Enter hostname" required>
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
                    <input type="text" class="form-control" id="host" value="${config.config?.host || ''}" placeholder="Enter hostname" required>
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
                    <input type="text" class="form-control" id="host" value="${config.config?.host || ''}" placeholder="Enter hostname" required>
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
                    <input type="text" class="form-control" id="hosts" value="${config.config?.hosts || ''}" placeholder="host1:9200,host2:9200" required>
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
        } else if (connectorId === 'snowflake') {
            modalBody += `
                <div class="mb-3">
                    <label for="account" class="form-label">Account</label>
                    <input type="text" class="form-control" id="account" value="${config.config?.account || ''}" placeholder="Enter Snowflake account" required>
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
                    <input type="text" class="form-control" id="server-hostname" value="${config.config?.server_hostname || ''}" placeholder="Enter Databricks workspace" required>
                </div>
                <div class="mb-3">
                    <label for="http-path" class="form-label">HTTP Path</label>
                    <input type="text" class="form-control" id="http-path" value="${config.config?.http_path || ''}" placeholder="/sql/1.0/warehouses/your-warehouse-id" required>
                </div>
                <div class="mb-3">
                    <label for="access-token" class="form-label">Access Token</label>
                    <input type="password" class="form-control" id="access-token" value="${config.config?.access_token || ''}" placeholder="Enter access token" required>
                </div>
            `;
        } else if (connectorId === 'bigquery') {
            modalBody += `
                <div class="mb-3">
                    <label for="project-id" class="form-label">Project ID</label>
                    <input type="text" class="form-control" id="project-id" value="${config.config?.project_id || ''}" placeholder="Enter project ID" required>
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
                    <input type="text" class="form-control" id="host" value="${config.config?.host || ''}" placeholder="Enter hostname" required>
                </div>
                <div class="mb-3">
                    <label for="port" class="form-label">Port</label>
                    <input type="number" class="form-control" id="port" value="${config.config?.port || ''}" placeholder="Enter port number" required>
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
                    <input type="password" class="form-control" id="bot-token" value="${config.config?.bot_token || ''}" placeholder="Enter bot token" required>
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
                    <input type="password" class="form-control" id="api-token" value="${config.config?.api_token || ''}" placeholder="Enter API token" required>
                    <small class="text-muted">Generate this from your Atlassian account settings</small>
                </div>
            `;
        } else if (connectorId === 'servicenow') {
            modalBody += `
                <div class="mb-3">
                    <label for="instance" class="form-label">Instance</label>
                    <input type="text" class="form-control" id="instance" value="${config.config?.instance || ''}" placeholder="Enter instance name" required>
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
                    <input type="password" class="form-control" id="api-key" value="${config.config?.api_key || ''}" placeholder="Enter HubSpot API key" required>
                    <small class="text-muted">Get this from your HubSpot account settings</small>
                </div>
            `;
        } else if (connectorId === 'zendesk') {
            modalBody += `
                <div class="mb-3">
                    <label for="subdomain" class="form-label">Subdomain</label>
                    <input type="text" class="form-control" id="subdomain" value="${config.config?.subdomain || ''}" placeholder="Enter subdomain" required>
                    <small class="text-muted">From your Zendesk URL: https://your-subdomain.zendesk.com</small>
                </div>
                <div class="mb-3">
                    <label for="email" class="form-label">Email</label>
                    <input type="email" class="form-control" id="email" value="${config.config?.email || ''}" placeholder="user@company.com" required>
                </div>
                <div class="mb-3">
                    <label for="api-token" class="form-label">API Token</label>
                    <input type="password" class="form-control" id="api-token" value="${config.config?.api_token || ''}" placeholder="Enter API token" required>
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
    
    const connectorIdMapping = {
        'bigquery': 'gcp',
        'storage': 'gcp',
        'blob': 'azure',
        'warehouse': 'data_warehouses',
        'workspace': 'data_warehouses',
        'datasets': 'bigquery'
    };
    
    connectorId = connectorIdMapping[connectorId] || connectorId;
    
    try {
        const enabled = document.getElementById('connector-enabled').checked;
        
        let config = { enabled };
        
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
            
            if (serviceAccountJson.trim()) {
                try {
                    JSON.parse(serviceAccountJson);
                    config.config.service_account_json = serviceAccountJson;
                } catch (e) {
                    showNotification('Invalid JSON format in Service Account JSON field', 'error');
                    return;
                }
            } else if (credentialsPath.trim()) {
                config.config.credentials_path = credentialsPath;
            }
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
        
        if (connectorId === 'gcp') {
            const projectId = document.getElementById('project-id').value;
            const serviceAccountJson = document.getElementById('service-account-json').value;
            const credentialsPath = document.getElementById('credentials-path').value;
            const servicesSelect = document.getElementById('services');
            const services = Array.from(servicesSelect.selectedOptions).map(option => option.value);
            
            if (!projectId.trim()) {
                showNotification('Please enter a Project ID', 'error');
                return;
            }
            
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
            
            const testResponse = await fetch(`/api/config/gcp/test`, {
                method: 'POST'
            });
            
            const testResult = await testResponse.json();
            
            if (testResult.status === 'success' && testResult.connection_status === 'connected') {
                showNotification('✅ GCP Connected! Discovering assets...', 'success');
                
                const discoverResponse = await fetch(`/api/discovery/test/gcp`, {
                    method: 'POST'
                });
                
                const discoverResult = await discoverResponse.json();
                
                if (discoverResult.status === 'success') {
                    const assetsCount = discoverResult.assets_discovered || 0;
                    showNotification(`✅ GCP Connected! Discovered ${assetsCount} assets`, 'success');
                    
                    if (discoverResult.assets && discoverResult.assets.length > 0) {
                        showAssetsModal(connectorId, discoverResult.assets);
                    }
                    
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
        
        const testResult = await apiCall(`/config/${connectorId}/test`, { method: 'POST' });
        
        if (testResult.status === 'success' || testResult.connection_status === 'connected') {
            showNotification('Connection successful! Discovering assets...', 'success');
            
            try {
                const discoveryResult = await apiCall(`/discovery/test/${connectorId}`, { method: 'POST' });
                
                if (discoveryResult.status === 'success') {
                    const assetsCount = discoveryResult.assets_discovered || 0;
                    showNotification(`Connection successful! Discovered ${assetsCount} assets`, 'success');
                    
                    if (discoveryResult.assets && discoveryResult.assets.length > 0) {
                        showAssetsModal(connectorId, discoveryResult.assets);
                    }
                    
                    if (window.location.hash === '#assets' || document.getElementById('assets-content')) {
                        loadAssets();
                    }
        } else {
                    showNotification('Connection successful, but asset discovery failed: ' + (discoveryResult.error || 'Unknown error'), 'warning');
                }
            } catch (discoveryError) {
                showNotification('Connection successful, but asset discovery failed', 'warning');
            }
        } else {
            showNotification('Connection test failed: ' + (testResult.error || testResult.message), 'danger');
        }
    } catch (error) {
        showNotification('Connection test failed: ' + (error.message || 'Unknown error'), 'danger');
    }
}

function showAssetsModal(connectorId, assets) {
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
    
    const existingModal = document.getElementById('assetsModal');
    if (existingModal) {
        existingModal.remove();
    }
    
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    
    const modal = new bootstrap.Modal(document.getElementById('assetsModal'));
    modal.show();
    
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
    
    document.querySelectorAll('#assetsModal .btn-group button').forEach(btn => {
        btn.classList.remove('active');
    });
    event.target.classList.add('active');
    
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
    const modal = bootstrap.Modal.getInstance(document.getElementById('assetsModal'));
    if (modal) {
        modal.hide();
    }
    
    showSection('assets');
    loadAssets();
}

async function loadAssets() {
    try {
        const response = await apiCall('/assets');
        
        window.currentAssetsData = response;
        
        let assetsData;
        if (response.assets) {
            assetsData = response.assets;
        } else if (response.inventory) {
            assetsData = response.inventory;
        } else if (response.assets_list) {
            assetsData = {};
            response.assets_list.forEach(asset => {
                const source = asset.source || 'unknown';
                if (!assetsData[source]) {
                    assetsData[source] = [];
                }
                assetsData[source].push(asset);
            });
        } else {
            assetsData = response;
        }
        
        currentAssets = assetsData || {};
        
        window.allAssets = [];
        Object.keys(currentAssets).forEach(source => {
            if (Array.isArray(currentAssets[source])) {
                currentAssets[source].forEach(asset => {
                    asset.source = asset.source || source;
                    asset.id = asset.id || asset.name;
                    window.allAssets.push(asset);
                });
            }
        });
        
        
        displayFilteredAssets(window.allAssets);
        
        populateFilterOptions();
        
        initializeAssetSearch();
        
        const urlParams = new URLSearchParams(window.location.search);
        const searchParam = urlParams.get('search');
        if (searchParam) {
            const searchInput = document.getElementById('asset-search');
            if (searchInput) {
                searchInput.value = searchParam;
                performSearch(searchParam);
            }
        }
        
        
    } catch (error) {
        document.getElementById('assets-table-body').innerHTML = 
            '<tr><td colspan="6" class="text-center text-danger">Failed to load assets. Please try refreshing.</td></tr>';
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
    
    typeFilter.innerHTML = '<option value="">All Asset Types</option>';
    Array.from(types).sort().forEach(type => {
        typeFilter.innerHTML += `<option value="${type}">${type}</option>`;
    });
    
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
        const modal = new bootstrap.Modal(document.getElementById('assetModal'));
        document.getElementById('asset-modal-title').textContent = 'Loading Asset Details...';
        document.getElementById('asset-overview-content').innerHTML = `
            <div class="text-center py-4">
                <div class="spinner-border text-primary" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
                <p class="mt-2 text-muted">Loading asset details...</p>
            </div>
        `;
        modal.show();
        
        const response = await apiCall(`/assets/${encodeURIComponent(assetName)}`);
        
        if (response && response.asset) {
            const asset = response.asset;
            
            document.getElementById('asset-modal-title').textContent = `Asset Details - ${asset.name}`;
            
            const overviewContent = `
                <div class="row">
                    <div class="col-md-6">
                        <h6>Basic Information</h6>
                        <table class="table table-sm">
                            <tr><td><strong>Name:</strong></td><td>${asset.name}</td></tr>
                            <tr><td><strong>Type:</strong></td><td><span class="badge bg-secondary">${asset.type}</span></td></tr>
                            <tr><td><strong>Source:</strong></td><td><span class="badge bg-info">${asset.source}</span></td></tr>
                            <tr><td><strong>Location:</strong></td><td><code>${asset.location}</code></td></tr>
                            <tr><td><strong>Size:</strong></td><td>${formatBytes(asset.size || 0)}</td></tr>
                            <tr><td><strong>Created:</strong></td><td>${formatDateTime(asset.created_date)}</td></tr>
                            <tr><td><strong>Modified:</strong></td><td>${formatDateTime(asset.modified_date)}</td></tr>
                            <tr><td><strong>Last Scanned:</strong></td><td>${formatDateTime(asset.last_scanned)}</td></tr>
                        </table>
                    </div>
                    <div class="col-md-6">
                        <h6>Tags</h6>
                        <div class="mb-3">
                            ${asset.tags && asset.tags.length > 0 ? 
                                asset.tags.map(tag => `<span class="badge bg-light text-dark me-1">${tag}</span>`).join('') : 
                                '<span class="text-muted">No tags</span>'}
                        </div>
                        <h6>Metadata</h6>
                        <pre class="bg-light p-2 rounded" style="max-height: 200px; overflow-y: auto;">${JSON.stringify(asset.metadata, null, 2)}</pre>
                    </div>
                </div>
            `;
            
            let schemaContent = '';
            if (asset.schema && Object.keys(asset.schema).length > 0) {
                if (asset.schema.fields && Array.isArray(asset.schema.fields)) {
                    schemaContent = `
                        <div class="row">
                            <div class="col-12">
                                <h6>Table Schema (${asset.schema.fields.length} columns)</h6>
                                <div class="table-responsive">
                                    <table class="table table-striped">
                                        <thead>
                                            <tr>
                                                <th>Column Name</th>
                                                <th>Data Type</th>
                                                <th>Mode</th>
                                                <th>Description</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            ${asset.schema.fields.map(field => `
                                                <tr>
                                                    <td><code>${field.name}</code></td>
                                                    <td><span class="badge bg-primary">${field.type}</span></td>
                                                    <td><span class="badge ${field.mode === 'REQUIRED' ? 'bg-danger' : 'bg-secondary'}">${field.mode || 'NULLABLE'}</span></td>
                                                    <td>${field.description || '<span class="text-muted">No description</span>'}</td>
                                                </tr>
                                            `).join('')}
                                        </tbody>
                                    </table>
                                </div>
                                ${asset.schema.num_rows ? `<p class="text-muted mt-2">Estimated rows: ${asset.schema.num_rows.toLocaleString()}</p>` : ''}
                            </div>
                        </div>
                    `;
                } else {
                    schemaContent = `
                        <div class="row">
                            <div class="col-12">
                                <h6>Schema Information</h6>
                                <pre class="bg-light p-3 rounded">${JSON.stringify(asset.schema, null, 2)}</pre>
                            </div>
                        </div>
                    `;
                }
            } else {
                schemaContent = '<div class="text-center py-4"><p class="text-muted">No schema information available</p></div>';
            }
            
            document.getElementById('asset-overview-content').innerHTML = overviewContent;
            document.getElementById('asset-schema-content').innerHTML = schemaContent;
            
            document.getElementById('asset-profiling-content').innerHTML = `
                <div class="text-center py-4">
                    <div class="spinner-border text-primary" role="status">
                        <span class="visually-hidden">Loading profiling data...</span>
                    </div>
                    <p class="mt-2 text-muted">Click to analyze data...</p>
                </div>
            `;
            
            const profilingTab = document.getElementById('asset-profiling-tab');
            profilingTab.replaceWith(profilingTab.cloneNode(true));
            document.getElementById('asset-profiling-tab').addEventListener('shown.bs.tab', function() {
                loadAssetProfiling(assetName);
            });
            
            const aiAnalysisTab = document.getElementById('asset-ai-analysis-tab');
            aiAnalysisTab.replaceWith(aiAnalysisTab.cloneNode(true));
            document.getElementById('asset-ai-analysis-tab').addEventListener('shown.bs.tab', function() {
                loadAssetAIAnalysis(assetName);
            });
            
            const piiScanTab = document.getElementById('asset-pii-scan-tab');
            piiScanTab.replaceWith(piiScanTab.cloneNode(true));
            document.getElementById('asset-pii-scan-tab').addEventListener('shown.bs.tab', function() {
                loadAssetPIIScan(assetName);
            });
            
        } else {
            document.getElementById('asset-modal-title').textContent = 'Error Loading Asset';
            document.getElementById('asset-overview-content').innerHTML = `
                <div class="alert alert-danger">
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    Failed to load asset details. ${response?.error || 'Asset not found'}
                </div>
            `;
        }
    } catch (error) {
        document.getElementById('asset-modal-title').textContent = 'Error Loading Asset';
        document.getElementById('asset-overview-content').innerHTML = `
            <div class="alert alert-danger">
                <i class="fas fa-exclamation-triangle me-2"></i>
                Failed to load asset details: ${error.message}
            </div>
        `;
        showNotification('Failed to load asset details', 'danger');
    }
}

async function loadAssetProfiling(assetName) {
    const profilingContent = document.getElementById('asset-profiling-content');
    
    profilingContent.innerHTML = `
        <div class="text-center py-4">
            <div class="spinner-border text-primary" role="status">
                <span class="visually-hidden">Analyzing data...</span>
            </div>
            <p class="mt-2 text-muted">Performing data profiling analysis...</p>
        </div>
    `;
    
    try {
        const response = await apiCall(`/assets/${encodeURIComponent(assetName)}/profiling`);
        
        if (response.status === 'success') {
            const profiling = response.profiling;
            
            let content = '<div class="row">';
            
            content += `
                <div class="col-md-4 mb-4">
                    <div class="card">
                        <div class="card-header">
                            <h6 class="mb-0"><i class="fas fa-chart-bar me-2"></i>Statistics</h6>
                        </div>
                        <div class="card-body">
                            <div class="row text-center">
                                <div class="col-12 mb-2">
                                    <h4 class="text-primary">${profiling.statistics.total_columns}</h4>
                                    <small class="text-muted">Total Columns</small>
                                </div>
                                ${profiling.statistics.estimated_rows ? `
                                <div class="col-12 mb-2">
                                    <h4 class="text-info">${profiling.statistics.estimated_rows.toLocaleString()}</h4>
                                    <small class="text-muted">Estimated Rows</small>
                                </div>
                                ` : ''}
                                <div class="col-12">
                                    <h4 class="text-secondary">${profiling.statistics.nullable_columns}</h4>
                                    <small class="text-muted">Nullable Columns</small>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            `;
            
            if (profiling.data_type_distribution && Object.keys(profiling.data_type_distribution).length > 0) {
                content += `
                    <div class="col-md-4 mb-4">
                        <div class="card">
                            <div class="card-header">
                                <h6 class="mb-0"><i class="fas fa-pie-chart me-2"></i>Data Type Distribution</h6>
                            </div>
                            <div class="card-body">
                                ${Object.entries(profiling.data_type_distribution).map(([type, info]) => `
                                    <div class="d-flex justify-content-between align-items-center mb-2">
                                        <span class="badge bg-primary">${type}</span>
                                        <div class="flex-grow-1 mx-2">
                                            <div class="progress" style="height: 8px;">
                                                <div class="progress-bar" role="progressbar" style="width: ${info.percentage}%"></div>
                                            </div>
                                        </div>
                                        <small class="text-muted">${info.count} (${info.percentage}%)</small>
                                    </div>
                                `).join('')}
                            </div>
                        </div>
                    </div>
                `;
            }
            
            content += `
                <div class="col-md-4 mb-4">
                    <div class="card">
                        <div class="card-header">
                            <h6 class="mb-0"><i class="fas fa-shield-alt me-2"></i>PII Analysis</h6>
                        </div>
                        <div class="card-body">
                            <div class="text-center mb-3">
                                <div class="alert ${profiling.pii_analysis.has_pii ? 'alert-warning' : 'alert-success'} mb-2">
                                    <i class="fas fa-${profiling.pii_analysis.has_pii ? 'exclamation-triangle' : 'check-circle'} me-2"></i>
                                    ${profiling.pii_analysis.has_pii ? 'PII Detected' : 'No PII Detected'}
                                </div>
                            </div>
                            
                            ${profiling.pii_analysis.has_pii ? `
                                <h6>PII Types Found:</h6>
                                <div class="mb-3">
                                    ${profiling.pii_analysis.pii_types.map(type => 
                                        `<span class="badge bg-warning text-dark me-1 mb-1">${type}</span>`
                                    ).join('')}
                                </div>
                                
                                <h6>Affected Columns:</h6>
                                <div class="list-group list-group-flush">
                                    ${profiling.pii_analysis.pii_columns.map(col => `
                                        <div class="list-group-item px-0">
                                            <div class="d-flex justify-content-between align-items-start">
                                                <div>
                                                    <code class="text-primary">${col.column}</code>
                                                    <small class="text-muted d-block">${col.type}</small>
                                                </div>
                                                <div>
                                                    ${col.pii_types.map(piiType => 
                                                        `<span class="badge bg-danger text-white me-1" style="font-size: 0.7em;">${piiType}</span>`
                                                    ).join('')}
                                                </div>
                                            </div>
                                        </div>
                                    `).join('')}
                                </div>
                            ` : '<p class="text-muted">No personally identifiable information detected in column names.</p>'}
                        </div>
                    </div>
                </div>
            `;
            
            content += '</div>';
            
            if (profiling.error) {
                content += `
                    <div class="alert alert-warning">
                        <i class="fas fa-exclamation-triangle me-2"></i>
                        <strong>Analysis Warning:</strong> ${profiling.error}
                    </div>
                `;
            }
            
            profilingContent.innerHTML = content;
        } else {
            profilingContent.innerHTML = `
                <div class="alert alert-danger">
                    <i class="fas fa-exclamation-circle me-2"></i>
                    <strong>Error:</strong> ${response.message || 'Failed to load profiling data'}
                </div>
            `;
        }
    } catch (error) {
        profilingContent.innerHTML = `
            <div class="alert alert-danger">
                <i class="fas fa-exclamation-circle me-2"></i>
                <strong>Error:</strong> Failed to load profiling data - ${error.message}
            </div>
        `;
    }
}

async function loadAssetAIAnalysis(assetName) {
    const aiAnalysisContent = document.getElementById('asset-ai-analysis-content');
    
    aiAnalysisContent.innerHTML = `
        <div class="text-center py-4">
            <div class="spinner-border text-primary" role="status">
                <span class="visually-hidden">Performing AI analysis...</span>
            </div>
            <p class="mt-2 text-muted">Analyzing data asset with Gemini AI...</p>
        </div>
    `;
    
    try {
        const response = await apiCall(`/assets/${encodeURIComponent(assetName)}/ai-analysis`);
        
        if (response.status === 'success') {
            const analysis = response.analysis;
            
            let content = `
                <div class="row">
                    <div class="col-md-12 mb-4">
                        <div class="card">
                            <div class="card-header bg-primary text-white">
                                <h6 class="mb-0"><i class="fas fa-brain me-2"></i>AI Analysis Results</h6>
                            </div>
                            <div class="card-body">
                                <div class="row">
                                    <div class="col-md-6 mb-3">
                                        <h6><i class="fas fa-info-circle me-2 text-info"></i>Data Summary</h6>
                                        <p class="text-muted">${analysis.summary || 'No summary available'}</p>
                                    </div>
                                    <div class="col-md-6 mb-3">
                                        <h6><i class="fas fa-lightbulb me-2 text-warning"></i>Key Insights</h6>
                                        <ul class="list-unstyled">
                                            ${analysis.insights ? analysis.insights.map(insight => `<li><i class="fas fa-arrow-right me-2 text-primary"></i>${insight}</li>`).join('') : '<li class="text-muted">No insights available</li>'}
                                        </ul>
                                    </div>
                                </div>
                                
                                <div class="row">
                                    <div class="col-md-6 mb-3">
                                        <h6><i class="fas fa-chart-line me-2 text-success"></i>Data Quality Assessment</h6>
                                        <div class="progress mb-2">
                                            <div class="progress-bar bg-success" role="progressbar" style="width: ${analysis.quality_score || 0}%">
                                                ${analysis.quality_score || 0}%
                                            </div>
                                        </div>
                                        <small class="text-muted">${analysis.quality_notes || 'Quality assessment not available'}</small>
                                    </div>
                                    <div class="col-md-6 mb-3">
                                        <h6><i class="fas fa-exclamation-triangle me-2 text-danger"></i>Potential Issues</h6>
                                        <ul class="list-unstyled">
                                            ${analysis.issues ? analysis.issues.map(issue => `<li><i class="fas fa-warning me-2 text-warning"></i>${issue}</li>`).join('') : '<li class="text-muted">No issues detected</li>'}
                                        </ul>
                                    </div>
                                </div>
                                
                                <div class="row">
                                    <div class="col-12">
                                        <h6><i class="fas fa-recommendations me-2 text-info"></i>Recommendations</h6>
                                        <div class="alert alert-info">
                                            ${analysis.recommendations || 'No specific recommendations available'}
                                        </div>
                                    </div>
                                </div>
                                
                                <div class="row">
                                    <div class="col-12">
                                        <small class="text-muted">
                                            <i class="fas fa-clock me-1"></i>Analysis completed at: ${new Date(response.timestamp).toLocaleString()}
                                            <br><i class="fas fa-robot me-1"></i>Powered by Google Gemini AI
                                        </small>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            `;
            
            aiAnalysisContent.innerHTML = content;
        } else {
            aiAnalysisContent.innerHTML = `
                <div class="alert alert-warning">
                    <i class="fas fa-exclamation-circle me-2"></i>
                    <strong>Analysis Unavailable:</strong> ${response.message || 'Failed to perform AI analysis'}
                </div>
            `;
        }
    } catch (error) {
        aiAnalysisContent.innerHTML = `
            <div class="alert alert-danger">
                <i class="fas fa-exclamation-circle me-2"></i>
                <strong>Error:</strong> Failed to perform AI analysis - ${error.message}
            </div>
        `;
    }
}

async function loadAssetPIIScan(assetName) {
    const piiScanContent = document.getElementById('asset-pii-scan-content');
    
    piiScanContent.innerHTML = `
        <div class="text-center py-4">
            <div class="spinner-border text-primary" role="status">
                <span class="visually-hidden">Scanning for PII...</span>
            </div>
            <p class="mt-2 text-muted">Scanning for personally identifiable information...</p>
        </div>
    `;
    
    try {
        const response = await apiCall(`/assets/${encodeURIComponent(assetName)}/pii-scan`);
        
        if (response.status === 'success') {
            const scan = response.scan;
            
            let content = `
                <div class="row">
                    <div class="col-md-12 mb-4">
                        <div class="card">
                            <div class="card-header bg-warning text-dark">
                                <h6 class="mb-0"><i class="fas fa-shield-alt me-2"></i>PII Scan Results</h6>
                            </div>
                            <div class="card-body">
                                <div class="row mb-4">
                                    <div class="col-md-3 text-center">
                                        <h3 class="text-danger">${scan.pii_fields_count || 0}</h3>
                                        <small class="text-muted">PII Fields Detected</small>
                                    </div>
                                    <div class="col-md-3 text-center">
                                        <h3 class="text-warning">${scan.sensitive_fields_count || 0}</h3>
                                        <small class="text-muted">Sensitive Fields</small>
                                    </div>
                                    <div class="col-md-3 text-center">
                                        <h3 class="text-info">${scan.total_fields_scanned || 0}</h3>
                                        <small class="text-muted">Total Fields Scanned</small>
                                    </div>
                                    <div class="col-md-3 text-center">
                                        <h3 class="${scan.risk_level === 'HIGH' ? 'text-danger' : scan.risk_level === 'MEDIUM' ? 'text-warning' : 'text-success'}">${scan.risk_level || 'LOW'}</h3>
                                        <small class="text-muted">Risk Level</small>
                                    </div>
                                </div>
                                
                                ${scan.pii_fields && scan.pii_fields.length > 0 ? `
                                <div class="row mb-4">
                                    <div class="col-12">
                                        <h6><i class="fas fa-user-secret me-2 text-danger"></i>Detected PII Fields</h6>
                                        <div class="table-responsive">
                                            <table class="table table-sm table-hover">
                                                <thead class="table-dark">
                                                    <tr>
                                                        <th>Field Name</th>
                                                        <th>PII Type</th>
                                                        <th>Confidence</th>
                                                        <th>Risk Level</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    ${scan.pii_fields.map(field => `
                                                        <tr>
                                                            <td><code>${field.field_name}</code></td>
                                                            <td><span class="badge bg-danger">${field.pii_type}</span></td>
                                                            <td>
                                                                <div class="progress" style="height: 20px;">
                                                                    <div class="progress-bar bg-info" style="width: ${field.confidence}%">${field.confidence}%</div>
                                                                </div>
                                                            </td>
                                                            <td><span class="badge bg-${field.risk_level === 'HIGH' ? 'danger' : field.risk_level === 'MEDIUM' ? 'warning' : 'success'}">${field.risk_level}</span></td>
                                                        </tr>
                                                    `).join('')}
                                                </tbody>
                                            </table>
                                        </div>
                                    </div>
                                </div>
                                ` : ''}
                                
                                ${scan.sensitive_fields && scan.sensitive_fields.length > 0 ? `
                                <div class="row mb-4">
                                    <div class="col-12">
                                        <h6><i class="fas fa-exclamation-triangle me-2 text-warning"></i>Sensitive Fields</h6>
                                        <div class="row">
                                            ${scan.sensitive_fields.map(field => `
                                                <div class="col-md-4 mb-2">
                                                    <div class="alert alert-warning py-2">
                                                        <small><strong>${field.field_name}</strong><br>
                                                        Type: ${field.sensitivity_type}<br>
                                                        Reason: ${field.reason}</small>
                                                    </div>
                                                </div>
                                            `).join('')}
                                        </div>
                                    </div>
                                </div>
                                ` : ''}
                                
                                <div class="row">
                                    <div class="col-12">
                                        <h6><i class="fas fa-shield-check me-2 text-success"></i>Compliance Recommendations</h6>
                                        <div class="alert alert-info">
                                            ${scan.compliance_notes || 'No specific compliance recommendations available'}
                                        </div>
                                    </div>
                                </div>
                                
                                <div class="row">
                                    <div class="col-12">
                                        <small class="text-muted">
                                            <i class="fas fa-clock me-1"></i>Scan completed at: ${new Date(response.timestamp).toLocaleString()}
                                            <br><i class="fas fa-robot me-1"></i>Powered by Google Gemini AI
                                        </small>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            `;
            
            piiScanContent.innerHTML = content;
        } else {
            piiScanContent.innerHTML = `
                <div class="alert alert-warning">
                    <i class="fas fa-exclamation-circle me-2"></i>
                    <strong>Scan Unavailable:</strong> ${response.message || 'Failed to perform PII scan'}
                </div>
            `;
        }
    } catch (error) {
        piiScanContent.innerHTML = `
            <div class="alert alert-danger">
                <i class="fas fa-exclamation-circle me-2"></i>
                <strong>Error:</strong> Failed to perform PII scan - ${error.message}
            </div>
        `;
    }
}

async function startFullDiscovery() {
    try {
        showNotification('Starting full discovery...', 'info');
        await apiCall('/discovery/scan', { method: 'POST' });
        showNotification('Discovery started successfully', 'success');
        
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
    }
}

let backgroundMonitoringInterval = null;
let monitoringEnabled = true;

function startBackgroundMonitoring() {
    if (backgroundMonitoringInterval) {
        clearInterval(backgroundMonitoringInterval);
    }
    
    backgroundMonitoringInterval = setInterval(async () => {
        if (monitoringEnabled) {
            await performBackgroundMonitoring();
        }
    }, 7000);
    
}

function stopBackgroundMonitoring() {
    if (backgroundMonitoringInterval) {
        clearInterval(backgroundMonitoringInterval);
        backgroundMonitoringInterval = null;
    }
}

async function performBackgroundMonitoring() {
    try {
        const healthResponse = await apiCall('/system/health');
        const connectorsResponse = await apiCall('/connectors');
        
        if (healthResponse.status === 'success') {
            updateSystemStatus();
        }
        
        const assetsResponse = await apiCall('/assets');
        if (assetsResponse.status === 'success' && assetsResponse.assets) {
            const currentAssetCount = assetsResponse.assets.length;
            const lastAssetCount = window.lastAssetCount || 0;
            
            if (currentAssetCount > lastAssetCount) {
                if (document.getElementById('assets-tab')?.classList.contains('active')) {
                    loadAssets();
                }
                updateSystemStatus();
            }
            
            window.lastAssetCount = currentAssetCount;
        }
        
        if (connectorsResponse.status === 'success') {
            const enabledCount = connectorsResponse.enabled_connectors || 0;
            const totalCount = connectorsResponse.total_connectors || 0;
            
            const badge = document.getElementById('total-connectors-badge');
            if (badge) {
                badge.textContent = `${totalCount} (${enabledCount} enabled)`;
            }
        }
        
    } catch (error) {
    }
}

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
        element.style.transition = 'opacity 0.3s ease';
        element.style.opacity = '0';
        setTimeout(() => {
            element.innerHTML = '';
            element.style.opacity = '1';
        }, 300);
    }
}

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
    
    notification.style.transform = 'translateX(100%)';
    notification.style.transition = 'transform 0.3s cubic-bezier(0.4, 0, 0.2, 1)';
    
    document.body.appendChild(notification);
    
    setTimeout(() => {
        notification.style.transform = 'translateX(0)';
    }, 10);
    
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
    refreshInterval = setInterval(() => {
        updateSystemStatus();
        
    }, 5000);
    
}

function startDynamicDashboard() {
    setInterval(() => {
        updateSystemStatus();
        loadRecentActivity();
    }, 3000);
    
    setInterval(() => {
        updateDiscoveryChart();
    }, 30000);
}

async function loadRecentActivity() {
    try {
        const response = await apiCall('/system/activity');
        
        if (response.status === 'success') {
            const activities = response.activities || [];
            const activityContainer = document.getElementById('recent-activity');
            
            if (activities.length === 0) {
                activityContainer.innerHTML = `
                    <div class="list-group-item text-center text-muted">
                        <i class="fas fa-clock me-2"></i>
                        No recent activity
                    </div>
                `;
                return;
            }
            
            let html = '<div class="list-group list-group-flush">';
            
            activities.slice(0, 5).forEach(activity => {
                const timeAgo = getTimeAgo(activity.timestamp);
                const icon = getActivityIcon(activity.type);
                
                html += `
                    <div class="list-group-item border-0 py-2">
                        <div class="d-flex align-items-start">
                            <div class="me-3">
                                <i class="fas fa-${icon} text-primary"></i>
                            </div>
                            <div class="flex-grow-1">
                                <div class="fw-medium">${activity.message}</div>
                                <small class="text-muted">${timeAgo}</small>
                            </div>
                        </div>
                    </div>
                `;
            });
            
            html += '</div>';
            activityContainer.innerHTML = html;
        }
    } catch (error) {
    }
}

function getActivityIcon(type) {
    const icons = {
        'asset_discovered': 'database',
        'connector_added': 'plug',
        'discovery_started': 'search',
        'discovery_completed': 'check-circle',
        'system_health': 'heartbeat'
    };
    return icons[type] || 'info-circle';
}

function getTimeAgo(timestamp) {
    if (!timestamp) return 'Unknown time';
    
    try {
        const now = new Date();
        const time = new Date(timestamp);
        const diffInSeconds = Math.floor((now - time) / 1000);
        
        if (diffInSeconds < 60) {
            return 'Just now';
        } else if (diffInSeconds < 3600) {
            const minutes = Math.floor(diffInSeconds / 60);
            return `${minutes} minute${minutes > 1 ? 's' : ''} ago`;
        } else if (diffInSeconds < 86400) {
            const hours = Math.floor(diffInSeconds / 3600);
            return `${hours} hour${hours > 1 ? 's' : ''} ago`;
        } else {
            const days = Math.floor(diffInSeconds / 86400);
            return `${days} day${days > 1 ? 's' : ''} ago`;
        }
    } catch (error) {
        return 'Unknown time';
    }
}

function updateDiscoveryChart(metrics) {
    if (!metrics || !discoveryChart) return;
    
    try {
        const ctx = document.getElementById('discoveryChart');
        if (!ctx) return;
        
        const assetsData = metrics.assets_by_type || {};
        const labels = Object.keys(assetsData);
        const data = Object.values(assetsData);
        
        if (discoveryChart.data) {
            discoveryChart.data.labels = labels;
            discoveryChart.data.datasets[0].data = data;
            discoveryChart.update('none'); // Smooth update without animation
        }
    } catch (error) {
    }
}

let currentWizardStep = 1;
let selectedConnectionType = null;
let connectionConfig = {};

function nextWizardStep() {
    if (currentWizardStep < 4) {
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
    document.querySelectorAll('.wizard-step').forEach((step, index) => {
        const stepNumber = index + 1;
        step.classList.remove('active', 'completed');
        
        if (stepNumber < currentWizardStep) {
            step.classList.add('completed');
        } else if (stepNumber === currentWizardStep) {
            step.classList.add('active');
        }
    });
    
    document.querySelectorAll('.wizard-step-content').forEach((content, index) => {
        content.classList.remove('active');
        if (index + 1 === currentWizardStep) {
            content.classList.add('active');
        }
    });
    
    const prevBtn = document.getElementById('wizard-prev-btn');
    const nextBtn = document.getElementById('wizard-next-btn');
    const saveBtn = document.getElementById('wizard-save-btn');
    
    prevBtn.style.display = currentWizardStep > 1 ? 'inline-block' : 'none';
    nextBtn.style.display = currentWizardStep < 4 ? 'inline-block' : 'none';
    saveBtn.style.display = currentWizardStep === 4 ? 'inline-block' : 'none';
    
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
            return validateConnectionForm();
        case 3:
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
    
    connectionConfig.connectorId = type;
    
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
                            <input type="text" class="form-control connection-form-input" id="db-host" placeholder="Enter hostname">
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
    
    requiredFields.forEach(field => {
        if (!field.value.trim()) {
            field.classList.add('is-invalid');
            isValid = false;
        } else {
            field.classList.remove('is-invalid');
        }
    });
    
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
    
    testButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Testing Connection...';
    testButton.disabled = true;
    
    resultsDiv.innerHTML = `
        <div class="connection-test-loading">
            <div class="spinner-border text-primary mb-3"></div>
            <p>Testing connection to your data source...</p>
        </div>
    `;
    resultsDiv.style.display = 'block';
    
    const modal = document.getElementById('connectorModal');
    let connectorId = modal ? modal.getAttribute('data-connector-id') : null;
    
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
        testButton.innerHTML = '<i class="fas fa-link me-2"></i>Test Connection';
        testButton.disabled = false;
        return;
    }
    
    testActualConnection(connectorId, testButton, resultsDiv);
}

async function testActualConnection(connectorId, testButton, resultsDiv) {
    try {
        if (connectorId === 'gcp') {
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
            
            const testResponse = await fetch(`/api/config/gcp/test`, {
                method: 'POST'
            });
            
            const testResult = await testResponse.json();
            
            if (testResult.status === 'success' && testResult.connection_status === 'connected') {
                const discoverResponse = await fetch(`/api/discovery/test/gcp`, {
                    method: 'POST'
                });
                
                const discoverResult = await discoverResponse.json();
                
                if (discoverResult.status === 'success' && discoverResult.assets && discoverResult.assets.length > 0) {
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
        
        const testResult = await apiCall(`/config/${connectorId}/test`, { method: 'POST' });
        
        if (testResult.status === 'success' || testResult.connection_status === 'connected') {
            try {
                const discoveryResult = await apiCall(`/discovery/test/${connectorId}`, { method: 'POST' });
                
                if (discoveryResult.status === 'success') {
                    const assetsCount = discoveryResult.assets_discovered || 0;
                    
                    await loadAssets();
                    
            resultsDiv.innerHTML = `
                <div class="connection-test-success">
                    <i class="fas fa-check-circle fa-3x mb-3"></i>
                    <h5>Connection Successful!</h5>
                            <p>Successfully connected and discovered <strong>${assetsCount}</strong> assets.</p>
                            ${assetsCount > 0 ? `
                                <button class="btn btn-outline-light mt-2" onclick="switchToAssetsTab(); bootstrap.Modal.getInstance(document.getElementById('connectorModal')).hide();">
                                    <i class="fas fa-database me-2"></i>View All Assets
                                </button>
                                <button class="btn btn-outline-light mt-2" onclick="showAssetsModal('${connectorId}', ${JSON.stringify(discoveryResult.assets).replace(/"/g, '&quot;')})">
                                    <i class="fas fa-list me-2"></i>View Discovery Results
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
        testButton.innerHTML = '<i class="fas fa-link me-2"></i>Test Connection';
        testButton.disabled = false;
    }
}

function loadConnectionSummary() {
    const summaryDiv = document.getElementById('connection-summary');
    
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
                
                setTimeout(() => loadDiscoveredAssets(), 100);
            }
            break;
    }
    
    summaryDiv.innerHTML = summaryHTML;
}

async function loadDiscoveredAssets() {
    const container = document.getElementById('discovered-assets-container');
    if (!container) {
        return;
    }
    
    try {
        const healthResponse = await fetch('/api/system/health');
        const healthData = await healthResponse.json();
        const gcpConnector = healthData.connector_status?.gcp;
        
        if (!gcpConnector || !gcpConnector.configured) {
            const projectId = document.getElementById('project-id')?.value;
            const serviceAccountJson = document.getElementById('service-account-json')?.value;
            
            if (projectId && serviceAccountJson) {
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
                    const errorData = await addResponse.json();
                    throw new Error(`Failed to add GCP connector: ${errorData.detail || 'Unknown error'}`);
                }
            } else {
                throw new Error('Project ID or Service Account JSON not found');
            }
        } else {
        }
        
        const response = await fetch('/api/discovery/test/gcp', {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (result.status === 'success' && result.assets && result.assets.length > 0) {
            const assets = result.assets;
            const datasets = assets.filter(asset => asset.type === 'bigquery_dataset');
            const tables = assets.filter(asset => asset.type === 'bigquery_table');
            
            let assetsHTML = '<div class="discovered-assets-list">';
            
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
        let connectorId = connectionConfig.connectorId;
        if (!connectorId) {
            showNotification('No connector selected', 'danger');
            return;
        }
        
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
        
        connectorId = connectorIdMapping[connectorId] || connectorId;

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

        const result = await apiCall(`/config/${connectorId}`, {
            method: 'POST',
            body: JSON.stringify(config)
        });

        if (result.status === 'success') {
            showNotification('Connection configuration saved successfully', 'success');
            
            const connectionData = {
                name: config.name || getConnectorDisplayName(connectorId),
                type: getConnectorDisplayName(connectorId),
                host: config.config?.host || config.config?.endpoint || config.config?.project_id || 'N/A',
                endpoint: config.config?.endpoint || config.config?.host,
                ...config.config
            };
            addUserConnection(connectionData);
            
            setTimeout(async () => {
                await discoverAssetsForConnection(connectorId, connectionData.name);
            }, 1000); // Small delay to ensure connection is fully saved
            
            const modal = bootstrap.Modal.getInstance(document.getElementById('connectorModal'));
            if (modal) {
                modal.hide();
            }
            
            loadConnectors();
            refreshUserConnections();
        } else {
            showNotification('Failed to save configuration: ' + (result.error || 'Unknown error'), 'danger');
        }
        
    } catch (error) {
        showNotification('Failed to save configuration: ' + error.message, 'danger');
    }
}

function openNewConnectionWizard() {
    currentWizardStep = 1;
    selectedConnectionType = null;
    connectionConfig = {};
    
    const step1Content = document.getElementById('step-1');
    step1Content.innerHTML = '<div class="text-center"><div class="spinner-border text-primary" role="status"></div></div>';
    
    const modal = new bootstrap.Modal(document.getElementById('connectorModal'));
    modal.show();
    
    document.querySelector('#connectorModal .modal-title').textContent = 'Configure New Connection';
    
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
        
        setTimeout(() => {
            document.querySelectorAll('.connection-type-card').forEach(card => {
                card.addEventListener('click', function() {
                    selectConnectionType(this.dataset.type);
                });
            });
        }, 100);
    }, 100);
    
    updateWizardStep();
}

function openConnectionWizard(connectorId, connectorName) {
    currentWizardStep = 1;
    selectedConnectionType = getConnectionTypeFromId(connectorId);
    connectionConfig = { connectorId, connectorName };
    
    const step1Content = document.getElementById('step-1');
    step1Content.innerHTML = '<div class="text-center"><div class="spinner-border text-primary" role="status"></div></div>';
    
    const modalElement = document.getElementById('connectorModal');
    modalElement.setAttribute('data-connector-id', connectorId);
    const modal = new bootstrap.Modal(modalElement);
    modal.show();
    
    document.querySelector('#connectorModal .modal-title').textContent = `Configure ${connectorName}`;
    
    setTimeout(() => {
        loadConnectorSpecificTypes(connectorId, connectorName);
    }, 100);
    
    updateWizardStep();
    
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
    
    const connectorTypes = {
        'azure': [
            { type: 'blob', icon: 'fas fa-cloud-arrow-up', title: 'Blob Storage', description: 'Connect to Azure Blob Storage containers' },
            { type: 'sql', icon: 'fas fa-database', title: 'Azure SQL', description: 'Connect to Azure SQL Database' },
            { type: 'synapse', icon: 'fas fa-chart-line', title: 'Synapse', description: 'Connect to Azure Synapse Analytics' },
            { type: 'cosmos', icon: 'fas fa-globe', title: 'Cosmos DB', description: 'Connect to Azure Cosmos DB' }
        ],
        'gcp': [
            { type: 'storage', icon: 'fas fa-cloud-arrow-up', title: 'Cloud Storage', description: 'Connect to Google Cloud Storage buckets' },
            { type: 'bigquery', icon: 'fas fa-chart-line', title: 'BigQuery', description: 'Connect to Google BigQuery data warehouse' },
            { type: 'sql', icon: 'fas fa-database', title: 'Cloud SQL', description: 'Connect to Google Cloud SQL instances' },
            { type: 'firestore', icon: 'fas fa-fire', title: 'Firestore', description: 'Connect to Google Firestore NoSQL database' }
        ],
        'salesforce': [
            { type: 'objects', icon: 'fas fa-cube', title: 'Standard Objects', description: 'Connect to Salesforce standard objects (Account, Contact, etc.)' },
            { type: 'custom', icon: 'fas fa-cogs', title: 'Custom Objects', description: 'Connect to custom Salesforce objects' },
            { type: 'reports', icon: 'fas fa-chart-line', title: 'Reports & Dashboards', description: 'Connect to Salesforce reports and dashboards' }
        ],
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
    
    const types = connectorTypes[connectorId.toLowerCase()] || [
        { type: 'database', icon: 'fas fa-database', title: 'Database', description: 'Connect to SQL databases like PostgreSQL, MySQL, Oracle' },
        { type: 'cloud', icon: 'fas fa-cloud', title: 'Cloud Storage', description: 'Connect to cloud storage services' },
        { type: 'network', icon: 'fas fa-server', title: 'Network Storage', description: 'Connect to NAS drives and SFTP servers' }
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
    
    setTimeout(() => {
        document.querySelectorAll('.connection-type-card').forEach(card => {
            card.addEventListener('click', function() {
                selectConnectionType(this.dataset.type);
            });
        });
    }, 100);
}

function getConnectionTypeFromId(connectorId) {
    const typeMapping = {
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
        
        'azure': 'blob',
        'gcp': 'storage',
        
        'snowflake': 'warehouse',
        'databricks': 'workspace',
        'bigquery': 'datasets',
        'teradata': 'database',
        'redshift': 'cluster',
        'clickhouse': 'database',
        
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
    showNotification(`Viewing details for ${connectorId}`, 'info');
    
}

document.addEventListener('DOMContentLoaded', function() {
    setTimeout(() => {
        document.querySelectorAll('.connection-type-card').forEach(card => {
            card.addEventListener('click', function() {
                selectConnectionType(this.dataset.type);
            });
        });
        
        refreshMyConnections();
        
        
    }, 500);
});

let userConnections = JSON.parse(localStorage.getItem('userConnections') || '[]');

function saveUserConnections() {
    localStorage.setItem('userConnections', JSON.stringify(userConnections));
}

function addUserConnection(connectionData) {
    const connection = {
        id: Date.now().toString(),
        name: connectionData.name,
        type: connectionData.type,
        status: 'connected',
        host: connectionData.host || connectionData.endpoint || 'N/A',
        lastTested: new Date().toISOString(),
        config: connectionData,
        createdAt: new Date().toISOString()
    };
    
    userConnections.push(connection);
    saveUserConnections();
    refreshMyConnections();
    return connection;
}

function removeUserConnection(connectionId) {
    userConnections = userConnections.filter(conn => conn.id !== connectionId);
    saveUserConnections();
    refreshMyConnections();
}

function updateConnectionStatus(connectionId, status, lastTested = null) {
    const connection = userConnections.find(conn => conn.id === connectionId);
    if (connection) {
        connection.status = status;
        if (lastTested) {
            connection.lastTested = lastTested;
        }
        saveUserConnections();
        refreshMyConnections();
    }
}

function createMyConnectionCard(connection) {
    const statusClass = connection.status === 'connected' ? 'success' : 
                       connection.status === 'error' ? 'danger' : 'warning';
    const statusIcon = connection.status === 'connected' ? 'check-circle' : 
                      connection.status === 'error' ? 'times-circle' : 'exclamation-circle';
    
    return `
        <div class="col-md-6 col-lg-4 mb-3">
            <div class="card border-left-${statusClass}">
                <div class="card-body">
                    <div class="d-flex justify-content-between align-items-start mb-2">
                        <div>
                            <h6 class="card-title mb-1">${connection.name}</h6>
                            <small class="text-muted">${connection.type}</small>
                        </div>
                        <span class="badge bg-${statusClass}">
                            <i class="fas fa-${statusIcon} me-1"></i>
                            ${connection.status.charAt(0).toUpperCase() + connection.status.slice(1)}
                        </span>
                    </div>
                    
                    <div class="connection-details mb-3">
                        <small class="text-muted d-block">
                            <i class="fas fa-server me-1"></i>
                            Host: ${connection.host}
                        </small>
                        <small class="text-muted d-block">
                            <i class="fas fa-clock me-1"></i>
                            Last tested: ${new Date(connection.lastTested).toLocaleDateString()}
                        </small>
                    </div>
                    
                    <div class="btn-group w-100" role="group">
                        <button class="btn btn-outline-primary btn-sm" onclick="testMyConnection('${connection.id}')" title="Test Connection">
                            <i class="fas fa-play me-1"></i>Test
                        </button>
                        <button class="btn btn-outline-info btn-sm" onclick="viewMyConnection('${connection.id}')" title="View Details">
                            <i class="fas fa-eye me-1"></i>View
                        </button>
                        <button class="btn btn-outline-danger btn-sm" onclick="removeMyConnection('${connection.id}')" title="Remove Connection">
                            <i class="fas fa-trash me-1"></i>Remove
                        </button>
                    </div>
                </div>
            </div>
        </div>
    `;
}

function refreshMyConnections() {
    const container = document.getElementById('my-connections');
    const countBadge = document.getElementById('active-connections-count');
    
    if (!container) return;
    
    if (userConnections.length === 0) {
        container.innerHTML = `
            <div class="col-12">
                <div class="text-center py-4">
                    <i class="fas fa-plug fa-3x text-muted mb-3"></i>
                    <h6 class="text-muted">No connections yet</h6>
                    <p class="text-muted">Create your first connection using the Available Connectors below</p>
                </div>
            </div>
        `;
    } else {
        container.innerHTML = userConnections.map(conn => createMyConnectionCard(conn)).join('');
    }
    
    if (countBadge) {
        countBadge.textContent = userConnections.length;
    }
}

async function testMyConnection(connectionId) {
    const connection = userConnections.find(conn => conn.id === connectionId);
    if (!connection) {
        showNotification('Connection not found', 'error');
        return;
    }
    
    try {
        showNotification(`Testing connection: ${connection.name}`, 'info');
        
        updateConnectionStatus(connectionId, 'testing');
        
        let connectorId = connection.type.toLowerCase().replace(/\s+/g, '_');
        if (connection.type.includes('Google Cloud')) connectorId = 'gcp';
        if (connection.type.includes('Azure')) connectorId = 'azure';
        
        const response = await apiCall(`/config/${connectorId}/test`, {
            method: 'POST'
        });
        
        if (response && (response.success || response.status === 'success' || response.connection_status === 'connected')) {
            updateConnectionStatus(connectionId, 'connected', new Date().toISOString());
            showNotification(`Connection test successful: ${connection.name}`, 'success');
            
            await discoverAssetsForConnection(connectorId, connection.name);
        } else {
            updateConnectionStatus(connectionId, 'error', new Date().toISOString());
            showNotification(`Connection test failed: ${connection.name} - ${response?.error || 'Unknown error'}`, 'error');
        }
    } catch (error) {
        updateConnectionStatus(connectionId, 'error', new Date().toISOString());
        showNotification(`Connection test failed: ${connection.name}`, 'error');
    }
}

function viewMyConnection(connectionId) {
    const connection = userConnections.find(conn => conn.id === connectionId);
    if (!connection) {
        showNotification('Connection not found', 'error');
        return;
    }
    
    try {
        const modalContent = `
        <div class="modal fade" id="connectionDetailsModal" tabindex="-1">
            <div class="modal-dialog modal-lg">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">
                            <i class="fas fa-info-circle me-2"></i>
                            Connection Details: ${connection.name}
                        </h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                    <div class="modal-body">
                        <div class="row">
                            <div class="col-md-6">
                                <h6>Basic Information</h6>
                                <table class="table table-sm">
                                    <tr><td><strong>Name:</strong></td><td>${connection.name}</td></tr>
                                    <tr><td><strong>Type:</strong></td><td>${connection.type}</td></tr>
                                    <tr><td><strong>Status:</strong></td><td><span class="badge bg-${connection.status === 'connected' ? 'success' : 'warning'}">${connection.status}</span></td></tr>
                                    <tr><td><strong>Host:</strong></td><td>${connection.host}</td></tr>
                                    <tr><td><strong>Created:</strong></td><td>${new Date(connection.createdAt).toLocaleString()}</td></tr>
                                    <tr><td><strong>Last Tested:</strong></td><td>${new Date(connection.lastTested).toLocaleString()}</td></tr>
                                </table>
            </div>
                            <div class="col-md-6">
                                <h6>Configuration</h6>
                                <pre class="bg-light p-3 rounded"><code>${JSON.stringify(connection.config, null, 2)}</code></pre>
                            </div>
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-primary" onclick="testMyConnection('${connection.id}')">
                            <i class="fas fa-play me-1"></i>Test Connection
                        </button>
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    const existingModal = document.getElementById('connectionDetailsModal');
    if (existingModal) {
        existingModal.remove();
    }
    
    document.body.insertAdjacentHTML('beforeend', modalContent);
    
    const modal = new bootstrap.Modal(document.getElementById('connectionDetailsModal'));
    modal.show();
    
        document.getElementById('connectionDetailsModal').addEventListener('hidden.bs.modal', function() {
            this.remove();
        });
    } catch (error) {
        showNotification('Failed to show connection details', 'error');
    }
}

function removeMyConnection(connectionId) {
    const connection = userConnections.find(conn => conn.id === connectionId);
    if (!connection) {
        showNotification('Connection not found', 'error');
        return;
    }
    
    if (confirm(`Are you sure you want to remove the connection "${connection.name}"?`)) {
        try {
            removeUserConnection(connectionId);
            showNotification(`Connection "${connection.name}" has been removed`, 'success');
        } catch (error) {
            showNotification('Failed to remove connection', 'error');
        }
    }
}

async function discoverAssetsForConnection(connectorId, connectionName) {
    try {
        showNotification(`Discovering assets for ${connectionName}...`, 'info');
        
        const response = await apiCall(`/discovery/test/${connectorId}`, {
            method: 'POST'
        });
        
        if (response && response.status === 'success') {
            const assetsCount = response.assets_discovered || 0;
            showNotification(`Discovery completed! Found ${assetsCount} assets from ${connectionName}`, 'success');
            
            await loadAssets();
            
            refreshDashboard();
            
            switchToAssetsTab();
            
            if (response.assets && response.assets.length > 0) {
                showDiscoveryResultsModal(connectionName, response.assets);
            }
        } else {
            showNotification(`Asset discovery failed for ${connectionName}: ${response?.message || 'Unknown error'}`, 'warning');
        }
    } catch (error) {
        showNotification(`Asset discovery failed for ${connectionName}`, 'error');
    }
}

function switchToAssetsTab() {
    const assetsTab = document.querySelector('#assets-tab');
    const assetsTabPane = document.querySelector('#assets');
    
    if (assetsTab && assetsTabPane) {
        document.querySelectorAll('.nav-link').forEach(tab => tab.classList.remove('active'));
        document.querySelectorAll('.tab-pane').forEach(pane => {
            pane.classList.remove('show', 'active');
        });
        
        assetsTab.classList.add('active');
        assetsTabPane.classList.add('show', 'active');
    }
}

function showDiscoveryResultsModal(connectionName, assets) {
    const modalContent = `
        <div class="modal fade" id="discoveryResultsModal" tabindex="-1">
            <div class="modal-dialog modal-xl">
                <div class="modal-content">
                    <div class="modal-header bg-success text-white">
                        <h5 class="modal-title">
                            <i class="fas fa-search me-2"></i>
                            Discovery Results: ${connectionName}
                        </h5>
                        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="alert alert-success">
                            <i class="fas fa-check-circle me-2"></i>
                            Successfully discovered <strong>${assets.length}</strong> assets from ${connectionName}
                        </div>
                        
                        <h6>Discovered Assets:</h6>
                        <div class="table-responsive">
                            <table class="table table-sm table-striped">
                                <thead>
                                    <tr>
                                        <th>Name</th>
                                        <th>Type</th>
                                        <th>Size</th>
                                        <th>Location</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${assets.map(asset => `
                                        <tr>
                                            <td><strong>${asset.name}</strong></td>
                                            <td><span class="badge bg-secondary">${asset.type}</span></td>
                                            <td>${formatBytes(asset.size || 0)}</td>
                                            <td><code class="small">${asset.location || 'N/A'}</code></td>
                                        </tr>
                                    `).join('')}
                                </tbody>
                            </table>
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-primary" onclick="switchToAssetsTab(); bootstrap.Modal.getInstance(document.getElementById('discoveryResultsModal')).hide();">
                            <i class="fas fa-database me-1"></i>View All Assets
                        </button>
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    const existingModal = document.getElementById('discoveryResultsModal');
    if (existingModal) {
        existingModal.remove();
    }
    
    document.body.insertAdjacentHTML('beforeend', modalContent);
    
    const modal = new bootstrap.Modal(document.getElementById('discoveryResultsModal'));
    modal.show();
    
    document.getElementById('discoveryResultsModal').addEventListener('hidden.bs.modal', function() {
        this.remove();
    });
}

function formatBytes(bytes, decimals = 2) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

window.refreshDashboard = refreshDashboard;
window.loadConnectors = loadConnectors;
window.openConnectorModal = openConnectorModal;
window.saveConnectorConfig = saveConnectorConfig;
window.testConnectorConnection = testConnectorConnection;
window.loadAssets = loadAssets;
window.searchAssets = searchAssets;
window.applyAssetFilters = applyAssetFilters;
window.showAssetDetails = showAssetDetails;
window.startFullDiscovery = startFullDiscovery;
window.showSourceSelection = showSourceSelection;
window.scanSpecificSource = scanSpecificSource;
window.nextWizardStep = nextWizardStep;
window.previousWizardStep = previousWizardStep;
window.testConnectionWizard = testConnectionWizard;
window.openNewConnectionWizard = openNewConnectionWizard;
window.openConnectionWizard = openConnectionWizard;
window.saveWizardConnection = saveWizardConnection;
window.testConnectorConnection = testConnectorConnection;
window.viewConnectorConnectionDetails = viewConnectorConnectionDetails;
window.deleteConnectorConnection = deleteConnectorConnection;
window.editConnection = editConnection;
window.testMyConnection = testMyConnection;
window.viewMyConnection = viewMyConnection;
window.removeMyConnection = removeMyConnection;
window.refreshMyConnections = refreshMyConnections;
window.discoverAssetsForConnection = discoverAssetsForConnection;
window.switchToAssetsTab = switchToAssetsTab;
window.showDiscoveryResultsModal = showDiscoveryResultsModal;

window.testButtonDebug = function() {
    alert('Button works!');
};

window.debugConnections = function() {
        testMyConnection: typeof window.testMyConnection,
        viewMyConnection: typeof window.viewMyConnection,
        removeMyConnection: typeof window.removeMyConnection
    });
};

function ensureLineageVariablesInitialized() {
    if (typeof window.currentLineageData === 'undefined') window.currentLineageData = null;
    if (typeof window.selectedAssetId === 'undefined') window.selectedAssetId = null;
    if (typeof window.lineageAssetsCache === 'undefined') window.lineageAssetsCache = [];
    if (typeof window.lastAssetCount === 'undefined') window.lastAssetCount = 0;
    if (typeof window.lineageInitialized === 'undefined') window.lineageInitialized = false;
}

window.currentLineageData = null;
window.selectedAssetId = null;
window.lineageAssetsCache = [];
window.lastAssetCount = 0;
window.lineageInitialized = false;

function autoRefreshLineageAssets() {
    ensureLineageVariablesInitialized();
    
    try {
        const lineageTab = document.getElementById('lineage-asset-select');
        if (!lineageTab) return;
        
        if (!window.lineageInitialized) {
            window.lastAssetCount = 0;
            window.lineageInitialized = true;
        }
        
        const currentAssetsCount = Object.values(currentAssets || {}).reduce((count, sourceAssets) => count + sourceAssets.length, 0);
        
        
        if (currentAssetsCount !== window.lastAssetCount) {
            window.lastAssetCount = currentAssetsCount;
            loadLineageAssetsFromCache();
            
            if (window.selectedAssetId) {
                loadAssetLineage(window.selectedAssetId);
            }
        }
    } catch (error) {
    }
}

function loadLineageAssetsFromCache() {
    try {
        const syncStatus = document.getElementById('lineage-sync-status');
        if (syncStatus) {
            syncStatus.style.display = 'inline-block';
        }
        
        const assets = [];
        
        for (const [source, sourceAssets] of Object.entries(currentAssets || {})) {
            sourceAssets.forEach(asset => {
                const lineageAsset = {
                    id: asset.metadata?.asset_fingerprint || generateAssetId(asset),
                    name: asset.name,
                    type: asset.type,
                    source: asset.source || source,
                    location: asset.location,
                    has_schema: Boolean(asset.schema && asset.schema.length > 0),
                    column_count: asset.schema ? asset.schema.length : 0,
                    metadata: asset.metadata || {}
                };
                assets.push(lineageAsset);
            });
        }
        
        window.lineageAssetsCache = assets;
        
        updateLineageAssetsDropdown(assets);
        
        const countBadge = document.getElementById('lineage-assets-count');
        if (countBadge) {
            countBadge.textContent = assets.length;
            
        }
        
        
        setTimeout(() => {
            const syncStatus = document.getElementById('lineage-sync-status');
            if (syncStatus) {
                syncStatus.style.display = 'none';
            }
        }, 1000);
        
    } catch (error) {
        
        const syncStatus = document.getElementById('lineage-sync-status');
        if (syncStatus) {
            syncStatus.style.display = 'none';
        }
    }
}

function generateAssetId(asset) {
    const data = `${asset.source || ''}-${asset.location || ''}-${asset.name || ''}`;
    return btoa(data).replace(/[^a-zA-Z0-9]/g, '').substring(0, 16);
}

function updateLineageAssetsDropdown(assets) {
    ensureLineageVariablesInitialized();
    
    if (!assets || !Array.isArray(assets)) {
        assets = []; // Default to empty array
    }
    
    
    const assetSelect = document.getElementById('lineage-asset-select');
    if (!assetSelect) {
        return;
    }
    
    const currentlySelected = assetSelect.value;
    assetSelect.innerHTML = '<option value="">Select an asset to view its lineage...</option>';
    
    if (assets.length > 0) {
        assets.sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    }
    if (assets.length > 0) {
    }
    
    assets.forEach((asset, index) => {
        if (!asset || !asset.id || !asset.name) {
            return;
        }
        
        const option = document.createElement('option');
        option.value = asset.id;
        option.textContent = `${asset.name} (${asset.type || 'unknown'}) - ${asset.source || 'unknown'}`;
        option.dataset.asset = JSON.stringify(asset);
        
        if (asset.id === currentlySelected) {
            option.selected = true;
        }
        
        assetSelect.appendChild(option);
        
        if (index < 3) {
        }
    });
    
    
    const previousCount = (window.lineageAssetsCache && Array.isArray(window.lineageAssetsCache)) ? window.lineageAssetsCache.length : 0;
    if (assets.length > previousCount && previousCount > 0) {
        const newAssets = assets.length - previousCount;
        showNotification(`🔄 ${newAssets} new asset${newAssets > 1 ? 's' : ''} added to lineage explorer`, 'success');
        
        const lineageTabButton = document.getElementById('lineage-tab');
        if (lineageTabButton) {
            lineageTabButton.style.animation = 'pulse 2s infinite';
            setTimeout(() => {
                lineageTabButton.style.animation = '';
            }, 6000);
        }
    }
    
    window.lineageAssetsCache = assets;
}

async function loadLineageAssets() {
    ensureLineageVariablesInitialized();
    
    try {
        
        const response = await apiCall('/lineage/assets');
        
        if (response && response.status === 'success') {
            const assets = response.assets || [];
            
            if (!Array.isArray(assets)) {
                const safeAssets = [];
                updateLineageAssetsDropdown(safeAssets);
                return;
            }
            
            if (assets.length > 0) {
            }
            
            if (!window.lineageInitialized) {
                window.lineageInitialized = true;
                window.lastAssetCount = assets.length;
            }
            
            updateLineageAssetsDropdown(assets);
            
            const countBadge = document.getElementById('lineage-assets-count');
            if (countBadge) {
                countBadge.textContent = assets.length;
            }
            
            
        } else {
            const message = response?.message || 'Unknown error';
            showNotification('Failed to load lineage assets: ' + message, 'danger');
            
            if (!window.lineageInitialized) {
                window.lineageInitialized = true;
                window.lastAssetCount = 0;
                window.lineageAssetsCache = [];
            }
        }
        
    } catch (error) {
        showNotification('Error loading lineage assets: ' + error.message, 'danger');
    }
}

async function selectAssetForLineage() {
    try {
        const assetSelect = document.getElementById('lineage-asset-select');
        if (!assetSelect) {
            return;
        }
        
        const selectedOption = assetSelect.selectedOptions[0];
        
        if (!selectedOption || !selectedOption.value) {
            clearLineageVisualization();
            return;
        }
        
        window.selectedAssetId = selectedOption.value;
        
        let asset = null;
        try {
            if (selectedOption.dataset && selectedOption.dataset.asset) {
                asset = JSON.parse(selectedOption.dataset.asset);
            }
        } catch (parseError) {
            showNotification('Error parsing asset data', 'danger');
            return;
        }
        
        if (asset) {
            updateLineageSummary(asset);
        }
        
        await loadAssetLineage(window.selectedAssetId);
        
    } catch (error) {
        showNotification('Error selecting asset for lineage: ' + error.message, 'danger');
    }
}

function updateLineageSummary(asset) {
    const summaryDiv = document.getElementById('lineage-summary');
    
    if (!summaryDiv) {
        return;
    }
    
    if (!asset) {
        summaryDiv.innerHTML = `
            <div class="border-start-warning p-3">
                <h6 class="fw-bold text-warning">No Asset Selected</h6>
                <p class="mb-0">Please select an asset to view lineage information.</p>
            </div>
        `;
        return;
    }
    
    summaryDiv.innerHTML = `
        <div class="border-start-primary p-3">
            <h6 class="fw-bold text-primary">${asset.name || 'Unknown Asset'}</h6>
            <p class="mb-1"><strong>Type:</strong> ${asset.type || 'Unknown'}</p>
            <p class="mb-1"><strong>Source:</strong> ${asset.source || 'Unknown'}</p>
            <p class="mb-1"><strong>Columns:</strong> ${asset.column_count || 'N/A'}</p>
            <p class="mb-0"><strong>Has Schema:</strong> ${asset.has_schema ? 'Yes' : 'No'}</p>
        </div>
    `;
}

async function loadAssetLineage(assetId) {
    try {
        const direction = document.getElementById('lineage-direction').value;
        const depth = parseInt(document.getElementById('lineage-depth').value);
        
        showLineageLoading();
        
        const response = await apiCall(`/lineage/${assetId}?direction=${direction}&depth=${depth}`);
        
        if (response.status === 'success') {
            window.currentLineageData = response.lineage;
            displayLineageVisualization(response.lineage);
            
            if (window.currentLineageData && window.currentLineageData.nodes) {
                const selectedAsset = window.currentLineageData.nodes.find(node => node && node.is_target);
                if (selectedAsset) {
                    updateAnalysisPanel(selectedAsset, window.currentLineageData);
                }
            }
            
            await loadColumnLineage(assetId);
            
        } else {
            showNotification('Failed to load asset lineage', 'danger');
            clearLineageVisualization();
        }
        
    } catch (error) {
        showNotification('Error loading asset lineage: ' + error.message, 'danger');
        clearLineageVisualization();
    }
}

function displayLineageVisualization(lineageData) {
    if (!lineageData) {
        showNotification('No lineage data available', 'warning');
        return;
    }
    
    const currentViewElement = document.querySelector('input[name="lineage-view"]:checked');
    const currentView = currentViewElement ? currentViewElement.id : 'graph-view';
    
    if (currentView === 'graph-view') {
        displayLineageGraph(lineageData);
    } else {
        displayLineageTable(lineageData);
    }
}

function displayLineageGraph(lineageData) {
    const graphContainer = document.getElementById('lineage-graph');
    
    if (!graphContainer) {
        return;
    }
    
    graphContainer.innerHTML = '';
    
    if (!lineageData || typeof lineageData !== 'object') {
        graphContainer.innerHTML = `
            <div class="d-flex align-items-center justify-content-center h-100 text-muted">
                <div class="text-center">
                    <i class="fas fa-exclamation-triangle fa-4x mb-3 text-warning"></i>
                    <h5>Invalid Lineage Data</h5>
                    <p>The lineage data received is not valid</p>
                </div>
            </div>
        `;
        return;
    }
    
    const nodes = lineageData.nodes || [];
    const edges = lineageData.edges || [];
    
    if (nodes.length === 0) {
        graphContainer.innerHTML = `
            <div class="d-flex align-items-center justify-content-center h-100 text-muted">
                <div class="text-center">
                    <i class="fas fa-project-diagram fa-4x mb-3 opacity-25"></i>
                    <h5>No Lineage Data</h5>
                    <p>No lineage relationships found for this asset</p>
                </div>
            </div>
        `;
        return;
    }
    
    const containerRect = graphContainer.getBoundingClientRect();
    const containerWidth = Math.max(800, containerRect.width || 800);
    const containerHeight = Math.min(600, Math.max(400, containerRect.height || 500));
    
    const svgWidth = 800;
    const svgHeight = 500;
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('width', '100%');
    svg.setAttribute('height', '100%');
    svg.setAttribute('viewBox', `0 0 ${svgWidth} ${svgHeight}`);
    svg.style.display = 'block';
    
    let isPanning = false;
    let startPoint = { x: 0, y: 0 };
    let currentTransform = { x: 0, y: 0, scale: 1 };
    
    const graphGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    graphGroup.setAttribute('id', 'graph-group');
    svg.appendChild(graphGroup);
    
    let edgeElements = [];
    
    const levelGroups = {};
    nodes.forEach(node => {
        if (!node || !node.id) {
            return;
        }
        
        const nodeLevel = node.level || 0; // Default to level 0 if not specified
        if (!levelGroups[nodeLevel]) {
            levelGroups[nodeLevel] = [];
        }
        levelGroups[nodeLevel].push(node);
    });
    
    const levels = Object.keys(levelGroups).map(Number).sort((a, b) => a - b);
    const centerX = svgWidth / 2;
    const centerY = svgHeight / 2;
    const levelSpacing = Math.min(120, svgHeight / (levels.length + 1));
    
    const nodePositions = {};
    levels.forEach((level, levelIndex) => {
        const levelNodes = levelGroups[level];
        const yOffset = level * levelSpacing;
        const nodeSpacing = Math.min(100, svgWidth / Math.max(levelNodes.length + 1, 1));
        
        levelNodes.forEach((node, nodeIndex) => {
            if (!node || !node.id) {
                return;
            }
            
            const x = centerX + (nodeIndex - (levelNodes.length - 1) / 2) * nodeSpacing;
            const y = centerY + yOffset;
            nodePositions[node.id] = { x, y, node };
        });
    });
    
    
    edges.forEach((edge, index) => {
        if (!edge || !edge.source || !edge.target) {
            return;
        }
        
        const sourceId = typeof edge.source === 'object' ? (edge.source ? edge.source.id : null) : edge.source;
        const targetId = typeof edge.target === 'object' ? (edge.target ? edge.target.id : null) : edge.target;
        
        if (!sourceId || !targetId) {
            return;
        }
        
        const sourcePos = nodePositions[sourceId];
        const targetPos = nodePositions[targetId];
        
        if (sourcePos && targetPos) {
            const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
            line.setAttribute('x1', sourcePos.x);
            line.setAttribute('y1', sourcePos.y);
            line.setAttribute('x2', targetPos.x);
            line.setAttribute('y2', targetPos.y);
            line.setAttribute('stroke', '#6c757d');
            line.setAttribute('stroke-width', '2');
            line.setAttribute('marker-end', 'url(#arrowhead)');
            line.setAttribute('data-edge-id', `edge-${index}`);
            line.setAttribute('data-source', edge.source);
            line.setAttribute('data-target', edge.target);
            
            edgeElements.push({
                element: line,
                source: sourceId,
                target: targetId
            });
            
            graphGroup.appendChild(line);
        }
    });
    
    const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
    const marker = document.createElementNS('http://www.w3.org/2000/svg', 'marker');
    marker.setAttribute('id', 'arrowhead');
    marker.setAttribute('markerWidth', '10');
    marker.setAttribute('markerHeight', '7');
    marker.setAttribute('refX', '10');
    marker.setAttribute('refY', '3.5');
    marker.setAttribute('orient', 'auto');
    
    const polygon = document.createElementNS('http://www.w3.org/2000/svg', 'polygon');
    polygon.setAttribute('points', '0 0, 10 3.5, 0 7');
    polygon.setAttribute('fill', '#6c757d');
    marker.appendChild(polygon);
    defs.appendChild(marker);
    svg.appendChild(defs);
    
    Object.values(nodePositions).forEach(({ x, y, node }) => {
        if (!node || !node.id || !node.name) {
            return;
        }
        
        const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        circle.setAttribute('cx', x);
        circle.setAttribute('cy', y);
        circle.setAttribute('r', '25');
        circle.setAttribute('fill', node.is_target ? '#0d6efd' : (node.direction === 'upstream' ? '#198754' : '#dc3545'));
        circle.setAttribute('stroke', '#fff');
        circle.setAttribute('stroke-width', '3');
        circle.setAttribute('class', 'lineage-node');
        circle.setAttribute('data-node-id', node.id);
        circle.style.cursor = 'grab';
        
        const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('x', x);
        text.setAttribute('y', y - 35);
        text.setAttribute('text-anchor', 'middle');
        text.setAttribute('fill', '#333');
        text.setAttribute('font-size', '12');
        text.setAttribute('font-weight', 'bold');
        text.textContent = node.name.length > 15 ? node.name.substring(0, 15) + '...' : node.name;
        text.setAttribute('pointer-events', 'none'); // Prevent text from interfering with drag
        
        const typeText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        typeText.setAttribute('x', x);
        typeText.setAttribute('y', y + 45);
        typeText.setAttribute('text-anchor', 'middle');
        typeText.setAttribute('fill', '#666');
        typeText.setAttribute('font-size', '10');
        typeText.textContent = node.type;
        typeText.setAttribute('pointer-events', 'none'); // Prevent text from interfering with drag
        
        let isDragging = false;
        let dragStart = { x: 0, y: 0 };
        let initialPos = { x: x, y: y };
        
        circle.addEventListener('mousedown', (e) => {
            isDragging = true;
            const rect = svg.getBoundingClientRect();
            dragStart.x = e.clientX - rect.left;
            dragStart.y = e.clientY - rect.top;
            initialPos.x = parseFloat(circle.getAttribute('cx'));
            initialPos.y = parseFloat(circle.getAttribute('cy'));
            circle.classList.add('dragging');
            circle.style.cursor = 'grabbing';
            e.preventDefault();
            e.stopPropagation();
        });
        
        svg.addEventListener('mousemove', (e) => {
            if (!isDragging || circle.getAttribute('data-node-id') !== node.id) return;
            
            const rect = svg.getBoundingClientRect();
            const currentX = e.clientX - rect.left;
            const currentY = e.clientY - rect.top;
            
            const newX = initialPos.x + (currentX - dragStart.x);
            const newY = initialPos.y + (currentY - dragStart.y);
            
            const boundedX = Math.max(30, Math.min(svgWidth - 30, newX));
            const boundedY = Math.max(40, Math.min(svgHeight - 50, newY));
            
            circle.setAttribute('cx', boundedX);
            circle.setAttribute('cy', boundedY);
            text.setAttribute('x', boundedX);
            text.setAttribute('y', boundedY - 35);
            typeText.setAttribute('x', boundedX);
            typeText.setAttribute('y', boundedY + 45);
            
            nodePositions[node.id].x = boundedX;
            nodePositions[node.id].y = boundedY;
            
            updateConnectedEdges(node.id, nodePositions, edgeElements);
        });
        
        svg.addEventListener('mouseup', () => {
            if (isDragging && circle.getAttribute('data-node-id') === node.id) {
                isDragging = false;
                circle.classList.remove('dragging');
                circle.style.cursor = 'grab';
            }
        });
        
        circle.addEventListener('click', (e) => {
            if (!isDragging) {
                if (handleEditModeNodeClick(node.id, circle)) {
                    return;
                }
                toggleNodeColumnView(node.id, circle, text, typeText);
            }
        });
        
        graphGroup.appendChild(circle);
        graphGroup.appendChild(text);
        graphGroup.appendChild(typeText);
    });
    
    graphContainer.appendChild(svg);
    
}

function updateConnectedEdges(nodeId, nodePositions, edgeElements) {
    edgeElements.forEach(edgeInfo => {
        if (edgeInfo.source === nodeId || edgeInfo.target === nodeId) {
            const sourcePos = nodePositions[edgeInfo.source];
            const targetPos = nodePositions[edgeInfo.target];
            
            if (sourcePos && targetPos) {
                const line = edgeInfo.element;
                line.setAttribute('x1', sourcePos.x);
                line.setAttribute('y1', sourcePos.y);
                line.setAttribute('x2', targetPos.x);
                line.setAttribute('y2', targetPos.y);
            }
        }
    });
}

async function toggleNodeColumnView(nodeId, circleElement, textElement, typeTextElement) {
    const isExpanded = circleElement.getAttribute('data-expanded') === 'true';
    
    if (isExpanded) {
        collapseNodeColumns(nodeId, circleElement, textElement, typeTextElement);
    } else {
        await expandNodeColumns(nodeId, circleElement, textElement, typeTextElement);
        
        await loadColumnLineage(nodeId);
    }
}

async function expandNodeColumns(nodeId, circleElement, textElement, typeTextElement) {
    try {
        
        const response = await apiCall(`/lineage/${nodeId}/columns`);
        
        if (response.status !== 'success' || !response.column_lineage?.columns?.length) {
            showNotification('No column data available for this asset', 'info');
            return;
        }
        
        const columns = response.column_lineage.columns;
        const svg = circleElement.closest('svg');
        const graphGroup = svg.querySelector('#graph-group');
        
        const nodeX = parseFloat(circleElement.getAttribute('cx'));
        const nodeY = parseFloat(circleElement.getAttribute('cy'));
        
        circleElement.style.display = 'none';
        textElement.style.display = 'none';
        typeTextElement.style.display = 'none';
        
        const expandedGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        expandedGroup.setAttribute('id', `expanded-${nodeId}`);
        expandedGroup.setAttribute('data-node-id', nodeId);
        
        const columnHeight = 25;
        const nodeWidth = 200;
        const totalHeight = Math.max(60, columns.length * columnHeight + 40);
        
        const nodeRect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        nodeRect.setAttribute('x', nodeX - nodeWidth/2);
        nodeRect.setAttribute('y', nodeY - totalHeight/2);
        nodeRect.setAttribute('width', nodeWidth);
        nodeRect.setAttribute('height', totalHeight);
        nodeRect.setAttribute('fill', circleElement.getAttribute('fill'));
        nodeRect.setAttribute('stroke', '#fff');
        nodeRect.setAttribute('stroke-width', '3');
        nodeRect.setAttribute('rx', '8');
        expandedGroup.appendChild(nodeRect);
        
        const titleText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        titleText.setAttribute('x', nodeX);
        titleText.setAttribute('y', nodeY - totalHeight/2 + 20);
        titleText.setAttribute('text-anchor', 'middle');
        titleText.setAttribute('fill', '#fff');
        titleText.setAttribute('font-size', '12');
        titleText.setAttribute('font-weight', 'bold');
        const nodeName = textElement.textContent;
        titleText.textContent = nodeName.length > 20 ? nodeName.substring(0, 20) + '...' : nodeName;
        expandedGroup.appendChild(titleText);
        
        const separatorLine = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        separatorLine.setAttribute('x1', nodeX - nodeWidth/2 + 10);
        separatorLine.setAttribute('y1', nodeY - totalHeight/2 + 30);
        separatorLine.setAttribute('x2', nodeX + nodeWidth/2 - 10);
        separatorLine.setAttribute('y2', nodeY - totalHeight/2 + 30);
        separatorLine.setAttribute('stroke', '#fff');
        separatorLine.setAttribute('stroke-width', '1');
        expandedGroup.appendChild(separatorLine);
        
        columns.forEach((column, index) => {
            const columnY = nodeY - totalHeight/2 + 45 + (index * columnHeight);
            
            const columnText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            columnText.setAttribute('x', nodeX - nodeWidth/2 + 15);
            columnText.setAttribute('y', columnY);
            columnText.setAttribute('fill', '#fff');
            columnText.setAttribute('font-size', '10');
            columnText.setAttribute('font-weight', '500');
            const columnName = column.name.length > 15 ? column.name.substring(0, 15) + '...' : column.name;
            columnText.textContent = columnName;
            expandedGroup.appendChild(columnText);
            
            const typeText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            typeText.setAttribute('x', nodeX + nodeWidth/2 - 15);
            typeText.setAttribute('y', columnY);
            typeText.setAttribute('text-anchor', 'end');
            typeText.setAttribute('fill', '#fff');
            typeText.setAttribute('font-size', '9');
            typeText.setAttribute('opacity', '0.8');
            typeText.textContent = column.type;
            expandedGroup.appendChild(typeText);
            
            const connectionPoint = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            connectionPoint.setAttribute('cx', nodeX + nodeWidth/2);
            connectionPoint.setAttribute('cy', columnY - 5);
            connectionPoint.setAttribute('r', '3');
            connectionPoint.setAttribute('fill', '#fff');
            connectionPoint.setAttribute('stroke', circleElement.getAttribute('fill'));
            connectionPoint.setAttribute('stroke-width', '2');
            connectionPoint.setAttribute('data-column', column.name);
            connectionPoint.setAttribute('data-node', nodeId);
            connectionPoint.style.cursor = 'pointer';
            
            connectionPoint.addEventListener('mouseenter', () => {
                connectionPoint.setAttribute('r', '5');
                highlightColumnMappings(nodeId, column.name);
            });
            
            connectionPoint.addEventListener('mouseleave', () => {
                connectionPoint.setAttribute('r', '3');
                clearColumnMappingHighlights();
            });
            
            expandedGroup.appendChild(connectionPoint);
        });
        
        const collapseButton = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        collapseButton.setAttribute('cx', nodeX + nodeWidth/2 - 15);
        collapseButton.setAttribute('cy', nodeY - totalHeight/2 + 15);
        collapseButton.setAttribute('r', '8');
        collapseButton.setAttribute('fill', '#dc3545');
        collapseButton.setAttribute('stroke', '#fff');
        collapseButton.setAttribute('stroke-width', '2');
        collapseButton.style.cursor = 'pointer';
        
        const collapseIcon = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        collapseIcon.setAttribute('x', nodeX + nodeWidth/2 - 15);
        collapseIcon.setAttribute('y', nodeY - totalHeight/2 + 19);
        collapseIcon.setAttribute('text-anchor', 'middle');
        collapseIcon.setAttribute('fill', '#fff');
        collapseIcon.setAttribute('font-size', '10');
        collapseIcon.setAttribute('font-weight', 'bold');
        collapseIcon.textContent = '×';
        collapseIcon.style.pointerEvents = 'none';
        
        collapseButton.addEventListener('click', () => {
            collapseNodeColumns(nodeId, circleElement, textElement, typeTextElement);
        });
        
        expandedGroup.appendChild(collapseButton);
        expandedGroup.appendChild(collapseIcon);
        
        graphGroup.appendChild(expandedGroup);
        
        circleElement.setAttribute('data-expanded', 'true');
        
        setTimeout(() => {
            drawColumnConnections(nodeId, columns);
        }, 100);
        
        
    } catch (error) {
        showNotification('Error loading column details: ' + error.message, 'danger');
    }
}

function collapseNodeColumns(nodeId, circleElement, textElement, typeTextElement) {
    const svg = circleElement.closest('svg');
    const expandedGroup = svg.querySelector(`#expanded-${nodeId}`);
    
    if (expandedGroup) {
        expandedGroup.remove();
    }
    
    circleElement.style.display = 'block';
    textElement.style.display = 'block';
    typeTextElement.style.display = 'block';
    
    circleElement.setAttribute('data-expanded', 'false');
    
    clearColumnMappingHighlights();
    clearColumnConnections(nodeId);
    
}

function highlightColumnMappings(nodeId, columnName) {
    if (!window.currentColumnLineage || !window.currentColumnLineage.columns) return;
    
    const column = window.currentColumnLineage.columns.find(col => col.name === columnName);
    if (!column) return;
    
    
    const svg = document.querySelector('#lineage-graph svg');
    const connectionPoints = svg.querySelectorAll(`[data-column="${columnName}"][data-node="${nodeId}"]`);
    
    connectionPoints.forEach(point => {
        point.setAttribute('fill', '#ffc107');
        point.setAttribute('stroke', '#ff6b35');
    });
}

function clearColumnMappingHighlights() {
    const svg = document.querySelector('#lineage-graph svg');
    if (!svg) return;
    
    const connectionPoints = svg.querySelectorAll('[data-column]');
    connectionPoints.forEach(point => {
        const nodeId = point.getAttribute('data-node');
        const originalNode = svg.querySelector(`[data-node-id="${nodeId}"]`);
        if (originalNode) {
            point.setAttribute('fill', '#fff');
            point.setAttribute('stroke', originalNode.getAttribute('fill'));
        }
    });
}

async function drawColumnConnections(sourceNodeId, sourceColumns) {
    try {
        
        const svg = document.querySelector('#lineage-graph svg');
        const graphGroup = svg.querySelector('#graph-group');
        
        const expandedNodes = svg.querySelectorAll('[id^="expanded-"]');
        
        for (const sourceColumn of sourceColumns) {
            const upstreamColumns = sourceColumn.upstream_columns || [];
            const downstreamColumns = sourceColumn.downstream_columns || [];
            
            for (const upstream of upstreamColumns) {
                await drawColumnConnection(
                    sourceNodeId, sourceColumn.name,
                    upstream.asset, upstream.column,
                    'upstream', upstream.confidence,
                    graphGroup
                );
            }
            
            for (const downstream of downstreamColumns) {
                await drawColumnConnection(
                    sourceNodeId, sourceColumn.name,
                    downstream.asset, downstream.column,
                    'downstream', downstream.confidence,
                    graphGroup
                );
            }
        }
        
        
    } catch (error) {
    }
}

async function drawColumnConnection(sourceNodeId, sourceColumnName, targetAssetName, targetColumnName, direction, confidence, graphGroup) {
    try {
        const targetNodeId = await findNodeIdByAssetName(targetAssetName);
        if (!targetNodeId) {
            return;
        }
        
        const targetExpandedNode = document.querySelector(`#expanded-${targetNodeId}`);
        if (!targetExpandedNode) {
            return;
        }
        
        const sourceConnectionPoint = document.querySelector(`[data-node="${sourceNodeId}"][data-column="${sourceColumnName}"]`);
        if (!sourceConnectionPoint) {
            return;
        }
        
        const targetConnectionPoint = document.querySelector(`[data-node="${targetNodeId}"][data-column="${targetColumnName}"]`);
        if (!targetConnectionPoint) {
            return;
        }
        
        const sourceX = parseFloat(sourceConnectionPoint.getAttribute('cx'));
        const sourceY = parseFloat(sourceConnectionPoint.getAttribute('cy'));
        const targetX = parseFloat(targetConnectionPoint.getAttribute('cx'));
        const targetY = parseFloat(targetConnectionPoint.getAttribute('cy'));
        
        const connectionLine = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        
        const midX = (sourceX + targetX) / 2;
        const controlOffset = Math.abs(targetX - sourceX) * 0.3;
        const controlX1 = sourceX + (direction === 'downstream' ? controlOffset : -controlOffset);
        const controlX2 = targetX + (direction === 'downstream' ? -controlOffset : controlOffset);
        
        const pathData = `M ${sourceX} ${sourceY} C ${controlX1} ${sourceY}, ${controlX2} ${targetY}, ${targetX} ${targetY}`;
        connectionLine.setAttribute('d', pathData);
        
        const lineColor = direction === 'upstream' ? '#28a745' : '#007bff';
        const lineOpacity = Math.max(0.4, confidence || 0.7);
        
        connectionLine.setAttribute('stroke', lineColor);
        connectionLine.setAttribute('stroke-width', '2');
        connectionLine.setAttribute('fill', 'none');
        connectionLine.setAttribute('opacity', lineOpacity);
        connectionLine.setAttribute('stroke-dasharray', confidence > 0.8 ? 'none' : '5,5');
        connectionLine.setAttribute('class', 'column-connection-line');
        connectionLine.setAttribute('data-source-node', sourceNodeId);
        connectionLine.setAttribute('data-target-node', targetNodeId);
        connectionLine.setAttribute('data-source-column', sourceColumnName);
        connectionLine.setAttribute('data-target-column', targetColumnName);
        connectionLine.setAttribute('data-direction', direction);
        
        connectionLine.addEventListener('mouseenter', () => {
            connectionLine.setAttribute('stroke-width', '3');
            connectionLine.setAttribute('opacity', '1');
            
            sourceConnectionPoint.setAttribute('r', '6');
            targetConnectionPoint.setAttribute('r', '6');
            sourceConnectionPoint.setAttribute('fill', lineColor);
            targetConnectionPoint.setAttribute('fill', lineColor);
        });
        
        connectionLine.addEventListener('mouseleave', () => {
            connectionLine.setAttribute('stroke-width', '2');
            connectionLine.setAttribute('opacity', lineOpacity);
            
            sourceConnectionPoint.setAttribute('r', '3');
            targetConnectionPoint.setAttribute('r', '3');
            sourceConnectionPoint.setAttribute('fill', '#fff');
            targetConnectionPoint.setAttribute('fill', '#fff');
        });
        
        connectionLine.addEventListener('click', () => {
            showColumnConnectionDetails(sourceColumnName, targetColumnName, direction, confidence);
        });
        
        graphGroup.insertBefore(connectionLine, graphGroup.firstChild);
        
        
    } catch (error) {
    }
}

async function findNodeIdByAssetName(assetName) {
    try {
        if (!window.currentLineageData || !window.currentLineageData.nodes) {
            return null;
        }
        
        const matchingNode = window.currentLineageData.nodes.find(node => 
            node.name === assetName || 
            node.name.includes(assetName) ||
            assetName.includes(node.name)
        );
        
        return matchingNode ? matchingNode.id : null;
        
    } catch (error) {
        return null;
    }
}

function clearColumnConnections(nodeId) {
    const svg = document.querySelector('#lineage-graph svg');
    if (!svg) return;
    
    const connectionLines = svg.querySelectorAll(`.column-connection-line[data-source-node="${nodeId}"], .column-connection-line[data-target-node="${nodeId}"]`);
    connectionLines.forEach(line => line.remove());
    
}

function showColumnConnectionDetails(sourceColumn, targetColumn, direction, confidence) {
    const details = `
        <strong>Column Mapping Details</strong><br>
        Source: ${sourceColumn}<br>
        Target: ${targetColumn}<br>
        Direction: ${direction}<br>
        Confidence: ${(confidence * 100).toFixed(1)}%
    `;
    
    showNotification(details, 'info', 5000);
}

function displayLineageTable(lineageData) {
    const tableBody = document.getElementById('lineage-table-body');
    const nodes = lineageData.nodes || [];
    
    if (nodes.length === 0) {
        tableBody.innerHTML = `
            <tr>
                <td colspan="7" class="text-center text-muted py-4">
                    No lineage data available for this asset.
                </td>
            </tr>
        `;
        return;
    }
    
    tableBody.innerHTML = '';
    
    nodes.forEach(node => {
        if (!node || !node.id) {
            return;
        }
        
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>
                <div class="d-flex align-items-center">
                    <i class="fas fa-${getAssetIcon(node.type)} me-2 text-${node.is_target ? 'primary' : (node.direction === 'upstream' ? 'success' : 'danger')}"></i>
                    <strong>${node.name}</strong>
                </div>
            </td>
            <td><span class="badge bg-secondary">${node.type}</span></td>
            <td>${node.source}</td>
            <td>
                <span class="badge bg-${node.is_target ? 'primary' : (node.direction === 'upstream' ? 'success' : 'danger')}">
                    ${node.is_target ? 'Target' : (node.direction === 'upstream' ? 'Upstream' : 'Downstream')}
                </span>
            </td>
            <td>${Math.abs(node.level)}</td>
            <td>${node.schema ? node.schema.length : 0} columns</td>
            <td>
                <button class="btn btn-sm btn-outline-primary" onclick="showColumnLineage('${node.id}')" title="View column lineage">
                    <i class="fas fa-columns"></i>
                </button>
            </td>
        `;
        tableBody.appendChild(row);
    });
}

function getAssetIcon(type) {
    const iconMap = {
        'table': 'table',
        'view': 'eye',
        'file': 'file',
        'database': 'database',
        'stream': 'stream',
        'api': 'exchange-alt'
    };
    return iconMap[type.toLowerCase()] || 'cube';
}

async function loadColumnLineage(assetId) {
    try {
        const response = await apiCall(`/lineage/${assetId}/columns`);
        
        
        if (response.status === 'success') {
            window.currentColumnLineage = response.column_lineage;
            
            if (window.currentColumnLineage && window.currentColumnLineage.columns && Array.isArray(window.currentColumnLineage.columns) && window.currentColumnLineage.columns.length > 0) {
                showColumnLineage(assetId);
            } else {
                showColumnLineageEmpty();
            }
        } else {
            showColumnLineageEmpty();
        }
        
    } catch (error) {
    }
}

function showColumnLineage(assetId) {
    const columnCard = document.getElementById('column-lineage-card');
    
    
    columnCard.style.display = 'block';
    
    if (window.currentColumnLineage && window.currentColumnLineage.columns) {
        const columns = window.currentColumnLineage.columns;
        
        updateColumnLineageHeader(assetId, columns);
        
        switchColumnView('detailed');
        
        populateDetailedView(columns);
        populateMatrixView(columns);
        populateFlowView(columns);
        
    } else {
        showColumnLineageEmpty();
    }
    
}

function updateColumnLineageHeader(assetId, columns) {
    if (!columns || !Array.isArray(columns)) {
        columns = [];
    }
    
    
    let assetName = 'Unknown Asset';
    if (window.currentLineageData && window.currentLineageData.nodes) {
        const asset = window.currentLineageData.nodes.find(node => node && node.id === assetId);
        if (asset) {
            assetName = asset.name;
        }
    }
    
    let totalRelationships = 0;
    columns.forEach(column => {
        const upstreamCount = (column.upstream_columns || []).length;
        const downstreamCount = (column.downstream_columns || []).length;
        totalRelationships += upstreamCount + downstreamCount;
    });
    
    
    document.getElementById('column-lineage-asset-name').textContent = assetName;
    document.getElementById('column-lineage-column-count').textContent = columns.length;
    document.getElementById('column-lineage-relationship-count').textContent = totalRelationships;
    
}

function switchColumnView(viewType) {
    document.getElementById('column-lineage-empty').style.display = 'none';
    document.getElementById('column-lineage-detailed').style.display = 'none';
    document.getElementById('column-lineage-matrix').style.display = 'none';
    document.getElementById('column-lineage-flow').style.display = 'none';
    
    document.querySelectorAll('[id^="column-view-"]').forEach(btn => {
        btn.classList.remove('active');
    });
    
    document.getElementById(`column-lineage-${viewType}`).style.display = 'block';
    document.getElementById(`column-view-${viewType}`).classList.add('active');
}

function populateDetailedView(columns) {
    const container = document.getElementById('column-lineage-cards');
    
    let html = '';
    columns.forEach(column => {
        const upstreamCount = (column.upstream_columns || []).length;
        const downstreamCount = (column.downstream_columns || []).length;
        const avgConfidence = calculateAverageConfidence(column);
        
        html += `
            <div class="col-md-6 mb-3">
                <div class="card h-100 border-start-primary">
                    <div class="card-header bg-light">
                        <div class="d-flex justify-content-between align-items-center">
                            <h6 class="mb-0">
                                <i class="fas fa-columns me-2 text-primary"></i>
                                ${column.name}
                            </h6>
                            <span class="badge bg-secondary">${column.type}</span>
                        </div>
                    </div>
                    <div class="card-body">
                        ${column.description ? `
                            <p class="text-muted small mb-3">${column.description}</p>
                        ` : ''}
                        
                        <!-- Relationship Summary -->
                        <div class="row mb-3">
                            <div class="col-4 text-center">
                                <div class="text-success">
                                    <i class="fas fa-arrow-left"></i>
                                    <div class="fw-bold">${upstreamCount}</div>
                                    <small>Upstream</small>
                                </div>
                            </div>
                            <div class="col-4 text-center">
                                <div class="text-info">
                                    <i class="fas fa-arrow-right"></i>
                                    <div class="fw-bold">${downstreamCount}</div>
                                    <small>Downstream</small>
                                </div>
                            </div>
                            <div class="col-4 text-center">
                                <div class="text-warning">
                                    <i class="fas fa-percentage"></i>
                                    <div class="fw-bold">${avgConfidence}%</div>
                                    <small>Confidence</small>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Upstream Sources -->
                        ${upstreamCount > 0 ? `
                            <div class="mb-3">
                                <h6 class="text-success mb-2">
                                    <i class="fas fa-arrow-left me-1"></i>Upstream Sources
                                </h6>
                                ${column.upstream_columns.map(upstream => `
                                    <div class="d-flex justify-content-between align-items-center mb-1 p-2 bg-light rounded">
                                        <div>
                                            <small class="fw-bold">${upstream.asset}</small><br>
                                            <small class="text-muted">${upstream.column}</small>
                                        </div>
                                        <span class="badge bg-success">${(upstream.confidence * 100).toFixed(0)}%</span>
                                    </div>
                                `).join('')}
                            </div>
                        ` : ''}
                        
                        <!-- Downstream Targets -->
                        ${downstreamCount > 0 ? `
                            <div class="mb-3">
                                <h6 class="text-info mb-2">
                                    <i class="fas fa-arrow-right me-1"></i>Downstream Targets
                                </h6>
                                ${column.downstream_columns.map(downstream => `
                                    <div class="d-flex justify-content-between align-items-center mb-1 p-2 bg-light rounded">
                                        <div>
                                            <small class="fw-bold">${downstream.asset}</small><br>
                                            <small class="text-muted">${downstream.column}</small>
                                        </div>
                                        <span class="badge bg-info">${(downstream.confidence * 100).toFixed(0)}%</span>
                                    </div>
                                `).join('')}
                            </div>
                        ` : ''}
                        
                        <!-- Transformations -->
                        ${column.transformations && column.transformations.length > 0 ? `
                            <div>
                                <h6 class="text-warning mb-2">
                                    <i class="fas fa-cogs me-1"></i>Transformations
                                </h6>
                                <div>
                                    ${column.transformations.map(transform => `
                                        <span class="badge bg-warning text-dark me-1 mb-1">${transform}</span>
                                    `).join('')}
                                </div>
                            </div>
                        ` : ''}
                    </div>
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html;
}

function populateMatrixView(columns) {
    const tbody = document.getElementById('column-lineage-matrix-body');
    
    let html = '';
    columns.forEach(column => {
        const upstreamList = (column.upstream_columns || []).map(u => `${u.asset}.${u.column}`).join(', ') || 'None';
        const downstreamList = (column.downstream_columns || []).map(d => `${d.asset}.${d.column}`).join(', ') || 'None';
        const transformList = (column.transformations || []).join(', ') || 'None';
        const avgConfidence = calculateAverageConfidence(column);
        
        html += `
            <tr>
                <td>
                    <strong>${column.name}</strong>
                    ${column.description ? `<br><small class="text-muted">${column.description}</small>` : ''}
                </td>
                <td><span class="badge bg-secondary">${column.type}</span></td>
                <td class="text-success">
                    ${upstreamList !== 'None' ? `<small>${upstreamList}</small>` : '<span class="text-muted">None</span>'}
                </td>
                <td class="text-info">
                    ${downstreamList !== 'None' ? `<small>${downstreamList}</small>` : '<span class="text-muted">None</span>'}
                </td>
                <td>
                    ${transformList !== 'None' ? 
                        column.transformations.map(t => `<span class="badge bg-warning text-dark me-1">${t}</span>`).join('') : 
                        '<span class="text-muted">None</span>'
                    }
                </td>
                <td>
                    <div class="progress" style="height: 20px;">
                        <div class="progress-bar bg-warning" role="progressbar" style="width: ${avgConfidence}%">
                            ${avgConfidence}%
                        </div>
                    </div>
                </td>
            </tr>
        `;
    });
    
    tbody.innerHTML = html;
}

function populateFlowView(columns) {
    const upstreamContainer = document.getElementById('column-flow-upstream');
    const currentContainer = document.getElementById('column-flow-current');
    const downstreamContainer = document.getElementById('column-flow-downstream');
    
    const upstreamAssets = new Set();
    const downstreamAssets = new Set();
    
    columns.forEach(column => {
        (column.upstream_columns || []).forEach(u => upstreamAssets.add(u.asset));
        (column.downstream_columns || []).forEach(d => downstreamAssets.add(d.asset));
    });
    
    let upstreamHtml = '';
    upstreamAssets.forEach(asset => {
        upstreamHtml += `
            <div class="mb-2 p-2 bg-light rounded">
                <div class="fw-bold text-success">${asset}</div>
                <small class="text-muted">Source Asset</small>
            </div>
        `;
    });
    upstreamContainer.innerHTML = upstreamHtml || '<div class="text-muted text-center py-3">No upstream sources</div>';
    
    let currentHtml = '';
    columns.forEach(column => {
        const relationshipCount = (column.upstream_columns || []).length + (column.downstream_columns || []).length;
        currentHtml += `
            <div class="mb-2 p-2 border rounded ${relationshipCount > 0 ? 'border-primary bg-primary bg-opacity-10' : 'border-secondary'}">
                <div class="d-flex justify-content-between align-items-center">
                    <div>
                        <div class="fw-bold">${column.name}</div>
                        <small class="text-muted">${column.type}</small>
                    </div>
                    ${relationshipCount > 0 ? `<span class="badge bg-primary">${relationshipCount}</span>` : ''}
                </div>
            </div>
        `;
    });
    currentContainer.innerHTML = currentHtml;
    
    let downstreamHtml = '';
    downstreamAssets.forEach(asset => {
        downstreamHtml += `
            <div class="mb-2 p-2 bg-light rounded">
                <div class="fw-bold text-info">${asset}</div>
                <small class="text-muted">Target Asset</small>
            </div>
        `;
    });
    downstreamContainer.innerHTML = downstreamHtml || '<div class="text-muted text-center py-3">No downstream targets</div>';
}

function showColumnLineageEmpty() {
    
    document.getElementById('column-lineage-empty').style.display = 'block';
    document.getElementById('column-lineage-detailed').style.display = 'none';
    document.getElementById('column-lineage-matrix').style.display = 'none';
    document.getElementById('column-lineage-flow').style.display = 'none';
    
    document.getElementById('column-lineage-asset-name').textContent = 'None';
    document.getElementById('column-lineage-column-count').textContent = '0';
    document.getElementById('column-lineage-relationship-count').textContent = '0';
    
    const columnCard = document.getElementById('column-lineage-card');
    columnCard.style.display = 'block';
}

function calculateAverageConfidence(column) {
    const allRelationships = [...(column.upstream_columns || []), ...(column.downstream_columns || [])];
    if (allRelationships.length === 0) return 0;
    
    const totalConfidence = allRelationships.reduce((sum, rel) => sum + (rel.confidence || 0), 0);
    return Math.round((totalConfidence / allRelationships.length) * 100);
}

function refreshColumnLineage() {
    if (!window.selectedAssetId) {
        showNotification('No asset selected for column lineage refresh', 'warning');
        return;
    }
    
    
    const columnCard = document.getElementById('column-lineage-card');
    columnCard.style.display = 'block';
    
    window.currentColumnLineage = null;
    
    loadColumnLineage(window.selectedAssetId);
}

window.testColumnLineage = async function() {
    
    const testAssetId = 'a56f8489aed6aae91a2750768f01453f';
    
    
    window.selectedAssetId = testAssetId;
    
    const columnCard = document.getElementById('column-lineage-card');
    columnCard.style.display = 'block';
    
    await loadColumnLineage(testAssetId);
    
};

function switchLineageView(viewType) {
    const graphView = document.getElementById('lineage-graph-view');
    const tableView = document.getElementById('lineage-table-view');
    
    if (viewType === 'graph') {
        graphView.style.display = 'block';
        tableView.style.display = 'none';
        
        if (window.currentLineageData) {
            displayLineageGraph(window.currentLineageData);
        }
    } else {
        graphView.style.display = 'none';
        tableView.style.display = 'block';
        
        if (window.currentLineageData) {
            displayLineageTable(window.currentLineageData);
        }
    }
}

function showLineageLoading() {
    const graphContainer = document.getElementById('lineage-graph');
    graphContainer.innerHTML = `
        <div class="d-flex align-items-center justify-content-center h-100">
            <div class="text-center">
                <div class="spinner-border text-primary mb-3" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
                <p class="text-muted">Loading lineage data...</p>
            </div>
        </div>
    `;
}

function clearLineageVisualization() {
    const graphContainer = document.getElementById('lineage-graph');
    const tableBody = document.getElementById('lineage-table-body');
    const summaryDiv = document.getElementById('lineage-summary');
    const columnCard = document.getElementById('column-lineage-card');
    
    graphContainer.innerHTML = `
        <div class="d-flex align-items-center justify-content-center h-100 text-muted">
            <div class="text-center">
                <i class="fas fa-project-diagram fa-4x mb-3 opacity-25"></i>
                <h5>No Asset Selected</h5>
                <p>Select an asset from the dropdown above to view its data lineage</p>
            </div>
        </div>
    `;
    
    tableBody.innerHTML = `
        <tr>
            <td colspan="7" class="text-center text-muted py-4">
                No lineage data available. Select an asset to view its lineage.
            </td>
        </tr>
    `;
    
    summaryDiv.innerHTML = `
        <div class="text-center text-muted py-3">
            <i class="fas fa-project-diagram fa-3x mb-3 opacity-50"></i>
            <p>Select an asset to view its lineage summary</p>
        </div>
    `;
    
    columnCard.style.display = 'none';
    
    window.currentLineageData = null;
    window.selectedAssetId = null;
}

function searchLineageAssets() {
    const searchTerm = document.getElementById('lineage-asset-search').value.toLowerCase();
    const assetSelect = document.getElementById('lineage-asset-select');
    
    Array.from(assetSelect.options).forEach(option => {
        if (option.value === '') return; // Skip the default option
        
        const asset = JSON.parse(option.dataset.asset);
        const searchText = `${asset.name} ${asset.type} ${asset.source}`.toLowerCase();
        
        if (searchText.includes(searchTerm)) {
            option.style.display = 'block';
        } else {
            option.style.display = 'none';
        }
    });
    
    if (searchTerm === '') {
        Array.from(assetSelect.options).forEach(option => {
            option.style.display = 'block';
        });
    }
}

function refreshLineage() {
    if (window.selectedAssetId) {
        loadAssetLineage(window.selectedAssetId);
    }
    
    if (currentAssets && Object.keys(currentAssets).length > 0) {
        loadLineageAssetsFromCache();
    } else {
        loadLineageAssets();
    }
    
    showNotification('Lineage data refreshed', 'success');
}

function fitLineageToScreen() {
    try {
        const graphContainer = document.getElementById('lineage-graph');
        if (!graphContainer) {
            showNotification('No lineage graph found to fit', 'warning');
            return;
        }
        
        const svg = graphContainer.querySelector('svg');
        if (!svg) {
            showNotification('No lineage visualization to fit', 'warning');
            return;
        }
        
        const nodes = svg.querySelectorAll('circle');
        if (nodes.length === 0) {
            showNotification('No nodes found in lineage graph', 'warning');
            return;
        }
        
        let minX = Infinity, minY = Infinity;
        let maxX = -Infinity, maxY = -Infinity;
        
        nodes.forEach(node => {
            const cx = parseFloat(node.getAttribute('cx')) || 0;
            const cy = parseFloat(node.getAttribute('cy')) || 0;
            const r = parseFloat(node.getAttribute('r')) || 20;
            
            minX = Math.min(minX, cx - r);
            minY = Math.min(minY, cy - r);
            maxX = Math.max(maxX, cx + r);
            maxY = Math.max(maxY, cy + r);
        });
        
        const padding = Math.max(50, Math.max(maxX - minX, maxY - minY) * 0.2);
        minX -= padding;
        minY -= padding;
        maxX += padding;
        maxY += padding;
        
        const width = maxX - minX;
        const height = maxY - minY;
        
        svg.setAttribute('viewBox', `${minX} ${minY} ${width} ${height}`);
        
        svg.style.width = '100%';
        svg.style.height = 'auto';
        svg.style.maxHeight = '600px';
        
        if (!window.graphFitNotificationShown) {
            showNotification('Graph fitted to screen successfully', 'success');
            window.graphFitNotificationShown = true;
        }
        
        
    } catch (error) {
        showNotification('Error fitting graph to screen: ' + error.message, 'danger');
    }
}

function exportLineage() {
    if (!window.currentLineageData) {
        showNotification('No lineage data to export', 'warning');
        return;
    }
    
    const exportData = {
        asset: window.selectedAssetId,
        timestamp: new Date().toISOString(),
        lineage: window.currentLineageData,
        column_lineage: window.currentColumnLineage
    };
    
    const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    const link = document.createElement('a');
    link.href = url;
    link.download = `lineage_${window.selectedAssetId}_${new Date().toISOString().split('T')[0]}.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    URL.revokeObjectURL(url);
    showNotification('Lineage data exported successfully', 'success');
}
window.lineageEditMode = false;
window.selectedSourceNode = null;
window.customRelationships = [];
window.pendingRelationship = null;

function toggleLineageEditMode() {
    window.lineageEditMode = !window.lineageEditMode;
    const toggleBtn = document.getElementById('toggle-edit-mode');
    const instructions = document.getElementById('edit-mode-instructions');
    
    if (window.lineageEditMode) {
        toggleBtn.innerHTML = '<i class="fas fa-eye me-1"></i>View Mode';
        toggleBtn.className = 'btn btn-warning btn-sm';
        instructions.style.display = 'block';
        document.getElementById('relationship-default-state').style.display = 'none';
        
        updateGraphForEditMode(true);
        showNotification('Edit mode enabled - Click nodes to create relationships', 'info');
    } else {
        toggleBtn.innerHTML = '<i class="fas fa-edit me-1"></i>Edit Mode';
        toggleBtn.className = 'btn btn-outline-primary btn-sm';
        instructions.style.display = 'none';
        document.getElementById('relationship-default-state').style.display = 'block';
        document.getElementById('relationship-form').style.display = 'none';
        
        window.selectedSourceNode = null;
        window.pendingRelationship = null;
        
        updateGraphForEditMode(false);
        showNotification('Edit mode disabled', 'info');
    }
}

function updateGraphForEditMode(isEditMode) {
    const nodes = document.querySelectorAll('.lineage-node');
    const edges = document.querySelectorAll('.lineage-edge');
    
    nodes.forEach(node => {
        if (isEditMode) {
            node.style.cursor = 'crosshair';
            node.setAttribute('data-editable', 'true');
        } else {
            node.style.cursor = 'pointer';
            node.removeAttribute('data-editable');
            node.classList.remove('selected-source', 'potential-target');
        }
    });
    
    edges.forEach(edge => {
        if (isEditMode) {
            edge.style.cursor = 'context-menu';
            edge.setAttribute('data-deletable', 'true');
        } else {
            edge.style.cursor = 'default';
            edge.removeAttribute('data-deletable');
        }
    });
}

function handleEditModeNodeClick(nodeId, nodeElement) {
    if (!window.lineageEditMode) return false;
    
    if (!window.selectedSourceNode) {
        window.selectedSourceNode = { id: nodeId, element: nodeElement };
        nodeElement.classList.add('selected-source');
        
        const allNodes = document.querySelectorAll('.lineage-node');
        allNodes.forEach(node => {
            if (node !== nodeElement) {
                node.classList.add('potential-target');
            }
        });
        
        showNotification('Source selected. Click another node to create relationship.', 'info');
        return true;
    } else if (window.selectedSourceNode.id !== nodeId) {
        window.pendingRelationship = {
            source: window.selectedSourceNode.id,
            target: nodeId
        };
        
        showRelationshipForm();
        return true;
    } else {
        deselectSourceNode();
        return true;
    }
}

function deselectSourceNode() {
    if (window.selectedSourceNode) {
        window.selectedSourceNode.element.classList.remove('selected-source');
        window.selectedSourceNode = null;
    }
    
    const allNodes = document.querySelectorAll('.lineage-node');
    allNodes.forEach(node => {
        node.classList.remove('potential-target');
    });
}

function addNewRelationship() {
    document.getElementById('relationship-default-state').style.display = 'none';
    document.getElementById('edit-mode-instructions').style.display = 'none';
    document.getElementById('relationship-form').style.display = 'block';
    
    populateAssetDropdowns();
    
    document.getElementById('source-asset-select').value = '';
    document.getElementById('target-asset-select').value = '';
    document.getElementById('relationship-type').value = 'feeds_into';
    document.getElementById('confidence-level').value = '0.9';
    document.getElementById('confidence-display').textContent = '0.9';
    document.getElementById('relationship-description').value = '';
}

function showRelationshipForm() {
    document.getElementById('relationship-default-state').style.display = 'none';
    document.getElementById('edit-mode-instructions').style.display = 'none';
    document.getElementById('relationship-form').style.display = 'block';
    
    populateAssetDropdowns();
    
    if (window.pendingRelationship) {
        document.getElementById('source-asset-select').value = window.pendingRelationship.source;
        document.getElementById('target-asset-select').value = window.pendingRelationship.target;
    }
}

function populateAssetDropdowns() {
    const sourceSelect = document.getElementById('source-asset-select');
    const targetSelect = document.getElementById('target-asset-select');
    
    sourceSelect.innerHTML = '<option value="">Select source asset...</option>';
    targetSelect.innerHTML = '<option value="">Select target asset...</option>';
    
    if (window.currentLineageData && window.currentLineageData.nodes) {
        window.currentLineageData.nodes.forEach(node => {
            if (!node || !node.id) {
                return;
            }
            
            const option1 = document.createElement('option');
            option1.value = node.id;
            option1.textContent = `${node.name} (${node.type})`;
            sourceSelect.appendChild(option1);
            
            const option2 = document.createElement('option');
            option2.value = node.id;
            option2.textContent = `${node.name} (${node.type})`;
            targetSelect.appendChild(option2);
        });
    }
}

document.addEventListener('DOMContentLoaded', function() {
    const confidenceSlider = document.getElementById('confidence-level');
    if (confidenceSlider) {
        confidenceSlider.addEventListener('input', function() {
            document.getElementById('confidence-display').textContent = this.value;
        });
    }
});

async function saveRelationship() {
    const sourceId = document.getElementById('source-asset-select').value;
    const targetId = document.getElementById('target-asset-select').value;
    const relationshipType = document.getElementById('relationship-type').value;
    const confidence = parseFloat(document.getElementById('confidence-level').value);
    const description = document.getElementById('relationship-description').value;
    
    if (!sourceId || !targetId) {
        showNotification('Please select both source and target assets', 'error');
        return;
    }
    
    if (sourceId === targetId) {
        showNotification('Source and target cannot be the same asset', 'error');
        return;
    }
    
    const relationship = {
        source: sourceId,
        target: targetId,
        relationship: relationshipType,
        confidence: confidence,
        description: description,
        created_by: 'user',
        created_at: new Date().toISOString()
    };
    
    try {
        const response = await apiCall('/api/lineage/relationships', {
            method: 'POST',
            body: JSON.stringify(relationship)
        });
        
        if (response.status === 'success') {
            window.customRelationships.push(relationship);
            
            if (window.selectedAssetId) {
                await loadLineage(window.selectedAssetId);
            }
            
            showNotification('Relationship saved successfully', 'success');
            cancelRelationship();
        } else {
            showNotification('Failed to save relationship: ' + response.message, 'error');
        }
    } catch (error) {
        showNotification('Error saving relationship', 'error');
    }
}

function cancelRelationship() {
    document.getElementById('relationship-form').style.display = 'none';
    
    if (window.lineageEditMode) {
        document.getElementById('edit-mode-instructions').style.display = 'block';
    } else {
        document.getElementById('relationship-default-state').style.display = 'block';
    }
    
    deselectSourceNode();
    window.pendingRelationship = null;
}

async function showRelationshipList() {
    try {
        const response = await apiCall('/api/lineage/relationships');
        
        if (response.status === 'success') {
            displayRelationshipList(response.relationships);
        } else {
            showNotification('Failed to load relationships', 'error');
        }
    } catch (error) {
        showNotification('Error loading relationships', 'error');
    }
}

function displayRelationshipList(relationships) {
    const modalHtml = `
        <div class="modal fade" id="relationshipListModal" tabindex="-1">
            <div class="modal-dialog modal-lg">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">All Lineage Relationships</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="table-responsive">
                            <table class="table table-striped">
                                <thead>
                                    <tr>
                                        <th>Source</th>
                                        <th>Target</th>
                                        <th>Type</th>
                                        <th>Confidence</th>
                                        <th>Created</th>
                                        <th>Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${relationships.map(rel => `
                                        <tr>
                                            <td>${rel.source_name || rel.source}</td>
                                            <td>${rel.target_name || rel.target}</td>
                                            <td><span class="badge bg-primary">${rel.relationship}</span></td>
                                            <td>${(rel.confidence * 100).toFixed(0)}%</td>
                                            <td>${new Date(rel.created_at).toLocaleDateString()}</td>
                                            <td>
                                                <button class="btn btn-sm btn-outline-danger" onclick="deleteRelationship('${rel.id || rel.source + '-' + rel.target}')">
                                                    <i class="fas fa-trash"></i>
                                                </button>
                                            </td>
                                        </tr>
                                    `).join('')}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    const existingModal = document.getElementById('relationshipListModal');
    if (existingModal) {
        existingModal.remove();
    }
    
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    
    const modal = new bootstrap.Modal(document.getElementById('relationshipListModal'));
    modal.show();
}

async function deleteRelationship(relationshipId) {
    if (!confirm('Are you sure you want to delete this relationship?')) {
        return;
    }
    
    try {
        const response = await apiCall(`/api/lineage/relationships/${relationshipId}`, {
            method: 'DELETE'
        });
        
        if (response.status === 'success') {
            showNotification('Relationship deleted successfully', 'success');
            
            if (window.selectedAssetId) {
                await loadLineage(window.selectedAssetId);
            }
            
            const modal = document.getElementById('relationshipListModal');
            if (modal && modal.style.display !== 'none') {
                showRelationshipList();
            }
        } else {
            showNotification('Failed to delete relationship', 'error');
        }
    } catch (error) {
        showNotification('Error deleting relationship', 'error');
    }
}


function updateAnalysisPanel(assetData, lineageData) {
    if (!assetData || !lineageData) return;
    
    
    updateOverviewTab(assetData, lineageData);
    
    updateTransformationsTab(assetData, lineageData);
    
    updateArchitectureTab(assetData, lineageData);
    
    updateCodeAnalysisTab(assetData, lineageData);
}

function updateOverviewTab(assetData, lineageData) {
    const upstreamCount = lineageData.nodes.filter(node => node.level < 0).length;
    const downstreamCount = lineageData.nodes.filter(node => node.level > 0).length;
    const currentCount = 1; // The selected asset itself
    
    document.getElementById('stat-upstream').textContent = upstreamCount;
    document.getElementById('stat-current').textContent = currentCount;
    document.getElementById('stat-downstream').textContent = downstreamCount;
    
    const summaryHtml = `
        <div class="asset-overview">
            <h6 class="text-primary">${assetData.name}</h6>
            <p class="mb-2"><strong>Type:</strong> ${assetData.type}</p>
            <p class="mb-2"><strong>Source:</strong> ${assetData.source}</p>
            <p class="mb-2"><strong>Total Connections:</strong> ${lineageData.edges.length}</p>
            <p class="mb-0"><strong>Schema Columns:</strong> ${assetData.schema ? assetData.schema.length : 0}</p>
            
            <div class="mt-3">
                <h6>Lineage Flow:</h6>
                <div class="flow-summary">
                    ${upstreamCount > 0 ? `<span class="badge bg-primary me-1">${upstreamCount} Upstream Sources</span>` : ''}
                    <span class="badge bg-success me-1">Current Asset</span>
                    ${downstreamCount > 0 ? `<span class="badge bg-info">${downstreamCount} Downstream Targets</span>` : ''}
                </div>
            </div>
        </div>
    `;
    
    document.getElementById('overview-summary').innerHTML = summaryHtml;
    
    updateDataFlowTimeline(lineageData);
}

function updateDataFlowTimeline(lineageData) {
    const timelineHtml = `
        <div class="timeline">
            <div class="timeline-item">
                <div class="timeline-marker bg-primary"></div>
                <div class="timeline-content">
                    <h6>Data Sources</h6>
                    <p class="mb-0">${lineageData.nodes.filter(n => n.level < 0).map(n => n.name).join(', ') || 'No upstream sources'}</p>
                </div>
            </div>
            <div class="timeline-item">
                <div class="timeline-marker bg-success"></div>
                <div class="timeline-content">
                    <h6>Processing Layer</h6>
                    <p class="mb-0">${lineageData.nodes.filter(n => n.level === 0)[0]?.name || 'Current Asset'}</p>
                </div>
            </div>
            <div class="timeline-item">
                <div class="timeline-marker bg-info"></div>
                <div class="timeline-content">
                    <h6>Output Targets</h6>
                    <p class="mb-0">${lineageData.nodes.filter(n => n.level > 0).map(n => n.name).join(', ') || 'No downstream targets'}</p>
                </div>
            </div>
        </div>
    `;
    
    document.getElementById('overview-timeline').innerHTML = timelineHtml;
}

function updateTransformationsTab(assetData, lineageData) {
    const transformations = extractTransformationsFromLineage(lineageData);
    
    const transformationsHtml = `
        <div class="transformations-list">
            ${transformations.length > 0 ? transformations.map(transform => `
                <div class="transformation-item card mb-2">
                    <div class="card-body">
                        <div class="d-flex justify-content-between align-items-start">
                            <div>
                                <h6 class="mb-1">${transform.type}</h6>
                                <p class="mb-1 text-muted">${transform.description}</p>
                                <small class="text-info">Confidence: ${(transform.confidence * 100).toFixed(0)}%</small>
                            </div>
                            <span class="badge bg-${getTransformationBadgeColor(transform.type)}">${transform.type}</span>
                        </div>
                    </div>
                </div>
            `).join('') : `
                <div class="text-center text-muted py-4">
                    <i class="fas fa-info-circle fa-2x mb-2"></i>
                    <p>No transformations detected for this asset</p>
                </div>
            `}
        </div>
    `;
    
    document.getElementById('transformations-list').innerHTML = transformationsHtml;
    
    updateTransformationChart(transformations);
}

function extractTransformationsFromLineage(lineageData) {
    const transformations = [];
    
    lineageData.edges.forEach(edge => {
        if (!edge || !edge.source || !edge.target) {
            return;
        }
        
        const sourceId = typeof edge.source === 'object' ? (edge.source ? edge.source.id : null) : edge.source;
        const targetId = typeof edge.target === 'object' ? (edge.target ? edge.target.id : null) : edge.target;
        
        if (!sourceId || !targetId) {
            return;
        }
        
        const sourceNode = lineageData.nodes.find(n => n && n.id === sourceId);
        const targetNode = lineageData.nodes.find(n => n && n.id === targetId);
        
        if (edge.relationship && edge.relationship !== 'feeds_into') {
            transformations.push({
                type: edge.relationship,
                description: edge.description || generateTransformationDescription(edge.relationship, sourceNode, targetNode),
                confidence: edge.confidence || 0.8,
                source: sourceNode?.name || edge.source,
                target: targetNode?.name || edge.target,
                isCustom: edge.custom || false
            });
        }
    });
    
    const targetNode = lineageData.nodes.find(n => n && n.is_target);
    const upstreamNodes = lineageData.nodes.filter(n => n && n.level < 0);
    
    if (targetNode && upstreamNodes.length > 0) {
        const schemaTransformations = detectSchemaTransformations(targetNode, upstreamNodes);
        transformations.push(...schemaTransformations);
        
        const typeTransformations = detectTypeBasedTransformations(targetNode, upstreamNodes);
        transformations.push(...typeTransformations);
        
        const patternTransformations = detectPatternTransformations(targetNode, upstreamNodes);
        transformations.push(...patternTransformations);
    }
    
    return transformations;
}

function generateTransformationDescription(relationshipType, sourceNode, targetNode) {
    const sourceName = sourceNode?.name || 'source';
    const targetName = targetNode?.name || 'target';
    
    const descriptions = {
        'derived_from': `${targetName} is derived from ${sourceName} through data transformation`,
        'transforms_to': `${sourceName} transforms data to create ${targetName}`,
        'aggregates_to': `${sourceName} aggregates data to produce ${targetName}`,
        'joins_with': `${sourceName} joins with other sources to create ${targetName}`,
        'filters_to': `${sourceName} applies filtering logic to create ${targetName}`,
        'enriches_to': `${sourceName} enriches data to create ${targetName}`
    };
    
    return descriptions[relationshipType] || `${relationshipType} transformation from ${sourceName} to ${targetName}`;
}

function detectSchemaTransformations(targetNode, upstreamNodes) {
    const transformations = [];
    const targetSchema = targetNode.schema || [];
    
    upstreamNodes.forEach(upstreamNode => {
        const upstreamSchema = upstreamNode.schema || [];
        
        if (targetSchema.length > 0 && upstreamSchema.length > 0) {
            if (targetSchema.length < upstreamSchema.length * 0.7) {
                transformations.push({
                    type: 'column_projection',
                    description: `Column reduction from ${upstreamSchema.length} to ${targetSchema.length} columns`,
                    confidence: 0.85,
                    source: upstreamNode.name,
                    target: targetNode.name,
                    isInferred: true
                });
            }
            
            if (targetSchema.length > upstreamSchema.length * 1.3) {
                transformations.push({
                    type: 'column_expansion',
                    description: `Column expansion from ${upstreamSchema.length} to ${targetSchema.length} columns`,
                    confidence: 0.80,
                    source: upstreamNode.name,
                    target: targetNode.name,
                    isInferred: true
                });
            }
            
            const commonColumns = findCommonColumns(upstreamSchema, targetSchema);
            if (commonColumns.length > 0) {
                transformations.push({
                    type: 'column_mapping',
                    description: `${commonColumns.length} columns mapped between assets`,
                    confidence: 0.90,
                    source: upstreamNode.name,
                    target: targetNode.name,
                    isInferred: true
                });
            }
        }
    });
    
    return transformations;
}

function detectTypeBasedTransformations(targetNode, upstreamNodes) {
    const transformations = [];
    const targetType = targetNode.type.toLowerCase();
    
    upstreamNodes.forEach(upstreamNode => {
        const upstreamType = upstreamNode.type.toLowerCase();
        
        if (upstreamType.includes('table') && targetType.includes('view')) {
            transformations.push({
                type: 'table_to_view',
                description: 'Table data transformed into view with business logic',
                confidence: 0.88,
                source: upstreamNode.name,
                target: targetNode.name,
                isInferred: true
            });
        }
        
        if (upstreamType.includes('raw') && !targetType.includes('raw')) {
            transformations.push({
                type: 'data_cleansing',
                description: 'Raw data cleansing and standardization',
                confidence: 0.85,
                source: upstreamNode.name,
                target: targetNode.name,
                isInferred: true
            });
        }
        
        if (upstreamType.includes('file') && targetType.includes('table')) {
            transformations.push({
                type: 'file_ingestion',
                description: 'File data ingested into structured table',
                confidence: 0.92,
                source: upstreamNode.name,
                target: targetNode.name,
                isInferred: true
            });
        }
    });
    
    return transformations;
}

function detectPatternTransformations(targetNode, upstreamNodes) {
    const transformations = [];
    const targetName = targetNode.name.toLowerCase();
    
    if (targetName.includes('agg') || targetName.includes('summary') || targetName.includes('total')) {
        transformations.push({
            type: 'aggregation',
            description: 'Data aggregation based on naming pattern',
            confidence: 0.75,
            source: upstreamNodes.map(n => n.name).join(', '),
            target: targetNode.name,
            isInferred: true
        });
    }
    
    if (targetName.includes('clean') || targetName.includes('processed') || targetName.includes('refined')) {
        transformations.push({
            type: 'data_processing',
            description: 'Data processing and refinement',
            confidence: 0.78,
            source: upstreamNodes.map(n => n.name).join(', '),
            target: targetNode.name,
            isInferred: true
        });
    }
    
    if (targetName.includes('mart') || targetName.includes('dim_') || targetName.includes('fact_')) {
        transformations.push({
            type: 'dimensional_modeling',
            description: 'Dimensional modeling transformation for analytics',
            confidence: 0.82,
            source: upstreamNodes.map(n => n.name).join(', '),
            target: targetNode.name,
            isInferred: true
        });
    }
    
    return transformations;
}

function findCommonColumns(schema1, schema2) {
    const cols1 = schema1.map(col => typeof col === 'string' ? col.toLowerCase() : col.name.toLowerCase());
    const cols2 = schema2.map(col => typeof col === 'string' ? col.toLowerCase() : col.name.toLowerCase());
    
    return cols1.filter(col => cols2.includes(col));
}

function getTransformationBadgeColor(type) {
    const colorMap = {
        'feeds_into': 'primary',
        'derived_from': 'success',
        'transforms_to': 'warning',
        'aggregates_to': 'info',
        'joins_with': 'secondary',
        'data_aggregation': 'info',
        'data_storage': 'success',
        'data_filtering': 'warning'
    };
    
    return colorMap[type] || 'secondary';
}

function updateTransformationChart(transformations) {
    const canvas = document.getElementById('transformationChart');
    if (!canvas) return;
    
    const ctx = canvas.getContext('2d');
    
    const typeCounts = {};
    transformations.forEach(t => {
        typeCounts[t.type] = (typeCounts[t.type] || 0) + 1;
    });
    
    const types = Object.keys(typeCounts);
    const counts = Object.values(typeCounts);
    
    if (types.length === 0) {
        ctx.fillStyle = '#6c757d';
        ctx.font = '14px Inter';
        ctx.textAlign = 'center';
        ctx.fillText('No transformations', canvas.width / 2, canvas.height / 2);
        return;
    }
    
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    
    const barWidth = canvas.width / types.length * 0.8;
    const maxCount = Math.max(...counts);
    
    types.forEach((type, index) => {
        const barHeight = (counts[index] / maxCount) * (canvas.height - 40);
        const x = index * (canvas.width / types.length) + (canvas.width / types.length - barWidth) / 2;
        const y = canvas.height - barHeight - 20;
        
        ctx.fillStyle = '#007bff';
        ctx.fillRect(x, y, barWidth, barHeight);
        
        ctx.fillStyle = '#495057';
        ctx.font = '10px Inter';
        ctx.textAlign = 'center';
        ctx.fillText(type.substring(0, 8) + '...', x + barWidth / 2, canvas.height - 5);
        
        ctx.fillStyle = '#fff';
        ctx.font = '12px Inter';
        ctx.fillText(counts[index], x + barWidth / 2, y + barHeight / 2 + 4);
    });
}

function updateArchitectureTab(assetData, lineageData) {
    const architectureDiagram = generateArchitectureDiagram(lineageData);
    document.getElementById('architecture-diagram').innerHTML = architectureDiagram;
    
    const dataLayers = analyzeDataLayers(lineageData);
    document.getElementById('data-layers').innerHTML = dataLayers;
}

function generateArchitectureDiagram(lineageData) {
    const layers = {};
    
    lineageData.nodes.forEach(node => {
        if (!node || typeof node.level === 'undefined') {
            return;
        }
        
        const level = node.level;
        if (!layers[level]) layers[level] = [];
        layers[level].push(node);
    });
    
    const sortedLevels = Object.keys(layers).sort((a, b) => parseInt(a) - parseInt(b));
    
    return `
        <div class="architecture-layers">
            ${sortedLevels.map(level => `
                <div class="architecture-layer mb-3">
                    <h6 class="layer-title">${getLevelName(parseInt(level))}</h6>
                    <div class="layer-nodes">
                        ${layers[level].map(node => `
                            <div class="arch-node ${node.is_target ? 'target-node' : ''}" title="${node.name}">
                                <i class="fas fa-${getNodeIcon(node.type)} me-1"></i>
                                ${node.name.length > 15 ? node.name.substring(0, 15) + '...' : node.name}
                            </div>
                        `).join('')}
                    </div>
                </div>
            `).join('')}
        </div>
    `;
}

function getLevelName(level) {
    if (level < 0) return `Source Layer ${Math.abs(level)}`;
    if (level === 0) return 'Processing Layer';
    return `Output Layer ${level}`;
}

function getNodeIcon(type) {
    const iconMap = {
        'table': 'table',
        'view': 'eye',
        'database': 'database',
        'file': 'file',
        'api': 'plug'
    };
    
    return iconMap[type.toLowerCase()] || 'circle';
}

function analyzeDataLayers(lineageData) {
    const layers = {
        source: lineageData.nodes.filter(n => n.level < 0),
        processing: lineageData.nodes.filter(n => n.level === 0),
        output: lineageData.nodes.filter(n => n.level > 0)
    };
    
    return `
        <div class="data-layers-analysis">
            <div class="layer-item mb-3">
                <div class="d-flex justify-content-between align-items-center">
                    <span><i class="fas fa-download text-primary me-2"></i>Source Layer</span>
                    <span class="badge bg-primary">${layers.source.length}</span>
                </div>
                <small class="text-muted">Raw data sources and inputs</small>
            </div>
            <div class="layer-item mb-3">
                <div class="d-flex justify-content-between align-items-center">
                    <span><i class="fas fa-cogs text-success me-2"></i>Processing Layer</span>
                    <span class="badge bg-success">${layers.processing.length}</span>
                </div>
                <small class="text-muted">Data transformation and business logic</small>
            </div>
            <div class="layer-item mb-3">
                <div class="d-flex justify-content-between align-items-center">
                    <span><i class="fas fa-upload text-info me-2"></i>Output Layer</span>
                    <span class="badge bg-info">${layers.output.length}</span>
                </div>
                <small class="text-muted">Processed data and final outputs</small>
            </div>
        </div>
    `;
}

function updateCodeAnalysisTab(assetData, lineageData) {
    updateQualityMetrics(assetData, lineageData);
}

function generateCodeAnalysis() {
    if (!window.currentLineageData || !window.selectedAssetId) {
        showNotification('Please select an asset first', 'warning');
        return;
    }
    
    const button = document.querySelector('#code-analysis button');
    const originalText = button.innerHTML;
    
    button.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Analyzing...';
    button.disabled = true;
    
    const selectedAsset = window.currentLineageData.nodes.find(node => node && node.is_target);
    const lineageData = window.currentLineageData;
    
    setTimeout(() => {
        const analysis = generateDynamicCodeAnalysis(selectedAsset, lineageData);
        
        document.getElementById('code-analysis-content').innerHTML = analysis.html;
        
        button.innerHTML = originalText;
        button.disabled = false;
        
        showNotification('Code analysis generated successfully', 'success');
    }, 2000);
}

function generateDynamicCodeAnalysis(assetData, lineageData) {
    const cyclomaticComplexity = calculateCyclomaticComplexity(lineageData);
    const dataFlowComplexity = calculateDataFlowComplexity(lineageData);
    
    const sqlPattern = generateSQLPattern(assetData, lineageData);
    
    const recommendations = generateDynamicRecommendations(assetData, lineageData);
    
    const html = `
        <div class="code-analysis-results">
            <div class="analysis-section mb-4">
                <h6><i class="fas fa-chart-line me-2"></i>Complexity Analysis</h6>
                <div class="complexity-metrics">
                    <div class="metric-bar mb-2">
                        <div class="d-flex justify-content-between">
                            <span>Cyclomatic Complexity</span>
                            <span>${cyclomaticComplexity.label}</span>
                        </div>
                        <div class="progress">
                            <div class="progress-bar bg-${cyclomaticComplexity.color}" style="width: ${cyclomaticComplexity.percentage}%"></div>
                        </div>
                        <small class="text-muted">Based on ${lineageData.edges.length} connections across ${lineageData.nodes.length} assets</small>
                    </div>
                    <div class="metric-bar mb-2">
                        <div class="d-flex justify-content-between">
                            <span>Data Flow Complexity</span>
                            <span>${dataFlowComplexity.label}</span>
                        </div>
                        <div class="progress">
                            <div class="progress-bar bg-${dataFlowComplexity.color}" style="width: ${dataFlowComplexity.percentage}%"></div>
                        </div>
                        <small class="text-muted">Based on ${Math.max(...lineageData.nodes.map(n => Math.abs(n.level)))} lineage levels</small>
                    </div>
                </div>
            </div>
            
            <div class="analysis-section mb-4">
                <h6><i class="fas fa-code me-2"></i>Generated ${getAssetLanguage(assetData)} Pattern</h6>
                <div class="code-preview">
                    <pre class="bg-light p-3 rounded"><code>${sqlPattern}</code></pre>
                </div>
                <small class="text-muted">Pattern inferred from asset type: ${assetData.type}, columns: ${assetData.schema ? assetData.schema.length : 0}</small>
            </div>
            
            <div class="analysis-section">
                <h6><i class="fas fa-lightbulb me-2"></i>Dynamic Recommendations</h6>
                <ul class="list-unstyled">
                    ${recommendations.map(rec => `
                        <li class="mb-2">
                            <i class="fas fa-${rec.icon} text-${rec.color} me-2"></i>
                            ${rec.text}
                        </li>
                    `).join('')}
                </ul>
            </div>
        </div>
    `;
    
    return { html };
}

function calculateCyclomaticComplexity(lineageData) {
    const nodeCount = lineageData.nodes.length;
    const edgeCount = lineageData.edges.length;
    const complexity = nodeCount > 0 ? (edgeCount / nodeCount) * 20 : 0;
    
    if (complexity < 20) return { label: 'Low', color: 'success', percentage: Math.max(complexity, 10) };
    if (complexity < 50) return { label: 'Medium', color: 'warning', percentage: complexity };
    return { label: 'High', color: 'danger', percentage: Math.min(complexity, 90) };
}

function calculateDataFlowComplexity(lineageData) {
    const maxDepth = Math.max(...lineageData.nodes.map(n => Math.abs(n.level)));
    const complexity = maxDepth * 15;
    
    if (complexity < 30) return { label: 'Low', color: 'success', percentage: Math.max(complexity, 15) };
    if (complexity < 60) return { label: 'Medium', color: 'warning', percentage: complexity };
    return { label: 'High', color: 'danger', percentage: Math.min(complexity, 85) };
}

function generateSQLPattern(assetData, lineageData) {
    const assetType = assetData.type.toLowerCase();
    const assetName = assetData.name;
    const schema = assetData.schema || [];
    const upstreamNodes = lineageData.nodes.filter(n => n.level < 0);
    
    const columnsList = schema.length > 0 
        ? schema.slice(0, 5).map(col => {
            const colName = typeof col === 'string' ? col : col.name;
            return `    ${colName}`;
        }).join(',\n')
        : '    column1,\n    column2,\n    column3';
    
    const fromClause = upstreamNodes.length > 0 
        ? upstreamNodes[0].name 
        : 'source_table';
    
    if (assetType.includes('view')) {
        return `-- Inferred VIEW pattern for: ${assetName}
CREATE VIEW ${assetName} AS
SELECT 
${columnsList}
FROM ${fromClause}
WHERE 1=1
  ${generateDynamicWhereClause(assetData, 'view')};`;
    } else if (assetType.includes('table')) {
        return `-- Inferred TABLE pattern for: ${assetName}
INSERT INTO ${assetName} (
${columnsList}
)
SELECT 
${columnsList}
FROM ${fromClause}
WHERE ${generateDynamicWhereClause(assetData, 'table')};`;
    } else if (assetType.includes('procedure') || assetType.includes('function')) {
        return `-- Inferred PROCEDURE pattern for: ${assetName}
CREATE PROCEDURE ${assetName}()
BEGIN
    -- Data transformation logic
    SELECT 
${columnsList}
    FROM ${fromClause}
    WHERE ${generateDynamicWhereClause(assetData, 'procedure')};
END;`;
    } else {
        return `-- Inferred transformation pattern for: ${assetName}
SELECT 
${columnsList}
FROM ${fromClause}
WHERE ${generateDynamicWhereClause(assetData, 'general')}
ORDER BY ${generateDynamicOrderClause(assetData)};`;
    }
}

function generateDynamicWhereClause(assetData, patternType) {
    const conditions = [];
    const schema = assetData.schema || [];
    
    const hasIdColumn = schema.some(col => {
        const colName = typeof col === 'string' ? col.toLowerCase() : col.name.toLowerCase();
        return colName.includes('id') && !colName.includes('valid');
    });
    
    const hasStatusColumn = schema.some(col => {
        const colName = typeof col === 'string' ? col.toLowerCase() : col.name.toLowerCase();
        return colName.includes('status') || colName.includes('active') || colName.includes('enabled');
    });
    
    const hasDateColumn = schema.some(col => {
        const colName = typeof col === 'string' ? col.toLowerCase() : col.name.toLowerCase();
        return colName.includes('date') || colName.includes('time') || colName.includes('created') || colName.includes('updated');
    });
    
    if (hasStatusColumn) {
        conditions.push('status = \'active\'');
    } else if (hasIdColumn) {
        conditions.push('id IS NOT NULL');
    }
    
    if (hasDateColumn) {
        if (patternType === 'view') {
            conditions.push('created_date >= CURRENT_DATE - INTERVAL \'30\' DAY');
        } else if (patternType === 'table') {
            conditions.push('processing_date = CURRENT_DATE');
        } else {
            conditions.push('created_date >= \'2024-01-01\'');
        }
    }
    
    if (schema.length > 10) {
        conditions.push('data_quality_score > 0.8');
    }
    
    if (conditions.length === 0) {
        return patternType === 'general' ? '1=1' : 'TRUE';
    }
    
    return conditions.join('\n  AND ');
}

function generateDynamicOrderClause(assetData) {
    const schema = assetData.schema || [];
    
    const orderColumns = [];
    
    const priorityColumns = ['created_date', 'updated_date', 'id', 'name', 'timestamp'];
    
    for (const priorityCol of priorityColumns) {
        const foundCol = schema.find(col => {
            const colName = typeof col === 'string' ? col.toLowerCase() : col.name.toLowerCase();
            return colName.includes(priorityCol);
        });
        
        if (foundCol) {
            const colName = typeof foundCol === 'string' ? foundCol : foundCol.name;
            orderColumns.push(`${colName} DESC`);
            break; // Use first found priority column
        }
    }
    
    if (orderColumns.length === 0 && schema.length > 0) {
        const firstCol = typeof schema[0] === 'string' ? schema[0] : schema[0].name;
        orderColumns.push(`${firstCol} DESC`);
    }
    
    return orderColumns.length > 0 ? orderColumns.join(', ') : '1';
}

function getAssetLanguage(assetData) {
    const type = assetData.type.toLowerCase();
    const source = assetData.source.toLowerCase();
    
    if (source.includes('bigquery') || source.includes('snowflake')) return 'SQL';
    if (source.includes('mongodb')) return 'MongoDB';
    if (source.includes('elasticsearch')) return 'Query DSL';
    if (type.includes('api')) return 'REST API';
    if (type.includes('file')) return 'Data Processing';
    
    return 'SQL';
}

function generateDynamicRecommendations(assetData, lineageData) {
    const recommendations = [];
    
    if (!assetData.schema || assetData.schema.length === 0) {
        recommendations.push({
            icon: 'exclamation-triangle',
            color: 'warning',
            text: 'No schema information detected - consider documenting column definitions'
        });
    } else if (assetData.schema.length > 50) {
        recommendations.push({
            icon: 'info-circle',
            color: 'info',
            text: `Large schema detected (${assetData.schema.length} columns) - consider normalizing data structure`
        });
    }
    
    const upstreamCount = lineageData.nodes.filter(n => n.level < 0).length;
    const downstreamCount = lineageData.nodes.filter(n => n.level > 0).length;
    
    if (upstreamCount === 0) {
        recommendations.push({
            icon: 'lightbulb',
            color: 'primary',
            text: 'No upstream sources detected - this appears to be a source system'
        });
    } else if (upstreamCount > 5) {
        recommendations.push({
            icon: 'exclamation-triangle',
            color: 'warning',
            text: `High upstream dependency (${upstreamCount} sources) - monitor data quality and availability`
        });
    }
    
    if (downstreamCount > 10) {
        recommendations.push({
            icon: 'shield-alt',
            color: 'danger',
            text: `Critical asset with ${downstreamCount} downstream dependencies - implement change management`
        });
    }
    
    const assetType = assetData.type.toLowerCase();
    if (assetType.includes('view')) {
        recommendations.push({
            icon: 'check-circle',
            color: 'success',
            text: 'View detected - ensure underlying tables are properly indexed'
        });
    }
    
    if (assetType.includes('table') && upstreamCount > 0) {
        recommendations.push({
            icon: 'clock',
            color: 'info',
            text: 'Consider implementing incremental loading for better performance'
        });
    }
    
    if (!assetData.metadata || Object.keys(assetData.metadata).length < 3) {
        recommendations.push({
            icon: 'file-alt',
            color: 'warning',
            text: 'Limited metadata available - add descriptions and business context'
        });
    }
    
    if (recommendations.length === 0) {
        recommendations.push({
            icon: 'check-circle',
            color: 'success',
            text: 'Asset appears well-structured with good lineage connectivity'
        });
        recommendations.push({
            icon: 'chart-line',
            color: 'info',
            text: 'Monitor data quality and performance metrics regularly'
        });
    }
    
    return recommendations;
}
window.allAssets = [];
window.filteredAssets = [];
window.searchRecommendations = [];
window.currentSearchTerm = '';
window.selectedRecommendationIndex = -1;

function initializeAssetSearch() {
    const searchInput = document.getElementById('asset-search');
    const recommendationsDropdown = document.getElementById('search-recommendations');
    
    
    if (!searchInput) {
        return;
    }
    
    searchInput.addEventListener('input', debounce(handleSearchInput, 300));
    
    searchInput.addEventListener('keyup', (e) => {
        if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp' && e.key !== 'Enter' && e.key !== 'Escape') {
            const searchTerm = e.target.value.trim();
            if (searchTerm.length >= 1) {
                performSearch(searchTerm);
            } else if (searchTerm.length === 0) {
                displayFilteredAssets(window.allAssets);
            }
        }
    });
    
    searchInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            const searchTerm = e.target.value.trim();
            performSearch(searchTerm);
            hideRecommendations();
        }
    });
    
    searchInput.addEventListener('keydown', (e) => {
        if (e.key !== 'Enter') {
            handleSearchKeydown(e);
        }
    });
    
    searchInput.addEventListener('focus', () => {
        if (window.currentSearchTerm && window.currentSearchTerm.length >= 1) {
            showRecommendations();
        }
    });
    
    document.addEventListener('click', (e) => {
        if (!e.target.closest('.search-container')) {
            hideRecommendations();
        }
    });
    
    window.triggerSearch = function() {
        const searchTerm = document.getElementById('asset-search').value.trim();
        if (searchTerm) {
            performSearch(searchTerm);
        } else {
            displayFilteredAssets(window.allAssets);
        }
    };
    
    window.clearAssetSearch = function() {
        clearSearch();
    };
    
    initializeAssetFilters();
}

function handleSearchInput(e) {
    const searchTerm = e.target.value.trim();
    window.currentSearchTerm = searchTerm;
    
    if (searchTerm.length >= 1) {
        generateSearchRecommendations(searchTerm);
        showRecommendations();
        performSearch(searchTerm);
    } else {
        hideRecommendations();
        displayFilteredAssets(window.allAssets);
    }
}

function generateSearchRecommendations(searchTerm) {
    const recommendations = [];
    const term = searchTerm.toLowerCase();
    
    const assetNames = [...new Set(window.allAssets.map(asset => asset.name))];
    const assetTypes = [...new Set(window.allAssets.map(asset => asset.type))];
    const assetSources = [...new Set(window.allAssets.map(asset => asset.source))];
    
    assetNames.forEach(name => {
        if (name.toLowerCase().includes(term)) {
            const matchingAssets = window.allAssets.filter(asset => 
                asset.name.toLowerCase().includes(term)
            );
            recommendations.push({
                type: 'name',
                value: name,
                label: name,
                subtitle: `${matchingAssets.length} asset${matchingAssets.length > 1 ? 's' : ''}`,
                icon: 'fas fa-file',
                category: 'Names',
                assets: matchingAssets
            });
        }
    });
    
    assetTypes.forEach(type => {
        if (type.toLowerCase().includes(term)) {
            const matchingAssets = window.allAssets.filter(asset => 
                asset.type.toLowerCase().includes(term)
            );
            recommendations.push({
                type: 'type',
                value: type,
                label: `Type: ${type}`,
                subtitle: `${matchingAssets.length} asset${matchingAssets.length > 1 ? 's' : ''}`,
                icon: getAssetTypeIcon(type),
                category: 'Types',
                assets: matchingAssets
            });
        }
    });
    
    assetSources.forEach(source => {
        if (source.toLowerCase().includes(term)) {
            const matchingAssets = window.allAssets.filter(asset => 
                asset.source.toLowerCase().includes(term)
            );
            recommendations.push({
                type: 'source',
                value: source,
                label: `Source: ${source}`,
                subtitle: `${matchingAssets.length} asset${matchingAssets.length > 1 ? 's' : ''}`,
                icon: 'fas fa-database',
                category: 'Sources',
                assets: matchingAssets
            });
        }
    });
    
    if (recommendations.length === 0) {
        const fuzzyMatches = findFuzzyMatches(term, assetNames.concat(assetTypes, assetSources));
        fuzzyMatches.forEach(match => {
            const matchingAssets = window.allAssets.filter(asset => 
                asset.name.toLowerCase().includes(match.toLowerCase()) ||
                asset.type.toLowerCase().includes(match.toLowerCase()) ||
                asset.source.toLowerCase().includes(match.toLowerCase())
            );
            
            if (matchingAssets.length > 0) {
                recommendations.push({
                    type: 'suggestion',
                    value: match,
                    label: `Did you mean "${match}"?`,
                    subtitle: `${matchingAssets.length} matching asset${matchingAssets.length > 1 ? 's' : ''}`,
                    icon: 'fas fa-lightbulb',
                    category: 'Suggestions',
                    assets: matchingAssets
                });
            }
        });
    }
    
    window.searchRecommendations = recommendations
        .sort((a, b) => b.assets.length - a.assets.length) // Sort by relevance
        .slice(0, 8); // Limit to 8 recommendations
    
    displayRecommendations();
}

function findFuzzyMatches(term, candidates) {
    const matches = [];
    const maxDistance = Math.floor(term.length / 3); // Allow 1 error per 3 characters
    
    candidates.forEach(candidate => {
        const distance = levenshteinDistance(term.toLowerCase(), candidate.toLowerCase());
        if (distance <= maxDistance && distance > 0) {
            matches.push(candidate);
        }
    });
    
    return matches.slice(0, 3); // Limit fuzzy matches
}

function levenshteinDistance(str1, str2) {
    const matrix = [];
    
    for (let i = 0; i <= str2.length; i++) {
        matrix[i] = [i];
    }
    
    for (let j = 0; j <= str1.length; j++) {
        matrix[0][j] = j;
    }
    
    for (let i = 1; i <= str2.length; i++) {
        for (let j = 1; j <= str1.length; j++) {
            if (str2.charAt(i - 1) === str1.charAt(j - 1)) {
                matrix[i][j] = matrix[i - 1][j - 1];
            } else {
                matrix[i][j] = Math.min(
                    matrix[i - 1][j - 1] + 1,
                    matrix[i][j - 1] + 1,
                    matrix[i - 1][j] + 1
                );
            }
        }
    }
    
    return matrix[str2.length][str1.length];
}

function displayRecommendations() {
    const recommendationsList = document.getElementById('recommendations-list');
    
    if (window.searchRecommendations.length === 0) {
        recommendationsList.innerHTML = `
            <div class="no-recommendations">
                <i class="fas fa-search me-2"></i>
                No matching assets found for "${window.currentSearchTerm}"
            </div>
        `;
        return;
    }
    
    const groupedRecs = groupBy(window.searchRecommendations, 'category');
    
    let html = '';
    Object.keys(groupedRecs).forEach(category => {
        if (category !== 'undefined') {
            html += `<div class="recommendation-category">
                <div class="category-header px-3 py-2 bg-light border-bottom">
                    <small class="text-muted font-weight-bold">${category}</small>
                </div>
            </div>`;
        }
        
        groupedRecs[category].forEach((rec, index) => {
            const globalIndex = window.searchRecommendations.indexOf(rec);
            html += `
                <div class="recommendation-item ${globalIndex === window.selectedRecommendationIndex ? 'active' : ''}" 
                     data-index="${globalIndex}" onclick="selectRecommendation(${globalIndex})">
                    <div class="recommendation-content">
                        <div class="recommendation-title">
                            <i class="${rec.icon} me-2"></i>
                            ${highlightMatch(rec.label, window.currentSearchTerm)}
                        </div>
                        <div class="recommendation-subtitle">${rec.subtitle}</div>
                    </div>
                    <span class="recommendation-type ${rec.type}">${rec.type}</span>
                </div>
            `;
        });
    });
    
    recommendationsList.innerHTML = html;
}

function highlightMatch(text, term) {
    if (!term) return text;
    
    const regex = new RegExp(`(${term})`, 'gi');
    return text.replace(regex, '<mark>$1</mark>');
}

function groupBy(array, property) {
    return array.reduce((groups, item) => {
        const group = item[property] || 'Other';
        groups[group] = groups[group] || [];
        groups[group].push(item);
        return groups;
    }, {});
}

function handleSearchKeydown(e) {
    const recommendationsVisible = document.getElementById('search-recommendations').style.display !== 'none';
    
    if (!recommendationsVisible || window.searchRecommendations.length === 0) return;
    
    switch (e.key) {
        case 'ArrowDown':
            e.preventDefault();
            window.selectedRecommendationIndex = Math.min(
                window.selectedRecommendationIndex + 1,
                window.searchRecommendations.length - 1
            );
            updateRecommendationSelection();
            break;
            
        case 'ArrowUp':
            e.preventDefault();
            window.selectedRecommendationIndex = Math.max(
                window.selectedRecommendationIndex - 1,
                -1
            );
            updateRecommendationSelection();
            break;
            
        case 'Enter':
            e.preventDefault();
            if (window.selectedRecommendationIndex >= 0) {
                selectRecommendation(window.selectedRecommendationIndex);
            } else {
                performSearch(window.currentSearchTerm);
            }
            break;
            
        case 'Escape':
            hideRecommendations();
            break;
    }
}

function updateRecommendationSelection() {
    const items = document.querySelectorAll('.recommendation-item');
    items.forEach((item, index) => {
        item.classList.toggle('active', index === window.selectedRecommendationIndex);
    });
}

function selectRecommendation(index) {
    const recommendation = window.searchRecommendations[index];
    if (!recommendation) return;
    
    document.getElementById('asset-search').value = recommendation.value;
    
    performSearch(recommendation.value);
    
    hideRecommendations();
    
    window.selectedRecommendationIndex = -1;
}

function performSearch(searchTerm) {
    
    if (!searchTerm.trim()) {
        displayFilteredAssets(window.allAssets);
        return;
    }
    
    const term = searchTerm.toLowerCase();
    
    const filtered = window.allAssets.filter(asset => {
        const nameMatch = asset.name.toLowerCase().includes(term);
        const typeMatch = asset.type.toLowerCase().includes(term);
        const sourceMatch = asset.source.toLowerCase().includes(term);
        const schemaMatch = asset.schema && asset.schema.some(col => {
            const colName = typeof col === 'string' ? col : col.name;
            return colName.toLowerCase().includes(term);
        });
        
        const matches = nameMatch || typeMatch || sourceMatch || schemaMatch;
        
        if (matches) {
        }
        
        return matches;
    });
    
    displayFilteredAssets(filtered);
    
    const url = new URL(window.location);
    if (searchTerm.trim()) {
        url.searchParams.set('search', searchTerm);
    } else {
        url.searchParams.delete('search');
    }
    window.history.replaceState({}, '', url);
}

function showRecommendations() {
    if (window.searchRecommendations.length > 0 || window.currentSearchTerm.length >= 1) {
        document.getElementById('search-recommendations').style.display = 'block';
    }
}

function hideRecommendations() {
    document.getElementById('search-recommendations').style.display = 'none';
    window.selectedRecommendationIndex = -1;
}

function clearSearch() {
    document.getElementById('asset-search').value = '';
    window.currentSearchTerm = '';
    hideRecommendations();
    displayFilteredAssets(window.allAssets);
    
    const url = new URL(window.location);
    url.searchParams.delete('search');
    window.history.replaceState({}, '', url);
}

function searchAssets() {
    const searchTerm = document.getElementById('asset-search').value.trim();
    performSearch(searchTerm);
    hideRecommendations();
}

function initializeAssetFilters() {
    const typeFilter = document.getElementById('asset-type-filter');
    const sourceFilter = document.getElementById('asset-source-filter');
    
    if (window.allAssets.length > 0) {
        populateFilterOptions();
    }
    
    if (typeFilter) {
        typeFilter.addEventListener('change', applyAssetFilters);
    }
    if (sourceFilter) {
        sourceFilter.addEventListener('change', applyAssetFilters);
    }
}

function populateFilterOptions() {
    const typeFilter = document.getElementById('asset-type-filter');
    const sourceFilter = document.getElementById('asset-source-filter');
    
    if (typeFilter && window.allAssets.length > 0) {
        const types = [...new Set(window.allAssets.map(asset => asset.type))].sort();
        typeFilter.innerHTML = '<option value="">All Asset Types</option>';
        types.forEach(type => {
            const count = window.allAssets.filter(asset => asset.type === type).length;
            typeFilter.innerHTML += `<option value="${type}">${type} <span class="filter-count">(${count})</span></option>`;
        });
    }
    
    if (sourceFilter && window.allAssets.length > 0) {
        const sources = [...new Set(window.allAssets.map(asset => asset.source))].sort();
        sourceFilter.innerHTML = '<option value="">All Sources</option>';
        sources.forEach(source => {
            const count = window.allAssets.filter(asset => asset.source === source).length;
            sourceFilter.innerHTML += `<option value="${source}">${source} <span class="filter-count">(${count})</span></option>`;
        });
    }
}

function applyAssetFilters() {
    const typeFilter = document.getElementById('asset-type-filter').value;
    const sourceFilter = document.getElementById('asset-source-filter').value;
    const searchTerm = document.getElementById('asset-search').value.toLowerCase();
    
    let filtered = window.allAssets;
    
    if (searchTerm) {
        filtered = filtered.filter(asset => {
            return asset.name.toLowerCase().includes(searchTerm) ||
                   asset.type.toLowerCase().includes(searchTerm) ||
                   asset.source.toLowerCase().includes(searchTerm) ||
                   (asset.schema && asset.schema.some(col => {
                       const colName = typeof col === 'string' ? col : col.name;
                       return colName.toLowerCase().includes(searchTerm);
                   }));
        });
    }
    
    if (typeFilter) {
        filtered = filtered.filter(asset => asset.type === typeFilter);
    }
    
    if (sourceFilter) {
        filtered = filtered.filter(asset => asset.source === sourceFilter);
    }
    
    displayFilteredAssets(filtered);
    
    const filterButton = document.querySelector('[onclick="applyAssetFilters()"]');
    if (typeFilter || sourceFilter) {
        filterButton.classList.add('filter-active');
        filterButton.innerHTML = `<i class="fas fa-filter me-1"></i> Filters Applied (${filtered.length})`;
    } else {
        filterButton.classList.remove('filter-active');
        filterButton.innerHTML = `<i class="fas fa-filter me-1"></i> Apply Filters`;
    }
}

function displayFilteredAssets(assets) {
    window.filteredAssets = assets;
    const tableBody = document.getElementById('assets-table-body');
    
    if (!tableBody) return;
    
    if (assets.length === 0) {
        tableBody.innerHTML = `
            <tr>
                <td colspan="6" class="text-center py-4">
                    <div class="text-muted">
                        <i class="fas fa-search fa-3x mb-3 opacity-25"></i>
                        <p>No assets found matching your criteria</p>
                        <button class="btn btn-outline-primary btn-sm" onclick="clearFiltersAndSearch()">
                            <i class="fas fa-times me-1"></i>Clear Filters
                        </button>
                    </div>
                </td>
            </tr>
        `;
        return;
    }
    
    tableBody.innerHTML = assets.map(asset => `
        <tr>
            <td>
                <div class="d-flex align-items-center">
                    <i class="${getAssetTypeIcon(asset.type)} me-2 text-primary"></i>
                    <div>
                        <div class="fw-semibold">${asset.name}</div>
                        ${asset.schema ? `<small class="text-muted">${asset.schema.length} columns</small>` : ''}
                    </div>
                </div>
            </td>
            <td><span class="badge bg-secondary">${asset.type}</span></td>
            <td><span class="badge bg-info">${asset.source}</span></td>
            <td>${formatFileSize(asset.size || 0)}</td>
            <td>${formatDate(asset.created_date || asset.discovered_date)}</td>
            <td>
                <button class="btn btn-sm btn-outline-primary" onclick="viewAssetDetails('${asset.id || asset.name}')" title="View Details">
                    <i class="fas fa-eye"></i>
                </button>
            </td>
        </tr>
    `).join('');
    
    updateAssetsCount(assets.length, window.allAssets.length);
}

function clearFiltersAndSearch() {
    document.getElementById('asset-search').value = '';
    document.getElementById('asset-type-filter').value = '';
    document.getElementById('asset-source-filter').value = '';
    
    window.currentSearchTerm = '';
    hideRecommendations();
    
    displayFilteredAssets(window.allAssets);
    
    const filterButton = document.querySelector('[onclick="applyAssetFilters()"]');
    filterButton.classList.remove('filter-active');
    filterButton.innerHTML = `<i class="fas fa-filter me-1"></i> Apply Filters`;
}

function updateAssetsCount(filtered, total) {
    const countDisplay = document.getElementById('assets-count');
    if (countDisplay) {
        countDisplay.textContent = filtered === total ? 
            `${total} assets` : 
            `${filtered} of ${total} assets`;
    }
}

function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

document.addEventListener('DOMContentLoaded', function() {
    initializeAssetSearch();
    
    const urlParams = new URLSearchParams(window.location.search);
    const searchParam = urlParams.get('search');
    if (searchParam) {
        const searchInput = document.getElementById('asset-search');
        if (searchInput) {
            searchInput.value = searchParam;
            performSearch(searchParam);
        }
    }
});


function getAssetTypeIcon(assetType) {
    const type = assetType.toLowerCase();
    const iconMap = {
        'table': 'fas fa-table',
        'view': 'fas fa-eye',
        'database': 'fas fa-database',
        'file': 'fas fa-file',
        'folder': 'fas fa-folder',
        'api': 'fas fa-plug',
        'stream': 'fas fa-stream',
        'queue': 'fas fa-list',
        'topic': 'fas fa-comments',
        'bucket': 'fas fa-bucket',
        'container': 'fas fa-box',
        'schema': 'fas fa-sitemap',
        'procedure': 'fas fa-cogs',
        'function': 'fas fa-function',
        'pipeline': 'fas fa-project-diagram'
    };
    
    for (const [key, icon] of Object.entries(iconMap)) {
        if (type.includes(key)) {
            return icon;
        }
    }
    
    return 'fas fa-cube'; // Default icon
}

function formatFileSize(bytes) {
    if (!bytes || bytes === 0) return 'Unknown';
    
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    
    return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
}

function formatDate(dateString) {
    if (!dateString) return 'Unknown';
    
    try {
        const date = new Date(dateString);
        return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'});
    } catch (e) {
        return dateString; // Return original if parsing fails
    }
}

function viewAssetDetails(assetId) {
    
    if (window.currentAssetsData && window.currentAssetsData.assets_list) {
        const asset = window.currentAssetsData.assets_list.find(a => a.id == assetId);
        if (asset) {
            showAssetDetails(asset.name);
            return;
        }
    }
    
    showAssetDetails(assetId);
}

function viewAssetLineage(assetId) {
    
    const asset = window.allAssets.find(a => a.id === assetId || a.name === assetId);
    
    if (asset) {
        showSection('lineage');
        
        const lineageAssetId = generateAssetId(asset);
        
        const lineageSelect = document.getElementById('lineage-asset-select');
        if (lineageSelect) {
            const options = lineageSelect.querySelectorAll('option');
            for (const option of options) {
                if (option.dataset.asset) {
                    const optionAsset = JSON.parse(option.dataset.asset);
                    if (optionAsset.name === asset.name || optionAsset.id === assetId) {
                        lineageSelect.value = option.value;
                        selectAssetForLineage(); // Trigger lineage loading
                        break;
                    }
                }
            }
        }
        
        showNotification(`Switched to lineage view for: ${asset.name}`, 'success');
    } else {
        showNotification(`Asset not found: ${assetId}`, 'warning');
    }
}

function updateQualityMetrics(assetData, lineageData) {
    const complexity = calculateComplexityScore(lineageData);
    const quality = calculateDataQuality(assetData);
    const performance = calculatePerformanceScore(lineageData);
    const maintainability = calculateMaintainabilityScore(assetData, lineageData);
    
    document.getElementById('complexity-score').textContent = complexity.score;
    document.getElementById('complexity-score').className = `badge bg-${complexity.color}`;
    
    document.getElementById('data-quality').textContent = quality.score;
    document.getElementById('data-quality').className = `badge bg-${quality.color}`;
    
    document.getElementById('performance-score').textContent = performance.score;
    document.getElementById('performance-score').className = `badge bg-${performance.color}`;
    
    document.getElementById('maintainability').textContent = maintainability.score;
    document.getElementById('maintainability').className = `badge bg-${maintainability.color}`;
}

function calculateComplexityScore(lineageData) {
    const nodeCount = lineageData.nodes.length;
    const edgeCount = lineageData.edges.length;
    const complexity = (edgeCount / nodeCount) * 10;
    
    if (complexity < 3) return { score: 'Low', color: 'success' };
    if (complexity < 6) return { score: 'Medium', color: 'warning' };
    return { score: 'High', color: 'danger' };
}

function calculateDataQuality(assetData) {
    const hasSchema = assetData.schema && assetData.schema.length > 0;
    const hasMetadata = assetData.metadata && Object.keys(assetData.metadata).length > 0;
    
    let score = 0;
    if (hasSchema) score += 50;
    if (hasMetadata) score += 30;
    score += 20; // Base score
    
    if (score >= 80) return { score: 'High', color: 'success' };
    if (score >= 60) return { score: 'Medium', color: 'warning' };
    return { score: 'Low', color: 'danger' };
}

function calculatePerformanceScore(lineageData) {
    const depth = Math.max(...lineageData.nodes.map(n => Math.abs(n.level)));
    
    if (depth <= 2) return { score: 'High', color: 'success' };
    if (depth <= 4) return { score: 'Medium', color: 'warning' };
    return { score: 'Low', color: 'danger' };
}

function calculateMaintainabilityScore(assetData, lineageData) {
    const hasDocumentation = assetData.metadata && assetData.metadata.description;
    const isSimpleFlow = lineageData.edges.length <= 5;
    
    let score = 0;
    if (hasDocumentation) score += 50;
    if (isSimpleFlow) score += 30;
    score += 20; // Base score
    
    if (score >= 80) return { score: 'High', color: 'success' };
    if (score >= 60) return { score: 'Medium', color: 'warning' };
    return { score: 'Low', color: 'danger' };
}
