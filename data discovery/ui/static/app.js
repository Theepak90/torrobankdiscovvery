// Data Discovery System - Frontend JavaScript

// Global variables
let currentConnectors = {};
let currentAssets = [];
let discoveryChart = null;
let refreshInterval = null;

// API Base URL
const API_BASE = '/api';

// Ensure Font Awesome icons are properly loaded
function ensureIconsLoaded() {
    // Check if Font Awesome is loaded
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
    
    // If Font Awesome is not loaded, try to reload it
    if (!checkFontAwesome()) {
        console.warn('Font Awesome not detected, attempting to reload...');
        
        // Create a new link element for Font Awesome
        const link = document.createElement('link');
        link.rel = 'stylesheet';
        link.href = 'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css';
        link.onload = () => {
            console.log('Font Awesome reloaded successfully');
            // Force a re-render of all icons
            document.querySelectorAll('.fas, .far, .fab, .fa').forEach(icon => {
                icon.style.display = 'none';
                icon.offsetHeight; // Trigger reflow
                icon.style.display = '';
            });
        };
        link.onerror = () => {
            console.error('Failed to load Font Awesome');
        };
        
        document.head.appendChild(link);
    } else {
        console.log('Font Awesome loaded successfully');
    }
    
    // Ensure all icons have proper styling
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

// Initialize the application
document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 Data Discovery System - UI Loaded');
    
    // Ensure Font Awesome icons are properly loaded
    ensureIconsLoaded();
    
    // Add loading animation to page
    addPageLoadAnimation();
    
    // Initialize dashboard
    initializeDashboard();
    
    // Set up auto-refresh with more frequent updates
    startAutoRefresh();
    
    // Load initial data with staggered animations
    setTimeout(() => loadSystemHealth(), 100);
    setTimeout(() => loadConnectors(), 200);
    setTimeout(() => loadMyConnections(), 250);
    setTimeout(() => loadAssets(), 300);
    setTimeout(() => loadRecentActivity(), 400);
    
    // Set up event listeners
    setupEventListeners();
    
    // Initialize tooltips
    initializeTooltips();
    
    // Start dynamic dashboard updates
    startDynamicDashboard();
    
    // Start automatic background monitoring
    startBackgroundMonitoring();
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
            } else if (target === '#lineage') {
                console.log('🎯 Switching to lineage tab, loading assets...');
                // Always load directly from API for now to debug
                loadLineageAssets();
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
        const response = await apiCall('/system/status');
        
        if (response.status === 'success') {
            const metrics = response.dashboard_metrics;
            const health = response.system_health;
            
            // Update status indicators
            document.getElementById('system-status').textContent = 
                health.status === 'healthy' ? 'System Online' : 'System Issues';
            
            // Update overview cards with animations
            animateCounter('total-assets', metrics.total_assets || 0);
            animateCounter('active-connectors', metrics.active_connectors || 0);
            
            // Animate text updates for better visual feedback
            animateTextUpdate('last-scan', 
                metrics.last_scan ? formatDateTime(metrics.last_scan) : 'Never');
            animateTextUpdate('monitoring-status', 
                metrics.monitoring_enabled ? 'Active' : 'Disabled');
            
            // Update monitoring card color
            const monitoringCard = document.querySelector('.bg-warning');
            if (metrics.monitoring_enabled) {
                monitoringCard.className = monitoringCard.className.replace('bg-warning', 'bg-success');
            } else {
                monitoringCard.className = monitoringCard.className.replace('bg-success', 'bg-warning');
            }
            
            // Update discovery chart if visible
            updateDiscoveryChart(metrics);
        }
        
    } catch (error) {
        console.error('Failed to update system status:', error);
    }
}

// Animate counter updates with enhanced visual feedback
function animateCounter(elementId, targetValue) {
    const element = document.getElementById(elementId);
    if (!element) return;
    
    const currentValue = parseInt(element.textContent) || 0;
    if (currentValue === targetValue) return;
    
    // Simple transition without glow effects
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
            // Remove scaling animation - just reset styles
            element.style.textShadow = 'none';
        }
    }, stepTime);
}

// Animate text updates with visual feedback
function animateTextUpdate(elementId, newText) {
    const element = document.getElementById(elementId);
    if (!element) return;
    
    // Only animate if the text actually changed
    if (element.textContent === newText) return;
    
    // Simple text update without scaling animations
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
                        <i class="fas fa-link fa-2x text-info"></i>
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
            // Refresh the connections list
            loadMyConnections();
            
            // Trigger asset discovery after successful connection
            await discoverAssetsForConnection(connectorId, `${connectorId} Connection`);
        } else {
            showNotification('Connection test failed', 'danger');
        }
        
    } catch (error) {
        console.error('Connection test failed:', error);
        showNotification('Connection test failed: ' + error.message, 'danger');
    }
}

async function viewConnectorConnectionDetails(connectorId) {
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

async function deleteConnectorConnection(connectorId) {
    if (!confirm(`Are you sure you want to remove the ${connectorId} connection?`)) {
        return;
    }
    
    try {
        // Use the correct endpoint that supports DELETE
        const response = await apiCall(`/connectors/${connectorId}`, {
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
        
        // Add configuration fields based on connector type
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
        // Database Connectors
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
        // Data Warehouse Connectors
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
        console.log('Assets API response:', response);
        
        // Store globally for other functions to access
        window.currentAssetsData = response;
        
        // Handle different response formats
        let assetsData;
        if (response.assets) {
            assetsData = response.assets;
        } else if (response.inventory) {
            assetsData = response.inventory;
        } else if (response.assets_list) {
            // Convert flat list to grouped format
            assetsData = {};
            response.assets_list.forEach(asset => {
                const source = asset.source || 'unknown';
                if (!assetsData[source]) {
                    assetsData[source] = [];
                }
                assetsData[source].push(asset);
            });
        } else {
            // Assume the response itself is the assets data
            assetsData = response;
        }
        
        currentAssets = assetsData || {};
        console.log('Processed assets data:', currentAssets);
        
        // Convert grouped assets to flat array for search functionality
        window.allAssets = [];
        Object.keys(currentAssets).forEach(source => {
            if (Array.isArray(currentAssets[source])) {
                currentAssets[source].forEach(asset => {
                    // Ensure asset has required properties
                    asset.source = asset.source || source;
                    asset.id = asset.id || asset.name;
                    window.allAssets.push(asset);
                });
            }
        });
        
        console.log('Flattened assets for search:', window.allAssets);
        
        // Use new search-enabled display function
        displayFilteredAssets(window.allAssets);
        
        // Populate filter options with the new assets
        populateFilterOptions();
        
        // Re-initialize search functionality with the new assets
        initializeAssetSearch();
        
        // Check if there's a search term in the URL and apply it
        const urlParams = new URLSearchParams(window.location.search);
        const searchParam = urlParams.get('search');
        if (searchParam) {
            const searchInput = document.getElementById('asset-search');
            if (searchInput) {
                searchInput.value = searchParam;
                performSearch(searchParam);
            }
        }
        
        // Auto-refresh lineage assets when regular assets are updated
        if (document.getElementById('lineage-asset-select')) {
            autoRefreshLineageAssets();
        }
        
    } catch (error) {
        console.error('Failed to load assets:', error);
        document.getElementById('assets-table-body').innerHTML = 
            '<tr><td colspan="6" class="text-center text-danger">Failed to load assets. Please try refreshing.</td></tr>';
    }
}

function renderAssetsTable(assets) {
    console.log('renderAssetsTable called with:', assets);
    const tbody = document.getElementById('assets-table-body');
    
    if (!assets || Object.keys(assets).length === 0) {
        console.log('No assets to render');
        tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted">No assets found</td></tr>';
        return;
    }
    
    let html = '';
    let allAssets = [];
    
    // Flatten assets from all sources
    Object.entries(assets).forEach(([source, sourceAssets]) => {
        console.log(`Processing source ${source} with assets:`, sourceAssets);
        if (Array.isArray(sourceAssets)) {
            allAssets.push(...sourceAssets.map(asset => ({ ...asset, source })));
        }
    });
    
    console.log('All flattened assets:', allAssets);
    
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
    console.log('showAssetDetails called with:', assetName);
    
    try {
        // Show loading state first
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
        console.log('Asset details response:', response);
        
        if (response && response.asset) {
            const asset = response.asset;
            
            // Set modal title
            document.getElementById('asset-modal-title').textContent = `Asset Details - ${asset.name}`;
            
            // Populate Overview tab
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
            
            // Populate Schema tab
            let schemaContent = '';
            if (asset.schema && Object.keys(asset.schema).length > 0) {
                if (asset.schema.fields && Array.isArray(asset.schema.fields)) {
                    // BigQuery style schema
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
                    // Generic schema
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
            
            // Set content
            document.getElementById('asset-overview-content').innerHTML = overviewContent;
            document.getElementById('asset-schema-content').innerHTML = schemaContent;
            
            // Reset profiling tab content
            document.getElementById('asset-profiling-content').innerHTML = `
                <div class="text-center py-4">
                    <div class="spinner-border text-primary" role="status">
                        <span class="visually-hidden">Loading profiling data...</span>
                    </div>
                    <p class="mt-2 text-muted">Click to analyze data...</p>
                </div>
            `;
            
            // Set up tab event listener for profiling (remove existing listeners first)
            const profilingTab = document.getElementById('asset-profiling-tab');
            profilingTab.replaceWith(profilingTab.cloneNode(true));
            document.getElementById('asset-profiling-tab').addEventListener('shown.bs.tab', function() {
                loadAssetProfiling(assetName);
            });
            
            // Set up tab event listener for AI Analysis
            const aiAnalysisTab = document.getElementById('asset-ai-analysis-tab');
            aiAnalysisTab.replaceWith(aiAnalysisTab.cloneNode(true));
            document.getElementById('asset-ai-analysis-tab').addEventListener('shown.bs.tab', function() {
                loadAssetAIAnalysis(assetName);
            });
            
            // Set up tab event listener for PII Scan
            const piiScanTab = document.getElementById('asset-pii-scan-tab');
            piiScanTab.replaceWith(piiScanTab.cloneNode(true));
            document.getElementById('asset-pii-scan-tab').addEventListener('shown.bs.tab', function() {
                loadAssetPIIScan(assetName);
            });
            
        } else {
            console.error('No asset data in response:', response);
            document.getElementById('asset-modal-title').textContent = 'Error Loading Asset';
            document.getElementById('asset-overview-content').innerHTML = `
                <div class="alert alert-danger">
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    Failed to load asset details. ${response?.error || 'Asset not found'}
                </div>
            `;
        }
    } catch (error) {
        console.error('Error loading asset details:', error);
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
    
    // Show loading state
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
            
            // Statistics Overview
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
            
            // Data Type Distribution
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
            
            // PII Analysis
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
            
            // Add error message if any
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

// AI Analysis Function
async function loadAssetAIAnalysis(assetName) {
    const aiAnalysisContent = document.getElementById('asset-ai-analysis-content');
    
    // Show loading state
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

// PII Scan Function
async function loadAssetPIIScan(assetName) {
    const piiScanContent = document.getElementById('asset-pii-scan-content');
    
    // Show loading state
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

// Automatic Background Monitoring
let backgroundMonitoringInterval = null;
let monitoringEnabled = true;

function startBackgroundMonitoring() {
    if (backgroundMonitoringInterval) {
        clearInterval(backgroundMonitoringInterval);
    }
    
    // Monitor every 7 seconds (between 5-10 seconds as requested)
    backgroundMonitoringInterval = setInterval(async () => {
        if (monitoringEnabled) {
            await performBackgroundMonitoring();
        }
    }, 7000);
    
    console.log('🔄 Background monitoring started (every 7 seconds)');
}

function stopBackgroundMonitoring() {
    if (backgroundMonitoringInterval) {
        clearInterval(backgroundMonitoringInterval);
        backgroundMonitoringInterval = null;
    }
    console.log('⏹️ Background monitoring stopped');
}

async function performBackgroundMonitoring() {
    try {
        // Check system health and connector status
        const healthResponse = await apiCall('/system/health');
        const connectorsResponse = await apiCall('/connectors');
        
        // Update dashboard metrics
        if (healthResponse.status === 'success') {
            updateSystemStatus();
        }
        
        // Check for new assets from enabled connectors
        const assetsResponse = await apiCall('/assets');
        if (assetsResponse.status === 'success' && assetsResponse.assets) {
            // Update assets count if it changed
            const currentAssetCount = assetsResponse.assets.length;
            const lastAssetCount = window.lastAssetCount || 0;
            
            if (currentAssetCount > lastAssetCount) {
                console.log(`📊 New assets discovered: ${currentAssetCount - lastAssetCount} new assets`);
                // Refresh assets view if user is on assets tab
                if (document.getElementById('assets-tab')?.classList.contains('active')) {
                    loadAssets();
                }
                // Update dashboard
                updateSystemStatus();
            }
            
            window.lastAssetCount = currentAssetCount;
        }
        
        // Update connector status
        if (connectorsResponse.status === 'success') {
            // Update connector counts
            const enabledCount = connectorsResponse.enabled_connectors || 0;
            const totalCount = connectorsResponse.total_connectors || 0;
            
            const badge = document.getElementById('total-connectors-badge');
            if (badge) {
                badge.textContent = `${totalCount} (${enabledCount} enabled)`;
            }
        }
        
    } catch (error) {
        console.warn('Background monitoring error:', error);
        // Don't show notifications for background monitoring errors to avoid spam
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
    
    // Auto-refresh lineage when assets are updated
    autoRefreshLineageAssets();
}

function startAutoRefresh() {
    // Refresh every 5 seconds for more dynamic updates
    refreshInterval = setInterval(() => {
        updateSystemStatus();
        
        // Auto-refresh lineage assets if lineage tab has been visited
        const lineageTab = document.getElementById('lineage-asset-select');
        if (lineageTab) {
            autoRefreshLineageAssets();
        }
    }, 5000);
    
    console.log('Auto-refresh started - updating every 5 seconds (including lineage)');
}

// Start dynamic dashboard with real-time updates
function startDynamicDashboard() {
    // Update dashboard metrics every 3 seconds for more dynamic updates
    setInterval(() => {
        updateSystemStatus();
        loadRecentActivity();
    }, 3000);
    
    // Update charts every 30 seconds
    setInterval(() => {
        updateDiscoveryChart();
    }, 30000);
}

// Load recent activity for dashboard
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
        console.error('Failed to load recent activity:', error);
    }
}

// Get activity icon based on type
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

// Get human-readable time ago
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

// Update discovery chart with real data
function updateDiscoveryChart(metrics) {
    if (!metrics || !discoveryChart) return;
    
    try {
        const ctx = document.getElementById('discoveryChart');
        if (!ctx) return;
        
        // Update chart with assets by type data
        const assetsData = metrics.assets_by_type || {};
        const labels = Object.keys(assetsData);
        const data = Object.values(assetsData);
        
        if (discoveryChart.data) {
            discoveryChart.data.labels = labels;
            discoveryChart.data.datasets[0].data = data;
            discoveryChart.update('none'); // Smooth update without animation
        }
    } catch (error) {
        console.error('Failed to update discovery chart:', error);
    }
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
        testButton.innerHTML = '<i class="fas fa-link me-2"></i>Test Connection';
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
                    
                    // Refresh assets tab
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
        testButton.innerHTML = '<i class="fas fa-link me-2"></i>Test Connection';
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
        // Check if GCP connector already exists
        const healthResponse = await fetch('/api/system/health');
        const healthData = await healthResponse.json();
        const gcpConnector = healthData.connector_status?.gcp;
        
        // Only add the connector if it doesn't exist or isn't configured
        if (!gcpConnector || !gcpConnector.configured) {
            const projectId = document.getElementById('project-id')?.value;
            const serviceAccountJson = document.getElementById('service-account-json')?.value;
            
            if (projectId && serviceAccountJson) {
                console.log('Adding GCP connector...');
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
                console.log('GCP connector added successfully');
            } else {
                throw new Error('Project ID or Service Account JSON not found');
            }
        } else {
            console.log('GCP connector already exists and is configured');
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
            
            // Add to My Connections
            const connectionData = {
                name: config.name || getConnectorDisplayName(connectorId),
                type: getConnectorDisplayName(connectorId),
                host: config.config?.host || config.config?.endpoint || config.config?.project_id || 'N/A',
                endpoint: config.config?.endpoint || config.config?.host,
                ...config.config
            };
            addUserConnection(connectionData);
            
            // Trigger asset discovery for the new connection
            setTimeout(async () => {
                await discoverAssetsForConnection(connectorId, connectionData.name);
            }, 1000); // Small delay to ensure connection is fully saved
            
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
            { type: 's3', icon: 'fas fa-cloud-arrow-up', title: 'S3 Storage', description: 'Connect to Amazon S3 buckets and objects' },
            { type: 'rds', icon: 'fas fa-database', title: 'RDS Database', description: 'Connect to Amazon RDS instances (MySQL, PostgreSQL, etc.)' },
            { type: 'redshift', icon: 'fas fa-chart-line', title: 'Redshift', description: 'Connect to Amazon Redshift data warehouse' },
            { type: 'dynamodb', icon: 'fas fa-table', title: 'DynamoDB', description: 'Connect to Amazon DynamoDB NoSQL database' }
        ],
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





// Initialize connection type selection
document.addEventListener('DOMContentLoaded', function() {
    // Add click handlers for connection type cards
    setTimeout(() => {
        document.querySelectorAll('.connection-type-card').forEach(card => {
            card.addEventListener('click', function() {
                selectConnectionType(this.dataset.type);
            });
        });
        
        // Initialize My Connections section
        refreshMyConnections();
        
        // Initialize My Connections display
        console.log('Initializing My Connections with', userConnections.length, 'connections');
        
    }, 500);
});

// My Connections Management
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
        
        // Update UI to show testing state
        updateConnectionStatus(connectionId, 'testing');
        
        // Map connection type to proper connector ID
        let connectorId = connection.type.toLowerCase().replace(/\s+/g, '_');
        if (connection.type.includes('Google Cloud')) connectorId = 'gcp';
        if (connection.type.includes('AWS')) connectorId = 'aws';
        if (connection.type.includes('Azure')) connectorId = 'azure';
        
        // Make API call to test the connection using the existing endpoint
        const response = await apiCall(`/config/${connectorId}/test`, {
            method: 'POST'
        });
        
        if (response && (response.success || response.status === 'success' || response.connection_status === 'connected')) {
            updateConnectionStatus(connectionId, 'connected', new Date().toISOString());
            showNotification(`Connection test successful: ${connection.name}`, 'success');
            
            // Trigger asset discovery after successful connection
            await discoverAssetsForConnection(connectorId, connection.name);
        } else {
            updateConnectionStatus(connectionId, 'error', new Date().toISOString());
            showNotification(`Connection test failed: ${connection.name} - ${response?.error || 'Unknown error'}`, 'error');
        }
    } catch (error) {
        console.error('Connection test failed:', error);
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
        // Create a detailed view modal or expand the card
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
    
    // Remove existing modal if any
    const existingModal = document.getElementById('connectionDetailsModal');
    if (existingModal) {
        existingModal.remove();
    }
    
    // Add modal to body
    document.body.insertAdjacentHTML('beforeend', modalContent);
    
    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('connectionDetailsModal'));
    modal.show();
    
        // Clean up modal after it's hidden
        document.getElementById('connectionDetailsModal').addEventListener('hidden.bs.modal', function() {
            this.remove();
        });
    } catch (error) {
        console.error('Error showing connection details:', error);
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
            console.error('Error removing connection:', error);
            showNotification('Failed to remove connection', 'error');
        }
    }
}

// Asset Discovery Integration
async function discoverAssetsForConnection(connectorId, connectionName) {
    try {
        showNotification(`Discovering assets for ${connectionName}...`, 'info');
        
        // Call discovery API
        const response = await apiCall(`/discovery/test/${connectorId}`, {
            method: 'POST'
        });
        
        if (response && response.status === 'success') {
            const assetsCount = response.assets_discovered || 0;
            console.log('Discovery response:', response);
            console.log('Discovered assets:', response.assets);
            showNotification(`Discovery completed! Found ${assetsCount} assets from ${connectionName}`, 'success');
            
            // Refresh the assets table to show new assets
            await loadAssets();
            
            // Update dashboard stats
            refreshDashboard();
            
            // Switch to Assets tab to show the results
            switchToAssetsTab();
            
            // Show discovery results modal if there are assets
            if (response.assets && response.assets.length > 0) {
                showDiscoveryResultsModal(connectionName, response.assets);
            }
        } else {
            showNotification(`Asset discovery failed for ${connectionName}: ${response?.message || 'Unknown error'}`, 'warning');
        }
    } catch (error) {
        console.error('Asset discovery failed:', error);
        showNotification(`Asset discovery failed for ${connectionName}`, 'error');
    }
}

function switchToAssetsTab() {
    // Switch to the Assets tab
    const assetsTab = document.querySelector('#assets-tab');
    const assetsTabPane = document.querySelector('#assets');
    
    if (assetsTab && assetsTabPane) {
        // Remove active class from all tabs
        document.querySelectorAll('.nav-link').forEach(tab => tab.classList.remove('active'));
        document.querySelectorAll('.tab-pane').forEach(pane => {
            pane.classList.remove('show', 'active');
        });
        
        // Activate assets tab
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
    
    // Remove existing modal if any
    const existingModal = document.getElementById('discoveryResultsModal');
    if (existingModal) {
        existingModal.remove();
    }
    
    // Add modal to body
    document.body.insertAdjacentHTML('beforeend', modalContent);
    
    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('discoveryResultsModal'));
    modal.show();
    
    // Clean up modal after it's hidden
    document.getElementById('discoveryResultsModal').addEventListener('hidden.bs.modal', function() {
        this.remove();
    });
}

// Helper function to format bytes (if not already defined)
function formatBytes(bytes, decimals = 2) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

// Export functions for global access
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

// Debug function to test if buttons work
window.testButtonDebug = function() {
    console.log('Button test function called');
    alert('Button works!');
};

// Debug function to check connections
window.debugConnections = function() {
    console.log('Current userConnections:', userConnections);
    console.log('Functions available:', {
        testMyConnection: typeof window.testMyConnection,
        viewMyConnection: typeof window.viewMyConnection,
        removeMyConnection: typeof window.removeMyConnection
    });
};

// Data Lineage Functions - Initialize all variables properly
window.currentLineageData = null;
window.selectedAssetId = null;
window.lineageAssetsCache = [];
window.lastAssetCount = 0;
window.lineageInitialized = false;

// Auto-refresh lineage assets when main assets are updated
function autoRefreshLineageAssets() {
    try {
        // Only refresh if the lineage tab is active or has been visited
        const lineageTab = document.getElementById('lineage-asset-select');
        if (!lineageTab) return;
        
        // Initialize if not done already
        if (!window.lineageInitialized) {
            window.lastAssetCount = 0;
            window.lineageInitialized = true;
        }
        
        // Check if the assets count has changed to trigger refresh
        const currentAssetsCount = Object.values(currentAssets || {}).reduce((count, sourceAssets) => count + sourceAssets.length, 0);
        
        console.log(`🔄 Asset count check: current=${currentAssetsCount}, last=${window.lastAssetCount}`);
        
        if (currentAssetsCount !== window.lastAssetCount) {
            console.log('📈 Asset count changed, refreshing lineage...');
            window.lastAssetCount = currentAssetsCount;
            loadLineageAssetsFromCache();
            
            // If an asset was previously selected, refresh its lineage
            if (window.selectedAssetId) {
                loadAssetLineage(window.selectedAssetId);
            }
        }
    } catch (error) {
        console.error('❌ Error in autoRefreshLineageAssets:', error);
    }
}

// Load lineage assets from current asset data (more efficient)
function loadLineageAssetsFromCache() {
    try {
        // Show sync status
        const syncStatus = document.getElementById('lineage-sync-status');
        if (syncStatus) {
            syncStatus.style.display = 'inline-block';
        }
        
        const assets = [];
        
        // Convert current assets to lineage format
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
        
        // Update the UI
        updateLineageAssetsDropdown(assets);
        
        // Update count badge
        const countBadge = document.getElementById('lineage-assets-count');
        if (countBadge) {
            countBadge.textContent = assets.length;
            
            // Simple update without scaling animation
        }
        
        console.log(`Lineage assets updated: ${assets.length} assets available`);
        
        // Hide sync status after successful update
        setTimeout(() => {
            const syncStatus = document.getElementById('lineage-sync-status');
            if (syncStatus) {
                syncStatus.style.display = 'none';
            }
        }, 1000);
        
    } catch (error) {
        console.error('Error loading lineage assets from cache:', error);
        
        // Hide sync status on error too
        const syncStatus = document.getElementById('lineage-sync-status');
        if (syncStatus) {
            syncStatus.style.display = 'none';
        }
    }
}

// Helper function to generate asset ID if not present
function generateAssetId(asset) {
    const data = `${asset.source || ''}-${asset.location || ''}-${asset.name || ''}`;
    return btoa(data).replace(/[^a-zA-Z0-9]/g, '').substring(0, 16);
}

// Update the assets dropdown in lineage tab
function updateLineageAssetsDropdown(assets) {
    console.log('🎛️ Updating lineage assets dropdown with', assets.length, 'assets');
    
    const assetSelect = document.getElementById('lineage-asset-select');
    if (!assetSelect) {
        console.error('❌ lineage-asset-select element not found');
        return;
    }
    
    const currentlySelected = assetSelect.value;
    assetSelect.innerHTML = '<option value="">Select an asset to view its lineage...</option>';
    
    // Sort assets by name for better UX
    assets.sort((a, b) => a.name.localeCompare(b.name));
    console.log('📋 First 5 assets to add:', assets.slice(0, 5).map(a => `${a.name} (${a.type})`));
    
    assets.forEach((asset, index) => {
        const option = document.createElement('option');
        option.value = asset.id;
        option.textContent = `${asset.name} (${asset.type}) - ${asset.source}`;
        option.dataset.asset = JSON.stringify(asset);
        
        // Restore selection if the same asset is still available
        if (asset.id === currentlySelected) {
            option.selected = true;
        }
        
        assetSelect.appendChild(option);
        
        if (index < 3) {
            console.log('➕ Added option:', option.textContent);
        }
    });
    
    console.log('✅ Dropdown now has', assetSelect.options.length - 1, 'asset options');
    
    // Show notification if new assets were added (only if we had assets before)
    const previousCount = window.lineageAssetsCache.length || 0;
    if (assets.length > previousCount && previousCount > 0) {
        const newAssets = assets.length - previousCount;
        showNotification(`🔄 ${newAssets} new asset${newAssets > 1 ? 's' : ''} added to lineage explorer`, 'success');
        
        // Add visual indicator on the lineage tab
        const lineageTabButton = document.getElementById('lineage-tab');
        if (lineageTabButton) {
            // Add a subtle pulse effect to show there's new content
            lineageTabButton.style.animation = 'pulse 2s infinite';
            setTimeout(() => {
                lineageTabButton.style.animation = '';
            }, 6000);
        }
    }
    
    // Update the cache after UI update
    window.lineageAssetsCache = assets;
}

async function loadLineageAssets() {
    try {
        console.log('🔄 Loading lineage assets directly from API...');
        
        // Always fetch fresh data from API for debugging
        const response = await apiCall('/lineage/assets');
        console.log('📡 Lineage API response:', response);
        
        if (response && response.status === 'success') {
            const assets = response.assets || [];
            console.log('✅ Received', assets.length, 'assets from lineage API');
            console.log('📋 Sample assets:', assets.slice(0, 3).map(a => a.name));
            
            // Initialize lineage system
            if (!window.lineageInitialized) {
                window.lineageInitialized = true;
                window.lastAssetCount = assets.length;
                console.log('🚀 Lineage system initialized');
            }
            
            // Update the UI using the new dropdown function
            updateLineageAssetsDropdown(assets);
            
            // Update count badge
            const countBadge = document.getElementById('lineage-assets-count');
            if (countBadge) {
                countBadge.textContent = assets.length;
                console.log('🎯 Updated count badge to:', assets.length);
            }
            
            console.log('✅ Lineage assets loaded successfully!');
            
        } else {
            console.error('❌ API returned error:', response);
            const message = response?.message || 'Unknown error';
            showNotification('Failed to load lineage assets: ' + message, 'danger');
            
            // Initialize empty state
            if (!window.lineageInitialized) {
                window.lineageInitialized = true;
                window.lastAssetCount = 0;
                window.lineageAssetsCache = [];
            }
        }
        
    } catch (error) {
        console.error('💥 Error loading lineage assets:', error);
        showNotification('Error loading lineage assets: ' + error.message, 'danger');
    }
}

async function selectAssetForLineage() {
    const assetSelect = document.getElementById('lineage-asset-select');
    const selectedOption = assetSelect.selectedOptions[0];
    
    if (!selectedOption || !selectedOption.value) {
        clearLineageVisualization();
        return;
    }
    
    window.selectedAssetId = selectedOption.value;
    const asset = JSON.parse(selectedOption.dataset.asset);
    
    // Update lineage summary
    updateLineageSummary(asset);
    
    // Load and display lineage
    await loadAssetLineage(window.selectedAssetId);
}

function updateLineageSummary(asset) {
    const summaryDiv = document.getElementById('lineage-summary');
    
    summaryDiv.innerHTML = `
        <div class="border-start-primary p-3">
            <h6 class="fw-bold text-primary">${asset.name}</h6>
            <p class="mb-1"><strong>Type:</strong> ${asset.type}</p>
            <p class="mb-1"><strong>Source:</strong> ${asset.source}</p>
            <p class="mb-1"><strong>Columns:</strong> ${asset.column_count}</p>
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
            
            // Update analysis panel
            const selectedAsset = window.currentLineageData.nodes.find(node => node.is_target);
            if (selectedAsset) {
                updateAnalysisPanel(selectedAsset, window.currentLineageData);
            }
            
            // Load column-level lineage
            await loadColumnLineage(assetId);
            
        } else {
            showNotification('Failed to load asset lineage', 'danger');
            clearLineageVisualization();
        }
        
    } catch (error) {
        console.error('Error loading asset lineage:', error);
        showNotification('Error loading asset lineage: ' + error.message, 'danger');
        clearLineageVisualization();
    }
}

function displayLineageVisualization(lineageData) {
    const currentView = document.querySelector('input[name="lineage-view"]:checked').id;
    
    if (currentView === 'graph-view') {
        displayLineageGraph(lineageData);
    } else {
        displayLineageTable(lineageData);
    }
}

function displayLineageGraph(lineageData) {
    const graphContainer = document.getElementById('lineage-graph');
    
    // Clear existing content
    graphContainer.innerHTML = '';
    
    // Create a simple force-directed graph visualization
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
    
    // Get container dimensions for responsive sizing
    const containerRect = graphContainer.getBoundingClientRect();
    const containerWidth = Math.max(800, containerRect.width || 800);
    const containerHeight = Math.min(600, Math.max(400, containerRect.height || 500));
    
    // Create SVG for the graph with fixed dimensions
    const svgWidth = 800;
    const svgHeight = 500;
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('width', '100%');
    svg.setAttribute('height', '100%');
    svg.setAttribute('viewBox', `0 0 ${svgWidth} ${svgHeight}`);
    svg.style.display = 'block';
    
    // Add pan and zoom functionality
    let isPanning = false;
    let startPoint = { x: 0, y: 0 };
    let currentTransform = { x: 0, y: 0, scale: 1 };
    
    // Create a group for all graph elements
    const graphGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    graphGroup.setAttribute('id', 'graph-group');
    svg.appendChild(graphGroup);
    
    // Store edge elements for easy updates (make it accessible to drag handlers)
    let edgeElements = [];
    
    // Group nodes by level for layout
    const levelGroups = {};
    nodes.forEach(node => {
        if (!levelGroups[node.level]) {
            levelGroups[node.level] = [];
        }
        levelGroups[node.level].push(node);
    });
    
    const levels = Object.keys(levelGroups).map(Number).sort((a, b) => a - b);
    const centerX = svgWidth / 2;
    const centerY = svgHeight / 2;
    const levelSpacing = Math.min(120, svgHeight / (levels.length + 1));
    
    // Position nodes
    const nodePositions = {};
    levels.forEach((level, levelIndex) => {
        const levelNodes = levelGroups[level];
        const yOffset = level * levelSpacing;
        const nodeSpacing = Math.min(100, svgWidth / Math.max(levelNodes.length + 1, 1));
        
        levelNodes.forEach((node, nodeIndex) => {
            const x = centerX + (nodeIndex - (levelNodes.length - 1) / 2) * nodeSpacing;
            const y = centerY + yOffset;
            nodePositions[node.id] = { x, y, node };
        });
    });
    
    // Draw edges
    
    edges.forEach((edge, index) => {
        const sourcePos = nodePositions[edge.source];
        const targetPos = nodePositions[edge.target];
        
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
                source: edge.source,
                target: edge.target
            });
            
            graphGroup.appendChild(line);
        }
    });
    
    // Add arrow marker definition
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
    
    // Draw nodes
    Object.values(nodePositions).forEach(({ x, y, node }) => {
        // Node circle
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
        
        // Node label
        const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('x', x);
        text.setAttribute('y', y - 35);
        text.setAttribute('text-anchor', 'middle');
        text.setAttribute('fill', '#333');
        text.setAttribute('font-size', '12');
        text.setAttribute('font-weight', 'bold');
        text.textContent = node.name.length > 15 ? node.name.substring(0, 15) + '...' : node.name;
        text.setAttribute('pointer-events', 'none'); // Prevent text from interfering with drag
        
        // Node type label
        const typeText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        typeText.setAttribute('x', x);
        typeText.setAttribute('y', y + 45);
        typeText.setAttribute('text-anchor', 'middle');
        typeText.setAttribute('fill', '#666');
        typeText.setAttribute('font-size', '10');
        typeText.textContent = node.type;
        typeText.setAttribute('pointer-events', 'none'); // Prevent text from interfering with drag
        
        // Make nodes draggable
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
            
            // Calculate new position
            const newX = initialPos.x + (currentX - dragStart.x);
            const newY = initialPos.y + (currentY - dragStart.y);
            
            // Keep nodes within SVG bounds
            const boundedX = Math.max(30, Math.min(svgWidth - 30, newX));
            const boundedY = Math.max(40, Math.min(svgHeight - 50, newY));
            
            // Update node position
            circle.setAttribute('cx', boundedX);
            circle.setAttribute('cy', boundedY);
            text.setAttribute('x', boundedX);
            text.setAttribute('y', boundedY - 35);
            typeText.setAttribute('x', boundedX);
            typeText.setAttribute('y', boundedY + 45);
            
            // Update position in nodePositions for edge updates
            nodePositions[node.id].x = boundedX;
            nodePositions[node.id].y = boundedY;
            
            // Update connected edges
            updateConnectedEdges(node.id, nodePositions, edgeElements);
        });
        
        svg.addEventListener('mouseup', () => {
            if (isDragging && circle.getAttribute('data-node-id') === node.id) {
                isDragging = false;
                circle.classList.remove('dragging');
                circle.style.cursor = 'grab';
            }
        });
        
        // Add click event for column lineage (only when not dragging)
        circle.addEventListener('click', (e) => {
            if (!isDragging) {
                // Check if we're in edit mode first
                if (handleEditModeNodeClick(node.id, circle)) {
                    // Edit mode handled the click
                    return;
                }
                // Normal mode - toggle column view
                toggleNodeColumnView(node.id, circle, text, typeText);
            }
        });
        
        graphGroup.appendChild(circle);
        graphGroup.appendChild(text);
        graphGroup.appendChild(typeText);
    });
    
    graphContainer.appendChild(svg);
    
    // Auto-fit the graph to screen after rendering
    setTimeout(() => {
        fitLineageToScreen();
    }, 100);
}

// Helper function to update connected edges when nodes are dragged
function updateConnectedEdges(nodeId, nodePositions, edgeElements) {
    // Update all edges connected to the dragged node
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

// Toggle between normal node view and expanded column view
async function toggleNodeColumnView(nodeId, circleElement, textElement, typeTextElement) {
    console.log('🎯 toggleNodeColumnView called for nodeId:', nodeId);
    const isExpanded = circleElement.getAttribute('data-expanded') === 'true';
    
    if (isExpanded) {
        // Collapse to normal view
        console.log('🔽 Collapsing node columns for:', nodeId);
        collapseNodeColumns(nodeId, circleElement, textElement, typeTextElement);
    } else {
        // Expand to show columns and load column lineage
        console.log('🔼 Expanding node columns for:', nodeId);
        await expandNodeColumns(nodeId, circleElement, textElement, typeTextElement);
        
        // Also load column lineage data for the column lineage section
        console.log('📊 Loading column lineage for clicked node:', nodeId);
        await loadColumnLineage(nodeId);
    }
}

// Expand node to show its columns
async function expandNodeColumns(nodeId, circleElement, textElement, typeTextElement) {
    try {
        console.log('🔍 Expanding columns for node:', nodeId);
        
        // Get column data for this asset
        const response = await apiCall(`/lineage/${nodeId}/columns`);
        
        if (response.status !== 'success' || !response.column_lineage.columns.length) {
            showNotification('No column data available for this asset', 'info');
            return;
        }
        
        const columns = response.column_lineage.columns;
        const svg = circleElement.closest('svg');
        const graphGroup = svg.querySelector('#graph-group');
        
        // Get current node position
        const nodeX = parseFloat(circleElement.getAttribute('cx'));
        const nodeY = parseFloat(circleElement.getAttribute('cy'));
        
        // Hide original node elements temporarily
        circleElement.style.display = 'none';
        textElement.style.display = 'none';
        typeTextElement.style.display = 'none';
        
        // Create expanded node group
        const expandedGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        expandedGroup.setAttribute('id', `expanded-${nodeId}`);
        expandedGroup.setAttribute('data-node-id', nodeId);
        
        // Calculate dimensions for expanded view
        const columnHeight = 25;
        const nodeWidth = 200;
        const totalHeight = Math.max(60, columns.length * columnHeight + 40);
        
        // Create main node rectangle
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
        
        // Add node title
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
        
        // Add separator line
        const separatorLine = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        separatorLine.setAttribute('x1', nodeX - nodeWidth/2 + 10);
        separatorLine.setAttribute('y1', nodeY - totalHeight/2 + 30);
        separatorLine.setAttribute('x2', nodeX + nodeWidth/2 - 10);
        separatorLine.setAttribute('y2', nodeY - totalHeight/2 + 30);
        separatorLine.setAttribute('stroke', '#fff');
        separatorLine.setAttribute('stroke-width', '1');
        expandedGroup.appendChild(separatorLine);
        
        // Add columns
        columns.forEach((column, index) => {
            const columnY = nodeY - totalHeight/2 + 45 + (index * columnHeight);
            
            // Column name
            const columnText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            columnText.setAttribute('x', nodeX - nodeWidth/2 + 15);
            columnText.setAttribute('y', columnY);
            columnText.setAttribute('fill', '#fff');
            columnText.setAttribute('font-size', '10');
            columnText.setAttribute('font-weight', '500');
            const columnName = column.name.length > 15 ? column.name.substring(0, 15) + '...' : column.name;
            columnText.textContent = columnName;
            expandedGroup.appendChild(columnText);
            
            // Column type
            const typeText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            typeText.setAttribute('x', nodeX + nodeWidth/2 - 15);
            typeText.setAttribute('y', columnY);
            typeText.setAttribute('text-anchor', 'end');
            typeText.setAttribute('fill', '#fff');
            typeText.setAttribute('font-size', '9');
            typeText.setAttribute('opacity', '0.8');
            typeText.textContent = column.type;
            expandedGroup.appendChild(typeText);
            
            // Add connection points for column mapping
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
            
            // Add hover effect for connection points
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
        
        // Add collapse button
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
        
        // Add to graph
        graphGroup.appendChild(expandedGroup);
        
        // Mark as expanded
        circleElement.setAttribute('data-expanded', 'true');
        
        // Draw column-to-column connections after expansion
        setTimeout(() => {
            drawColumnConnections(nodeId, columns);
        }, 100);
        
        console.log('✅ Node expanded with', columns.length, 'columns');
        
    } catch (error) {
        console.error('❌ Error expanding node columns:', error);
        showNotification('Error loading column details: ' + error.message, 'danger');
    }
}

// Collapse node back to normal view
function collapseNodeColumns(nodeId, circleElement, textElement, typeTextElement) {
    const svg = circleElement.closest('svg');
    const expandedGroup = svg.querySelector(`#expanded-${nodeId}`);
    
    if (expandedGroup) {
        expandedGroup.remove();
    }
    
    // Show original node elements
    circleElement.style.display = 'block';
    textElement.style.display = 'block';
    typeTextElement.style.display = 'block';
    
    // Mark as collapsed
    circleElement.setAttribute('data-expanded', 'false');
    
    // Clear any column mapping highlights and connections
    clearColumnMappingHighlights();
    clearColumnConnections(nodeId);
    
    console.log('✅ Node collapsed to normal view');
}

// Highlight column mappings between nodes
function highlightColumnMappings(nodeId, columnName) {
    if (!window.currentColumnLineage || !window.currentColumnLineage.columns) return;
    
    const column = window.currentColumnLineage.columns.find(col => col.name === columnName);
    if (!column) return;
    
    console.log('🔗 Highlighting mappings for column:', columnName);
    
    // Add visual indicators for upstream/downstream relationships
    const svg = document.querySelector('#lineage-graph svg');
    const connectionPoints = svg.querySelectorAll(`[data-column="${columnName}"][data-node="${nodeId}"]`);
    
    connectionPoints.forEach(point => {
        point.setAttribute('fill', '#ffc107');
        point.setAttribute('stroke', '#ff6b35');
    });
}

// Clear column mapping highlights
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

// Draw column-to-column connection lines
async function drawColumnConnections(sourceNodeId, sourceColumns) {
    try {
        console.log('🔗 Drawing column connections for node:', sourceNodeId);
        
        const svg = document.querySelector('#lineage-graph svg');
        const graphGroup = svg.querySelector('#graph-group');
        
        // Get all expanded nodes in the graph
        const expandedNodes = svg.querySelectorAll('[id^="expanded-"]');
        
        for (const sourceColumn of sourceColumns) {
            // Get upstream and downstream relationships for this column
            const upstreamColumns = sourceColumn.upstream_columns || [];
            const downstreamColumns = sourceColumn.downstream_columns || [];
            
            // Draw connections to upstream columns
            for (const upstream of upstreamColumns) {
                await drawColumnConnection(
                    sourceNodeId, sourceColumn.name,
                    upstream.asset, upstream.column,
                    'upstream', upstream.confidence,
                    graphGroup
                );
            }
            
            // Draw connections to downstream columns
            for (const downstream of downstreamColumns) {
                await drawColumnConnection(
                    sourceNodeId, sourceColumn.name,
                    downstream.asset, downstream.column,
                    'downstream', downstream.confidence,
                    graphGroup
                );
            }
        }
        
        console.log('✅ Column connections drawn');
        
    } catch (error) {
        console.error('❌ Error drawing column connections:', error);
    }
}

// Draw a single column-to-column connection line
async function drawColumnConnection(sourceNodeId, sourceColumnName, targetAssetName, targetColumnName, direction, confidence, graphGroup) {
    try {
        // Find the target node by asset name
        const targetNodeId = await findNodeIdByAssetName(targetAssetName);
        if (!targetNodeId) {
            console.log(`⚠️ Target node not found for asset: ${targetAssetName}`);
            return;
        }
        
        // Check if target node is expanded
        const targetExpandedNode = document.querySelector(`#expanded-${targetNodeId}`);
        if (!targetExpandedNode) {
            console.log(`⚠️ Target node not expanded: ${targetAssetName}`);
            return;
        }
        
        // Find source column connection point
        const sourceConnectionPoint = document.querySelector(`[data-node="${sourceNodeId}"][data-column="${sourceColumnName}"]`);
        if (!sourceConnectionPoint) {
            console.log(`⚠️ Source connection point not found: ${sourceColumnName}`);
            return;
        }
        
        // Find target column connection point
        const targetConnectionPoint = document.querySelector(`[data-node="${targetNodeId}"][data-column="${targetColumnName}"]`);
        if (!targetConnectionPoint) {
            console.log(`⚠️ Target connection point not found: ${targetColumnName}`);
            return;
        }
        
        // Get coordinates
        const sourceX = parseFloat(sourceConnectionPoint.getAttribute('cx'));
        const sourceY = parseFloat(sourceConnectionPoint.getAttribute('cy'));
        const targetX = parseFloat(targetConnectionPoint.getAttribute('cx'));
        const targetY = parseFloat(targetConnectionPoint.getAttribute('cy'));
        
        // Create curved connection line
        const connectionLine = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        
        // Calculate control points for smooth curve
        const midX = (sourceX + targetX) / 2;
        const controlOffset = Math.abs(targetX - sourceX) * 0.3;
        const controlX1 = sourceX + (direction === 'downstream' ? controlOffset : -controlOffset);
        const controlX2 = targetX + (direction === 'downstream' ? -controlOffset : controlOffset);
        
        // Create curved path
        const pathData = `M ${sourceX} ${sourceY} C ${controlX1} ${sourceY}, ${controlX2} ${targetY}, ${targetX} ${targetY}`;
        connectionLine.setAttribute('d', pathData);
        
        // Style the connection line
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
        
        // Add hover effects
        connectionLine.addEventListener('mouseenter', () => {
            connectionLine.setAttribute('stroke-width', '3');
            connectionLine.setAttribute('opacity', '1');
            
            // Highlight connected columns
            sourceConnectionPoint.setAttribute('r', '6');
            targetConnectionPoint.setAttribute('r', '6');
            sourceConnectionPoint.setAttribute('fill', lineColor);
            targetConnectionPoint.setAttribute('fill', lineColor);
        });
        
        connectionLine.addEventListener('mouseleave', () => {
            connectionLine.setAttribute('stroke-width', '2');
            connectionLine.setAttribute('opacity', lineOpacity);
            
            // Reset column highlights
            sourceConnectionPoint.setAttribute('r', '3');
            targetConnectionPoint.setAttribute('r', '3');
            sourceConnectionPoint.setAttribute('fill', '#fff');
            targetConnectionPoint.setAttribute('fill', '#fff');
        });
        
        // Add click handler for details
        connectionLine.addEventListener('click', () => {
            showColumnConnectionDetails(sourceColumnName, targetColumnName, direction, confidence);
        });
        
        // Add to graph (behind nodes)
        graphGroup.insertBefore(connectionLine, graphGroup.firstChild);
        
        console.log(`✅ Column connection drawn: ${sourceColumnName} → ${targetColumnName} (${direction})`);
        
    } catch (error) {
        console.error('❌ Error drawing column connection:', error);
    }
}

// Find node ID by asset name
async function findNodeIdByAssetName(assetName) {
    try {
        // Get current lineage data
        if (!window.currentLineageData || !window.currentLineageData.nodes) {
            return null;
        }
        
        // Find node with matching name
        const matchingNode = window.currentLineageData.nodes.find(node => 
            node.name === assetName || 
            node.name.includes(assetName) ||
            assetName.includes(node.name)
        );
        
        return matchingNode ? matchingNode.id : null;
        
    } catch (error) {
        console.error('❌ Error finding node ID:', error);
        return null;
    }
}

// Clear column connection lines for a specific node
function clearColumnConnections(nodeId) {
    const svg = document.querySelector('#lineage-graph svg');
    if (!svg) return;
    
    // Remove all connection lines involving this node
    const connectionLines = svg.querySelectorAll(`.column-connection-line[data-source-node="${nodeId}"], .column-connection-line[data-target-node="${nodeId}"]`);
    connectionLines.forEach(line => line.remove());
    
    console.log(`🧹 Cleared column connections for node: ${nodeId}`);
}

// Show details about a column connection
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
        console.log('🔍 Loading column lineage for asset:', assetId);
        console.log('🔍 API URL will be: /lineage/' + assetId + '/columns');
        const response = await apiCall(`/lineage/${assetId}/columns`);
        
        console.log('📊 Column lineage response:', response);
        console.log('📊 Response status:', response?.status);
        console.log('📊 Response columns:', response?.column_lineage?.columns?.length);
        
        if (response.status === 'success') {
            // Store column lineage data for display
            window.currentColumnLineage = response.column_lineage;
            console.log('✅ Column lineage loaded:', window.currentColumnLineage);
            console.log('✅ Columns count:', window.currentColumnLineage?.columns?.length || 0);
            
            // Automatically show column lineage if we have data
            if (window.currentColumnLineage && window.currentColumnLineage.columns && window.currentColumnLineage.columns.length > 0) {
                console.log('🎯 Calling showColumnLineage with', window.currentColumnLineage.columns.length, 'columns');
                showColumnLineage(assetId);
            } else {
                console.warn('⚠️ No columns found in response');
                showColumnLineageEmpty();
            }
        } else {
            console.warn('⚠️ Column lineage API returned error:', response.message);
            showColumnLineageEmpty();
        }
        
    } catch (error) {
        console.error('❌ Error loading column lineage:', error);
    }
}

function showColumnLineage(assetId) {
    const columnCard = document.getElementById('column-lineage-card');
    
    console.log('🎯 showColumnLineage called with assetId:', assetId);
    console.log('🎯 window.currentColumnLineage:', window.currentColumnLineage);
    console.log('🎯 columns available:', window.currentColumnLineage?.columns?.length || 0);
    
    // Show the column lineage card
    columnCard.style.display = 'block';
    
    if (window.currentColumnLineage && window.currentColumnLineage.columns) {
        const columns = window.currentColumnLineage.columns;
        
        // Update header information
        updateColumnLineageHeader(assetId, columns);
        
        // Show detailed view by default
        switchColumnView('detailed');
        
        // Populate all views
        populateDetailedView(columns);
        populateMatrixView(columns);
        populateFlowView(columns);
        
    } else {
        // Show empty state
        showColumnLineageEmpty();
    }
    
    // Scroll to column lineage card
    columnCard.scrollIntoView({ behavior: 'smooth' });
}

// Update column lineage header with asset info
function updateColumnLineageHeader(assetId, columns) {
    console.log('📊 updateColumnLineageHeader called with:', assetId, 'columns:', columns.length);
    
    // Get asset name from current lineage data
    let assetName = 'Unknown Asset';
    if (window.currentLineageData && window.currentLineageData.nodes) {
        const asset = window.currentLineageData.nodes.find(node => node.id === assetId);
        if (asset) {
            assetName = asset.name;
            console.log('📊 Found asset name:', assetName);
        }
    }
    
    // Calculate relationship counts
    let totalRelationships = 0;
    columns.forEach(column => {
        const upstreamCount = (column.upstream_columns || []).length;
        const downstreamCount = (column.downstream_columns || []).length;
        totalRelationships += upstreamCount + downstreamCount;
        console.log(`📊 Column ${column.name}: ${upstreamCount} upstream, ${downstreamCount} downstream`);
    });
    
    console.log('📊 Total relationships calculated:', totalRelationships);
    
    // Update badges
    document.getElementById('column-lineage-asset-name').textContent = assetName;
    document.getElementById('column-lineage-column-count').textContent = columns.length;
    document.getElementById('column-lineage-relationship-count').textContent = totalRelationships;
    
    console.log('📊 Header updated successfully');
}

// Switch between different column lineage views
function switchColumnView(viewType) {
    // Hide all views
    document.getElementById('column-lineage-empty').style.display = 'none';
    document.getElementById('column-lineage-detailed').style.display = 'none';
    document.getElementById('column-lineage-matrix').style.display = 'none';
    document.getElementById('column-lineage-flow').style.display = 'none';
    
    // Remove active class from all buttons
    document.querySelectorAll('[id^="column-view-"]').forEach(btn => {
        btn.classList.remove('active');
    });
    
    // Show selected view and activate button
    document.getElementById(`column-lineage-${viewType}`).style.display = 'block';
    document.getElementById(`column-view-${viewType}`).classList.add('active');
}

// Populate detailed card view
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

// Populate matrix table view
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

// Populate flow diagram view
function populateFlowView(columns) {
    const upstreamContainer = document.getElementById('column-flow-upstream');
    const currentContainer = document.getElementById('column-flow-current');
    const downstreamContainer = document.getElementById('column-flow-downstream');
    
    // Collect all unique upstream and downstream assets
    const upstreamAssets = new Set();
    const downstreamAssets = new Set();
    
    columns.forEach(column => {
        (column.upstream_columns || []).forEach(u => upstreamAssets.add(u.asset));
        (column.downstream_columns || []).forEach(d => downstreamAssets.add(d.asset));
    });
    
    // Populate upstream
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
    
    // Populate current asset columns
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
    
    // Populate downstream
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

// Show empty state
function showColumnLineageEmpty() {
    console.log('⚠️ showColumnLineageEmpty() called - displaying empty state');
    
    document.getElementById('column-lineage-empty').style.display = 'block';
    document.getElementById('column-lineage-detailed').style.display = 'none';
    document.getElementById('column-lineage-matrix').style.display = 'none';
    document.getElementById('column-lineage-flow').style.display = 'none';
    
    // Reset header
    document.getElementById('column-lineage-asset-name').textContent = 'None';
    document.getElementById('column-lineage-column-count').textContent = '0';
    document.getElementById('column-lineage-relationship-count').textContent = '0';
    
    // Show the column lineage card
    const columnCard = document.getElementById('column-lineage-card');
    columnCard.style.display = 'block';
}

// Calculate average confidence for a column
function calculateAverageConfidence(column) {
    const allRelationships = [...(column.upstream_columns || []), ...(column.downstream_columns || [])];
    if (allRelationships.length === 0) return 0;
    
    const totalConfidence = allRelationships.reduce((sum, rel) => sum + (rel.confidence || 0), 0);
    return Math.round((totalConfidence / allRelationships.length) * 100);
}

// Refresh column lineage data
function refreshColumnLineage() {
    if (!window.selectedAssetId) {
        showNotification('No asset selected for column lineage refresh', 'warning');
        return;
    }
    
    console.log('🔄 Manually refreshing column lineage for:', window.selectedAssetId);
    console.log('🔄 Current selectedAssetId:', window.selectedAssetId);
    
    // Force show the column lineage card first
    const columnCard = document.getElementById('column-lineage-card');
    columnCard.style.display = 'block';
    
    // Clear any existing data
    window.currentColumnLineage = null;
    
    // Load fresh data
    loadColumnLineage(window.selectedAssetId);
}

// Debug function to test column lineage with a known working asset
window.testColumnLineage = async function() {
    console.log('🧪 Testing column lineage with known working asset...');
    
    // Use the customer_table asset we know works
    const testAssetId = 'a56f8489aed6aae91a2750768f01453f';
    
    console.log('🧪 Testing with asset ID:', testAssetId);
    
    // Set as selected asset
    window.selectedAssetId = testAssetId;
    
    // Force show column card
    const columnCard = document.getElementById('column-lineage-card');
    columnCard.style.display = 'block';
    
    // Load column data
    await loadColumnLineage(testAssetId);
    
    console.log('🧪 Test complete. Check column lineage section.');
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
    // Always refresh assets first, then lineage
    if (window.selectedAssetId) {
        loadAssetLineage(window.selectedAssetId);
    }
    
    // Refresh the lineage assets list
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
        
        // Get all nodes (circles) in the SVG
        const nodes = svg.querySelectorAll('circle');
        if (nodes.length === 0) {
            showNotification('No nodes found in lineage graph', 'warning');
            return;
        }
        
        // Calculate bounding box of all nodes
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
        
        // Add padding (20% of the range)
        const padding = Math.max(50, Math.max(maxX - minX, maxY - minY) * 0.2);
        minX -= padding;
        minY -= padding;
        maxX += padding;
        maxY += padding;
        
        const width = maxX - minX;
        const height = maxY - minY;
        
        // Update the SVG viewBox to fit all nodes
        svg.setAttribute('viewBox', `${minX} ${minY} ${width} ${height}`);
        
        // Ensure SVG maintains aspect ratio and fits container
        svg.style.width = '100%';
        svg.style.height = 'auto';
        svg.style.maxHeight = '600px';
        
        showNotification('Graph fitted to screen successfully', 'success');
        
        console.log(`🎯 Fitted graph: viewBox(${minX.toFixed(1)}, ${minY.toFixed(1)}, ${width.toFixed(1)}, ${height.toFixed(1)})`);
        
    } catch (error) {
        console.error('❌ Error fitting graph to screen:', error);
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

// ========================================
// LINEAGE RELATIONSHIP MANAGEMENT
// ========================================

// Global variables for relationship editing
window.lineageEditMode = false;
window.selectedSourceNode = null;
window.customRelationships = [];
window.pendingRelationship = null;

// Toggle edit mode for lineage relationships
function toggleLineageEditMode() {
    window.lineageEditMode = !window.lineageEditMode;
    const toggleBtn = document.getElementById('toggle-edit-mode');
    const instructions = document.getElementById('edit-mode-instructions');
    
    if (window.lineageEditMode) {
        toggleBtn.innerHTML = '<i class="fas fa-eye me-1"></i>View Mode';
        toggleBtn.className = 'btn btn-warning btn-sm';
        instructions.style.display = 'block';
        document.getElementById('relationship-default-state').style.display = 'none';
        
        // Update graph to show edit mode
        updateGraphForEditMode(true);
        showNotification('Edit mode enabled - Click nodes to create relationships', 'info');
    } else {
        toggleBtn.innerHTML = '<i class="fas fa-edit me-1"></i>Edit Mode';
        toggleBtn.className = 'btn btn-outline-primary btn-sm';
        instructions.style.display = 'none';
        document.getElementById('relationship-default-state').style.display = 'block';
        document.getElementById('relationship-form').style.display = 'none';
        
        // Reset selection
        window.selectedSourceNode = null;
        window.pendingRelationship = null;
        
        // Update graph to show normal mode
        updateGraphForEditMode(false);
        showNotification('Edit mode disabled', 'info');
    }
}

// Update graph appearance for edit mode
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

// Handle node clicks in edit mode
function handleEditModeNodeClick(nodeId, nodeElement) {
    if (!window.lineageEditMode) return false;
    
    if (!window.selectedSourceNode) {
        // Select source node
        window.selectedSourceNode = { id: nodeId, element: nodeElement };
        nodeElement.classList.add('selected-source');
        
        // Highlight potential targets
        const allNodes = document.querySelectorAll('.lineage-node');
        allNodes.forEach(node => {
            if (node !== nodeElement) {
                node.classList.add('potential-target');
            }
        });
        
        showNotification('Source selected. Click another node to create relationship.', 'info');
        return true;
    } else if (window.selectedSourceNode.id !== nodeId) {
        // Create relationship
        window.pendingRelationship = {
            source: window.selectedSourceNode.id,
            target: nodeId
        };
        
        // Show relationship form
        showRelationshipForm();
        return true;
    } else {
        // Deselect if clicking same node
        deselectSourceNode();
        return true;
    }
}

// Deselect source node
function deselectSourceNode() {
    if (window.selectedSourceNode) {
        window.selectedSourceNode.element.classList.remove('selected-source');
        window.selectedSourceNode = null;
    }
    
    // Remove potential target highlighting
    const allNodes = document.querySelectorAll('.lineage-node');
    allNodes.forEach(node => {
        node.classList.remove('potential-target');
    });
}

// Show the add relationship form
function addNewRelationship() {
    document.getElementById('relationship-default-state').style.display = 'none';
    document.getElementById('edit-mode-instructions').style.display = 'none';
    document.getElementById('relationship-form').style.display = 'block';
    
    // Populate asset dropdowns
    populateAssetDropdowns();
    
    // Reset form
    document.getElementById('source-asset-select').value = '';
    document.getElementById('target-asset-select').value = '';
    document.getElementById('relationship-type').value = 'feeds_into';
    document.getElementById('confidence-level').value = '0.9';
    document.getElementById('confidence-display').textContent = '0.9';
    document.getElementById('relationship-description').value = '';
}

// Show relationship form with pre-filled data
function showRelationshipForm() {
    document.getElementById('relationship-default-state').style.display = 'none';
    document.getElementById('edit-mode-instructions').style.display = 'none';
    document.getElementById('relationship-form').style.display = 'block';
    
    // Populate asset dropdowns
    populateAssetDropdowns();
    
    // Pre-fill if we have pending relationship
    if (window.pendingRelationship) {
        document.getElementById('source-asset-select').value = window.pendingRelationship.source;
        document.getElementById('target-asset-select').value = window.pendingRelationship.target;
    }
}

// Populate asset dropdown options
function populateAssetDropdowns() {
    const sourceSelect = document.getElementById('source-asset-select');
    const targetSelect = document.getElementById('target-asset-select');
    
    // Clear existing options
    sourceSelect.innerHTML = '<option value="">Select source asset...</option>';
    targetSelect.innerHTML = '<option value="">Select target asset...</option>';
    
    // Get current assets from lineage data
    if (window.currentLineageData && window.currentLineageData.nodes) {
        window.currentLineageData.nodes.forEach(node => {
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

// Update confidence display
document.addEventListener('DOMContentLoaded', function() {
    const confidenceSlider = document.getElementById('confidence-level');
    if (confidenceSlider) {
        confidenceSlider.addEventListener('input', function() {
            document.getElementById('confidence-display').textContent = this.value;
        });
    }
});

// Save relationship
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
        // Save to backend
        const response = await apiCall('/api/lineage/relationships', {
            method: 'POST',
            body: JSON.stringify(relationship)
        });
        
        if (response.status === 'success') {
            // Add to local storage
            window.customRelationships.push(relationship);
            
            // Refresh lineage view
            if (window.selectedAssetId) {
                await loadLineage(window.selectedAssetId);
            }
            
            showNotification('Relationship saved successfully', 'success');
            cancelRelationship();
        } else {
            showNotification('Failed to save relationship: ' + response.message, 'error');
        }
    } catch (error) {
        console.error('Error saving relationship:', error);
        showNotification('Error saving relationship', 'error');
    }
}

// Cancel relationship creation
function cancelRelationship() {
    document.getElementById('relationship-form').style.display = 'none';
    
    if (window.lineageEditMode) {
        document.getElementById('edit-mode-instructions').style.display = 'block';
    } else {
        document.getElementById('relationship-default-state').style.display = 'block';
    }
    
    // Reset selections
    deselectSourceNode();
    window.pendingRelationship = null;
}

// Show list of all relationships
async function showRelationshipList() {
    try {
        const response = await apiCall('/api/lineage/relationships');
        
        if (response.status === 'success') {
            displayRelationshipList(response.relationships);
        } else {
            showNotification('Failed to load relationships', 'error');
        }
    } catch (error) {
        console.error('Error loading relationships:', error);
        showNotification('Error loading relationships', 'error');
    }
}

// Display relationship list in modal or panel
function displayRelationshipList(relationships) {
    // Create modal for relationship list
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
    
    // Remove existing modal
    const existingModal = document.getElementById('relationshipListModal');
    if (existingModal) {
        existingModal.remove();
    }
    
    // Add modal to body
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    
    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('relationshipListModal'));
    modal.show();
}

// Delete relationship
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
            
            // Refresh lineage view
            if (window.selectedAssetId) {
                await loadLineage(window.selectedAssetId);
            }
            
            // Refresh relationship list if modal is open
            const modal = document.getElementById('relationshipListModal');
            if (modal && modal.style.display !== 'none') {
                showRelationshipList();
            }
        } else {
            showNotification('Failed to delete relationship', 'error');
        }
    } catch (error) {
        console.error('Error deleting relationship:', error);
        showNotification('Error deleting relationship', 'error');
    }
}

// ========================================
// LINEAGE ANALYSIS FUNCTIONS
// ========================================

// Update analysis panel when asset is selected
function updateAnalysisPanel(assetData, lineageData) {
    if (!assetData || !lineageData) return;
    
    console.log('🔍 Updating analysis panel for asset:', assetData.name);
    
    // Update Overview tab
    updateOverviewTab(assetData, lineageData);
    
    // Update Transformations tab
    updateTransformationsTab(assetData, lineageData);
    
    // Update Architecture tab
    updateArchitectureTab(assetData, lineageData);
    
    // Update Code Analysis tab (if needed)
    updateCodeAnalysisTab(assetData, lineageData);
}

// Update Overview tab content
function updateOverviewTab(assetData, lineageData) {
    // Update statistics
    const upstreamCount = lineageData.nodes.filter(node => node.level < 0).length;
    const downstreamCount = lineageData.nodes.filter(node => node.level > 0).length;
    const currentCount = 1; // The selected asset itself
    
    document.getElementById('stat-upstream').textContent = upstreamCount;
    document.getElementById('stat-current').textContent = currentCount;
    document.getElementById('stat-downstream').textContent = downstreamCount;
    
    // Update summary
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
    
    // Update timeline
    updateDataFlowTimeline(lineageData);
}

// Update data flow timeline
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

// Update Transformations tab
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
    
    // Update transformation chart
    updateTransformationChart(transformations);
}

// Extract transformations from lineage data
function extractTransformationsFromLineage(lineageData) {
    const transformations = [];
    
    // Extract from edges with actual relationship data
    lineageData.edges.forEach(edge => {
        const sourceNode = lineageData.nodes.find(n => n.id === edge.source);
        const targetNode = lineageData.nodes.find(n => n.id === edge.target);
        
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
    
    // Analyze asset types and schema for additional transformations
    const targetNode = lineageData.nodes.find(n => n.is_target);
    const upstreamNodes = lineageData.nodes.filter(n => n.level < 0);
    
    if (targetNode && upstreamNodes.length > 0) {
        // Schema-based transformation detection
        const schemaTransformations = detectSchemaTransformations(targetNode, upstreamNodes);
        transformations.push(...schemaTransformations);
        
        // Type-based transformation detection
        const typeTransformations = detectTypeBasedTransformations(targetNode, upstreamNodes);
        transformations.push(...typeTransformations);
        
        // Pattern-based transformation detection
        const patternTransformations = detectPatternTransformations(targetNode, upstreamNodes);
        transformations.push(...patternTransformations);
    }
    
    return transformations;
}

// Generate dynamic transformation descriptions
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

// Detect schema-based transformations
function detectSchemaTransformations(targetNode, upstreamNodes) {
    const transformations = [];
    const targetSchema = targetNode.schema || [];
    
    upstreamNodes.forEach(upstreamNode => {
        const upstreamSchema = upstreamNode.schema || [];
        
        if (targetSchema.length > 0 && upstreamSchema.length > 0) {
            // Column count comparison
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
            
            // Column name analysis
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

// Detect type-based transformations
function detectTypeBasedTransformations(targetNode, upstreamNodes) {
    const transformations = [];
    const targetType = targetNode.type.toLowerCase();
    
    upstreamNodes.forEach(upstreamNode => {
        const upstreamType = upstreamNode.type.toLowerCase();
        
        // Different type combinations suggest different transformations
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

// Detect pattern-based transformations
function detectPatternTransformations(targetNode, upstreamNodes) {
    const transformations = [];
    const targetName = targetNode.name.toLowerCase();
    
    // Name pattern analysis
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

// Find common columns between schemas
function findCommonColumns(schema1, schema2) {
    const cols1 = schema1.map(col => typeof col === 'string' ? col.toLowerCase() : col.name.toLowerCase());
    const cols2 = schema2.map(col => typeof col === 'string' ? col.toLowerCase() : col.name.toLowerCase());
    
    return cols1.filter(col => cols2.includes(col));
}

// Get badge color for transformation type
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

// Update transformation chart
function updateTransformationChart(transformations) {
    const canvas = document.getElementById('transformationChart');
    if (!canvas) return;
    
    const ctx = canvas.getContext('2d');
    
    // Group transformations by type
    const typeCounts = {};
    transformations.forEach(t => {
        typeCounts[t.type] = (typeCounts[t.type] || 0) + 1;
    });
    
    // Create simple bar chart
    const types = Object.keys(typeCounts);
    const counts = Object.values(typeCounts);
    
    if (types.length === 0) {
        ctx.fillStyle = '#6c757d';
        ctx.font = '14px Inter';
        ctx.textAlign = 'center';
        ctx.fillText('No transformations', canvas.width / 2, canvas.height / 2);
        return;
    }
    
    // Clear canvas
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    
    // Draw bars
    const barWidth = canvas.width / types.length * 0.8;
    const maxCount = Math.max(...counts);
    
    types.forEach((type, index) => {
        const barHeight = (counts[index] / maxCount) * (canvas.height - 40);
        const x = index * (canvas.width / types.length) + (canvas.width / types.length - barWidth) / 2;
        const y = canvas.height - barHeight - 20;
        
        // Draw bar
        ctx.fillStyle = '#007bff';
        ctx.fillRect(x, y, barWidth, barHeight);
        
        // Draw label
        ctx.fillStyle = '#495057';
        ctx.font = '10px Inter';
        ctx.textAlign = 'center';
        ctx.fillText(type.substring(0, 8) + '...', x + barWidth / 2, canvas.height - 5);
        
        // Draw count
        ctx.fillStyle = '#fff';
        ctx.font = '12px Inter';
        ctx.fillText(counts[index], x + barWidth / 2, y + barHeight / 2 + 4);
    });
}

// Update Architecture tab
function updateArchitectureTab(assetData, lineageData) {
    const architectureDiagram = generateArchitectureDiagram(lineageData);
    document.getElementById('architecture-diagram').innerHTML = architectureDiagram;
    
    const dataLayers = analyzeDataLayers(lineageData);
    document.getElementById('data-layers').innerHTML = dataLayers;
}

// Generate architecture diagram
function generateArchitectureDiagram(lineageData) {
    const layers = {};
    
    // Group nodes by level
    lineageData.nodes.forEach(node => {
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

// Get level name for architecture
function getLevelName(level) {
    if (level < 0) return `Source Layer ${Math.abs(level)}`;
    if (level === 0) return 'Processing Layer';
    return `Output Layer ${level}`;
}

// Get node icon based on type
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

// Analyze data layers
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

// Update Code Analysis tab
function updateCodeAnalysisTab(assetData, lineageData) {
    // This will be populated when generateCodeAnalysis() is called
    updateQualityMetrics(assetData, lineageData);
}

// Generate code analysis
function generateCodeAnalysis() {
    if (!window.currentLineageData || !window.selectedAssetId) {
        showNotification('Please select an asset first', 'warning');
        return;
    }
    
    const button = document.querySelector('#code-analysis button');
    const originalText = button.innerHTML;
    
    button.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Analyzing...';
    button.disabled = true;
    
    // Get current asset data
    const selectedAsset = window.currentLineageData.nodes.find(node => node.is_target);
    const lineageData = window.currentLineageData;
    
    // Simulate analysis with actual data
    setTimeout(() => {
        const analysis = generateDynamicCodeAnalysis(selectedAsset, lineageData);
        
        document.getElementById('code-analysis-content').innerHTML = analysis.html;
        
        button.innerHTML = originalText;
        button.disabled = false;
        
        showNotification('Code analysis generated successfully', 'success');
    }, 2000);
}

// Generate dynamic code analysis based on actual data
function generateDynamicCodeAnalysis(assetData, lineageData) {
    // Calculate complexity metrics dynamically
    const cyclomaticComplexity = calculateCyclomaticComplexity(lineageData);
    const dataFlowComplexity = calculateDataFlowComplexity(lineageData);
    
    // Generate SQL pattern based on actual asset structure
    const sqlPattern = generateSQLPattern(assetData, lineageData);
    
    // Generate dynamic recommendations
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

// Calculate cyclomatic complexity dynamically
function calculateCyclomaticComplexity(lineageData) {
    const nodeCount = lineageData.nodes.length;
    const edgeCount = lineageData.edges.length;
    const complexity = nodeCount > 0 ? (edgeCount / nodeCount) * 20 : 0;
    
    if (complexity < 20) return { label: 'Low', color: 'success', percentage: Math.max(complexity, 10) };
    if (complexity < 50) return { label: 'Medium', color: 'warning', percentage: complexity };
    return { label: 'High', color: 'danger', percentage: Math.min(complexity, 90) };
}

// Calculate data flow complexity dynamically
function calculateDataFlowComplexity(lineageData) {
    const maxDepth = Math.max(...lineageData.nodes.map(n => Math.abs(n.level)));
    const complexity = maxDepth * 15;
    
    if (complexity < 30) return { label: 'Low', color: 'success', percentage: Math.max(complexity, 15) };
    if (complexity < 60) return { label: 'Medium', color: 'warning', percentage: complexity };
    return { label: 'High', color: 'danger', percentage: Math.min(complexity, 85) };
}

// Generate SQL pattern based on actual asset data
function generateSQLPattern(assetData, lineageData) {
    const assetType = assetData.type.toLowerCase();
    const assetName = assetData.name;
    const schema = assetData.schema || [];
    const upstreamNodes = lineageData.nodes.filter(n => n.level < 0);
    
    // Generate columns list from actual schema
    const columnsList = schema.length > 0 
        ? schema.slice(0, 5).map(col => {
            const colName = typeof col === 'string' ? col : col.name;
            return `    ${colName}`;
        }).join(',\n')
        : '    column1,\n    column2,\n    column3';
    
    // Generate FROM clause from upstream assets
    const fromClause = upstreamNodes.length > 0 
        ? upstreamNodes[0].name 
        : 'source_table';
    
    // Generate different patterns based on asset type
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

// Generate dynamic WHERE clause based on asset data
function generateDynamicWhereClause(assetData, patternType) {
    const conditions = [];
    const schema = assetData.schema || [];
    
    // Look for common column patterns in schema
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
    
    // Generate conditions based on detected columns
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
    
    // Add data quality checks if schema is complex
    if (schema.length > 10) {
        conditions.push('data_quality_score > 0.8');
    }
    
    // Return appropriate WHERE clause
    if (conditions.length === 0) {
        return patternType === 'general' ? '1=1' : 'TRUE';
    }
    
    return conditions.join('\n  AND ');
}

// Generate dynamic ORDER BY clause based on asset data
function generateDynamicOrderClause(assetData) {
    const schema = assetData.schema || [];
    
    // Look for common ordering columns
    const orderColumns = [];
    
    // Priority order for common column names
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
    
    // If no priority columns found, use first available column
    if (orderColumns.length === 0 && schema.length > 0) {
        const firstCol = typeof schema[0] === 'string' ? schema[0] : schema[0].name;
        orderColumns.push(`${firstCol} DESC`);
    }
    
    return orderColumns.length > 0 ? orderColumns.join(', ') : '1';
}

// Get appropriate language based on asset type
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

// Generate dynamic recommendations based on actual data
function generateDynamicRecommendations(assetData, lineageData) {
    const recommendations = [];
    
    // Schema-based recommendations
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
    
    // Lineage complexity recommendations
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
    
    // Type-specific recommendations
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
    
    // Metadata recommendations
    if (!assetData.metadata || Object.keys(assetData.metadata).length < 3) {
        recommendations.push({
            icon: 'file-alt',
            color: 'warning',
            text: 'Limited metadata available - add descriptions and business context'
        });
    }
    
    // Default recommendations if none generated
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

// ========================================
// ENHANCED ASSET SEARCH & FILTERING
// ========================================

// Global variables for search and filtering
window.allAssets = [];
window.filteredAssets = [];
window.searchRecommendations = [];
window.currentSearchTerm = '';
window.selectedRecommendationIndex = -1;

// Initialize search functionality
function initializeAssetSearch() {
    const searchInput = document.getElementById('asset-search');
    const recommendationsDropdown = document.getElementById('search-recommendations');
    
    console.log('Initializing asset search...');
    console.log('Search input element:', searchInput);
    console.log('Recommendations dropdown:', recommendationsDropdown);
    
    if (!searchInput) {
        console.error('Search input element not found!');
        return;
    }
    
    // Real-time search with recommendations
    searchInput.addEventListener('input', debounce(handleSearchInput, 300));
    
    // Also add immediate search on keyup for better responsiveness
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
    
    // Add Enter key support for immediate search
    searchInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            const searchTerm = e.target.value.trim();
            performSearch(searchTerm);
            hideRecommendations();
        }
    });
    
    // Handle keyboard navigation (but not for Enter key which is handled above)
    searchInput.addEventListener('keydown', (e) => {
        if (e.key !== 'Enter') {
            handleSearchKeydown(e);
        }
    });
    
    // Handle focus events
    searchInput.addEventListener('focus', () => {
        if (window.currentSearchTerm && window.currentSearchTerm.length >= 1) {
            showRecommendations();
        }
    });
    
    // Hide recommendations when clicking outside
    document.addEventListener('click', (e) => {
        if (!e.target.closest('.search-container')) {
            hideRecommendations();
        }
    });
    
    // Add a manual search trigger function
    window.triggerSearch = function() {
        const searchTerm = document.getElementById('asset-search').value.trim();
        if (searchTerm) {
            performSearch(searchTerm);
        } else {
            displayFilteredAssets(window.allAssets);
        }
    };
    
    // Add a function to clear search
    window.clearAssetSearch = function() {
        clearSearch();
    };
    
    // Initialize filters
    initializeAssetFilters();
}

// Handle search input with real-time recommendations
function handleSearchInput(e) {
    const searchTerm = e.target.value.trim();
    console.log('handleSearchInput called with:', searchTerm);
    window.currentSearchTerm = searchTerm;
    
    if (searchTerm.length >= 1) {
        generateSearchRecommendations(searchTerm);
        showRecommendations();
        // Also perform the actual search/filtering
        performSearch(searchTerm);
    } else {
        hideRecommendations();
        // Show all assets when search is empty
        displayFilteredAssets(window.allAssets);
    }
}

// Generate intelligent search recommendations
function generateSearchRecommendations(searchTerm) {
    const recommendations = [];
    const term = searchTerm.toLowerCase();
    
    // Get unique values for recommendations
    const assetNames = [...new Set(window.allAssets.map(asset => asset.name))];
    const assetTypes = [...new Set(window.allAssets.map(asset => asset.type))];
    const assetSources = [...new Set(window.allAssets.map(asset => asset.source))];
    
    // Name-based recommendations
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
    
    // Type-based recommendations
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
    
    // Source-based recommendations
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
    
    // Smart suggestions based on partial matches
    if (recommendations.length === 0) {
        // Fuzzy matching for typos
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
    
    // Sort and limit recommendations
    window.searchRecommendations = recommendations
        .sort((a, b) => b.assets.length - a.assets.length) // Sort by relevance
        .slice(0, 8); // Limit to 8 recommendations
    
    displayRecommendations();
}

// Find fuzzy matches for typo tolerance
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

// Calculate Levenshtein distance for fuzzy matching
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

// Display recommendations dropdown
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
    
    // Group recommendations by category
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

// Highlight matching text in recommendations
function highlightMatch(text, term) {
    if (!term) return text;
    
    const regex = new RegExp(`(${term})`, 'gi');
    return text.replace(regex, '<mark>$1</mark>');
}

// Group array by property
function groupBy(array, property) {
    return array.reduce((groups, item) => {
        const group = item[property] || 'Other';
        groups[group] = groups[group] || [];
        groups[group].push(item);
        return groups;
    }, {});
}

// Handle keyboard navigation in recommendations
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

// Update visual selection of recommendations
function updateRecommendationSelection() {
    const items = document.querySelectorAll('.recommendation-item');
    items.forEach((item, index) => {
        item.classList.toggle('active', index === window.selectedRecommendationIndex);
    });
}

// Select a recommendation
function selectRecommendation(index) {
    const recommendation = window.searchRecommendations[index];
    if (!recommendation) return;
    
    // Update search input
    document.getElementById('asset-search').value = recommendation.value;
    
    // Perform search with the selected recommendation
    performSearch(recommendation.value);
    
    // Hide recommendations
    hideRecommendations();
    
    // Reset selection
    window.selectedRecommendationIndex = -1;
}

// Perform actual search
function performSearch(searchTerm) {
    console.log('performSearch called with:', searchTerm);
    console.log('Total assets available:', window.allAssets.length);
    
    if (!searchTerm.trim()) {
        console.log('Empty search term, showing all assets');
        displayFilteredAssets(window.allAssets);
        return;
    }
    
    const term = searchTerm.toLowerCase();
    console.log('Searching for term:', term);
    
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
            console.log('Asset matches:', asset.name, {nameMatch, typeMatch, sourceMatch, schemaMatch});
        }
        
        return matches;
    });
    
    console.log('Filtered results:', filtered.length, 'assets');
    displayFilteredAssets(filtered);
    
    // Update URL with search parameter
    const url = new URL(window.location);
    if (searchTerm.trim()) {
        url.searchParams.set('search', searchTerm);
    } else {
        url.searchParams.delete('search');
    }
    window.history.replaceState({}, '', url);
}

// Show recommendations dropdown
function showRecommendations() {
    if (window.searchRecommendations.length > 0 || window.currentSearchTerm.length >= 1) {
        document.getElementById('search-recommendations').style.display = 'block';
    }
}

// Hide recommendations dropdown
function hideRecommendations() {
    document.getElementById('search-recommendations').style.display = 'none';
    window.selectedRecommendationIndex = -1;
}

// Clear search
function clearSearch() {
    console.log('Clearing search...');
    document.getElementById('asset-search').value = '';
    window.currentSearchTerm = '';
    hideRecommendations();
    displayFilteredAssets(window.allAssets);
    
    // Clear URL parameter
    const url = new URL(window.location);
    url.searchParams.delete('search');
    window.history.replaceState({}, '', url);
}

// Enhanced search function (called by search button)
function searchAssets() {
    const searchTerm = document.getElementById('asset-search').value.trim();
    console.log('searchAssets called with:', searchTerm);
    performSearch(searchTerm);
    hideRecommendations();
}

// Initialize asset filters
function initializeAssetFilters() {
    const typeFilter = document.getElementById('asset-type-filter');
    const sourceFilter = document.getElementById('asset-source-filter');
    
    // Populate filter options when assets are loaded
    if (window.allAssets.length > 0) {
        populateFilterOptions();
    }
    
    // Add event listeners for filters
    if (typeFilter) {
        typeFilter.addEventListener('change', applyAssetFilters);
    }
    if (sourceFilter) {
        sourceFilter.addEventListener('change', applyAssetFilters);
    }
}

// Populate filter dropdown options
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

// Apply asset filters
function applyAssetFilters() {
    const typeFilter = document.getElementById('asset-type-filter').value;
    const sourceFilter = document.getElementById('asset-source-filter').value;
    const searchTerm = document.getElementById('asset-search').value.toLowerCase();
    
    let filtered = window.allAssets;
    
    // Apply search filter
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
    
    // Apply type filter
    if (typeFilter) {
        filtered = filtered.filter(asset => asset.type === typeFilter);
    }
    
    // Apply source filter
    if (sourceFilter) {
        filtered = filtered.filter(asset => asset.source === sourceFilter);
    }
    
    displayFilteredAssets(filtered);
    
    // Update filter button appearance
    const filterButton = document.querySelector('[onclick="applyAssetFilters()"]');
    if (typeFilter || sourceFilter) {
        filterButton.classList.add('filter-active');
        filterButton.innerHTML = `<i class="fas fa-filter me-1"></i> Filters Applied (${filtered.length})`;
    } else {
        filterButton.classList.remove('filter-active');
        filterButton.innerHTML = `<i class="fas fa-filter me-1"></i> Apply Filters`;
    }
}

// Display filtered assets in the table
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
    
    // Update assets count
    updateAssetsCount(assets.length, window.allAssets.length);
}

// Clear all filters and search
function clearFiltersAndSearch() {
    document.getElementById('asset-search').value = '';
    document.getElementById('asset-type-filter').value = '';
    document.getElementById('asset-source-filter').value = '';
    
    window.currentSearchTerm = '';
    hideRecommendations();
    
    displayFilteredAssets(window.allAssets);
    
    // Reset filter button
    const filterButton = document.querySelector('[onclick="applyAssetFilters()"]');
    filterButton.classList.remove('filter-active');
    filterButton.innerHTML = `<i class="fas fa-filter me-1"></i> Apply Filters`;
}

// Update assets count display
function updateAssetsCount(filtered, total) {
    const countDisplay = document.getElementById('assets-count');
    if (countDisplay) {
        countDisplay.textContent = filtered === total ? 
            `${total} assets` : 
            `${filtered} of ${total} assets`;
    }
}

// Utility function for debouncing
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

// Initialize search when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeAssetSearch();
    
    // Check if there's a search term in the URL
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

// ========================================
// UTILITY FUNCTIONS FOR ASSET DISPLAY
// ========================================

// Get icon for asset type
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
    
    // Find matching icon or use default
    for (const [key, icon] of Object.entries(iconMap)) {
        if (type.includes(key)) {
            return icon;
        }
    }
    
    return 'fas fa-cube'; // Default icon
}

// Format file size
function formatFileSize(bytes) {
    if (!bytes || bytes === 0) return 'Unknown';
    
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    
    return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
}

// Format date
function formatDate(dateString) {
    if (!dateString) return 'Unknown';
    
    try {
        const date = new Date(dateString);
        return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'});
    } catch (e) {
        return dateString; // Return original if parsing fails
    }
}

// View asset details - redirect to main function
function viewAssetDetails(assetId) {
    console.log('viewAssetDetails called with ID:', assetId);
    
    // Find asset by ID in current assets data
    if (window.currentAssetsData && window.currentAssetsData.assets_list) {
        const asset = window.currentAssetsData.assets_list.find(a => a.id == assetId);
        if (asset) {
            console.log('Found asset by ID, calling showAssetDetails with name:', asset.name);
            showAssetDetails(asset.name);
            return;
        }
    }
    
    // Fallback: try to use the ID as asset name
    console.log('Asset not found by ID, trying ID as name:', assetId);
    showAssetDetails(assetId);
}

// View asset lineage (integration with existing lineage functionality)
function viewAssetLineage(assetId) {
    console.log('Viewing lineage for asset:', assetId);
    
    // Find the asset in our data
    const asset = window.allAssets.find(a => a.id === assetId || a.name === assetId);
    
    if (asset) {
        // Switch to lineage tab
        showSection('lineage');
        
        // Generate asset ID for lineage
        const lineageAssetId = generateAssetId(asset);
        
        // Select the asset in lineage dropdown if it exists
        const lineageSelect = document.getElementById('lineage-asset-select');
        if (lineageSelect) {
            // Try to find and select the asset
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

// Update quality metrics
function updateQualityMetrics(assetData, lineageData) {
    // Calculate metrics based on lineage data
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

// Calculate complexity score
function calculateComplexityScore(lineageData) {
    const nodeCount = lineageData.nodes.length;
    const edgeCount = lineageData.edges.length;
    const complexity = (edgeCount / nodeCount) * 10;
    
    if (complexity < 3) return { score: 'Low', color: 'success' };
    if (complexity < 6) return { score: 'Medium', color: 'warning' };
    return { score: 'High', color: 'danger' };
}

// Calculate data quality score
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

// Calculate performance score
function calculatePerformanceScore(lineageData) {
    const depth = Math.max(...lineageData.nodes.map(n => Math.abs(n.level)));
    
    if (depth <= 2) return { score: 'High', color: 'success' };
    if (depth <= 4) return { score: 'Medium', color: 'warning' };
    return { score: 'Low', color: 'danger' };
}

// Calculate maintainability score
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

