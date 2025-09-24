"""
SaaS Connector - Discovers data assets in various SaaS platforms
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
import requests

from .base_connector import BaseConnector

# Import SaaS libraries with fallbacks
try:
    from simple_salesforce import Salesforce
except ImportError:
    Salesforce = None

try:
    import pysnow
except ImportError:
    pysnow = None

try:
    from slack_sdk import WebClient
except ImportError:
    WebClient = None

try:
    from jira import JIRA
except ImportError:
    JIRA = None
class SaaSConnector(BaseConnector):
    """
    Connector for discovering data assets in various SaaS platforms
    """
    
    # Metadata for dynamic discovery
    connector_type = "saas_platforms"
    connector_name = "SaaS Platforms"
    description = "Discover data assets from SaaS platforms including Salesforce, ServiceNow, Slack, Jira, HubSpot, Zendesk, Google Analytics, Workday, and Tableau"
    category = "saas_platforms"
    supported_services = ["Salesforce", "ServiceNow", "Slack", "Jira", "HubSpot", "Zendesk", "Google Analytics", "Workday", "Tableau"]
    required_config_fields = ["saas_connections"]
    optional_config_fields = ["connection_timeout"]
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.saas_platforms = config.get('saas_connections', [])
    
    def discover_assets(self) -> List[Dict[str, Any]]:
        """Discover SaaS platform assets"""
        self.logger.info("Starting SaaS platform asset discovery")
        assets = []
        
        for saas_config in self.saas_platforms:
            platform_type = saas_config.get('type', '').lower()
            
            try:
                if platform_type == 'salesforce':
                    assets.extend(self._discover_salesforce_assets(saas_config))
                elif platform_type == 'servicenow':
                    assets.extend(self._discover_servicenow_assets(saas_config))
                elif platform_type == 'slack':
                    assets.extend(self._discover_slack_assets(saas_config))
                elif platform_type == 'jira':
                    assets.extend(self._discover_jira_assets(saas_config))
                elif platform_type == 'hubspot':
                    assets.extend(self._discover_hubspot_assets(saas_config))
                elif platform_type == 'zendesk':
                    assets.extend(self._discover_zendesk_assets(saas_config))
                elif platform_type == 'google_analytics':
                    assets.extend(self._discover_google_analytics_assets(saas_config))
                elif platform_type == 'mailchimp':
                    assets.extend(self._discover_mailchimp_assets(saas_config))
                elif platform_type == 'workday':
                    assets.extend(self._discover_workday_assets(saas_config))
                elif platform_type == 'adp':
                    assets.extend(self._discover_adp_assets(saas_config))
                elif platform_type == 'quickbooks':
                    assets.extend(self._discover_quickbooks_assets(saas_config))
                elif platform_type == 'microsoft_teams':
                    assets.extend(self._discover_teams_assets(saas_config))
                elif platform_type == 'zoom':
                    assets.extend(self._discover_zoom_assets(saas_config))
                elif platform_type == 'tableau':
                    assets.extend(self._discover_tableau_assets(saas_config))
                elif platform_type == 'power_bi':
                    assets.extend(self._discover_powerbi_assets(saas_config))
                else:
                    self.logger.warning(f"Unsupported SaaS platform type: {platform_type}")
                    
            except Exception as e:
                self.logger.error(f"Error discovering assets from {platform_type} platform: {e}")
        
        self.logger.info(f"Discovered {len(assets)} SaaS platform assets")
        return assets
    
    def _discover_salesforce_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Salesforce assets"""
        assets = []
        if not Salesforce:
            self.logger.warning("simple-salesforce not installed")
            return assets
        
        try:
            sf = Salesforce(
                username=config['username'],
                password=config['password'],
                security_token=config.get('security_token', ''),
                domain=config.get('domain', 'login')
            )
            
            objects = sf.describe()["sobjects"]
            
            for obj in objects:
                if obj['queryable'] and not obj['name'].endswith('__History'):
                    asset = {
                        'name': obj['name'],
                        'type': 'salesforce_object',
                        'source': 'salesforce',
                        'location': f"salesforce://{config.get('domain', 'login')}.salesforce.com/{obj['name']}",
                        'created_date': datetime.now(),
                        'size': 0,
                        'metadata': {
                            'platform_type': 'salesforce',
                            'label': obj['label'],
                            'custom': obj['custom'],
                            'queryable': obj['queryable'],
                            'createable': obj['createable']
                        }
                    }
                    assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Salesforce: {e}")
        
        return assets
    
    def _discover_servicenow_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover ServiceNow assets"""
        assets = []
        if not pysnow:
            self.logger.warning("pysnow not installed")
            return assets
        
        try:
            client = pysnow.Client(
                instance=config['instance'],
                user=config['username'],
                password=config['password']
            )
            
            tables = client.resource(api_path='/table/sys_db_object')
            response = tables.get(query={'sys_scope': 'global'})
            
            for record in response.all():
                if record['name'] and not record['name'].startswith('sys_'):
                    asset = {
                        'name': record['name'],
                        'type': 'servicenow_table',
                        'source': 'servicenow',
                        'location': f"servicenow://{config['instance']}.service-now.com/{record['name']}",
                        'created_date': datetime.now(),
                        'size': 0,
                        'metadata': {
                            'platform_type': 'servicenow',
                            'label': record.get('label', ''),
                            'super_class': record.get('super_class', ''),
                            'instance': config['instance']
                        }
                    }
                    assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to ServiceNow: {e}")
        
        return assets
    
    def _discover_slack_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Slack assets"""
        assets = []
        if not WebClient:
            self.logger.warning("slack-sdk not installed")
            return assets
        
        try:
            client = WebClient(token=config['bot_token'])
            
            response = client.conversations_list()
            
            for channel in response['channels']:
                asset = {
                    'name': channel['name'],
                    'type': 'slack_channel',
                    'source': 'slack',
                    'location': f"slack://workspace/{channel['id']}",
                    'created_date': datetime.fromtimestamp(channel['created']),
                    'size': channel.get('num_members', 0),
                    'metadata': {
                        'platform_type': 'slack',
                        'is_private': channel.get('is_private', False),
                        'is_archived': channel.get('is_archived', False),
                        'purpose': channel.get('purpose', {}).get('value', ''),
                        'topic': channel.get('topic', {}).get('value', '')
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Slack: {e}")
        
        return assets
    
    def _discover_jira_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Jira assets"""
        assets = []
        if not JIRA:
            self.logger.warning("jira not installed")
            return assets
        
        try:
            jira = JIRA(
                server=config['server'],
                basic_auth=(config['username'], config['api_token'])
            )
            
            projects = jira.projects()
            
            for project in projects:
                asset = {
                    'name': project.key,
                    'type': 'jira_project',
                    'source': 'jira',
                    'location': f"jira://{config['server']}/projects/{project.key}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'platform_type': 'jira',
                        'name': project.name,
                        'project_type': project.projectTypeKey,
                        'lead': project.lead.displayName if hasattr(project, 'lead') else ''
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Jira: {e}")
        
        return assets
    
    def _discover_hubspot_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover HubSpot assets"""
        assets = []
        
        try:
            headers = {'Authorization': f"Bearer {config['api_key']}"}
            
            objects = ['contacts', 'companies', 'deals', 'tickets', 'products', 'line_items']
            
            for obj_type in objects:
                try:
                    response = requests.get(
                        f"https://api.hubapi.com/crm/v3/objects/{obj_type}",
                        headers=headers,
                        params={'limit': 1}  # Just to check if object exists
                    )
                    
                    if response.status_code == 200:
                        asset = {
                            'name': obj_type,
                            'type': 'hubspot_object',
                            'source': 'hubspot',
                            'location': f"hubspot://api/crm/v3/objects/{obj_type}",
                            'created_date': datetime.now(),
                            'size': response.json().get('total', 0),
                            'metadata': {
                                'platform_type': 'hubspot',
                                'object_type': obj_type,
                                'api_version': 'v3'
                            }
                        }
                        assets.append(asset)
                        
                except Exception as e:
                    self.logger.error(f"Error getting HubSpot object {obj_type}: {e}")
            
        except Exception as e:
            self.logger.error(f"Error connecting to HubSpot: {e}")
        
        return assets
    
    def _discover_zendesk_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Zendesk assets"""
        assets = []
        
        try:
            base_url = f"https://{config['subdomain']}.zendesk.com/api/v2"
            auth = (f"{config['email']}/token", config['api_token'])
            
            response = requests.get(f"{base_url}/ticket_fields.json", auth=auth)
            
            if response.status_code == 200:
                fields = response.json()['ticket_fields']
                
                for field in fields:
                    asset = {
                        'name': field['title'],
                        'type': 'zendesk_field',
                        'source': 'zendesk',
                        'location': f"zendesk://{config['subdomain']}.zendesk.com/fields/{field['id']}",
                        'created_date': datetime.fromisoformat(field['created_at'].replace('Z', '+00:00')),
                        'size': 0,
                        'metadata': {
                            'platform_type': 'zendesk',
                            'field_type': field['type'],
                            'active': field['active'],
                            'required': field.get('required', False)
                        }
                    }
                    assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Zendesk: {e}")
        
        return assets
    
    def _discover_google_analytics_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Google Analytics assets"""
        assets = []
        
        try:
            from google.analytics.data_v1beta import BetaAnalyticsDataClient
            from google.analytics.admin_v1beta import AnalyticsAdminServiceClient
            from google.analytics.admin_v1beta.types import ListAccountsRequest
            
            admin_client = AnalyticsAdminServiceClient(credentials=config.get('credentials'))
            
            request = ListAccountsRequest()
            accounts = admin_client.list_accounts(request=request)
            
            for account in accounts:
                # List properties for each account
                properties_request = ListPropertiesRequest(parent=account.name)
                properties = admin_client.list_properties(request=properties_request)
                
                for property in properties:
                    asset = {
                        'name': property.display_name,
                        'type': 'google_analytics_property',
                        'source': 'google_analytics',
                        'location': f"ga://property/{property.name.split('/')[-1]}",
                        'created_date': property.create_time,
                        'size': 0,
                        'metadata': {
                            'platform_type': 'google_analytics',
                            'property_id': property.name.split('/')[-1],
                            'account_id': account.name.split('/')[-1],
                            'time_zone': property.time_zone,
                            'currency_code': property.currency_code
                        }
                    }
                    assets.append(asset)
            
        except ImportError:
            self.logger.warning("Google Analytics libraries not installed. Install with: pip install google-analytics-data google-analytics-admin")
            ga_properties = config.get('properties', [])
            for prop in ga_properties:
                asset = {
                    'name': prop.get('name', 'GA Property'),
                    'type': 'google_analytics_property',
                    'source': 'google_analytics',
                    'location': f"ga://property/{prop.get('id', '')}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'platform_type': 'google_analytics',
                        'property_id': prop.get('id', ''),
                        'account_id': prop.get('account_id', '')
                    }
                }
                assets.append(asset)
        except Exception as e:
            self.logger.error(f"Error connecting to Google Analytics: {e}")
        
        return assets
    
    def _discover_mailchimp_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Mailchimp assets"""
        assets = []
        
        try:
            headers = {'Authorization': f"Bearer {config['api_key']}"}
            dc = config['api_key'].split('-')[-1]  # Data center from API key
            
            response = requests.get(
                f"https://{dc}.api.mailchimp.com/3.0/lists",
                headers=headers
            )
            
            if response.status_code == 200:
                lists = response.json()['lists']
                
                for list_item in lists:
                    asset = {
                        'name': list_item['name'],
                        'type': 'mailchimp_list',
                        'source': 'mailchimp',
                        'location': f"mailchimp://lists/{list_item['id']}",
                        'created_date': datetime.fromisoformat(list_item['date_created'].replace('Z', '+00:00')),
                        'size': list_item['stats']['member_count'],
                        'metadata': {
                            'platform_type': 'mailchimp',
                            'list_id': list_item['id'],
                            'member_count': list_item['stats']['member_count'],
                            'permission_reminder': list_item.get('permission_reminder', '')
                        }
                    }
                    assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Mailchimp: {e}")
        
        return assets
    
    def _discover_workday_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Workday assets"""
        assets = []
        
        try:
            import requests
            from requests.auth import HTTPBasicAuth
            
            tenant = config.get('tenant')
            username = config.get('username')
            password = config.get('password')
            
            if not all([tenant, username, password]):
                self.logger.warning("Workday credentials not provided, using placeholder discovery")
                workday_objects = [
                    'employees', 'organizations', 'positions', 'jobs',
                    'compensation', 'benefits', 'time_tracking'
                ]
                
                for obj_type in workday_objects:
                    asset = {
                        'name': obj_type,
                        'type': 'workday_object',
                        'source': 'workday',
                        'location': f"workday://{tenant}/{obj_type}",
                        'created_date': datetime.now(),
                        'size': 0,
                        'metadata': {
                            'platform_type': 'workday',
                            'object_type': obj_type,
                            'tenant': tenant
                        }
                    }
                    assets.append(asset)
                return assets
            
            base_url = f"https://{tenant}.workday.com/ccx/api/v1"
            auth = HTTPBasicAuth(username, password)
            
            workday_objects = [
                'workers', 'organizations', 'positions', 'jobs',
                'compensation', 'benefits', 'time_tracking', 'payroll'
            ]
            
            for obj_type in workday_objects:
                try:
                    # Try to get object metadata
                    response = requests.get(
                        f"{base_url}/{obj_type}",
                        auth=auth,
                        params={'limit': 1},
                        timeout=10
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        total_count = data.get('total', 0)
                        
                        asset = {
                            'name': obj_type,
                            'type': 'workday_object',
                            'source': 'workday',
                            'location': f"workday://{tenant}/{obj_type}",
                            'created_date': datetime.now(),
                            'size': total_count,
                            'metadata': {
                                'platform_type': 'workday',
                                'object_type': obj_type,
                                'tenant': tenant,
                                'total_records': total_count,
                                'api_version': 'v1'
                            }
                        }
                        assets.append(asset)
                    else:
                        self.logger.warning(f"Workday API returned {response.status_code} for {obj_type}")
                        
                except Exception as e:
                    self.logger.warning(f"Error accessing Workday {obj_type}: {e}")
            
        except ImportError:
            self.logger.warning("requests library not available for Workday API calls")
        except Exception as e:
            self.logger.error(f"Error connecting to Workday: {e}")
        
        return assets
    
    def _discover_adp_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover ADP assets"""
        assets = []
        
        try:
            # Placeholder for ADP discovery logic
            adp_objects = [
                'workers', 'payroll', 'time_cards', 'benefits',
                'positions', 'organizations'
            ]
            
            for obj_type in adp_objects:
                asset = {
                    'name': obj_type,
                    'type': 'adp_object',
                    'source': 'adp',
                    'location': f"adp://api/{obj_type}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'platform_type': 'adp',
                        'object_type': obj_type
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to ADP: {e}")
        
        return assets
    
    def _discover_quickbooks_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover QuickBooks assets"""
        assets = []
        
        try:
            # Placeholder for QuickBooks discovery logic
            qb_objects = [
                'customers', 'vendors', 'items', 'accounts',
                'invoices', 'bills', 'payments', 'employees'
            ]
            
            for obj_type in qb_objects:
                asset = {
                    'name': obj_type,
                    'type': 'quickbooks_object',
                    'source': 'quickbooks',
                    'location': f"quickbooks://company/{config.get('company_id', '')}/{obj_type}",
                    'created_date': datetime.now(),
                    'size': 0,
                    'metadata': {
                        'platform_type': 'quickbooks',
                        'object_type': obj_type,
                        'company_id': config.get('company_id', '')
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to QuickBooks: {e}")
        
        return assets
    
    def _discover_teams_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Microsoft Teams assets"""
        assets = []
        
        try:
            headers = {'Authorization': f"Bearer {config['access_token']}"}
            
            response = requests.get(
                "https://graph.microsoft.com/v1.0/groups?$filter=resourceProvisioningOptions/Any(x:x eq 'Team')",
                headers=headers
            )
            
            if response.status_code == 200:
                teams = response.json()['value']
                
                for team in teams:
                    asset = {
                        'name': team['displayName'],
                        'type': 'teams_team',
                        'source': 'microsoft_teams',
                        'location': f"teams://team/{team['id']}",
                        'created_date': datetime.fromisoformat(team['createdDateTime'].replace('Z', '+00:00')),
                        'size': 0,
                        'metadata': {
                            'platform_type': 'microsoft_teams',
                            'team_id': team['id'],
                            'description': team.get('description', ''),
                            'visibility': team.get('visibility', '')
                        }
                    }
                    assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Microsoft Teams: {e}")
        
        return assets
    
    def _discover_zoom_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Zoom assets"""
        assets = []
        
        try:
            headers = {'Authorization': f"Bearer {config['jwt_token']}"}
            
            response = requests.get(
                "https://api.zoom.us/v2/users",
                headers=headers
            )
            
            if response.status_code == 200:
                users = response.json()['users']
                
                asset = {
                    'name': 'zoom_users',
                    'type': 'zoom_users',
                    'source': 'zoom',
                    'location': "zoom://users",
                    'created_date': datetime.now(),
                    'size': len(users),
                    'metadata': {
                        'platform_type': 'zoom',
                        'user_count': len(users)
                    }
                }
                assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Zoom: {e}")
        
        return assets
    
    def _discover_tableau_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Tableau assets"""
        assets = []
        
        try:
            import requests
            from requests.auth import HTTPBasicAuth
            
            server = config.get('server')
            username = config.get('username')
            password = config.get('password')
            site_id = config.get('site_id', '')
            
            if not all([server, username, password]):
                self.logger.warning("Tableau credentials not provided, using placeholder discovery")
                tableau_objects = [
                    'workbooks', 'datasources', 'projects', 'users',
                    'sites', 'views', 'flows'
                ]
                
                for obj_type in tableau_objects:
                    asset = {
                        'name': obj_type,
                        'type': 'tableau_object',
                        'source': 'tableau',
                        'location': f"tableau://{server}/{obj_type}",
                        'created_date': datetime.now(),
                        'size': 0,
                        'metadata': {
                            'platform_type': 'tableau',
                            'object_type': obj_type,
                            'server': server
                        }
                    }
                    assets.append(asset)
                return assets
            
            base_url = f"https://{server}/api/3.18"
            auth = HTTPBasicAuth(username, password)
            
            signin_data = {
                'credentials': {
                    'name': username,
                    'password': password,
                    'site': {'contentUrl': site_id}
                }
            }
            
            signin_response = requests.post(
                f"{base_url}/auth/signin",
                json=signin_data,
                auth=auth,
                timeout=10
            )
            
            if signin_response.status_code != 200:
                self.logger.error(f"Tableau authentication failed: {signin_response.status_code}")
                return assets
            
            signin_json = signin_response.json()
            token = signin_json['credentials']['token']
            site_id = signin_json['credentials']['site']['id']
            
            headers = {
                'X-Tableau-Auth': token,
                'Content-Type': 'application/json'
            }
            
            # Discover different Tableau objects
            tableau_objects = [
                ('workbooks', 'workbook'),
                ('datasources', 'datasource'),
                ('projects', 'project'),
                ('users', 'user'),
                ('sites', 'site'),
                ('views', 'view'),
                ('flows', 'flow')
            ]
            
            for obj_type, obj_name in tableau_objects:
                try:
                    response = requests.get(
                        f"{base_url}/sites/{site_id}/{obj_type}",
                        headers=headers,
                        timeout=10
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        objects = data.get(obj_type, {}).get(obj_name, [])
                        
                        for obj in objects:
                            asset = {
                                'name': obj.get('name', obj.get('id', obj_name)),
                                'type': f'tableau_{obj_name}',
                                'source': 'tableau',
                                'location': f"tableau://{server}/{obj_type}/{obj.get('id', '')}",
                                'created_date': datetime.fromisoformat(obj.get('createdAt', '').replace('Z', '+00:00')) if obj.get('createdAt') else datetime.now(),
                                'size': 0,
                                'metadata': {
                                    'platform_type': 'tableau',
                                    'object_type': obj_name,
                                    'server': server,
                                    'site_id': site_id,
                                    'object_id': obj.get('id', ''),
                                    'description': obj.get('description', ''),
                                    'owner': obj.get('owner', {}).get('name', '') if obj.get('owner') else ''
                                }
                            }
                            assets.append(asset)
                    else:
                        self.logger.warning(f"Tableau API returned {response.status_code} for {obj_type}")
                        
                except Exception as e:
                    self.logger.warning(f"Error accessing Tableau {obj_type}: {e}")
            
            try:
                requests.post(f"{base_url}/auth/signout", headers=headers, timeout=5)
            except:
                pass  # Ignore signout errors
            
        except ImportError:
            self.logger.warning("requests library not available for Tableau API calls")
        except Exception as e:
            self.logger.error(f"Error connecting to Tableau: {e}")
        
        return assets
    
    def _discover_powerbi_assets(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover Power BI assets"""
        assets = []
        
        try:
            headers = {'Authorization': f"Bearer {config['access_token']}"}
            
            response = requests.get(
                "https://api.powerbi.com/v1.0/myorg/groups",
                headers=headers
            )
            
            if response.status_code == 200:
                workspaces = response.json()['value']
                
                for workspace in workspaces:
                    asset = {
                        'name': workspace['name'],
                        'type': 'powerbi_workspace',
                        'source': 'power_bi',
                        'location': f"powerbi://workspace/{workspace['id']}",
                        'created_date': datetime.now(),
                        'size': 0,
                        'metadata': {
                            'platform_type': 'power_bi',
                            'workspace_id': workspace['id'],
                            'type': workspace.get('type', ''),
                            'state': workspace.get('state', '')
                        }
                    }
                    assets.append(asset)
            
        except Exception as e:
            self.logger.error(f"Error connecting to Power BI: {e}")
        
        return assets
    
    def test_connection(self) -> bool:
        """Test SaaS platform connections"""
        try:
            if not self.saas_platforms:
                self.logger.error("No SaaS platforms configured")
                return False
            
            connection_tested = False
            
            for saas_config in self.saas_platforms:
                platform_type = saas_config.get('type', '').lower()
                
                try:
                    if platform_type == 'salesforce':
                        if Salesforce:
                            sf = Salesforce(
                                username=saas_config.get('username'),
                                password=saas_config.get('password'),
                                security_token=saas_config.get('security_token'),
                                domain=saas_config.get('domain', 'login')
                            )
                            # Test with a simple query
                            sf.query("SELECT Id FROM User LIMIT 1")
                            self.logger.info("Salesforce connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning("Salesforce library not available")
                    
                    elif platform_type == 'servicenow':
                        if pysnow:
                            s = pysnow.Client(
                                instance=saas_config.get('instance'),
                                user=saas_config.get('username'),
                                password=saas_config.get('password')
                            )
                            table = s.resource(api_path='/table/sys_user')
                            list(table.get(limit=1))
                            self.logger.info("ServiceNow connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning("ServiceNow library not available")
                    
                    elif platform_type == 'slack':
                        if WebClient:
                            client = WebClient(token=saas_config.get('bot_token'))
                            # Test with auth.test
                            response = client.auth_test()
                            if response["ok"]:
                                self.logger.info("Slack connection test successful")
                                connection_tested = True
                        else:
                            self.logger.warning("Slack library not available")
                    
                    elif platform_type == 'jira':
                        if JIRA:
                            jira = JIRA(
                                server=saas_config.get('server'),
                                basic_auth=(saas_config.get('username'), saas_config.get('api_token'))
                            )
                            jira.server_info()
                            self.logger.info("Jira connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning("Jira library not available")
                    
                    elif platform_type in ['hubspot', 'zendesk']:
                        # Test with HTTP request
                        api_key = saas_config.get('api_key')
                        if platform_type == 'hubspot':
                            url = f"https://api.hubapi.com/contacts/v1/lists/all/contacts/all?hapikey={api_key}&count=1"
                        else:  # zendesk
                            subdomain = saas_config.get('subdomain')
                            url = f"https://{subdomain}.zendesk.com/api/v2/users/me.json"
                        
                        headers = {'Authorization': f'Bearer {api_key}'} if platform_type == 'zendesk' else {}
                        response = requests.get(url, headers=headers, timeout=10)
                        
                        if response.status_code == 200:
                            self.logger.info(f"{platform_type.capitalize()} connection test successful")
                            connection_tested = True
                        else:
                            self.logger.warning(f"{platform_type.capitalize()} connection test failed: {response.status_code}")
                    
                except Exception as e:
                    self.logger.warning(f"{platform_type.capitalize()} connection test failed: {e}")
            
            if connection_tested:
                self.logger.info("SaaS connection test successful")
                return True
            else:
                self.logger.error("No SaaS platforms could be tested")
                return False
                
        except Exception as e:
            self.logger.error(f"SaaS connection test failed: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate SaaS connector configuration"""
        if not self.saas_platforms:
            self.logger.error("No SaaS platform connections configured")
            return False
        
        for saas_config in self.saas_platforms:
            if not saas_config.get('type'):
                self.logger.error("SaaS platform type not specified in configuration")
                return False
        
        return True
