# Torro Data Discovery System - User Journey Diagram

## High-Level User Journey Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           TORRO DATA DISCOVERY SYSTEM                          │
│                              USER JOURNEY MAP                                  │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PERSONA 1     │    │   PERSONA 2     │    │   PERSONA 3     │    │   PERSONA 4     │
│ Data Engineer   │    │ Data Analyst    │    │Data Governance  │    │ Business User   │
│ (Primary)       │    │ (Secondary)     │    │ Manager         │    │ (Consumer)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │                       │
         └───────────────────────┼───────────────────────┼───────────────────────┘
                                 │                       │
                                 ▼                       ▼
                    ┌─────────────────────────────────────────┐
                    │         PHASE 1: ONBOARDING            │
                    │                                         │
                    │  ┌─────────────┐    ┌─────────────┐    │
                    │  │ First Login │───▶│System Overview│   │
                    │  │             │    │             │    │
                    │  │ • Welcome   │    │ • Dashboard │    │
                    │  │ • Wizard    │    │ • Health    │    │
                    │  │ • Metrics   │    │ • Activity  │    │
                    │  └─────────────┘    └─────────────┘    │
                    └─────────────────────────────────────────┘
                                         │
                                         ▼
                    ┌─────────────────────────────────────────┐
                    │    PHASE 2: DATA SOURCE CONFIG         │
                    │                                         │
                    │  ┌─────────────┐    ┌─────────────┐    │
                    │  │Connector    │───▶│Connection   │    │
                    │  │Setup        │    │Wizard       │    │
                    │  │             │    │             │    │
                    │  │ • 120+      │    │ • 4 Steps   │    │
                    │  │   Connectors│    │ • Test      │    │
                    │  │ • Categories│    │ • Validate  │    │
                    │  └─────────────┘    └─────────────┘    │
                    └─────────────────────────────────────────┘
                                         │
                                         ▼
                    ┌─────────────────────────────────────────┐
                    │     PHASE 3: DATA DISCOVERY            │
                    │                                         │
                    │  ┌─────────────┐    ┌─────────────┐    │
                    │  │Discovery    │───▶│Discovery    │    │
                    │  │Config       │    │Execution    │    │
                    │  │             │    │             │    │
                    │  │ • Controls  │    │ • Scanning  │    │
                    │  │ • Status    │    │ • Progress  │    │
                    │  │ • Options   │    │ • Results   │    │
                    │  └─────────────┘    └─────────────┘    │
                    └─────────────────────────────────────────┘
                                         │
                                         ▼
                    ┌─────────────────────────────────────────┐
                    │   PHASE 4: ASSET EXPLORATION           │
                    │                                         │
                    │  ┌─────────────┐    ┌─────────────┐    │
                    │  │Asset        │───▶│Asset        │    │
                    │  │Catalog      │    │Details      │    │
                    │  │             │    │             │    │
                    │  │ • Search    │    │ • Overview  │    │
                    │  │ • Filter    │    │ • Schema    │    │
                    │  │ • Browse    │    │ • Profiling │    │
                    │  │             │    │ • AI Analysis│   │
                    │  │             │    │ • PII Scan  │    │
                    │  └─────────────┘    └─────────────┘    │
                    └─────────────────────────────────────────┘
                                         │
                                         ▼
                    ┌─────────────────────────────────────────┐
                    │   PHASE 5: DATA LINEAGE & RELATIONS    │
                    │                                         │
                    │  ┌─────────────┐    ┌─────────────┐    │
                    │  │Lineage      │───▶│Lineage      │    │
                    │  │Exploration  │    │Visualization│    │
                    │  │             │    │             │    │
                    │  │ • Selection │    │ • Graph     │    │
                    │  │ • Controls  │    │ • Nodes     │    │
                    │  │ • Direction │    │ • Edges     │    │
                    │  │             │    │ • Columns   │    │
                    │  └─────────────┘    └─────────────┘    │
                    │                                         │
                    │  ┌─────────────┐                       │
                    │  │Relationship │                       │
                    │  │Management   │                       │
                    │  │             │                       │
                    │  │ • Edit Mode │                       │
                    │  │ • Custom    │                       │
                    │  │ • Confidence│                       │
                    │  └─────────────┘                       │
                    └─────────────────────────────────────────┘
                                         │
                                         ▼
                    ┌─────────────────────────────────────────┐
                    │   PHASE 6: MONITORING & MAINTENANCE    │
                    │                                         │
                    │  ┌─────────────┐    ┌─────────────┐    │
                    │  │Continuous   │───▶│System       │    │
                    │  │Monitoring   │    │Health       │    │
                    │  │             │    │Monitoring   │    │
                    │  │ • Background│    │             │    │
                    │  │ • Real-time │    │ • Metrics   │    │
                    │  │ • Alerts    │    │ • Status    │    │
                    │  │ • Config    │    │ • Performance│   │
                    │  └─────────────┘    └─────────────┘    │
                    └─────────────────────────────────────────┘
```

## Detailed User Flow Diagrams

### Flow 1: New Data Source Onboarding
```
┌─────────────────────────────────────────────────────────────────┐
│                    NEW DATA SOURCE ONBOARDING                  │
└─────────────────────────────────────────────────────────────────┘

User Need: Connect New Data Source
    │
    ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Navigate to │───▶│ Select      │───▶│ Configure   │───▶│ Test        │
│ Connectors  │    │ Connector   │    │ Connection  │    │ Connection  │
│ Tab         │    │ Type        │    │ Parameters  │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
    │                   │                   │                   │
    ▼                   ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ View 120+   │    │ Choose from │    │ Fill Form   │    │ Real-time   │
│ Connectors  │    │ Categories: │    │ Fields      │    │ Validation  │
│             │    │ • Cloud     │    │ • Host      │    │ • Success   │
│ • Cloud     │    │ • Database  │    │ • Creds     │    │ • Error     │
│ • Database  │    │ • Network   │    │ • Options   │    │ • Preview   │
│ • Network   │    │ • API/SaaS  │    │ • Security  │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
    │                   │                   │                   │
    ▼                   ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Click "New  │    │ Visual      │    │ Dynamic     │    │ Execute     │
│ Connection" │    │ Cards with  │    │ Form Based  │    │ Connection  │
│ Button      │    │ Icons       │    │ on Type     │    │ Test        │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
    │                   │                   │                   │
    ▼                   ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Open 4-Step │    │ Step 1:     │    │ Step 2:     │    │ Step 3:     │
│ Wizard      │    │ Type        │    │ Config      │    │ Test        │
│             │    │ Selection   │    │ Form        │    │ Connection  │
│ • Guided    │    │             │    │             │    │             │
│ • Visual    │    │ • Database  │    │ • Host      │    │ • Loading   │
│ • Progress  │    │ • Cloud     │    │ • Port      │    │ • Results   │
│             │    │ • API/SaaS  │    │ • Username  │    │ • Feedback  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
    │                   │                   │                   │
    ▼                   ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Step 4:     │    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Summary     │    │ Save        │    │ Run         │    │ Configure   │
│             │    │ Connection  │    │ Discovery   │    │ Monitoring  │
│ • Review    │    │             │    │ Scan        │    │ (Optional)  │
│ • Save      │    │ • Add to    │    │             │    │             │
│ • Complete  │    │   My        │    │ • Full      │    │ • Auto      │
│             │    │   Connections│   │   Scan      │    │   Refresh   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

### Flow 2: Data Asset Discovery
```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA ASSET DISCOVERY                      │
└─────────────────────────────────────────────────────────────────┘

User Need: Find Specific Data
    │
    ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Navigate to │───▶│ Use Search  │───▶│ Apply       │───▶│ Review      │
│ Assets Tab  │    │ Function    │    │ Filters     │    │ Results     │
│             │    │             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
    │                   │                   │                   │
    ▼                   ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ View Asset  │    │ Search by:  │    │ Filter by:  │    │ Browse      │
│ Table       │    │ • Name      │    │ • Type      │    │ Results     │
│             │    │ • Type      │    │ • Source    │    │             │
│ • Name      │    │ • Source    │    │ • Date      │    │ • Sortable  │
│ • Type      │    │ • Content   │    │ • Size      │    │ • Paginated │
│ • Source    │    │             │    │ • Quality   │    │ • Detailed  │
│ • Size      │    │ • Auto-     │    │             │    │             │
│ • Date      │    │   complete  │    │ • Advanced  │    │ • Export    │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
    │                   │                   │                   │
    ▼                   ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Click Asset │    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ for Details │    │ View Asset  │    │ Examine     │    │ Check PII   │
│             │    │ Details     │    │ Schema      │    │ Scan        │
│ • Modal     │    │ Modal       │    │             │    │ Results     │
│ • Tabs      │    │             │    │ • Columns   │    │             │
│ • Rich Info │    │ • Overview  │    │ • Types     │    │ • Privacy   │
│             │    │ • Schema    │    │ • Keys      │    │   Risk      │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
    │                   │                   │                   │
    ▼                   ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Examine     │    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Data        │    │ View AI     │    │ Access      │    │ Make        │
│ Profiling   │    │ Analysis    │    │ Data for    │    │ Data-Driven │
│             │    │             │    │ Analysis    │    │ Decision    │
│ • Quality   │    │ • Insights  │    │             │    │             │
│ • Stats     │    │ • Patterns  │    │ • Download  │    │ • Business  │
│ • Metrics   │    │ • Recommendations│ • API      │    │   Value     │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

### Flow 3: Data Lineage Investigation
```
┌─────────────────────────────────────────────────────────────────┐
│                   DATA LINEAGE INVESTIGATION                   │
└─────────────────────────────────────────────────────────────────┘

User Need: Understand Data Relationships
    │
    ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Navigate to │───▶│ Select      │───▶│ Configure   │───▶│ View        │
│ Lineage Tab │    │ Asset       │    │ Parameters  │    │ Lineage     │
│             │    │             │    │             │    │ Graph       │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
    │                   │                   │                   │
    ▼                   ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ View Lineage│    │ Choose from │    │ Set:        │    │ Interactive │
│ Tools       │    │ Discovered  │    │ • Direction │    │ Graph       │
│             │    │ Assets      │    │ • Depth     │    │             │
│ • Selection │    │             │    │ • Levels    │    │ • Nodes     │
│ • Controls  │    │ • Dropdown  │    │ • Filters   │    │ • Edges     │
│ • Management│    │ • Search    │    │             │    │ • Zoom      │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
    │                   │                   │                   │
    ▼                   ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Explore     │    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Upstream    │    │ Explore     │    │ Examine     │    │ Add Custom  │
│ Sources     │    │ Downstream  │    │ Column-     │    │ Relationships│
│             │    │ Targets     │    │ Level       │    │             │
│ • Click     │    │             │    │ Lineage     │    │ • Edit Mode │
│ • Navigate  │    │ • Click     │    │             │    │ • Visual    │
│ • Drill     │    │ • Navigate  │    │ • Detailed  │    │ • Form      │
│   Down      │    │ • Drill     │    │ • Matrix    │    │ • Confidence│
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
    │                   │                   │                   │
    ▼                   ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Export      │    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Lineage     │    │ Generate    │    │ Update      │    │ Document    │
│ Documentation│   │ Reports     │    │ Governance  │    │ Findings    │
│             │    │             │    │ Policies    │    │             │
│ • PDF       │    │ • Analysis  │    │             │    │ • Notes     │
│ • JSON      │    │ • Summary   │    │ • Compliance│    │ • Tags      │
│ • CSV       │    │ • Metrics   │    │ • Access    │    │ • Share     │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

## Key Touchpoints & Interactions

### Dashboard Interactions
```
┌─────────────────────────────────────────────────────────────────┐
│                        DASHBOARD TOUCHPOINTS                   │
└─────────────────────────────────────────────────────────────────┘

┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ System      │    │ Recent      │    │ Discovery   │    │ System      │
│ Health      │    │ Activity    │    │ Statistics  │    │ Status      │
│             │    │             │    │             │    │             │
│ • Connector │    │ • New       │    │ • Chart     │    │ • Online    │
│   Status    │    │   Assets    │    │ • Trends    │    │ • Offline   │
│ • Performance│   │ • Scans     │    │ • Metrics   │    │ • Warning   │
│ • Alerts    │    │ • Errors    │    │ • History   │    │ • Error     │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

### Connector Management
```
┌─────────────────────────────────────────────────────────────────┐
│                     CONNECTOR MANAGEMENT                        │
└─────────────────────────────────────────────────────────────────┘

┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ My          │    │ Available   │    │ Connection  │    │ Test        │
│ Connections │    │ Connectors  │    │ Wizard      │    │ Results     │
│             │    │             │    │             │    │             │
│ • Active    │    │ • Cloud     │    │ • 4 Steps   │    │ • Success   │
│ • Configured│    │ • Database  │    │ • Guided    │    │ • Error     │
│ • Status    │    │ • Network   │    │ • Validation│    │ • Preview   │
│ • Health    │    │ • API/SaaS  │    │ • Testing   │    │ • Assets    │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

### Asset Exploration
```
┌─────────────────────────────────────────────────────────────────┐
│                       ASSET EXPLORATION                        │
└─────────────────────────────────────────────────────────────────┘

┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Search &    │    │ Asset       │    │ Detailed    │    │ AI-Powered  │
│ Filter      │    │ Table       │    │ Modal       │    │ Analysis    │
│             │    │             │    │             │    │             │
│ • Text      │    │ • Sortable  │    │ • Overview  │    │ • Insights  │
│ • Type      │    │ • Paginated │    │ • Schema    │    │ • Patterns  │
│ • Source    │    │ • Actions   │    │ • Profiling │    │ • Quality   │
│ • Date      │    │ • Export    │    │ • PII Scan  │    │ • Recommendations│
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

## Success Metrics Dashboard
```
┌─────────────────────────────────────────────────────────────────┐
│                      SUCCESS METRICS                           │
└─────────────────────────────────────────────────────────────────┘

┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ User        │    │ System      │    │ Data        │    │ Business    │
│ Engagement  │    │ Performance │    │ Quality     │    │ Impact      │
│             │    │             │    │             │    │             │
│ • Time      │    │ • Sources   │    │ • Score     │    │ • Decisions │
│ • Sessions  │    │ • Assets    │    │ • Issues    │    │ • ROI       │
│ • Features  │    │ • Scans     │    │ • Compliance│    │ • Efficiency│
│ • Tasks     │    │ • Health    │    │ • PII       │    │ • Value     │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

This comprehensive user journey diagram shows the complete flow from initial onboarding through advanced data lineage exploration, highlighting all key touchpoints, interactions, and decision points in the Torro Data Discovery System.
