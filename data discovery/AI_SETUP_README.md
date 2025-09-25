# AI Analysis Setup Guide

## Overview
The Data Discovery platform includes AI-powered analysis features for data assets using Google's Gemini AI. To enable these features, you need to configure a Google Gemini API key.

## Features Requiring API Key
- **AI Analysis**: Automated data quality assessment and insights
- **PII Scan**: Detection of personally identifiable information
- **Data Profiling**: Enhanced analysis with AI recommendations

## Setup Instructions

### 1. Get Google Gemini API Key
1. Visit [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Sign in with your Google account
3. Click "Create API Key"
4. Copy the generated API key

### 2. Configure Environment Variable

#### Option A: Set Environment Variable (Recommended)
```bash
export GEMINI_API_KEY="your_api_key_here"
```

#### Option B: Create .env File
Create a `.env` file in the `data discovery` directory:
```
GEMINI_API_KEY=your_api_key_here
```

#### Option C: Set in Terminal Before Starting Server
```bash
cd "data discovery"
GEMINI_API_KEY="your_api_key_here" python web_api.py
```

### 3. Restart the Backend Server
After setting the API key, restart the backend server:
```bash
# Stop the current server (Ctrl+C)
# Then restart:
cd "data discovery"
python web_api.py
```

## Verification
1. Open the Data Discovery UI: http://localhost:8080
2. Navigate to any asset
3. Click on the "AI Analysis" tab
4. You should see AI-generated insights instead of the configuration error

## Troubleshooting

### Error: "AI analysis requires Google Gemini API key"
- Ensure the GEMINI_API_KEY environment variable is set
- Verify the API key is valid and active
- Restart the backend server after setting the variable

### Error: "AI model temporarily unavailable"
- Check your internet connection
- Verify the API key has proper permissions
- Try again in a few minutes

### Error: "API error: 404"
- The Gemini model version may have changed
- Contact support for model version updates

## API Usage and Costs
- Google Gemini API has usage limits and may incur costs
- Check [Google AI Pricing](https://ai.google.dev/pricing) for current rates
- Monitor your usage in the Google AI Studio dashboard

## Security Notes
- Never commit API keys to version control
- Use environment variables or secure configuration management
- Rotate API keys regularly for security

## Support
If you encounter issues with AI analysis setup, please check:
1. API key configuration
2. Network connectivity
3. Google AI service status
4. Backend server logs for detailed error messages
