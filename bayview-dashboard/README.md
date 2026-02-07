# Bayview Counseling — Lead Intelligence Dashboard

Real-time analytics dashboard that pulls live data from Google Sheets and displays lead intake metrics, room rental revenue, team performance, cash flow projections, and marketing ROI.

![Dashboard](https://img.shields.io/badge/Status-Live-brightgreen) ![Python](https://img.shields.io/badge/Python-3.12-blue) ![Flask](https://img.shields.io/badge/Flask-3.1-lightgrey)

## Features

- **📊 6 Dashboard Tabs**: Overview, Marketing, Team, Revenue, Room Rental, Forecast
- **🔄 Auto-refresh**: Data pulls from Google Sheets every 15 minutes (configurable)
- **📈 19,800+ leads** tracked from May 2017 to present
- **🏢 Room rental tracking**: 470+ weeks, $3.4M+ all-time revenue
- **💰 Cash flow projections**: 3-month forward with low/med/high scenarios
- **👥 Team performance**: Booking rates, lead assignments, marketing participation

## Quick Start

```bash
# Clone
git clone https://github.com/YOUR_USER/bayview-dashboard.git
cd bayview-dashboard

# Install
pip install -r requirements.txt

# Run
python app.py
# → http://localhost:5000
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Dashboard UI |
| `/api/data` | GET | Full JSON dataset |
| `/api/refresh` | POST | Force data refresh |
| `/api/status` | GET | Health check + stats |

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PORT` | `5000` | Server port |
| `REFRESH_MINUTES` | `15` | Auto-refresh interval |

## Deploy

### Render (recommended)
1. Push to GitHub
2. Connect repo on [render.com](https://render.com)
3. Select "Web Service" → Python → auto-detects `Procfile`

### Railway
```bash
railway init
railway up
```

### Docker
```bash
docker build -t bayview-dashboard .
docker run -p 8080:8080 bayview-dashboard
```

## Data Sources

Dashboard reads from two Google Sheets published as CSV:
- **Lead Intake Form** — All client inquiries and referrals
- **Room Rental Tracking** — Weekly therapist room rental revenue

These are configured in `data_processor.py` and update automatically when the source spreadsheets are edited.

## Architecture

```
app.py              — Flask server + scheduler
data_processor.py   — CSV fetch + analytics engine
templates/
  index.html        — React dashboard (CDN-loaded)
```

The server fetches fresh CSV data on startup and every 15 minutes, processes it into analytics JSON (~220KB), and serves it to the React frontend via `/api/data`.
