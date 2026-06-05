# GCN Slack Listener

> Real-time Slack notifications for GCN alerts — currently focused on Fermi GBM, with support for Einstein Probe WXT and other GCN Kafka topics.

[![Python](https://img.shields.io/badge/Python-3.9+-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![GCN Kafka](https://img.shields.io/badge/GCN-Kafka-orange)](https://gcn.nasa.gov/docs/client)

---

## What It Does

Subscribes to NASA's [GCN Kafka alert stream](https://gcn.nasa.gov/docs/client) and forwards parsed alerts to a configured Slack channel — enabling real-time transient notifications for follow-up teams without needing to monitor GCN directly.

```
GCN Kafka stream (Fermi GBM, Einstein Probe WXT, ...)
        ↓
gcn-kafka Python consumer
        ↓
Alert parsing + formatting
        ↓
Slack webhook → #alerts channel
```

---

## Supported Alert Topics

| Mission | Topic | Alert Type |
|---|---|---|
| Fermi GBM | `gcn.classic.voevent.FERMI_GBM_FIN_POS` | Final GRB position |
| Fermi GBM | `gcn.classic.voevent.FERMI_GBM_SUBTHRESH` | Sub-threshold events |
| Einstein Probe WXT | `gcn.notices.einstein_probe.wxt.alert` | X-ray transients |

---

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/Hemanthb1/GCN-Slack-listener.git
cd GCN-Slack-listener
pip install -r requirements.txt
```

### 2. Configure credentials

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```bash
# GCN Kafka credentials (from https://gcn.nasa.gov/profile)
GCN_CLIENT_ID=your_gcn_client_id
GCN_CLIENT_SECRET=your_gcn_client_secret

# Slack webhook URL (from your Slack app settings)
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/your/webhook/url

# Optional: observer location for visibility filtering
OBSERVER_LAT=-30.2407       # e.g. SOAR telescope
OBSERVER_LON=-70.7366
OBSERVER_ELEVATION=2738     # metres
```

### 3. Run the listener

```bash
python "gcn listener/gcn_listener.py"
```

---

## Example Slack Alert

```
🚨 *Fermi GBM Alert* — GRB 240301A
━━━━━━━━━━━━━━━━━━━━━━━━━━
📍 RA: 123.45°  Dec: -30.12°
⏱️  Trigger time: 2024-03-01 14:32:11 UTC
💥 T90: 12.4 s
🔭 Error radius: 3.2°
🌙 Moon separation: 45.2°
📡 Source: gcn.classic.voevent.FERMI_GBM_FIN_POS
```

---

## Configuration

All configuration is via `.env`. See `.env.example` for all available options:

| Variable | Description | Required |
|---|---|---|
| `GCN_CLIENT_ID` | GCN Kafka client ID | ✓ |
| `GCN_CLIENT_SECRET` | GCN Kafka client secret | ✓ |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL | ✓ |
| `OBSERVER_LAT` | Observatory latitude (deg) | Optional |
| `OBSERVER_LON` | Observatory longitude (deg) | Optional |
| `OBSERVER_ELEVATION` | Observatory elevation (m) | Optional |

---

## Roadmap

- [ ] Visibility plots (altitude vs time) attached to Slack messages — [Issue #1](https://github.com/Hemanthb1/GCN-Slack-listener/issues/1)
- [ ] Airmass and rise/set times per alert
- [ ] Moon separation and phase filter
- [ ] Multi-observatory support
- [ ] LVK GW skymap overlap for coincident alerts
- [ ] LSST/ZTF alert stream integration

---

## Requirements

```
gcn-kafka
slack-sdk
python-dotenv
astropy
astroplan    # for upcoming visibility features
```

---

## Related Projects

- 🌐 [GW-AGN Dashboard](https://github.com/Hemanthb1/gw-agn-dashboard) — real-time GW follow-up dashboard
- 🐍 [GW_AGN_watcher](https://github.com/Hemanthb1/GW_AGN_watcher) — GW skymap × ZTF crossmatching pipeline
- 🌐 [NASA GCN](https://gcn.nasa.gov) — General Coordinates Network

---

## Author

**Hemanth Bommireddy**
PhD candidate, Universidad de Chile
📧 hemanth.bommireddy195@gmail.com
🔗 [ORCID](https://orcid.org/0009-0007-4271-6444) · [InspireHEP](https://inspirehep.net/authors/2902490)
