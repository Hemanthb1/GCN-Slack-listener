"""
GCN Fermi GBM Kafka Listener
=============================
Real-time Fermi GBM alert consumer via NASA GCN Kafka streams
with Slack distribution for rapid follow-up coordination.

Consumes:
    - FERMI_GBM_ALERT
    - FERMI_GBM_FLT_POS
    - FERMI_GBM_GND_POS
    - FERMI_GBM_FIN_POS

Only distributes Ground Position and Final Position notices
to avoid Slack spam. Automatically downloads FITS skymaps
when available.

Usage:
    cp .env.example .env   # fill in credentials
    python listener.py

Environment variables:
    GCN_CLIENT_ID      GCN Kafka client ID
    GCN_CLIENT_SECRET  GCN Kafka client secret
    SLACK_WEBHOOK      Slack incoming webhook URL
    SKYMAP_DIR         Directory to save FITS skymaps (default: ./skymaps)
"""

import os
import re
import time
import logging
import threading

import requests
from dotenv import load_dotenv
from gcn_kafka import Consumer

# ── Load environment ──────────────────────────────────────────────────────────
load_dotenv()

GCN_CLIENT_ID     = os.environ["GCN_CLIENT_ID"]
GCN_CLIENT_SECRET = os.environ["GCN_CLIENT_SECRET"]
SLACK_WEBHOOK     = os.environ["SLACK_WEBHOOK"]
SKYMAP_DIR        = os.environ.get("SKYMAP_DIR", "./skymaps")

os.makedirs(SKYMAP_DIR, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Topics ────────────────────────────────────────────────────────────────────
TOPICS = [
    "gcn.classic.text.FERMI_GBM_ALERT",
    "gcn.classic.text.FERMI_GBM_FLT_POS",
    "gcn.classic.text.FERMI_GBM_GND_POS",
    "gcn.classic.text.FERMI_GBM_FIN_POS",
]

# Only send Slack alerts for these notice types
# Avoids spamming for every notice in the sequence:
# Alert → FLT_POS → GND_POS → GND_POS → FIN_POS
SEND_FOR_TYPES = {
    "Fermi-GBM Ground Position",
    "Fermi-GBM Final Position",
}


# ── Slack ─────────────────────────────────────────────────────────────────────
def send_to_slack(text: str, blocks: list = None) -> None:
    """Send a message to the configured Slack webhook."""
    try:
        payload = {"text": text}
        if blocks:
            payload["blocks"] = blocks
        requests.post(SLACK_WEBHOOK, json=payload, timeout=5)
    except Exception as e:
        log.error(f"Slack error: {e}")


def send_async(text: str, blocks: list = None) -> None:
    """Send a Slack message in a background thread."""
    threading.Thread(
        target=send_to_slack,
        args=(text, blocks),
        daemon=True,
    ).start()


# ── Skymap fetcher ────────────────────────────────────────────────────────────
def fetch_skymap(url: str, trigger: str, delay: int = 120) -> None:
    """
    Download a FITS skymap in a background thread.

    The skymap file is not available immediately after the notice —
    HEASARC typically takes ~90 seconds to generate it. This function
    waits for the specified delay before attempting the download.

    Parameters
    ----------
    url : str
        Full URL to the FITS skymap file.
    trigger : str
        GBM trigger number — used as the output filename.
    delay : int
        Seconds to wait before attempting download (default: 120).
    """
    def _fetch():
        log.info(f"Waiting {delay}s for skymap to be ready: {url}")
        time.sleep(delay)
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                fname = os.path.join(SKYMAP_DIR, f"skymap_{trigger}.fits")
                with open(fname, "wb") as f:
                    f.write(r.content)
                log.info(f"Skymap saved to {fname}")
            else:
                log.warning(f"Skymap fetch failed: HTTP {r.status_code}")
        except Exception as e:
            log.error(f"Skymap fetch error: {e}")

    threading.Thread(target=_fetch, daemon=True).start()


# ── Notice parser ─────────────────────────────────────────────────────────────
def parse_fermi_text(text: str, topic: str) -> None:
    """
    Parse a GCN classic text notice and send a Slack alert.

    Handles multi-token values like:
        GRB_RA:   234.533d {+15h 38m 08s} (J2000),
    by extracting only the leading numeric value via regex.

    Parameters
    ----------
    text : str
        Raw GCN notice text.
    topic : str
        Kafka topic the notice arrived on.
    """
    # Parse key: value pairs
    data = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("//") or line.startswith("COMMENTS"):
            continue
        if ":" not in line:
            continue
        key, _, val = line.partition(":")
        data[key.strip()] = val.strip()

    def extract_float(key: str) -> float | None:
        """Extract the first number from a value string."""
        raw = data.get(key, "")
        match = re.search(r"[+-]?\d+\.?\d*", raw)
        return float(match.group()) if match else None

    def extract_comments() -> list[str]:
        """Extract all COMMENTS lines from the notice."""
        return [
            line.strip().replace("COMMENTS:", "").strip()
            for line in text.splitlines()
            if line.strip().startswith("COMMENTS:")
        ]

    # Extract fields
    trigger     = data.get("TRIGGER_NUM", "unknown")
    notice_type = data.get("NOTICE_TYPE", topic.split(".")[-1])
    grb_time    = data.get("GRB_TIME", "unknown")
    signif      = extract_float("DATA_SIGNIF") or extract_float("TRIGGER_SIGNIF")
    ra          = extract_float("GRB_RA")
    dec         = extract_float("GRB_DEC")
    error       = extract_float("GRB_ERROR")
    most_likely = data.get("MOST_LIKELY")
    comments    = extract_comments()
    lc_url      = data.get("LC_URL", "")
    skymap_url  = data.get("POS_MAP_URL", "")

    if trigger == "unknown":
        log.warning("Skipping malformed notice — no TRIGGER_NUM found")
        return

    # Position string
    if ra is not None and dec is not None:
        pos_str = f"RA={ra:.3f}°, Dec={dec:+.3f}°"
        err_str = f"{error:.2f}°" if error else "unknown"
    else:
        pos_str = "Position not yet available"
        err_str = "—"

    # Slack fields
    fields = [
        {"type": "mrkdwn", "text": f"*Notice Type:*\n{notice_type}"},
        {"type": "mrkdwn", "text": f"*Time (UT):*\n{grb_time}"},
        {"type": "mrkdwn", "text": f"*Position:*\n{pos_str}"},
        {"type": "mrkdwn", "text": f"*Error Radius:*\n{err_str}"},
    ]

    if signif:
        fields.append(
            {"type": "mrkdwn", "text": f"*Significance:*\n{signif:.1f}σ"}
        )

    if most_likely:
        fields.append(
            {"type": "mrkdwn", "text": f"*Most Likely:*\n{most_likely}"}
        )

    # Slack blocks
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"🛸 Fermi GBM #{trigger}"},
        },
        {
            "type": "section",
            "fields": fields[:10],  # Slack max is 10 fields per section
        },
    ]

    comment_text = "\n".join(f"• {c}" for c in comments if c)
    if comment_text:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Comments:*\n{comment_text}"},
        })

    if lc_url:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"<{lc_url}|🔭 View Lightcurve>"},
        })

    if skymap_url:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"<{skymap_url}|🗺️ Download Skymap (FITS)>\n"
                    f"_Note: file available ~90s after notice_"
                ),
            },
        })
        fetch_skymap(skymap_url, trigger, delay=120)

    send_async(f"Fermi GBM Alert: trigger {trigger}", blocks)
    log.info(
        f"Sent Slack alert — trigger {trigger} | {notice_type} | "
        f"skymap: {skymap_url or 'none'}"
    )


# ── Deduplication ─────────────────────────────────────────────────────────────
def should_send(text: str) -> bool:
    """
    Return True only for Ground Position and Final Position notices.

    Each GBM trigger produces up to 5 notices in sequence:
        Alert → FLT_POS → GND_POS → GND_POS → FIN_POS

    We only send Slack alerts for GND_POS (real coordinates, fast)
    and FIN_POS (most accurate), avoiding redundant notifications.
    """
    for line in text.splitlines():
        if line.strip().startswith("NOTICE_TYPE:"):
            notice_type = line.split(":", 1)[1].strip()
            return any(t in notice_type for t in SEND_FOR_TYPES)
    return False


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    """
    Start the GCN Kafka consumer loop with automatic reconnection.

    Subscribes to Fermi GBM topics and processes incoming notices.
    Reconnects automatically on connection loss with a 5-second delay.
    """
    while True:
        try:
            log.info("Starting GCN Kafka consumer...")

            consumer = Consumer(
                client_id=GCN_CLIENT_ID,
                client_secret=GCN_CLIENT_SECRET,
                config={
                    "auto.offset.reset":      "latest",
                    "session.timeout.ms":     60000,
                    "max.poll.interval.ms":   300000,
                    "request.timeout.ms":     120000,
                    "socket.keepalive.enable": True,
                    "reconnect.backoff.ms":   1000,
                    "reconnect.backoff.max.ms": 10000,
                },
            )

            consumer.subscribe(TOPICS)
            log.info(f"Subscribed to {len(TOPICS)} topics. Waiting for alerts...")

            while True:
                messages = consumer.consume(timeout=5)

                if not messages:
                    continue

                for message in messages:
                    if message.error():
                        log.error(f"Kafka error: {message.error()}")
                        continue

                    topic = message.topic()
                    raw   = message.value()

                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", errors="ignore")

                    try:
                        log.info(f"Received message from {topic}")

                        if "FERMI" in topic:
                            if should_send(raw):
                                parse_fermi_text(raw, topic)
                            else:
                                for line in raw.splitlines():
                                    if line.strip().startswith("NOTICE_TYPE:"):
                                        notice_type = line.split(":", 1)[1].strip()
                                        log.info(f"Skipping: {notice_type}")
                                        break

                    except Exception as e:
                        log.error(f"Parse error: {repr(e)}")

        except Exception as e:
            log.error(f"Connection lost: {repr(e)}")
            log.info("Reconnecting in 5 seconds...")
            time.sleep(5)


if __name__ == "__main__":
    send_async("GCN Fermi GBM listener started")
    main()
