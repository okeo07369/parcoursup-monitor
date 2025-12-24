#!/usr/bin/env python3
"""
Parcoursup Formation Monitor
Automatically detects new formations and sends email notifications
"""

import json
import os
import sys
import time
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Set, Optional
import math

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_URL = "https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/fr-esr-cartographie_formations_parcoursup/records"
DATA_DIR = Path("data")
CONFIG_FILE = Path("config.json")
CURRENT_DATA_FILE = DATA_DIR / "current_formations.json"
PREVIOUS_DATA_FILE = DATA_DIR / "previous_formations.json"

# API Settings
REQUEST_DELAY = 0.5  # seconds between requests
MAX_RETRIES = 3
TIMEOUT = 30
RECORDS_PER_PAGE = 100

# Email Settings (from environment variables for security)
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def load_config() -> Dict:
    """Load configuration from config.json"""
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"⚠️  Config file not found: {CONFIG_FILE}")
        print("Creating default config file...")
        default_config = create_default_config()
        save_config(default_config)
        return default_config
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing config file: {e}")
        sys.exit(1)


def create_default_config() -> Dict:
    """Create default configuration"""
    return {
        "filters": {
            "session": ["2025", "2026"],
            "departements": ["75", "77", "93", "94"],
            "types_formation": [],  # Empty = all types
            "keywords_include": [],
            "keywords_exclude": [],
            "statut": [],  # Empty = all statuses
            "max_distance_km": None,  # None = no distance filter
            "home_coordinates": {
                "lat": 48.8534,
                "lon": 2.6381
            }
        },
        "email": {
            "recipient": "your-email@example.com",
            "subject_prefix": "[Parcoursup Monitor]",
            "send_if_no_new": True
        }
    }


def save_config(config: Dict):
    """Save configuration to file"""
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def ensure_data_directory():
    """Create data directory if it doesn't exist"""
    DATA_DIR.mkdir(exist_ok=True)


def log(message: str):
    """Print timestamped log message"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


# ============================================================================
# API FETCHING FUNCTIONS
# ============================================================================

def fetch_page(offset: int, limit: int, session_year: str) -> Optional[Dict]:
    """
    Fetch a single page of formations from the API
    
    Args:
        offset: Pagination offset
        limit: Number of records per page
        session_year: Year to filter (e.g., "2025")
    
    Returns:
        API response as dictionary, or None if error
    """
    params = {
        "limit": limit,
        "offset": offset,
        "where": f"session='{session_year}'"
    }
    
    headers = {
        "User-Agent": "ParcoursupMonitor/1.0 (Educational)",
        "Accept": "application/json"
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            log(f"Fetching page: offset={offset}, limit={limit}")
            response = requests.get(
                BASE_URL,
                params=params,
                headers=headers,
                timeout=TIMEOUT
            )
            response.raise_for_status()
            time.sleep(REQUEST_DELAY)
            return response.json()
            
        except requests.exceptions.Timeout:
            log(f"⚠️  Timeout on attempt {attempt + 1}/{MAX_RETRIES}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                log("❌ Max retries reached for timeout")
                return None
                
        except requests.exceptions.RequestException as e:
            log(f"⚠️  Request error on attempt {attempt + 1}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                log("❌ Max retries reached")
                return None
    
    return None


def fetch_all_formations(session_years: List[str]) -> List[Dict]:
    """
    Fetch all formations for specified years with pagination
    
    Args:
        session_years: List of years to fetch (e.g., ["2025", "2026"])
    
    Returns:
        List of all formation records
    """
    all_formations = []
    
    for year in session_years:
        log(f"📥 Fetching formations for session {year}...")
        offset = 0
        
        while True:
            response = fetch_page(offset, RECORDS_PER_PAGE, year)
            
            if response is None:
                log(f"❌ Failed to fetch data for year {year}")
                break
            
            records = response.get("results", [])
            
            if not records:
                log(f"✅ Completed fetching {year}: {len(all_formations)} total records")
                break
            
            all_formations.extend(records)
            offset += RECORDS_PER_PAGE
            
            # Safety check
            if offset > 100000:
                log("⚠️  Safety limit reached (100k records)")
                break
    
    log(f"📊 Total formations fetched: {len(all_formations)}")
    return all_formations


# ============================================================================
# DATA PROCESSING FUNCTIONS
# ============================================================================

def get_unique_id(formation: Dict) -> str:
    """
    Generate unique identifier for a formation
    
    Priority: code_uai > fallback composite key
    """
    # Try UAI code first
    if "code_uai" in formation:
        return f"uai_{formation['code_uai']}"
    
    # Fallback: composite key
    name = formation.get("libelle_formation", "unknown")
    etabl = formation.get("libelle_etablissement", "unknown")
    ville = formation.get("ville", "unknown")
    session = formation.get("session", "unknown")
    
    return f"composite_{session}_{name}_{etabl}_{ville}".lower().replace(" ", "_")


def load_previous_formations() -> List[Dict]:
    """Load previous formations from file"""
    if not PREVIOUS_DATA_FILE.exists():
        log("ℹ️  No previous data found (first run)")
        return []
    
    try:
        with open(PREVIOUS_DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            log(f"📂 Loaded {len(data)} previous formations")
            return data
    except json.JSONDecodeError as e:
        log(f"⚠️  Error reading previous data: {e}")
        return []


def save_current_formations(formations: List[Dict]):
    """Save current formations to file"""
    with open(CURRENT_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(formations, f, indent=2, ensure_ascii=False)
    log(f"💾 Saved {len(formations)} formations to {CURRENT_DATA_FILE}")


def update_previous_formations():
    """Copy current to previous for next run"""
    if CURRENT_DATA_FILE.exists():
        with open(CURRENT_DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        with open(PREVIOUS_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        log(f"🔄 Updated previous formations file")


def detect_new_formations(current: List[Dict], previous: List[Dict]) -> List[Dict]:
    """
    Compare current and previous data to find new formations
    
    Returns:
        List of new formations not in previous dataset
    """
    if not previous:
        log("ℹ️  First run: No comparison performed")
        return []
    
    previous_ids = {get_unique_id(f) for f in previous}
    new_formations = [f for f in current if get_unique_id(f) not in previous_ids]
    
    log(f"🆕 Found {len(new_formations)} new formations")
    return new_formations


# ============================================================================
# FILTERING FUNCTIONS
# ============================================================================

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance between two coordinates using Haversine formula
    
    Returns:
        Distance in kilometers
    """
    R = 6371  # Earth radius in km
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = (math.sin(delta_lat / 2) ** 2 +
         math.cos(lat1_rad) * math.cos(lat2_rad) *
         math.sin(delta_lon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c


def apply_filters(formations: List[Dict], config: Dict) -> List[Dict]:
    """
    Apply filtering criteria from config to formations
    
    Returns:
        List of formations matching all filters
    """
    if not formations:
        return []
    
    filters = config.get("filters", {})
    filtered = formations
    
    # Filter by departement
    if filters.get("departements"):
        depts = set(filters["departements"])
        filtered = [f for f in filtered 
                   if f.get("departement") in depts or
                      f.get("code_departement") in depts]
        log(f"🔍 After departement filter: {len(filtered)} formations")
    
    # Filter by formation type
    if filters.get("types_formation"):
        types = [t.lower() for t in filters["types_formation"]]
        filtered = [f for f in filtered 
                   if any(t in f.get("type_formation", "").lower() for t in types) or
                      any(t in f.get("filiere", "").lower() for t in types)]
        log(f"🔍 After type filter: {len(filtered)} formations")
    
    # Filter by status (public/private)
    if filters.get("statut"):
        statuts = [s.lower() for s in filters["statut"]]
        filtered = [f for f in filtered 
                   if f.get("statut_etablissement", "").lower() in statuts]
        log(f"🔍 After status filter: {len(filtered)} formations")
    
    # Filter by keywords (include)
    if filters.get("keywords_include"):
        keywords_inc = [k.lower() for k in filters["keywords_include"]]
        filtered = [f for f in filtered 
                   if any(kw in json.dumps(f, ensure_ascii=False).lower() 
                         for kw in keywords_inc)]
        log(f"🔍 After include keywords filter: {len(filtered)} formations")
    
    # Filter by keywords (exclude)
    if filters.get("keywords_exclude"):
        keywords_exc = [k.lower() for k in filters["keywords_exclude"]]
        filtered = [f for f in filtered 
                   if not any(kw in json.dumps(f, ensure_ascii=False).lower() 
                             for kw in keywords_exc)]
        log(f"🔍 After exclude keywords filter: {len(filtered)} formations")
    
    # Filter by distance
    max_distance = filters.get("max_distance_km")
    home_coords = filters.get("home_coordinates", {})
    
    if max_distance and home_coords.get("lat") and home_coords.get("lon"):
        home_lat = home_coords["lat"]
        home_lon = home_coords["lon"]
        
        distance_filtered = []
        for f in filtered:
            coords = f.get("coordonnees") or f.get("coordinates") or {}
            if isinstance(coords, dict):
                lat = coords.get("lat") or coords.get("latitude")
                lon = coords.get("lon") or coords.get("longitude")
                
                if lat and lon:
                    distance = calculate_distance(home_lat, home_lon, lat, lon)
                    if distance <= max_distance:
                        f["_distance_km"] = round(distance, 1)
                        distance_filtered.append(f)
        
        filtered = distance_filtered
        log(f"🔍 After distance filter (<{max_distance}km): {len(filtered)} formations")
    
    return filtered


# ============================================================================
# EMAIL NOTIFICATION FUNCTIONS
# ============================================================================

def format_formation_html(formation: Dict) -> str:
    """Format a single formation as HTML"""
    name = formation.get("libelle_formation", "N/A")
    etabl = formation.get("libelle_etablissement", "N/A")
    ville = formation.get("ville", "N/A")
    dept = formation.get("departement", "N/A")
    type_form = formation.get("type_formation", "N/A")
    statut = formation.get("statut_etablissement", "N/A")
    distance = formation.get("_distance_km")
    
    html = f"""
    <div style="border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px; background-color: #f9f9f9;">
        <h3 style="margin: 0 0 10px 0; color: #2c3e50;">{name}</h3>
        <p style="margin: 5px 0;"><strong>Établissement:</strong> {etabl}</p>
        <p style="margin: 5px 0;"><strong>Localisation:</strong> {ville} ({dept})</p>
        <p style="margin: 5px 0;"><strong>Type:</strong> {type_form}</p>
        <p style="margin: 5px 0;"><strong>Statut:</strong> {statut}</p>
    """
    
    if distance is not None:
        html += f'<p style="margin: 5px 0;"><strong>Distance:</strong> {distance} km</p>'
    
    html += "</div>"
    return html


def create_email_html(new_formations: List[Dict], total_fetched: int, is_first_run: bool) -> str:
    """Create HTML email body"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    html = f"""
    <html>
    <head></head>
    <body style="font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto;">
        <h1 style="color: #3498db;">🎓 Parcoursup Monitor Report</h1>
        <p style="color: #7f8c8d;">Generated on: {timestamp}</p>
        <hr style="border: 1px solid #ecf0f1;">
    """
    
    if is_first_run:
        html += f"""
        <div style="background-color: #fff3cd; padding: 15px; border-radius: 5px; margin: 20px 0;">
            <h2 style="color: #856404;">ℹ️ First Run</h2>
            <p>This is the first execution of the monitor. No comparison was performed.</p>
            <p><strong>Total formations fetched:</strong> {total_fetched}</p>
            <p>Future runs will detect new formations added since today.</p>
        </div>
        """
    elif not new_formations:
        html += """
        <div style="background-color: #d1ecf1; padding: 15px; border-radius: 5px; margin: 20px 0;">
            <h2 style="color: #0c5460;">✅ No New Formations</h2>
            <p>No new formations matching your criteria were found since the last check.</p>
        </div>
        """
    else:
        html += f"""
        <div style="background-color: #d4edda; padding: 15px; border-radius: 5px; margin: 20px 0;">
            <h2 style="color: #155724;">🆕 {len(new_formations)} New Formation(s) Found!</h2>
            <p>The following formations match your filtering criteria:</p>
        </div>
        """
        
        for formation in new_formations:
            html += format_formation_html(formation)
    
    html += """
        <hr style="border: 1px solid #ecf0f1; margin-top: 30px;">
        <p style="color: #7f8c8d; font-size: 12px;">
            This is an automated notification from Parcoursup Monitor.<br>
            To modify your filtering criteria, edit the config.json file in your repository.
        </p>
    </body>
    </html>
    """
    
    return html


def send_email_notification(
    recipient: str,
    subject: str,
    html_body: str,
    sender_email: Optional[str] = None,
    sender_password: Optional[str] = None
):
    """
    Send email notification via Gmail SMTP
    
    Args:
        recipient: Recipient email address
        subject: Email subject
        html_body: HTML email body
        sender_email: Gmail address (from env var if not provided)
        sender_password: Gmail app password (from env var if not provided)
    """
    # Get credentials from environment variables if not provided
    sender_email = sender_email or os.environ.get("SENDER_EMAIL")
    sender_password = sender_password or os.environ.get("EMAIL_PASSWORD")
    
    if not sender_email or not sender_password:
        log("❌ Email credentials not found in environment variables")
        log("   Set SENDER_EMAIL and EMAIL_PASSWORD in GitHub Secrets")
        return False
    
    try:
        # Create message
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = sender_email
        msg["To"] = recipient
        
        # Attach HTML body
        html_part = MIMEText(html_body, "html", "utf-8")
        msg.attach(html_part)
        
        # Send email
        log(f"📧 Sending email to {recipient}...")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        
        log("✅ Email sent successfully")
        return True
        
    except Exception as e:
        log(f"❌ Failed to send email: {e}")
        return False


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    log("=" * 60)
    log("🚀 Starting Parcoursup Formation Monitor")
    log("=" * 60)
    
    # Setup
    ensure_data_directory()
    config = load_config()
    
    # Validate config
    if config["email"]["recipient"] == "your-email@example.com":
        log("⚠️  WARNING: Default email recipient detected!")
        log("   Please update config.json with your email address")
    
    # Load previous data
    previous_formations = load_previous_formations()
    is_first_run = len(previous_formations) == 0
    
    # Fetch current data
    session_years = config["filters"].get("session", ["2025"])
    current_formations = fetch_all_formations(session_years)
    
    if not current_formations:
        log("❌ No formations fetched. Aborting.")
        sys.exit(1)
    
    # Save current data
    save_current_formations(current_formations)
    
    # Detect new formations
    new_formations = detect_new_formations(current_formations, previous_formations)
    
    # Apply filters to new formations
    if new_formations:
        filtered_new = apply_filters(new_formations, config)
        log(f"✅ {len(filtered_new)} new formations match your criteria")
    else:
        filtered_new = []
    
    # Send email notification
    email_config = config.get("email", {})
    should_send = filtered_new or email_config.get("send_if_no_new", True) or is_first_run
    
    if should_send:
        subject = f"{email_config.get('subject_prefix', '[Parcoursup]')} "
        if is_first_run:
            subject += "First Run - Setup Complete"
        elif filtered_new:
            subject += f"{len(filtered_new)} New Formation(s) Found"
        else:
            subject += "No New Formations"
        
        html_body = create_email_html(filtered_new, len(current_formations), is_first_run)
        send_email_notification(email_config["recipient"], subject, html_body)
    else:
        log("ℹ️  No email sent (no new formations and send_if_no_new=false)")
    
    # Update previous formations for next run
    update_previous_formations()
    
    log("=" * 60)
    log("✅ Monitor execution completed successfully")
    log("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("\n⚠️  Execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        log(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
