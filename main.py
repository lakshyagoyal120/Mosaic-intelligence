"""
Mosaic Wellness — Competitor Ad Intelligence Scraper
=====================================================
Pulls 6 months of competitor ads from Meta Ads Library API.
Automatically finds Page IDs by name, then fetches all ads.
Saves everything to a structured CSV (importable to Google Sheets).

HOW TO USE:
1. Go to replit.com → New Repl → Python
2. Paste this entire script
3. Replace YOUR_ACCESS_TOKEN_HERE with your actual Meta system token
4. Click Run
5. Download the output CSV files from the Files panel on the left
"""

import requests
import csv
import time
import os
from datetime import datetime, timedelta

# ============================================================
# CONFIGURATION — ONLY EDIT THIS SECTION
# ============================================================

ACCESS_TOKEN = "EAANCLtuoY4YBQ1SyzjzHkJ9E7W5802am8ffSOvijTTV0MZAbaMAVrqnVj44blDT1iKB2dAzF13yZA1VDM0sOp3IJyfbL4FxiZAg4UE49xJH1OSumSZBRSPWek53xhmdVvHlU3h53ZC8ptT2DWsDw385ptLTcYskfHexrRcDdLzbtDY6gs3ZCqyyIXIA5EZBvkJRIORz2n8ZCv39F7xyNzcyTw8uc8PsGhGS0DtNvK9pGCx0WQQVLGd95"

SIX_MONTHS_AGO = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")

COMPETITORS = {
    "Man Matters": [
        "Mars by GHC",
        "Traya Health",
        "Plix",
        "The Man Company",
        "Bold Care",
        "Beardo",
        "Bombay Shaving Company"
    ],
    "Be Bodywise": [
        "Chemist At Play",
        "Minimalist",
        "Pilgrim",
        "OZiva",
        "Wellbeing Nutrition",
        "Plix Life",
        "ThriveCo",
        "Bare Anatomy"
    ],
    "Little Joys": [
        "Gritzo",
        "BabyOrgano",
        "NutriBears",
        "Plix Kids",
        "Happi Kidz",
        "ChilRun",
        "Tikitoro"
    ]
}

# ============================================================
# STEP 1 — FIND PAGE IDs BY COMPETITOR NAME
# ============================================================

def get_page_id(brand_name):
    """Search Meta for a Page ID given a brand name."""
    url = "https://graph.facebook.com/v19.0/pages/search"
    params = {
        "q": brand_name,
        "access_token": ACCESS_TOKEN,
        "fields": "id,name,verification_status,fan_count"
    }
    try:
        response = requests.get(url, params=params)
        data = response.json()
        if "data" in data and len(data["data"]) > 0:
            # Return the first (most relevant) result
            page = data["data"][0]
            print(f"  ✓ Found: {page['name']} (ID: {page['id']})")
            return page["id"], page["name"]
        else:
            print(f"  ✗ Not found: {brand_name}")
            return None, None
    except Exception as e:
        print(f"  ✗ Error searching {brand_name}: {e}")
        return None, None


# ============================================================
# STEP 2 — FETCH ALL ADS FOR A PAGE ID (WITH PAGINATION)
# ============================================================

def fetch_ads_for_page(page_id, page_name, mosaic_brand):
    """Fetch all ads from the last 6 months for a given Page ID."""
    all_ads = []
    url = "https://graph.facebook.com/v19.0/ads_archive"
    params = {
        "access_token": ACCESS_TOKEN,
        "search_page_ids": page_id,
        "ad_reached_countries": "['IN']",
        "ad_active_status": "ALL",
        "ad_delivery_date_min": SIX_MONTHS_AGO,
        "fields": (
            "id,"
            "page_name,"
            "ad_creative_bodies,"
            "ad_creative_link_captions,"
            "ad_creative_link_titles,"
            "ad_delivery_start_time,"
            "ad_delivery_stop_time,"
            "ad_snapshot_url,"
            "publisher_platforms,"
            "impressions,"
            "spend,"
            "languages,"
            "ad_creative_link_descriptions"
        ),
        "limit": 25
    }

    page_count = 0
    while url:
        try:
            if page_count == 0:
                response = requests.get(url, params=params)
            else:
                response = requests.get(url)  # pagination URL already has params

            data = response.json()

            if "error" in data:
                print(f"    API Error: {data['error'].get('message', 'Unknown error')}")
                break

            ads = data.get("data", [])
            page_count += 1
            print(f"    Page {page_count}: {len(ads)} ads fetched")

            for ad in ads:
                # Calculate days running
                start = ad.get("ad_delivery_start_time", "")
                stop = ad.get("ad_delivery_stop_time", "")
                days_running = ""
                if start:
                    start_dt = datetime.strptime(start[:10], "%Y-%m-%d")
                    if stop:
                        stop_dt = datetime.strptime(stop[:10], "%Y-%m-%d")
                    else:
                        stop_dt = datetime.now()
                        stop = "Still Active"
                    days_running = (stop_dt - start_dt).days

                # Extract creative text
                bodies = ad.get("ad_creative_bodies", [])
                ad_copy = " | ".join(bodies) if bodies else ""

                captions = ad.get("ad_creative_link_captions", [])
                caption = " | ".join(captions) if captions else ""

                titles = ad.get("ad_creative_link_titles", [])
                headline = " | ".join(titles) if titles else ""

                descriptions = ad.get("ad_creative_link_descriptions", [])
                description = " | ".join(descriptions) if descriptions else ""

                platforms = ad.get("publisher_platforms", [])
                platforms_str = ", ".join(platforms) if platforms else ""

                # Spend proxy label
                if isinstance(days_running, int):
                    if days_running >= 30:
                        spend_signal = "High (30+ days)"
                    elif days_running >= 7:
                        spend_signal = "Medium (7–30 days)"
                    else:
                        spend_signal = "Low (<7 days)"
                else:
                    spend_signal = ""

                all_ads.append({
                    "mosaic_brand": mosaic_brand,
                    "competitor_name": page_name,
                    "ad_id": ad.get("id", ""),
                    "ad_copy": ad_copy,
                    "headline": headline,
                    "caption": caption,
                    "description": description,
                    "start_date": start[:10] if start else "",
                    "stop_date": stop[:10] if isinstance(stop, str) and len(stop) >= 10 else stop,
                    "days_running": days_running,
                    "spend_signal": spend_signal,
                    "platforms": platforms_str,
                    "ad_snapshot_url": ad.get("ad_snapshot_url", ""),
                    "status": "Active" if not ad.get("ad_delivery_stop_time") else "Inactive",
                    "messaging_theme": "",   # To be filled by AI layer later
                    "tone": "",              # To be filled by AI layer later
                    "core_claim": ""         # To be filled by AI layer later
                })

            # Handle pagination
            paging = data.get("paging", {})
            next_url = paging.get("next")
            url = next_url if next_url else None
            if url:
                time.sleep(0.5)  # Be respectful to the API

        except Exception as e:
            print(f"    Error fetching ads: {e}")
            break

    return all_ads


# ============================================================
# STEP 3 — SAVE TO CSV
# ============================================================

def save_to_csv(all_ads, mosaic_brand):
    """Save ads to a CSV file named after the Mosaic brand."""
    if not all_ads:
        print(f"  No ads to save for {mosaic_brand}")
        return

    filename = f"{mosaic_brand.replace(' ', '_')}_competitor_ads.csv"
    fieldnames = [
        "mosaic_brand", "competitor_name", "ad_id", "ad_copy",
        "headline", "caption", "description", "start_date", "stop_date",
        "days_running", "spend_signal", "platforms", "ad_snapshot_url",
        "status", "messaging_theme", "tone", "core_claim"
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_ads)

    print(f"\n  ✓ Saved {len(all_ads)} ads → {filename}")
    return filename


# ============================================================
# MAIN — RUN EVERYTHING
# ============================================================

def main():
    print("=" * 60)
    print("MOSAIC WELLNESS — COMPETITOR AD INTELLIGENCE SCRAPER")
    print(f"Fetching ads from {SIX_MONTHS_AGO} to today")
    print("=" * 60)

    all_files = []

    for mosaic_brand, competitor_names in COMPETITORS.items():
        print(f"\n{'='*60}")
        print(f"BRAND: {mosaic_brand}")
        print(f"{'='*60}")

        brand_ads = []

        for name in competitor_names:
            print(f"\nSearching for: {name}")
            page_id, found_name = get_page_id(name)
            time.sleep(0.3)  # avoid rate limiting

            if page_id:
                print(f"  Fetching ads for {found_name}...")
                ads = fetch_ads_for_page(page_id, found_name, mosaic_brand)
                print(f"  Total ads collected: {len(ads)}")
                brand_ads.extend(ads)
                time.sleep(0.5)

        filename = save_to_csv(brand_ads, mosaic_brand)
        if filename:
            all_files.append(filename)

    # Also save one master file with everything combined
    print(f"\n{'='*60}")
    print("Saving master combined file...")
    all_ads_combined = []
    for mosaic_brand, _ in COMPETITORS.items():
        filename = f"{mosaic_brand.replace(' ', '_')}_competitor_ads.csv"
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                all_ads_combined.extend(list(reader))

    if all_ads_combined:
        with open("MASTER_all_competitor_ads.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_ads_combined[0].keys())
            writer.writeheader()
            writer.writerows(all_ads_combined)
        print(f"✓ Master file saved: MASTER_all_competitor_ads.csv ({len(all_ads_combined)} total ads)")

    print(f"\n{'='*60}")
    print("DONE. Files created:")
    for f in all_files:
        print(f"  → {f}")
    print("  → MASTER_all_competitor_ads.csv")
    print("\nNext step: Upload CSVs to Google Sheets for dashboard use.")
    print("=" * 60)


if __name__ == "__main__":
    main()
