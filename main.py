import requests
import csv
import time
import os
from datetime import datetime, timedelta

ACCESS_TOKEN = os.environ.get("META_TOKEN")
SIX_MONTHS_AGO = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")

COMPETITORS = {
    "Man_Matters": [
        {"name": "Mars by GHC", "page_id": "61554821668151"},
        {"name": "Traya Health", "page_id": "100064028731127"},
        {"name": "Plix", "page_id": "100068172912258"},
        {"name": "The Man Company", "page_id": "100069249150594"},
        {"name": "Bold Care", "page_id": "100063980497701"},
        {"name": "Beardo", "page_id": "100064406093330"},
        {"name": "Bombay Shaving Company", "page_id": "100064833274810"},
    ],
    "Be_Bodywise": [
        {"name": "Chemist At Play", "page_id": "100068200141112"},
        {"name": "Minimalist", "page_id": "100064022683090"},
        {"name": "Pilgrim", "page_id": "100064155150428"},
        {"name": "OZiva", "page_id": "100064855864058"},
        {"name": "Wellbeing Nutrition", "page_id": "100063856003261"},
        {"name": "Plix Life", "page_id": "100068172912258"},
        {"name": "ThriveCo", "page_id": "100063755334355"},
        {"name": "Bare Anatomy", "page_id": "100063852180255"},
    ],
    "Little_Joys": [
        {"name": "Gritzo", "page_id": "61554271873373"},
        {"name": "BabyOrgano", "page_id": "100063684084326"},
        {"name": "NutriBears", "page_id": "100063725419286"},
        {"name": "Plix Kids", "page_id": "100068172912258"},
        {"name": "Happi Kidz", "page_id": "100070756422318"},
        {"name": "ChilRun", "page_id": "61566743875524"},
        {"name": "Tikitoro", "page_id": "100075914183705"},
    ]
}

def fetch_ads_for_page(page_id, page_name, mosaic_brand):
    all_ads = []
    url = "https://graph.facebook.com/v19.0/ads_archive"
    params = {
        "access_token": ACCESS_TOKEN,
        "search_page_ids": page_id,
        "ad_reached_countries": "['IN']",
        "ad_active_status": "ALL",
        "ad_delivery_date_min": SIX_MONTHS_AGO,
        "fields": "id,page_name,ad_creative_bodies,ad_creative_link_captions,ad_creative_link_titles,ad_creative_link_descriptions,ad_delivery_start_time,ad_delivery_stop_time,ad_snapshot_url,publisher_platforms",
        "limit": 25
    }
    page_count = 0
    current_url = url
    current_params = params

    while current_url:
        try:
            if page_count == 0:
                response = requests.get(current_url, params=current_params)
            else:
                response = requests.get(current_url)
            data = response.json()
            if "error" in data:
                print(f"    API Error: {data['error'].get('message', 'Unknown')}")
                break
            ads = data.get("data", [])
            page_count += 1
            print(f"    Page {page_count}: {len(ads)} ads")
            for ad in ads:
                start = ad.get("ad_delivery_start_time", "")
                stop = ad.get("ad_delivery_stop_time", "")
                days_running = ""
                status = "Active"
                if start:
                    start_dt = datetime.strptime(start[:10], "%Y-%m-%d")
                    if stop:
                        stop_dt = datetime.strptime(stop[:10], "%Y-%m-%d")
                        status = "Inactive"
                    else:
                        stop_dt = datetime.now()
                    days_running = (stop_dt - start_dt).days
                if isinstance(days_running, int):
                    if days_running >= 30:
                        spend_signal = "High (30+ days)"
                    elif days_running >= 7:
                        spend_signal = "Medium (7-30 days)"
                    else:
                        spend_signal = "Low (<7 days)"
                else:
                    spend_signal = ""
                bodies = ad.get("ad_creative_bodies", [])
                titles = ad.get("ad_creative_link_titles", [])
                captions = ad.get("ad_creative_link_captions", [])
                descriptions = ad.get("ad_creative_link_descriptions", [])
                platforms = ad.get("publisher_platforms", [])
                all_ads.append({
                    "mosaic_brand": mosaic_brand,
                    "competitor_name": page_name,
                    "ad_id": ad.get("id", ""),
                    "ad_copy": " | ".join(bodies) if bodies else "",
                    "headline": " | ".join(titles) if titles else "",
                    "caption": " | ".join(captions) if captions else "",
                    "description": " | ".join(descriptions) if descriptions else "",
                    "start_date": start[:10] if start else "",
                    "stop_date": stop[:10] if stop else "Still Active",
                    "days_running": days_running,
                    "spend_signal": spend_signal,
                    "status": status,
                    "platforms": ", ".join(platforms) if platforms else "",
                    "ad_snapshot_url": ad.get("ad_snapshot_url", ""),
                    "messaging_theme": "",
                    "tone": "",
                    "core_claim": ""
                })
            next_url = data.get("paging", {}).get("next")
            current_url = next_url if next_url else None
            current_params = {}
            if current_url:
                time.sleep(0.5)
        except Exception as e:
            print(f"    Exception: {e}")
            break
    return all_ads

def save_to_csv(ads, filename):
    if not ads:
        print(f"  No ads to save -> {filename}")
        return
    fieldnames = ["mosaic_brand", "competitor_name", "ad_id", "ad_copy", "headline", "caption", "description", "start_date", "stop_date", "days_running", "spend_signal", "status", "platforms", "ad_snapshot_url", "messaging_theme", "tone", "core_claim"]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(ads)
    print(f"  Saved {len(ads)} ads -> {filename}")

def main():
    print("=" * 60)
    print("MOSAIC WELLNESS - COMPETITOR AD INTELLIGENCE SCRAPER")
    print(f"Date range: {SIX_MONTHS_AGO} to today")
    print("=" * 60)
    all_ads_combined = []
    for mosaic_brand, competitors in COMPETITORS.items():
        print(f"\nBRAND GROUP: {mosaic_brand}")
        brand_ads = []
        for comp in competitors:
            print(f"\n-> Fetching: {comp['name']} (ID: {comp['page_id']})")
            ads = fetch_ads_for_page(comp["page_id"], comp["name"], mosaic_brand)
            print(f"  Total: {len(ads)} ads collected")
            brand_ads.extend(ads)
            time.sleep(0.5)
        save_to_csv(brand_ads, f"{mosaic_brand}_competitor_ads.csv")
        all_ads_combined.extend(brand_ads)
    save_to_csv(all_ads_combined, "MASTER_all_competitor_ads.csv")
    print(f"\nCOMPLETE. Total ads: {len(all_ads_combined)}")

if __name__ == "__main__":
    main()
