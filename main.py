import requests
import csv
import time
import os
from datetime import datetime, timedelta
from supabase import create_client

supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SECRET_KEY"))

ACCESS_TOKEN = os.environ.get("META_TOKEN")
FOUR_YEARS_AGO = datetime.now() - timedelta(days=1460)

COMPETITORS = {
    "Man_Matters": [
        {"name": "Mars by GHC", "page_id": "168479659686677"},
        {"name": "Traya Health", "page_id": "1067448693440461"},
        {"name": "Plix", "page_id": "444025482768886"},
        {"name": "The Man Company", "page_id": "631488936979872"},
        {"name": "Bold Care", "page_id": "104897991742825"},
        {"name": "Beardo", "page_id": "1642692419320655"},
        {"name": "Bombay Shaving Company", "page_id": "1737772143174400"},
    ],
    "Be_Bodywise": [
        {"name": "Chemist At Play", "page_id": "104769401793203"},
        {"name": "Minimalist", "page_id": "498594610347001"},
        {"name": "Pilgrim", "page_id": "111982120196545"},
        {"name": "OZiva", "page_id": "603903383105760"},
        {"name": "Wellbeing Nutrition", "page_id": "107959920549143"},
        {"name": "Plix Life", "page_id": "444025482768886"},
        {"name": "ThriveCo", "page_id": "100622398157395"},
        {"name": "Bare Anatomy", "page_id": "244335642891847"},
    ],
    "Little_Joys": [
        {"name": "Gritzo", "page_id": "177588558775715"},
        {"name": "BabyOrgano", "page_id": "553299865139117"},
        {"name": "Plix Kids", "page_id": "444025482768886"},
        {"name": "Happi Kidz Gummies", "page_id": "640242872713297"},
        {"name": "Tikitoro", "page_id": "109025094618552"},
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

                if start:
                    start_dt = datetime.strptime(start[:10], "%Y-%m-%d")
                    if start_dt < FOUR_YEARS_AGO:
                        continue

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

    try:
        # Deduplicate by ad_id before pushing to Supabase
        seen = set()
        unique_ads = []
        for ad in ads:
            if ad["ad_id"] not in seen:
                seen.add(ad["ad_id"])
                unique_ads.append(ad)
        supabase.table("competitor_ads").upsert(unique_ads, on_conflict="ad_id").execute()
        print(f"  Pushed {len(unique_ads)} ads to Supabase")
    except Exception as e:
        print(f"  Supabase error: {e}")

def main():
    print("=" * 60)
    print("MOSAIC WELLNESS - COMPETITOR AD INTELLIGENCE SCRAPER")
    print(f"Date range: last 4 years")
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
