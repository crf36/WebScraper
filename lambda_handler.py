import json
from main_scraper import run_main_scraper

def handler(event, context):
    city = event.get("city")
    
    if not city:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "No city provided"})
        }
    
    try:
        run_main_scraper(city)
        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Scraper complete for {city}"})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }