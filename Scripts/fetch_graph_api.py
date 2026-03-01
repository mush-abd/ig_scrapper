# import requests
# import config
# import json

# def fetch_business_discovery(target_username):
#     """
#     Fetches Instagram business metrics for a specific handle.
#     """
#     # Pull values from config.py
#     user_id = config.INSTAGRAM_ACCOUNT_ID
#     token = config.ACCESS_TOKEN
    
#     url = f"https://graph.facebook.com/v25.0/{user_id}"
    
#     params = {
#         'fields': f'business_discovery.username({target_username}){{followers_count,media_count,media}}',
#         'access_token': token
#     }

#     try:
#         response = requests.get(url, params=params)
#         response.raise_for_status()
#         return response.json()

#     except requests.exceptions.RequestException as e:
#         print(f"Error fetching data: {e}")
#         return None

# if __name__ == "__main__":
#     # You can now easily swap the target handle here
#     handle = "mush.writes"
#     data = fetch_business_discovery(handle)
    
#     if data:
#         print(f"Successfully fetched data for @{handle}:")
#         print(json.dumps(data, indent=4))

import requests
import config
import json

def fetch_instagram_profile():
    # Use the specific ID from your curl command
    # Or replace with config.INSTAGRAM_ACCOUNT_ID if that is the same ID
    user_id = config.INSTAGRAM_ACCOUNT_ID
    
    url = f"https://graph.facebook.com/v25.0/{user_id}"
    
    # Define the fields you want to retrieve
    params = {
        'fields': 'biography,id,username,website',
        'access_token': config.ACCESS_TOKEN
    }

    try:
        response = requests.get(url, params=params)
        
        # Raise an error for 4xx/5xx responses
        response.raise_for_status()
        
        data = response.json()
        return data

    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
        print(f"Response: {response.json()}")
    except Exception as err:
        print(f"An unexpected error occurred: {err}")

if __name__ == "__main__":
    profile_data = fetch_instagram_profile()
    
    if profile_data:
        print(json.dumps(profile_data, indent=4))