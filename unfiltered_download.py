import time
import json
import requests
import pandas as pd
import os
import gc
import datetime
import threading

# Shared variable to track total downloads
total_downloads = 0
download_lock = threading.Lock()

def exponential_backoff(retries, base_delay=10, max_delay=600):
    return min(base_delay * (2 ** retries), max_delay)

def normalize_data(post):
    flattened_post = {}
    for key, value in post.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                flattened_post[f"{key}_{subkey}"] = subvalue
        elif isinstance(value, list):
            flattened_post[key] = json.dumps(value)
        else:
            flattened_post[key] = value
    return flattened_post

def write_posts_to_csv(posts, base_csv_dir, flush_size=100):
    if not posts:
        return

    columns = [
        'platformId', 'platform', 'date', 'updated', 'type', 'message',
        'expandedLinks', 'link', 'postUrl', 'subscriberCount', 'score', 'media',
        'statistics_actual_likeCount', 'statistics_actual_shareCount',
        'statistics_actual_commentCount', 'statistics_actual_loveCount',
        'statistics_actual_wowCount', 'statistics_actual_hahaCount',
        'statistics_actual_sadCount', 'statistics_actual_angryCount',
        'statistics_actual_thankfulCount', 'statistics_actual_careCount',
        'statistics_expected_likeCount', 'statistics_expected_shareCount',
        'statistics_expected_commentCount', 'statistics_expected_loveCount',
        'statistics_expected_wowCount', 'statistics_expected_hahaCount',
        'statistics_expected_sadCount', 'statistics_expected_angryCount',
        'statistics_expected_thankfulCount', 'statistics_expected_careCount',
        'account_id', 'account_name', 'account_handle', 'account_profileImage',
        'account_subscriberCount', 'account_url', 'account_platform',
        'account_platformId', 'account_accountType',
        'account_pageAdminTopCountry', 'account_pageDescription',
        'account_pageCreatedDate', 'account_pageCategory', 'account_verified',
        'brandedContentSponsor_id', 'brandedContentSponsor_name',
        'brandedContentSponsor_handle', 'brandedContentSponsor_profileImage',
        'brandedContentSponsor_subscriberCount', 'brandedContentSponsor_url',
        'brandedContentSponsor_platform', 'brandedContentSponsor_platformId',
        'brandedContentSponsor_accountType',
        'brandedContentSponsor_pageDescription',
        'brandedContentSponsor_pageCreatedDate',
        'brandedContentSponsor_pageCategory', 'brandedContentSponsor_verified',
        'history', 'languageCode', 'legacyId', 'id'
    ]
    processed_posts = [normalize_data(post) for post in posts]
    df = pd.DataFrame(processed_posts, columns=columns)
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    grouped = df.groupby(df['date'].str[:10])
    for day, group in grouped:
        start_time = time.time()
        month_dir = os.path.join(base_csv_dir, day[:7])
        day_file_path = os.path.join(month_dir, f"{day}.csv")
        os.makedirs(month_dir, exist_ok=True)
        mode = 'a' if os.path.exists(day_file_path) else 'w'
        group.to_csv(day_file_path, mode=mode, header=mode=='w', index=False)
        end_time = time.time()
        duration = end_time - start_time

        if len(group) >= flush_size:
            print(f"Processed and flushed {len(group)} posts to disk for date: {group['date'].iloc[0]} in {duration:.2f} seconds")

    del df
    gc.collect()

def fetch_data(api_key, base_url, start_date, end_date, base_csv_dir, max_posts=100, max_retries=10):
    global total_downloads

    current_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    total_collected = 0

    while current_date < end_date:
        retries = 0
        offset = 0
        day_collected = 0
        start_date_str = current_date.strftime('%Y-%m-%d')
        next_date = current_date + datetime.timedelta(days=1)
        end_date_str = next_date.strftime('%Y-%m-%d')

        while True:
            print(f"Using token: {api_key} for date range: {start_date_str} to {end_date_str}")
            try:
                params = {
                    'token': api_key,
                    'startDate': start_date_str,
                    'endDate': end_date_str,
                    'count': max_posts,
                    'sortBy': 'date',
                    'language': 'en',
                    'includeHistory': True,
                    'pageAdminTopCountry': 'US',
                    'offset': offset,
                    'searchTerm': 'a'
                }
                response = requests.get(base_url, params=params)
                response.raise_for_status()
                data = response.json()

                if response.status_code == 429:
                    sleep_time = exponential_backoff(retries)
                    print(f"Rate limit exceeded. Retrying after {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    retries += 1
                    continue
                elif response.status_code == 504:
                    print(f"Server Timeout (504). Retrying after {exponential_backoff(retries)} seconds...")
                    time.sleep(exponential_backoff(retries))
                    retries += 1
                    continue
                elif response.status_code == 503:
                    sleep_time = exponential_backoff(retries)
                    print(f"Server unavailable (503). Retrying after {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    retries += 1
                    continue

                posts = data.get('result', {}).get('posts', [])
                if posts:
                    num_posts = len(posts)
                    total_collected += num_posts
                    day_collected += num_posts
                    with download_lock:
                        total_downloads += num_posts
                    print(f"Retrieved {num_posts} posts, Total collected: {total_collected}")
                    write_posts_to_csv(posts, base_csv_dir)
                    offset += num_posts
                    if num_posts < max_posts:
                        print("No more posts found or end of dataset reached for the day.")
                        break
                else:
                    print("No more posts found for the day.")
                    break

            except json.JSONDecodeError:
                print("Failed to parse JSON from response, retrying...")
                retries += 1
                continue
            except requests.RequestException as e:
                if retries < max_retries:
                    sleep_time = exponential_backoff(retries)
                    print(f"HTTP error: {e}. Retrying after {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    retries += 1
                else:
                    print("Max retries reached, aborting operation.")
                    break

            finally:
                if retries == 0:
                    time.sleep(10)

        print(f"Collected {day_collected} posts for date: {start_date_str}")
        current_date += datetime.timedelta(days=1)

def threaded_fetch(api_key, base_url, start_date, end_date, base_csv_dir):
    fetch_data(api_key, base_url, start_date, end_date, base_csv_dir)

def main_concurrent_fetch(api_keys, base_url, start_date, end_date, base_csv_dir):
    global total_downloads

    start_time = time.time()
    current_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    start_date_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')

    while current_date >= start_date_dt:
        threads = []
        for i, api_key in enumerate(api_keys):
            if current_date < start_date_dt:
                break
            day_end = current_date + datetime.timedelta(days=1)
            thread = threading.Thread(target=threaded_fetch, args=(api_key, base_url, current_date.strftime('%Y-%m-%d'), day_end.strftime('%Y-%m-%d'), base_csv_dir))
            threads.append(thread)
            current_date -= datetime.timedelta(days=1)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

    end_time = time.time()
    total_duration = end_time - start_time

    print(f"Total posts downloaded: {total_downloads}")
    print(f"Total time taken: {total_duration:.2f} seconds")

api_keys = [
    'GEfJJnVL2VCT0LvbdW3tMqPbowlNhuzCm5GDC87z', #8
    'XMU6j0ncTQeGoiYUfzHeOUedwpVXJJWfpvj6WmMl', #9
    'DljINxO3qvOseEfpl279grzaxLpquW7m9hnpVcbH', #10
    'HV1zEhoFWQzXOHm9IE7ORosEHdhK7L4pxCepjfjM', #11
    "KsssrhoYyml2A6cOXQpNhXwBe4oPSMUbohepV9fT" #12
]

base_url = 'https://api.crowdtangle.com/posts'
start_date = '2016-02-01'
end_date = '2020-12-31'
base_csv_dir = "crowdtangle_unfiltered_data"

main_concurrent_fetch(api_keys, base_url, start_date, end_date, base_csv_dir)


