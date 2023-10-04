
import asyncio
from pyppeteer import launch
import requests
import urllib.parse
import os

HUBSPOT_API_KEY = os.environ.get('HUBSPOT_API_KEY')  # Assuming the API key is stored as an environment variable
    
def is_login_redirect(url):
    parsed = urllib.parse.urlparse(url)
    if 'sign_in' in parsed.path and 'destination' in parsed.query:
        return True
    return False

async def get_crawl_delay_from_robots_txt(base_url):
    try:
        response = requests.get(f"{base_url}/robots.txt")
        if response.status_code == 200:
            lines = response.text.splitlines()
            for line in lines:
                if "Crawl-delay" in line:
                    return int(line.split(":")[1].strip())
    except Exception as e:
        print(f"Error fetching or parsing robots.txt: {e}")
    return None

async def get_all_links(page):
    try:
        links = await page.querySelectorAllEval('a[href]', '(elements) => elements.map(a => a.href)')
        return set(links)
    except Exception as e:
        print(f"Error fetching links: {e}")
        return set()

async def count_internal_pages(links, base_url):
    return len([link for link in links if base_url in link])

def strip_url_fragment(url):
    return url.split('#')[0]

async def count_files_and_images(page):
    try:
        images = await page.querySelectorAllEval('img[src]', '(elements) => elements.map(img => img.src)')
        file_extensions = ['.pdf', '.doc', '.docx', '.zip', '.rar', '.ppt', '.pptx', '.xls', '.xlsx', '.csv']
        files = await page.querySelectorAllEval('a[href]', '(elements) => elements.map(a => a.href)')
        files = [link for link in files if any(ext in link for ext in file_extensions)]
        return files, images
    except Exception as e:
        print(f"Error fetching files and images: {e}")
        return [], []

async def crawl_website(url, visited_urls=set()):
    browser = await launch(headless=True)
    page = await browser.newPage()
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

    crawl_delay = await get_crawl_delay_from_robots_txt(url)
    if crawl_delay is None:
        crawl_delay = 1

    pages_set = set()
    files_set = set()

    urls_to_crawl = {url}
    
    while urls_to_crawl:
        if len(pages_set) + len(files_set) > 200:
            break

        current_url = strip_url_fragment(urls_to_crawl.pop())

        if is_login_redirect(current_url):
            continue

        if current_url in visited_urls:
            continue

        file_extensions = ['.pdf', '.doc', '.docx', '.zip', '.rar', '.ppt', '.pptx', 
                   '.xls', '.xlsx', '.csv', '.jpg', '.jpeg', '.png', '.gif',
                   '.mp3', '.wav', '.ogg', '.m4a', '.mp4', '.avi', '.mov', '.mkv']
        if any(ext in current_url for ext in file_extensions):
            files_set.add(current_url)
            continue
        
        visited_urls.add(current_url)
        pages_set.add(current_url)
        
        try:

            response = await page.goto(current_url)
            await asyncio.sleep(crawl_delay)
            if not response:
                print(f"Failed to get a response from {current_url}. Skipping.")
                continue

            if response.status != 200:
                print(f"Skipping {current_url} due to status code {response.status}")
                continue

            links = await get_all_links(page)
            internal_links = [strip_url_fragment(link) for link in links if url in link]
            urls_to_crawl.update(internal_links)
            files_on_page, images_on_page = await count_files_and_images(page)
            files_set.update(files_on_page + images_on_page)
        except Exception as e:
            print(f"Error processing URL {current_url}: {e}")
    
    await browser.close()
    return pages_set, files_set

def recommended_pricing_package(total_count):
    if total_count < 50:
        return "Core Setup"
    elif total_count < 200:
        return "Classic Setup"
    else:
        return "Complete+ Setup"

def fetch_current_site_url_from_hubspot(deal_id):
    endpoint = f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Accept": "application/json"
    }
    try:
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            deal_data = response.json()
            current_site_url = deal_data['properties']['Current site']
            return current_site_url
        else:
            print(f"Error fetching deal data from HubSpot: {response.text}")
            return None
    except Exception as e:
        print(f"Error fetching deal data: {e}")
        return None

def update_hubspot_with_recommended_package(deal_id, package):
    endpoint = f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    data = {
        "properties": {
            "Recommended Migration Package": package
        }
    }
    try:
        response = requests.patch(endpoint, headers=headers, json=data)
        if response.status_code != 200:
            print(f"Error updating HubSpot deal: {response.text}")
    except Exception as e:
        print(f"Error updating HubSpot deal: {e}")

def lambda_handler(event, context):
    # Extract deal_id from the incoming event
    deal_id = event.get('deal_id')
    if not deal_id:
        return {
            'statusCode': 400,
            'body': 'deal_id not provided'
        }

    # Fetch the 'Current site' URL from HubSpot
    url = fetch_current_site_url_from_hubspot(deal_id)
    if not url:
        return {
            'statusCode': 400,
            'body': 'Could not fetch Current site URL from HubSpot'
        }

    # Crawl the website and get the sets of pages and files
    try:
        pages, files = asyncio.run(crawl_website(url))
        total_count = len(pages) + len(files)
    except Exception as e:
        print(f"Error during website crawling: {e}")
        return {
            'statusCode': 500,
            'body': 'Error during website crawling or accessing the URL.'
        }
    
    # If total count is under 5, return a specific message
    if total_count < 5:
        return {
            'statusCode': 200,
            'body': {
                'message': 'Total count is below threshold or website inaccessible. Manual verification required.'
            }
        }
    
    # Determine the recommended package based on the total count
    package = recommended_pricing_package(total_count)

    # Update the HubSpot deal with the recommended package
    update_hubspot_with_recommended_package(deal_id, package)

    return {
        'statusCode': 200,
        'body': {
            'Recommended Migration Package': package
        }
    }