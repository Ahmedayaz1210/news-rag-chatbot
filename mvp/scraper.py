import requests
from bs4 import BeautifulSoup
import json
import os
from urllib.parse import urlparse

def initialize_json_file(filename="scraped_data.json"):
    """Initialize the JSON file if it doesn't exist"""
    if not os.path.exists(filename):
        initial_data = {
            "articles": [],
            "metadata": {
                "total_articles": 0,
                "scraped_date": "",
                "date_range": ""
            }
        }
        with open(filename, 'w') as f:
            json.dump(initial_data, f, indent=2)
        print(f"Created {filename}")
    else:
        print(f"{filename} already exists")


def add_article_to_json(article_data, filename="scraped_data.json"):
    """Add a single article to the JSON file"""
    with open(filename, 'r') as f:
        data = json.load(f)
    
    # Add the article
    data["articles"].append(article_data)
    data["metadata"]["total_articles"] = len(data["articles"])
    
    # Save back to file
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Added article: {article_data['title'][:50]}...")

def scrape_article(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Making a GET request
    r = requests.get(url, headers=headers)
    
    # check status code for response received
    # success code - 200
    if r.status_code == 200:
        soup = BeautifulSoup(r.content, 'html.parser')
        s = soup.find('div', class_='site-main')
        title_header = soup.find('header', class_='entry-header')
        title_footer = soup.find('footer', class_='entry-footer')
        author_name = title_footer.find('span', class_='author vcard')
        date_time_published = title_footer.find('span', class_='posted-on')
        news_category = title_footer.find('span', class_='tags-links')
        news_category = news_category.find_all('a') if news_category else None
        content = []
        for p in soup.find_all('p'):
            # Stop when we hit author info
            if p.find_parent('div', class_='author-info'):
                break
            content.append(p.text)
        return {
            'title': title_header.h1.text if title_header else 'No Title',
            'content': ' '.join(content),
            'author': author_name.text.strip().replace("by ", '') if author_name else 'Unknown Author',
            'date_time_published': date_time_published.text.strip() if date_time_published else 'Unknown Date',
            'news_category': ', '.join([a.text for a in news_category]) if news_category else 'Uncategorized',
            'url': url
        }
    else:
        print(f"Failed to retrieve article: {r.status_code}")
        return None
    

def get_article_urls_from_archive(archive_url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    r = requests.get(archive_url, headers=headers)
    if r.status_code == 200:
        soup = BeautifulSoup(r.content, 'html.parser')
        
        # Get ALL article links - look for links that appear to be news articles
        article_links = []
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # Skip internal navigation and non-article links
            if any(skip in href for skip in [
                'antiwar.com/who.php', 'antiwar.com/search', 'antiwar.com/contact',
                'antiwar.com/donate', 'antiwar.com/blog/', 'antiwar.com/latest.php',
                'antiwar.com/viewpoints.php', 'antiwar.com/regions', 'antiwar.com/past',
                'javascript:', 'mailto:', '#', 'twitter.com', 'youtube.com',
                'amazon-adsystem.com', 'googletagservices.com', 'randolphbourne.org'
            ]):
                continue
                
            # Look for actual article URLs - these usually have dates or news content
            if any(indicator in href for indicator in [
                '/2024/', '/2025/', 'news.', 'article', '/news/', '/politics/',
                '/world/', '/international/', '/war/', '/conflict/', '/middle-east/',
                'aljazeera.com', 'middleeasteye.net', 'theamericanconservative.com',
                'dropsitenews.com', 'mondoweiss.net', 'thecradle.co', 'newlinesmag.com',
                'libertarianinstitute.org', 'original.antiwar.com'
            ]):
                # Make sure it's a full URL
                if href.startswith('http'):
                    article_links.append(href)
        
        # Remove duplicates and return
        article_links = list(set(article_links))
        return article_links
    else:
        print(f"Failed to get archive: {r.status_code}")
        return []


# Test the updated function
archive_url = "https://www.antiwar.com/past/20250601.html"
print(f"Getting ALL article URLs from: {archive_url}")

article_urls = get_article_urls_from_archive(archive_url)

print(f"\nFound {len(article_urls)} total article links:")
print("="*50)

# Group by domain for better visualization

url_by_domain = {}
for url in article_urls:
    domain = urlparse(url).netloc
    if domain not in url_by_domain:
        url_by_domain[domain] = []
    url_by_domain[domain].append(url)

# Print grouped by domain
for domain, urls in url_by_domain.items():
    print(f"\n{domain} ({len(urls)} articles):")
    for url in urls[:5]:  # Show first 5 from each domain
        print(f"  - {url}")
    if len(urls) > 5:
        print(f"  ... and {len(urls) - 5} more")

print(f"\nTotal articles found: {len(article_urls)}")
print("External domains found:", len(url_by_domain))