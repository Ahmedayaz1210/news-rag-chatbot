import requests
from bs4 import BeautifulSoup
import json
import os
import time

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
        
        # Print HTML structure to understand the page layout
        print("=== SAVING HTML STRUCTURE TO FILE ===")
        with open('page_structure.html', 'w', encoding='utf-8') as f:
            f.write(soup.prettify())
        print("HTML structure saved to 'page_structure.html'")
        
        # Also print table structure specifically
        print("=== TABLE STRUCTURE ANALYSIS ===")
        tables = soup.find_all('table')
        print(f"Found {len(tables)} tables on the page")
        
        for i, table in enumerate(tables):
            tds = table.find_all('td')
            print(f"Table {i}: {len(tds)} TD elements")
            if len(tds) >= 3:  # If it has 3+ columns (left, middle, right)
                print(f"  - This might be the main layout table")
                for j, td in enumerate(tds[:3]):  # Look at first 3 TDs
                    links_in_td = td.find_all('a', href=True)
                    news_links = [link['href'] for link in links_in_td if 'news.antiwar.com' in link.get('href', '')]
                    print(f"    TD {j}: {len(links_in_td)} total links, {len(news_links)} news links")
        
        # Your original link extraction (for now)
        article_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith('https://news.antiwar.com/2025/'):
                article_links.append(href)
        
        # Remove duplicates
        article_links = list(set(article_links))
        print(f"Total unique article links found: {len(article_links)}")
        return article_links
    else:
        print(f"Failed to get archive: {r.status_code}")
        return []
    

# Just run the archive analysis for now - don't scrape articles yet
archive_url = f"https://www.antiwar.com/past/20250601.html"
print(f"Analyzing archive structure: {archive_url}")

article_urls = get_article_urls_from_archive(archive_url)
print("\nFound these article URLs:")
for url in article_urls:
    print(f"  - {url}")

print(f"\nAnalysis complete. Check 'page_structure.html' file for full HTML structure.")
print("Run this script to understand the page layout, then we'll improve the link extraction.")