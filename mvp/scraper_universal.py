import requests
from bs4 import BeautifulSoup
from newspaper import Article
import json
import time
import html
import re
from concurrent.futures import ThreadPoolExecutor


headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

def extract_date_from_archive_url(archive_url):
    """Extract date from antiwar.com archive URL
    Input: 'https://www.antiwar.com/past/20250601.html'
    Output: '20250601'
    """
    match = re.search(r'/(\d{8})\.html', archive_url)
    if match:
        year, month, day = match.group(1)[:4], match.group(1)[4:6], match.group(1)[6:8]
        return f"{year}-{month}-{day}"
    return None

def get_all_article_urls(archive_url):
    """Get all article URLs from antiwar.com archive page
    Input: 'https://www.antiwar.com/past/20250601.html'
    Output: ['https://www.aljazeera.com/...', 'https://www.bbc.com/...', ...]
    """
    
    r = requests.get(archive_url, headers=headers)
    if r.status_code == 200:
        soup = BeautifulSoup(r.content, 'html.parser')
        article_links = []
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # Skip obvious non-article links
            if any(skip in href for skip in [
                # Internal antiwar.com navigation
                'antiwar.com/who.php', 'antiwar.com/search', 'antiwar.com/contact',
                'antiwar.com/donate', 'antiwar.com/latest.php',
                'antiwar.com/viewpoints.php', 'antiwar.com/regions', 
                'antiwar.com/shops.php', 'antiwar.com/privacy.php',
                'antiwar.com/casualties/', 'antiwar.com/syndication.php', 'antiwar.com/submissions.php',
                'antiwar.com/reprint.php', 'antiwar.com/doverimages/', 'antiwar.com/newsletter/',
                
                # Author pages and navigation
                '/author/', '/columnists/', 'scotthorton.org',
                
                # Archive and redirect links
                'archive.ph/', 'archive.is/',
                
                # Technical/social stuff
                'javascript:', 'mailto:', '#', 'twitter.com', 'youtube.com', 'facebook.com',
                'instagram.com', 'linkedin.com', 'telegram.org',
                
                # Ads and trackers
                'amazon-adsystem.com', 'googletagservices.com', 'google.com/ads',
                
                # Other non-news
                'randolphbourne.org'
            ]):
                continue

            # Skip home pages and root domains (but keep specific articles)
            if href.rstrip('/') in ['https://www.antiwar.com', 'https://antiwar.com', 
                                    'https://original.antiwar.com', 'https://news.antiwar.com',
                                    'https://www.antiwar.com/blog', 'https://antiwar.com/blog']:
                continue
                
            # Must be a full HTTP URL
            if not href.startswith('http'):
                continue
                
            # Include everything else
            article_links.append(href)
        
        # Remove duplicates
        article_links = list(set(article_links))
        print(f"Found {len(article_links)} URLs after filtering")
        return article_links
    else:
        print(f"Failed to get archive: {r.status_code}")
        return []
    

def scrape_single_article(url):
    """Scrape one article using newspaper3k - this runs in each thread
    Input: 'https://www.aljazeera.com/news/...'
    Output: {'url': '...', 'title': '...', 'authors': [...], 'content': '...', 'scrape_status': 'success'}
    """
    try:
        article = Article(url)
        article.download()
        article.parse()
        
        # Clean the content
        content = article.text
        if content:
            # Remove excessive whitespace and newlines
            content = ' '.join(content.split())
            
            # Remove common editor notes (case insensitive)
            editor_patterns = [
                "Editor's note:", "Editor's Note:", "EDITOR'S NOTE:",
                "Editorial note:", "Editorial Note:", "EDITORIAL NOTE:",
                "Note to readers:", "Note to Readers:", "NOTE TO READERS:",
                "Disclaimer:", "DISCLAIMER:",
                "This story was updated", "This article was updated",
                "Updated at", "Last updated"
            ]
            
            for pattern in editor_patterns:
                # Find the pattern and remove everything from that point to the next sentence
                if pattern.lower() in content.lower():
                    start_idx = content.lower().find(pattern.lower())
                    # Find the end of the sentence (next period + space or end of string)
                    end_idx = content.find('. ', start_idx)
                    if end_idx == -1:
                        end_idx = len(content)
                    else:
                        end_idx += 1  # Include the period
                    
                    content = content[:start_idx] + content[end_idx:].strip()
                    break  # Only remove the first occurrence
        
        return {
            'url': url,
            'title': article.title.strip() if article.title else None,
            'authors': article.authors if article.authors else [],
            'content': content.strip() if content else None,
            'scrape_status': 'success'
        }
    except Exception as e:
        return {
            'url': url,
            'title': None,
            'authors': [],
            'content': None,
            'scrape_status': f'failed: {str(e)}'
        }

def clean_scraped_content(text, authors):
    """Clean content - will add more fixes as we find issues"""
    
    if text:
        text = html.unescape(text)
        text = text.replace("\\'", "'").replace('\\"', '"')
        text = text.replace("\\n", " ").replace("\\r", " ")  # Fix newline escapes
        text = ' '.join(text.split())  # Clean whitespace
    
    if authors:
        # Improved author cleaning
        clean_authors = []
        for author in authors:
            if author and len(author) < 30:  # Shorter limit
                author = html.unescape(author.strip())
                # Skip obvious non-names
                skip_words = ['reporter', 'producer', 'served', 'worked', 'covering', 'department', 'is a', 'has been', 'where he', 'also']
                if not any(word in author.lower() for word in skip_words):
                    clean_authors.append(author)
        authors = clean_authors[:2]  # Max 2 real authors
    
    return text, authors

def scrape_all_articles_threaded(urls):
    """Scrape multiple articles using threading
    Input: ['url1', 'url2', 'url3', ...]
    Output: [{'url': 'url1', 'title': '...', ...}, {'url': 'url2', ...}, ...]
    """
    print(f"Starting to scrape {len(urls)} articles with 5 threads...")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        articles = list(executor.map(scrape_single_article, urls))
    
    return articles

def calculate_metrics(articles, total_time):
    """Calculate scraping metrics
    Input: articles list + total_time in seconds
    Output: {'total_articles_found': 110, 'successful_scrapes': 95, ...}
    """
    successful = [a for a in articles if a['scrape_status'] == 'success']
    failed = [a for a in articles if a['scrape_status'] != 'success']
    
    # Calculate total data size (in MB)
    total_chars = 0
    for article in successful:
        if article['content']:
            total_chars += len(article['content'])
        if article['title']:
            total_chars += len(article['title'])
        if article['authors']:
            total_chars += sum(len(str(author)) for author in article['authors'])
    
    # Rough conversion: 1 character ≈ 1 byte, 1MB = 1,048,576 bytes
    total_data_mb = total_chars / (1024 * 1024)
    
    return {
        'total_articles_found': len(articles),
        'successful_scrapes': len(successful),
        'failed_scrapes': len(failed),
        'success_rate_percent': round((len(successful) / len(articles)) * 100, 2) if articles else 0,
        'total_scraping_time_seconds': round(total_time, 2),
        'average_time_per_article': round(total_time / len(articles), 2) if articles else 0,
        'total_data_size_mb': round(total_data_mb, 3),
        'average_article_size_chars': round(total_chars / len(successful), 0) if successful else 0
    }

def save_to_json(data, filename):
    """Save data to JSON file"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Data saved to: {filename}")

def create_final_json_structure(archive_url, articles):
    """Create the final JSON structure - no metrics"""
    date = extract_date_from_archive_url(archive_url)
    
    return {
        "archive_url": archive_url,
        "date": date,
        "articles": articles
    }

def scrape_full_archive_page(archive_url):
    """Main function - scrape everything from one archive page
    Input: 'https://www.antiwar.com/past/20250601.html'
    Output: Complete JSON with all articles + metrics
    """
    print(f"Starting to scrape archive page: {archive_url}")
    start_time = time.time()
    
    # Step 1: Extract date
    date = extract_date_from_archive_url(archive_url)
    print(f"Archive date: {date}")
    
    # Step 2: Get all URLs
    print("Getting all article URLs...")
    all_urls = get_all_article_urls(archive_url)
    
    if not all_urls:
        print("No URLs found! Exiting.")
        return None
    
    # Step 3: Scrape all articles with threading
    articles = scrape_all_articles_threaded(all_urls)
    
    # Step 4: Calculate metrics (for display only)
    end_time = time.time()
    total_time = end_time - start_time
    metrics = calculate_metrics(articles, total_time)

    # NEW: Show failed articles
    show_failed_articles(articles)
    
    # Step 5: Create final JSON structure
    final_data = create_final_json_structure(archive_url, articles)
    
    # Step 6: Save to file
    filename = f"scraped_{date}.json"
    save_to_json(final_data, filename)
    
    # Display metrics to user
    print(f"\nFINAL METRICS:")
    print(f"==============")
    for key, value in metrics.items():
        print(f"{key}: {value}")
    
    return final_data
    
def show_failed_articles(articles):
    """Show which articles failed and why"""
    failed = [a for a in articles if a['scrape_status'] != 'success']
    
    if failed:
        print(f"\nFAILED ARTICLES ({len(failed)} total):")
        print("=" * 50)
        for article in failed:
            print(f"URL: {article['url']}")
            print(f"Error: {article['scrape_status']}")
            print("-" * 30)
    else:
        print("All articles scraped successfully!")
    
if __name__ == "__main__":
    # Test the complete pipeline
    archive_url = "https://www.antiwar.com/past/20250601.html"
    
    result = scrape_full_archive_page(archive_url)
    
    if result:
        print(f"\nSUCCESS! Scraped {len(result['articles'])} articles")
        print(f"Saved to: scraped_{result['date']}.json")
    else:
        print("FAILED to scrape archive page")
