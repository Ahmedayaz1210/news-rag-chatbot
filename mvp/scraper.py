"""
News Article Scraper with Checkpoint System and Section Mapping
===============================================================

A robust web scraper that extracts articles from antiwar.com daily archives
with crash recovery, progress tracking, and automatic section categorization.

Features:
- Multi-threaded scraping for performance
- Checkpoint system (saves progress every 20 articles)
- Automatic crash recovery and resume functionality
- Section header extraction and article categorization
- Clean JSON output with progress tracking
- Comprehensive error handling and reporting

Author: Ahmed Ayaz
Date: June 2025
"""

import requests
from bs4 import BeautifulSoup
from newspaper import Article
import json
import time
import html
import re
from concurrent.futures import ThreadPoolExecutor
import os

# Global configuration
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
CHUNK_SIZE = 20  # Save progress every N articles
MAX_WORKERS = 5  # Number of concurrent threads

START_DATE = "2025-02-25"  # Change this
END_DATE = "2025-02-28"    # Change this

def date_to_archive_url(date):
    """Convert YYYY-MM-DD to https://www.antiwar.com/past/YYYYMMDD.html"""
    return f"https://www.antiwar.com/past/{date.replace('-', '')}.html"

def create_year_directory(year):
    """Create data/YEAR/ directory"""
    year_dir = os.path.join('..','data', str(year))
    os.makedirs(year_dir, exist_ok=True)
    return year_dir

def extract_date_from_archive_url(archive_url):
    """
    Extract date from antiwar.com archive URL.
    
    Args:
        archive_url (str): URL like 'https://www.antiwar.com/past/20250601.html'
        
    Returns:
        str: Formatted date like '2025-06-01' or None if invalid
    """
    match = re.search(r'/(\d{8})\.html', archive_url)
    if match:
        year, month, day = match.group(1)[:4], match.group(1)[4:6], match.group(1)[6:8]
        return f"{year}-{month}-{day}"
    return None



def extract_section_mappings_from_soup(soup, all_urls):
    """
    Extract section mappings from existing BeautifulSoup object.
    This avoids making an extra HTTP request by reusing the soup from get_all_article_urls().
    
    Args:
        soup (BeautifulSoup): Already parsed HTML soup object
        all_urls (list): List of all article URLs found on the page
        
    Returns:
        dict: Dictionary mapping URLs to list of sections {url: [section1, section2, ...]}
    """
    url_to_sections = {}
    
    # 1. Handle main news section (appears before any hotspots)
    main_news_table = soup.find('table', {'bgcolor': '#F3F5F6'})
    if main_news_table:
        main_news_links = main_news_table.find_all('a', href=True)
        main_news_count = 0
        
        for link in main_news_links:
            href = link['href']
            if href in all_urls and href not in url_to_sections:
                url_to_sections[href] = set(['News'])
                main_news_count += 1
        
        print(f"Found {main_news_count} articles in main news section")
    
    # 2. Handle Viewpoints section (uses class="border2")
    viewpoints_headers = soup.find_all('td', class_='border2')
    for header in viewpoints_headers:
        header_text = header.get_text(strip=True)
        if 'viewpoints' in header_text.lower():
            # Find the parent table and get all links in it
            parent_table = header.find_parent('table')
            if parent_table:
                viewpoints_links = parent_table.find_all('a', href=True)
                viewpoints_count = 0
                
                for link in viewpoints_links:
                    href = link['href']
                    if href in all_urls:
                        if href not in url_to_sections:
                            url_to_sections[href] = set()
                        url_to_sections[href].add('Viewpoints')
                        viewpoints_count += 1
                
                print(f"Found {viewpoints_count} articles in Viewpoints section")
    
    # 3. Handle Spotlight section (uses class="spotheadlines")
    spotlight_headers = soup.find_all('td', class_='spotheadlines')
    for header in spotlight_headers:
        # Find the parent table and get all links in it
        parent_table = header.find_parent('table')
        if parent_table:
            spotlight_links = parent_table.find_all('a', href=True)
            spotlight_count = 0
            
            for link in spotlight_links:
                href = link['href']
                if href in all_urls:
                    if href not in url_to_sections:
                        url_to_sections[href] = set()
                    url_to_sections[href].add('Spotlight')
                    spotlight_count += 1
            
            print(f"Found {spotlight_count} articles in Spotlight section")
    
    # 4. Handle hotspot sections (original logic)
    section_headers = soup.find_all('td', class_='hotspot')
    print(f"Found {len(section_headers)} hotspot section headers")
    
    for header in section_headers:
        section_name = header.get_text(strip=True)
        
        # Find the parent table row, then look for the next row with articles
        parent_tr = header.find_parent('tr')
        if not parent_tr:
            continue
            
        articles_found = 0
        
        # Look for the next tr element that contains the article table
        next_element = parent_tr.find_next_sibling('tr')
        while next_element:
            # Look for article links in this row
            article_links = next_element.find_all('a', href=True)
            
            for link in article_links:
                href = link['href']
                
                # Skip non-HTTP URLs
                if not href.startswith('http'):
                    continue
                    
                # Only include URLs that are in our main URL list (already filtered)
                if href in all_urls:
                    if href not in url_to_sections:
                        url_to_sections[href] = set()
                    url_to_sections[href].add(section_name)
                    articles_found += 1
            
            # Stop when we hit the next section header or run out of rows
            next_element = next_element.find_next_sibling('tr')
            if not next_element:
                break
            if next_element.find('td', class_='hotspot'):
                break
        
        print(f"  Found {articles_found} articles in hotspot section '{section_name}'")
    
    # Create complete mapping for ALL URLs
    complete_mapping = {}
    sectioned_count = 0
    
    for url in all_urls:
        if url in url_to_sections:
            # URL has explicit sections from HTML structure
            complete_mapping[url] = sorted(list(url_to_sections[url]))
            sectioned_count += 1
        else:
            # URL doesn't have explicit sections - assign based on URL pattern
            if 'original.antiwar.com' in url:
                complete_mapping[url] = ['Viewpoints']
            elif 'news.antiwar.com' in url:
                complete_mapping[url] = ['News']
            elif 'antiwar.com/blog' in url:
                complete_mapping[url] = ['Blog']
            elif 'antiwar.com/news/?articleid=' in url:
                # Handle internal antiwar.com news articles (like URL #7)
                complete_mapping[url] = ['News']
            else:
                # External news sites without explicit sections
                complete_mapping[url] = ['Mixed News']
    
    print(f"Section mapping complete: {sectioned_count} URLs with explicit sections, {len(all_urls)} total URLs mapped")
    return complete_mapping



def get_all_article_urls(archive_url):
    """
    Extract all article URLs from antiwar.com daily archive page AND their section mappings.
    This function now returns both URLs and section categorization in a single HTTP request.
    
    Args:
        archive_url (str): Daily archive URL
        
    Returns:
        tuple: (list of URLs, dict of section mappings)
            - URLs: List of article URLs found on the page
            - Section mappings: Dictionary {url: [section1, section2, ...]}
    """
    try:
        response = requests.get(archive_url, headers=HEADERS)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch archive page: {e}")
        return [], {}
    
    soup = BeautifulSoup(response.content, 'html.parser')
    article_links = []
    
    # Define URLs to skip (navigation, internal pages, social media, etc.)
    skip_patterns = [
        # Internal antiwar.com navigation
        'antiwar.com/who.php', 'antiwar.com/search', 'antiwar.com/contact',
        'antiwar.com/donate', 'antiwar.com/latest.php', 'antiwar.com/viewpoints.php', 
        'antiwar.com/regions', 'antiwar.com/shops.php', 'antiwar.com/privacy.php',
        'antiwar.com/casualties/', 'antiwar.com/syndication.php', 'antiwar.com/submissions.php',
        'antiwar.com/reprint.php', 'antiwar.com/doverimages/', 'antiwar.com/newsletter/',
        
        # Author pages and navigation
        '/author/', '/columnists/', 'scotthorton.org',
        
        # Archive and redirect links
        'archive.ph/', 'archive.is/',
        
        # Social media and technical
        'javascript:', 'mailto:', '#', 'twitter.com', 'youtube.com', 'facebook.com',
        'instagram.com', 'linkedin.com', 'telegram.org',
        
        # Ads and trackers
        'amazon-adsystem.com', 'googletagservices.com', 'google.com/ads',
        
        # Other non-news sites
        'randolphbourne.org'
    ]
    
    # Home page URLs to exclude
    home_pages = [
        'https://www.antiwar.com', 'https://antiwar.com', 
        'https://original.antiwar.com', 'https://news.antiwar.com',
        'https://www.antiwar.com/blog', 'https://antiwar.com/blog'
    ]
    
    # Extract and filter links
    for link in soup.find_all('a', href=True):
        href = link['href']
        
        # Skip non-HTTP URLs
        if not href.startswith('http'):
            continue
            
        # Skip home pages
        if href.rstrip('/') in home_pages:
            continue
            
        # Skip URLs matching skip patterns
        if any(skip in href for skip in skip_patterns):
            continue
            
        article_links.append(href)
    
    # Remove duplicates and create final URL list
    # Converting back to list so can use its methods
    unique_links = list(set(article_links))
    print(f"Found {len(unique_links)} unique article URLs")
    
    # Get section mappings using the SAME response/soup to avoid extra HTTP call
    section_mappings = extract_section_mappings_from_soup(soup, unique_links)
    
    return unique_links, section_mappings


def scrape_single_article_with_fallback(url):
    """
    Scrape a single article with archive.is fallback for failed URLs.
    
    Args:
        url (str): Article URL to scrape
        
    Returns:
        dict: Article data with url, title, authors, content, and scrape_status
    """
    # Try original URL first
    result = scrape_single_article(url)
    
    if result['scrape_status'] == 'success':
        return result
    
    # Original failed - try archive.is
    print(f"Original failed ({result['scrape_status']}), trying archive.is...")
    time.sleep(2)  # Rate limiting
    
    archive_url = f"https://archive.is/{url}"
    archive_result = scrape_single_article(archive_url)
    
    if archive_result['scrape_status'] == 'success':
        # Keep original URL for tracking
        archive_result['url'] = url  
        return archive_result
    
    # Both failed
    return {
        'url': url,
        'title': None,
        'authors': [],
        'content': None,
        'scrape_status': f'failed: original and archive both failed'
    }

def scrape_single_article(url):
    """
    Scrape a single article using newspaper3k library.
    
    Args:
        url (str): Article URL to scrape
        
    Returns:
        dict: Article data with url, title, authors, content, and scrape_status
    """
    try:
        # Download and parse article using newspaper3k
        article = Article(url)
        article.config.request_timeout = 15  # Increase from 7 to 15 seconds
        article.download()
        article.parse()

        # Clean the content
        content = article.text
        if content:
            # Remove excessive whitespace
            content = ' '.join(content.split())
            
            # Remove common editor notes that add noise
            editor_patterns = [
                "Editor's note:", "Editor's Note:", "EDITOR'S NOTE:",
                "Editorial note:", "Editorial Note:", "EDITORIAL NOTE:",
                "Note to readers:", "Note to Readers:", "NOTE TO READERS:",
                "Disclaimer:", "DISCLAIMER:",
                "This story was updated", "This article was updated",
                "Updated at", "Last updated"
            ]
            
            # Remove first occurrence of editor notes
            for pattern in editor_patterns:
                if pattern.lower() in content.lower():
                    start_idx = content.lower().find(pattern.lower())
                    end_idx = content.find('. ', start_idx)
                    if end_idx == -1:
                        end_idx = len(content)
                    else:
                        end_idx += 1
                    content = content[:start_idx] + content[end_idx:].strip()
                    break
        
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


def check_existing_progress(date):
    year = date.split('-')[0]  # Extract year from 2025-07-12
    year_dir = create_year_directory(year)
    
    progress_filename = os.path.join(year_dir, f"progress_{year}.json")
    scraped_filename = os.path.join(year_dir, f"scraped_{date}.json")
    
    # Check if daily scraped file exists (means this date is done)
    if os.path.exists(scraped_filename):
        print(f"Date {date} already completed")
        return "completed", []
    
    # Load yearly progress file
    if os.path.exists(progress_filename):
        with open(progress_filename, 'r', encoding='utf-8') as f:
            progress_data = json.load(f)
        print(f"Found existing progress for year {year}")
        return progress_data, []
    
    print("No existing progress found - starting fresh")
    return None, []


def filter_remaining_urls(all_urls, completed_urls):
    """
    Filter out already-completed URLs from the full URL list.
    This prevents re-scraping articles that were already processed before a crash.
    
    Args:
        all_urls (list): Complete list of URLs to scrape
        completed_urls (list): URLs that have already been scraped
        
    Returns:
        list: URLs that still need to be scraped
    """
    remaining_urls = [url for url in all_urls if url not in completed_urls]
    skipped_count = len(all_urls) - len(remaining_urls)
    
    print(f"URL filtering results:")
    print(f"  - Total URLs found: {len(all_urls)}")
    print(f"  - Already completed: {skipped_count}")
    print(f"  - Remaining to scrape: {len(remaining_urls)}")
    
    return remaining_urls


def save_articles_to_file(articles, date, archive_url):
    year = date.split('-')[0]
    year_dir = create_year_directory(year)
    filename = os.path.join(year_dir, f"scraped_{date}.json")
    
    # Load existing data or create new structure
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        data = {
            "archive_url": archive_url,
            "date": date,
            "articles": []
        }
    
    # Add new articles and save
    data["articles"].extend(articles)
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"Added {len(articles)} articles to {filename} (total: {len(data['articles'])})")


def update_progress_file(archive_url, date, chunk_articles, chunk_number, total_urls):
    year = date.split('-')[0]
    year_dir = create_year_directory(year)
    progress_filename = os.path.join(year_dir, f"progress_{year}.json")
    
    # Load existing progress or create new
    if os.path.exists(progress_filename):
        with open(progress_filename, 'r', encoding='utf-8') as f:
            progress_data = json.load(f)
    else:
        progress_data = {
            "year": year
            }
    
    # Load existing progress for this specific date OR create new
    date_key = f"date_{date}"
    if date_key in progress_data:
        current_date_progress = progress_data[date_key]
    else:
        current_date_progress = {
            "date": date,
            "archive_url": archive_url,
            "total_articles": total_urls,
            "completed_articles": 0,
            "failed_articles": 0,
            "chunks_completed": 0
        }
    
    # ADD to existing counts (don't reset!)
    for article in chunk_articles:
        if article['scrape_status'] == 'success':
            current_date_progress['completed_articles'] += 1
        else:
            current_date_progress['failed_articles'] += 1
    
    # Update chunk number
    current_date_progress['chunks_completed'] = chunk_number
    
    # Save back to progress data
    progress_data[date_key] = current_date_progress
    
    with open(progress_filename, 'w', encoding='utf-8') as f:
        json.dump(progress_data, f, indent=2, ensure_ascii=False)
    
    print(f"Updated progress for {date}: {current_date_progress['completed_articles']}/{total_urls} articles")


def scrape_articles_with_checkpoints(urls, archive_url, date, 
                                     url_sections):
    """
    Scrape articles in chunks with checkpoint saving for crash recovery.
    Now includes section categorization for each article.
    
    Args:
        urls (list): URLs to scrape
        archive_url (str): Original archive URL
        date (str): Date string for filenames
        url_sections (dict): Mapping of URLs to their section categories
        
    Returns:
        list: All scraped articles with section information
    """
    all_articles = []
    total_urls = len(urls)
    
    print(f"Scraping {total_urls} articles in chunks of {CHUNK_SIZE}")
    
    # Process URLs in chunks
    for i in range(0, total_urls, CHUNK_SIZE):
        chunk_urls = urls[i:i+CHUNK_SIZE]
        chunk_number = i//CHUNK_SIZE + 1
        
        print(f"\nScraping chunk {chunk_number}: articles {i+1} to {min(i+CHUNK_SIZE, total_urls)}")
        print(f"URLs in this chunk: {len(chunk_urls)}")
        
        # Scrape chunk using multiple threads for speed
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            chunk_articles = list(executor.map(scrape_single_article_with_fallback, chunk_urls))
        
        # Add metadata and section information to each article
        for j, article in enumerate(chunk_articles):
            url = chunk_urls[j]
            article['progress_index'] = i + j
            # Add section categories from our mapping
            article['categories'] = url_sections[url]
        
        # Save chunk and update progress immediately
        save_articles_to_file(chunk_articles, date, archive_url)
        update_progress_file(archive_url, date, chunk_articles, chunk_number, total_urls)
        
        all_articles.extend(chunk_articles)
        
        # Show chunk completion stats
        successful = len([a for a in chunk_articles if a['scrape_status'] == 'success'])
        failed = len(chunk_articles) - successful
        print(f"Chunk {chunk_number} completed: {successful} success, {failed} failed")
    
    return all_articles


def calculate_metrics(articles, total_time):
    """
    Calculate and return scraping performance metrics.
    
    Args:
        articles (list): List of scraped articles
        total_time (float): Total scraping time in seconds
        
    Returns:
        dict: Performance metrics
    """
    successful = [a for a in articles if a['scrape_status'] == 'success']
    failed = [a for a in articles if a['scrape_status'] != 'success']
    
    # Calculate total data size in MB
    total_chars = 0
    for article in successful:
        if article['content']:
            total_chars += len(article['content'])
        if article['title']:
            total_chars += len(article['title'])
        if article['authors']:
            total_chars += sum(len(str(author)) for author in article['authors'])
    
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


def show_failed_articles(articles):
    """
    Display details about articles that failed to scrape.
    
    Args:
        articles (list): List of articles to check for failures
    """
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


def scrape_full_archive_page(archive_url):
    """
    Main function to scrape an entire antiwar.com daily archive page.
    Includes resume functionality, checkpoint system, and section categorization.
    
    Args:
        archive_url (str): URL of the daily archive page to scrape
        
    Returns:
        dict: Final scraped data with all articles and their sections, or None if failed
    """
    print(f"Starting to scrape archive page: {archive_url}")
    start_time = time.time()
    
    # Extract date from URL for filenames
    date = extract_date_from_archive_url(archive_url)
    if not date:
        print("ERROR: Could not extract date from archive URL")
        return None
    
    print(f"Archive date: {date}")
    
    # Check for existing progress (resume functionality)
    progress_data, completed_urls = check_existing_progress(date)
    
    # Get all article URLs and section mappings from the archive page
    print("\nGetting all article URLs and section mappings...")
    all_urls, url_sections = get_all_article_urls(archive_url)
    
    if not all_urls:
        print("No URLs found! Exiting.")
        return None
    
    # Show section statistics
    section_counts = {}
    for sections in url_sections.values():
        for section in sections:
            section_counts[section] = section_counts.get(section, 0) + 1
    
    print(f"\nSection distribution:")
    for section, count in sorted(section_counts.items()):
        print(f"  {section}: {count} articles")
    
    # Check if all articles are already completed
    if progress_data == "completed":
        print("\n🎉 All articles already completed! Loading existing data...")
        year = date.split('-')[0]
        year_dir = create_year_directory(year)
        final_filename = os.path.join(year_dir, f"scraped_{date}.json")
        with open(final_filename, 'r', encoding='utf-8') as f:
            final_data = json.load(f)
        print(f"Loaded {len(final_data['articles'])} existing articles")
        return final_data
    elif progress_data:
        remaining_urls = filter_remaining_urls(all_urls, completed_urls)
    else:
        remaining_urls = all_urls
        print(f"No existing progress - will scrape all {len(all_urls)} URLs")
    
    # Scrape remaining articles with checkpoint system and section mapping
    print(f"\n📥 Scraping {len(remaining_urls)} remaining articles...")
    new_articles = scrape_articles_with_checkpoints(remaining_urls, archive_url, date, url_sections)
    
   # Load final combined data
    year = date.split('-')[0]
    year_dir = create_year_directory(year)
    final_filename = os.path.join(year_dir, f"scraped_{date}.json")
    with open(final_filename, 'r', encoding='utf-8') as f:
        final_data = json.load(f)
    
    # Calculate and display metrics for newly scraped articles
    end_time = time.time()
    total_time = end_time - start_time
    
    if new_articles:
        metrics = calculate_metrics(new_articles, total_time)
        show_failed_articles(new_articles)
        
        print(f"\nNEW ARTICLES METRICS:")
        print(f"===================")
        for key, value in metrics.items():
            print(f"{key}: {value}")
    
    print(f"\n✅ TOTAL: {len(final_data['articles'])} articles in final file")
    return final_data


def main():
    from datetime import datetime, timedelta
    
    print("=== News Article Scraper ===")
    
    # Generate date range
    start = datetime.strptime(START_DATE, '%Y-%m-%d')
    end = datetime.strptime(END_DATE, '%Y-%m-%d')
    
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    
    print(f"Will scrape {len(dates)} dates: {START_DATE} to {END_DATE}")
    
    for date in dates:
        print(f"\n{'='*50}")
        print(f"Processing {date}")
        print(f"{'='*50}")
        
        archive_url = date_to_archive_url(date)
        result = scrape_full_archive_page(archive_url)
        
        if result:
            print(f"✅ {date}: {len(result['articles'])} articles")
        else:
            print(f"❌ {date}: Failed")
        
        time.sleep(5)  # Be nice to server




if __name__ == "__main__":
    main()