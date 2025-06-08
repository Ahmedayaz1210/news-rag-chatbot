import requests
from bs4 import BeautifulSoup

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
            'content': '\n'.join(content),
            'author': author_name.text.strip() if author_name else 'Unknown Author',
            'date_time_published': date_time_published.text.strip() if date_time_published else 'Unknown Date',
            'news_category': ', '.join([a.text for a in news_category]) if news_category else 'Uncategorized',
            'url': url
        }
    else:
        print(f"Failed to retrieve article: {r.status_code}")
        return None

print(scrape_article('https://news.antiwar.com/2025/06/05/israeli-attacks-kill-67-palestinians-in-gaza-over-24-hours/'))  # Replace with a valid URL to test
