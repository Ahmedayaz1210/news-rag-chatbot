from newspaper import Article
import time

def test_newspaper3k_on_url(url):
    """Test newspaper3k on a single URL and show results"""
    print(f"\n{'='*60}")
    print(f"Testing: {url}")
    print(f"{'='*60}")
    
    try:
        # Create Article object
        article = Article(url)
        
        # Download the HTML
        print("Downloading article...")
        article.download()
        
        # Parse the content
        print("Parsing content...")
        article.parse()
        
        # Show results
        print(f"✅ SUCCESS!")
        print(f"Title: {article.title}")
        print(f"Author(s): {article.authors}")
        print(f"Publish Date: {article.publish_date}")
        print(f"Content Length: {len(article.text)} characters")
        print(f"Content Preview: {article.text[:200]}...")
        
        return {
            'success': True,
            'title': article.title,
            'authors': article.authors,
            'date': article.publish_date,
            'content': article.text,
            'url': url
        }
        
    except Exception as e:
        print(f"❌ FAILED: {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'url': url
        }

# Test URLs from your actual data
# Test URLs from your actual data - let's try more diverse sites
test_urls = [
    # Original working ones
    "https://www.aljazeera.com/news/2025/5/29/cambodia-pm-urges-calm-after-border-clash-with-thailand-leaves-soldier-dead",
    "https://www.middleeasteye.net/news/what-does-pkks-disbanding-mean-turkeys-pro-kurdish-movement",
    "https://www.cbsnews.com/news/gaza-humanitarian-foundation-ghf-breaching-rules-switzerland-authorities-say/",
    
    # Additional test sites from your data
    "https://apnews.com/article/south-korea-navy-patrol-plane-crash-mountain-f180debf926b7200e5875e67a0d0df9b",
    "https://www.newarab.com/news/egypt-ex-presidential-hopeful-tantawi-freed-jail-lawyer",
    "https://thedefensepost.com/2025/05/29/france-base-nuclear-facelift/",
    "https://www.dropsitenews.com/p/trump-gaza-ceasefire-proposal-hamas-israel-witkoff",
    "https://libertarianinstitute.org/news/erik-prince-working-with-haitian-government-in-fight-against-armed-groups/"
]

if __name__ == "__main__":
    print("Testing newspaper3k on real URLs from your antiwar.com archive...")
    
    results = []
    
    for url in test_urls:
        result = test_newspaper3k_on_url(url)
        results.append(result)
        
        # Be polite - wait 2 seconds between requests
        time.sleep(2)
    
    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    print(f"✅ Successful: {len(successful)}/{len(results)}")
    print(f"❌ Failed: {len(failed)}/{len(results)}")
    
    if failed:
        print("\nFailed URLs:")
        for fail in failed:
            print(f"  - {fail['url']}: {fail['error']}")