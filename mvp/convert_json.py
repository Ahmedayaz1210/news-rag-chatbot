import json
from urllib.parse import urlparse
import os

def clean_author_names(authors_list):
    """Remove CSS junk and keep only real author names"""
    if not authors_list:
        return []
    
    clean_authors = []
    
    for author in authors_list:
        # Skip obvious CSS junk
        if any(junk in author for junk in [
            'Wp-Block-', 'Class', 'Display', 'Height', 'Width', 
            'Vertical-Align', 'Where Img', 'Auto Max-Width', 'Author',
            'View' 
        ]):
            continue
            
        # Skip if it's too long (probably CSS)
        if len(author) > 50:
            continue
            
        # Keep if it looks like a real name
        clean_authors.append(author)
    
    return clean_authors

def extract_source_name(url):
    """Extract clean source name from URL"""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    
    # Remove 'www.' if present
    if domain.startswith('www.'):
        domain = domain[4:]
    
    # Manual mapping for common sources
    source_mapping = {
        'original.antiwar.com': 'Antiwar.com',
        'news.antiwar.com': 'Antiwar.com',
        'antiwar.com': 'Antiwar.com',
        'aljazeera.com': 'Al Jazeera',
        'cbsnews.com': 'CBS News',
        'apnews.com': 'Associated Press',
        'reuters.com': 'Reuters',
        'middleeasteye.net': 'Middle East Eye',
        'newarab.com': 'The New Arab',
        'atlantanewsfirst.com': 'Atlanta News First',
        'taskandpurpose.com': 'Task & Purpose'
    }
    
    # Check if we have a manual mapping
    if domain in source_mapping:
        return source_mapping[domain]
    
    # Otherwise, clean up the domain name
    if domain.endswith('.com') or domain.endswith('.net') or domain.endswith('.org'):
        domain = domain[:-4]
    
    return domain.replace('-', ' ').replace('_', ' ').title()

def format_authors(clean_authors):
    """Format author list into readable string"""
    if not clean_authors:
        return "an unknown author"
    elif len(clean_authors) == 1:
        return clean_authors[0]
    elif len(clean_authors) == 2:
        return f"{clean_authors[0]} and {clean_authors[1]}"
    else:
        return ", ".join(clean_authors[:-1]) + f", and {clean_authors[-1]}"

def create_narrative_text(article, date):
    """Convert article to narrative format"""
    clean_authors = clean_author_names(article['authors'])
    source_name = extract_source_name(article['url'])
    formatted_authors = format_authors(clean_authors)
    title = article['title']
    
    # Create the narrative header
    narrative = f"The following is an article titled '{title}' from {source_name} from {date} written by {formatted_authors}. The text of the article follows.\n\n"
    
    # Add the article content
    narrative += article['content']
    
    return narrative

def read_json_file(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

# Process all articles and create individual files
if __name__ == "__main__":
    data = read_json_file('scraped_2025-06-01.json')
    
    # Create output directory
    output_dir = 'narrative_articles'
    os.makedirs(output_dir, exist_ok=True)
    
    processed_count = 0
    
    for i, article in enumerate(data['articles']):
        # Skip failed articles
        if article['scrape_status'] != 'success':
            print(f"Skipping article {i+1}: {article['scrape_status']}")
            continue
        
        # Skip articles with no content
        if not article['content'] or article['content'].strip() == '':
            print(f"Skipping article {i+1}: No content")
            continue
            
        processed_count += 1
        
        # Create narrative text
        narrative_text = create_narrative_text(article, data['date'])
        
        # Create filename
        filename = f"article_{processed_count:03d}.txt"
        filepath = os.path.join(output_dir, filename)
        
        # Write to file
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(narrative_text)
        
        print(f"Created {filename} - {article['title'][:60]}...")
    
    print(f"\n✅ Successfully created {processed_count} article files in '{output_dir}' folder")