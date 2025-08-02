import json
from urllib.parse import urlparse
import os
import glob

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
    title = article['title'] or "Untitled Article"
    
    # Create the narrative header
    narrative = f"The following is an article titled '{title}' from {source_name} from {date} written by {formatted_authors}. The text of the article follows.\n\n"
    
    # Add the article content
    narrative += article['content']
    
    return narrative

def process_year(year):
    """Process all JSON files for a specific year"""
    
    # Set up paths
    data_dir = os.path.join('..', 'data', str(year))  
    output_dir = os.path.join('..', 'txt_data', str(year))
    
    # Check if data directory exists
    if not os.path.exists(data_dir):
        print(f"❌ Data directory not found: {data_dir}")
        return
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all JSON files (exclude progress files)
    json_pattern = os.path.join(data_dir, f"scraped_{year}-*.json")
    json_files = glob.glob(json_pattern)
    
    if not json_files:
        print(f"❌ No JSON files found in {data_dir}")
        return
    
    print(f"📁 Found {len(json_files)} JSON files for year {year}")
    
    total_processed = 0
    total_skipped = 0
    
    # Sort files by date to process chronologically
    json_files.sort()
    
    for json_file in json_files:
        print(f"\n📄 Processing: {os.path.basename(json_file)}")
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"❌ Error reading {json_file}: {e}")
            continue
        
        date = data.get('date', 'unknown-date')
        articles = data.get('articles', [])
        
        daily_count = 0
        daily_skipped = 0
        
        for article in articles:
            # Skip failed articles
            if article.get('scrape_status') != 'success':
                daily_skipped += 1
                continue
            
            # Skip articles with no content
            if not article.get('content') or article['content'].strip() == '':
                daily_skipped += 1
                continue
            
            daily_count += 1
            
            # Create narrative text
            narrative_text = create_narrative_text(article, date)
            
            # Create filename: article_001_2025-07-18.txt
            filename = f"article_{daily_count:03d}_{date}.txt"
            filepath = os.path.join(output_dir, filename)
            
            # Write to file
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(narrative_text)
                
                # Show progress
                title_preview = (article.get('title') or 'Untitled')[:50]
                print(f"  ✅ {filename} - {title_preview}...")
                
            except Exception as e:
                print(f"  ❌ Error writing {filename}: {e}")
                daily_skipped += 1
                daily_count -= 1
        
        total_processed += daily_count
        total_skipped += daily_skipped
        
        print(f"  📊 Day summary: {daily_count} processed, {daily_skipped} skipped")
    
    print(f"\n🎉 Year {year} complete!")
    print(f"📊 Total: {total_processed} articles processed, {total_skipped} skipped")
    print(f"📁 Output folder: {output_dir}")

if __name__ == "__main__":
    # CHANGE THIS YEAR TO PROCESS DIFFERENT YEARS
    YEAR_TO_PROCESS = 2025  # ← EDIT THIS
    
    print(f"=== JSON to TXT Converter for Year {YEAR_TO_PROCESS} ===\n")
    
    process_year(YEAR_TO_PROCESS)
    
    print(f"\n✅ Conversion complete for year {YEAR_TO_PROCESS}!")
    print(f"📁 Check the 'txt_data/{YEAR_TO_PROCESS}/' folder for your files.")