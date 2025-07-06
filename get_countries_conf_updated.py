import sqlite3
import requests
import csv
from collections import Counter
import time
import urllib.parse
from urllib.parse import quote
import sys
import os

# Fix Unicode encoding issues for Windows console
if sys.platform.startswith('win'):
    # Set console to UTF-8 encoding
    os.system('chcp 65001 >nul')
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Connect to DB
conn = sqlite3.connect("DBLP.db")
cursor = conn.cursor()

# Query using the correct column names: 'name' (paper title) and 'authors'
cursor.execute("SELECT name, authors FROM conf_papers")
rows = cursor.fetchall()

# Filter rows with valid data
valid_papers = [(title, authors) for title, authors in rows if title and authors]

print(f"Found {len(valid_papers)} papers with title and author information.")
print(f"Sample data:")
for i, (title, authors) in enumerate(valid_papers[:3]):
    # Safe string handling for Unicode characters
    safe_title = title.encode('ascii', 'ignore').decode('ascii') if title else "No title"
    safe_authors = authors.encode('ascii', 'ignore').decode('ascii') if authors else "No authors"
    print(f"  {i+1}. Title: {safe_title[:60]}...")
    print(f"     Authors: {safe_authors[:80]}...")
    print()

class AuthorCountryLookup:
    def __init__(self):
        self.paper_cache = {}
        self.author_cache = {}
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Academic Research Script (mailto:research@example.com)',
            'Accept': 'application/json'
        })
    
    def clean_title(self, title):
        """Clean title for better matching"""
        return title.lower().strip()
    
    def safe_print(self, text):
        """Safely print text with Unicode characters"""
        try:
            print(text)
        except UnicodeEncodeError:
            # Remove non-ASCII characters if printing fails
            safe_text = text.encode('ascii', 'ignore').decode('ascii')
            print(safe_text)
    
    def get_country_from_openalex(self, author_name):
        """Get country from OpenAlex API"""
        if author_name in self.author_cache:
            return self.author_cache[author_name]
        
        try:
            encoded_name = urllib.parse.quote(author_name)
            url = f"https://api.openalex.org/authors?search={encoded_name}&per-page=1"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'results' in data and len(data['results']) > 0:
                first_result = data['results'][0]
                
                # Try last_known_institution first
                institution = first_result.get("last_known_institution")
                if institution and institution.get("country_code"):
                    country_code = institution.get("country_code")
                    self.author_cache[author_name] = country_code
                    return country_code
                
                # Try affiliations as backup
                affiliations = first_result.get("affiliations", [])
                for affiliation in affiliations:
                    institution = affiliation.get("institution", {})
                    if institution.get("country_code"):
                        country_code = institution.get("country_code")
                        self.author_cache[author_name] = country_code
                        return country_code
                        
        except Exception as e:
            safe_name = author_name.encode('ascii', 'ignore').decode('ascii')
            print(f"OpenAlex error for {safe_name}: {e}")
        
        self.author_cache[author_name] = None
        return None
    
    def get_semantic_scholar_paper(self, title, authors):
        """Query Semantic Scholar API for paper details"""
        clean_title = self.clean_title(title)
        cache_key = f"ss_{clean_title}"
        
        if cache_key in self.paper_cache:
            return self.paper_cache[cache_key]
        
        try:
            # Construct a query with both title and first author
            first_author = authors[0] if authors else ""
            query = f"{title} {first_author}"
            encoded_query = quote(query)
            
            url = f"https://api.semanticscholar.org/graph/v1/paper/search?query={encoded_query}&fields=title,authors,year,venue,openAccessPdf"
            
            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                # Find the best matching paper
                if 'data' in data and data['data']:
                    for paper in data['data']:
                        # Simple match: check if the paper title contains our search title
                        # or vice versa (for cases where our title contains extraneous info)
                        paper_title = paper.get('title', '').lower()
                        if paper_title in clean_title or clean_title in paper_title:
                            self.paper_cache[cache_key] = paper
                            return paper
            
            # If no paper found
            self.paper_cache[cache_key] = None
            return None
            
        except Exception as e:
            safe_title = title.encode('ascii', 'ignore').decode('ascii') if title else "Unknown"
            print(f"Semantic Scholar error for {safe_title}: {e}")
            self.paper_cache[cache_key] = None
            return None
    
    def extract_country_from_semantic_scholar(self, paper):
        """Extract country information from Semantic Scholar paper data"""
        if not paper or 'authors' not in paper:
            return None
        
        for author in paper['authors']:
            # Check if author has affiliations
            if 'affiliations' in author:
                for affiliation in author['affiliations']:
                    # Look for country information in affiliation name
                    affiliation_name = affiliation.lower()
                    
                    # Simple country detection from affiliation text
                    country_keywords = {
                        # South Asia
                        'india': 'IN', 'delhi': 'IN', 'mumbai': 'IN', 'bangalore': 'IN', 'chennai': 'IN', 
                        'hyderabad': 'IN', 'kolkata': 'IN', 'pune': 'IN', 'iit': 'IN', 'iisc': 'IN',
                        'indian institute': 'IN', 'nit': 'IN', 'bits': 'IN', 'anna university': 'IN',
                        
                        'pakistan': 'PK', 'karachi': 'PK', 'lahore': 'PK', 'islamabad': 'PK', 
                        'rawalpindi': 'PK', 'faisalabad': 'PK', 'peshawar': 'PK', 'quetta': 'PK',
                        'multan': 'PK', 'gujranwala': 'PK', 'lums': 'PK', 'nust': 'PK', 'comsats': 'PK',
                        
                        'bangladesh': 'BD', 'dhaka': 'BD', 'chittagong': 'BD', 'sylhet': 'BD',
                        'rajshahi': 'BD', 'buet': 'BD', 'du': 'BD',
                        
                        'sri lanka': 'LK', 'colombo': 'LK', 'kandy': 'LK', 'moratuwa': 'LK',
                        
                        'nepal': 'NP', 'kathmandu': 'NP', 'tribhuvan': 'NP',
                        
                        # East Asia
                        'china': 'CN', 'beijing': 'CN', 'shanghai': 'CN', 'guangzhou': 'CN', 
                        'shenzhen': 'CN', 'hangzhou': 'CN', 'nanjing': 'CN', 'wuhan': 'CN',
                        'chengdu': 'CN', 'xi\'an': 'CN', 'tianjin': 'CN', 'tsinghua': 'CN', 
                        'peking': 'CN', 'fudan': 'CN', 'zhejiang': 'CN', 'sjtu': 'CN',
                        'chinese academy': 'CN', 'cas': 'CN', 'pku': 'CN',
                        
                        'japan': 'JP', 'tokyo': 'JP', 'osaka': 'JP', 'kyoto': 'JP', 'nagoya': 'JP',
                        'yokohama': 'JP', 'kobe': 'JP', 'sendai': 'JP', 'sapporo': 'JP',
                        'todai': 'JP', 'kyodai': 'JP', 'waseda': 'JP', 'keio': 'JP',
                        
                        'korea': 'KR', 'south korea': 'KR', 'seoul': 'KR', 'busan': 'KR', 
                        'incheon': 'KR', 'daegu': 'KR', 'kaist': 'KR', 'snu': 'KR', 
                        'yonsei': 'KR', 'postech': 'KR',
                        
                        # Southeast Asia
                        'singapore': 'SG', 'nus': 'SG', 'ntu': 'SG', 'smu': 'SG',
                        
                        'malaysia': 'MY', 'kuala lumpur': 'MY', 'johor': 'MY', 'penang': 'MY',
                        'um': 'MY', 'utm': 'MY', 'usm': 'MY', 'upm': 'MY',
                        
                        'thailand': 'TH', 'bangkok': 'TH', 'chiang mai': 'TH', 'chulalongkorn': 'TH',
                        
                        'indonesia': 'ID', 'jakarta': 'ID', 'bandung': 'ID', 'surabaya': 'ID',
                        'itb': 'ID', 'ui': 'ID', 'ugm': 'ID',
                        
                        'philippines': 'PH', 'manila': 'PH', 'quezon': 'PH', 'up': 'PH', 'ateneo': 'PH',
                        
                        'vietnam': 'VN', 'hanoi': 'VN', 'ho chi minh': 'VN', 'hcmc': 'VN',
                        
                        # Middle East
                        'iran': 'IR', 'tehran': 'IR', 'isfahan': 'IR', 'mashhad': 'IR', 'shiraz': 'IR',
                        'sharif': 'IR', 'ut': 'IR',
                        
                        'turkey': 'TR', 'istanbul': 'TR', 'ankara': 'TR', 'izmir': 'TR',
                        'bogazici': 'TR', 'metu': 'TR', 'bilkent': 'TR',
                        
                        'israel': 'IL', 'tel aviv': 'IL', 'jerusalem': 'IL', 'haifa': 'IL',
                        'technion': 'IL', 'hebrew university': 'IL', 'weizmann': 'IL',
                        
                        'saudi arabia': 'SA', 'riyadh': 'SA', 'jeddah': 'SA', 'kaust': 'SA',
                        'king saud': 'SA', 'kfupm': 'SA',
                        
                        'uae': 'AE', 'dubai': 'AE', 'abu dhabi': 'AE', 'aub': 'AE',
                        
                        # Europe
                        'united kingdom': 'GB', 'uk': 'GB', 'england': 'GB', 'britain': 'GB',
                        'cambridge': 'GB', 'oxford': 'GB', 'london': 'GB', 'manchester': 'GB',
                        'edinburgh': 'GB', 'glasgow': 'GB', 'bristol': 'GB', 'imperial': 'GB',
                        'ucl': 'GB', 'kcl': 'GB', 'lse': 'GB',
                        
                        'germany': 'DE', 'berlin': 'DE', 'munich': 'DE', 'hamburg': 'DE',
                        'cologne': 'DE', 'frankfurt': 'DE', 'stuttgart': 'DE', 'dusseldorf': 'DE',
                        'max planck': 'DE', 'tum': 'DE', 'kit': 'DE', 'rwth': 'DE',
                        
                        'france': 'FR', 'paris': 'FR', 'lyon': 'FR', 'marseille': 'FR',
                        'toulouse': 'FR', 'sorbonne': 'FR', 'inria': 'FR', 'cnrs': 'FR',
                        
                        'italy': 'IT', 'rome': 'IT', 'milan': 'IT', 'naples': 'IT', 'turin': 'IT',
                        'bologna': 'IT', 'florence': 'IT', 'genoa': 'IT',
                        
                        'spain': 'ES', 'madrid': 'ES', 'barcelona': 'ES', 'valencia': 'ES',
                        'seville': 'ES', 'upc': 'ES', 'uam': 'ES',
                        
                        'netherlands': 'NL', 'amsterdam': 'NL', 'rotterdam': 'NL', 'utrecht': 'NL',
                        'delft': 'NL', 'eindhoven': 'NL', 'tue': 'NL',
                        
                        'sweden': 'SE', 'stockholm': 'SE', 'gothenburg': 'SE', 'kth': 'SE',
                        'chalmers': 'SE', 'lund': 'SE',
                        
                        'switzerland': 'CH', 'zurich': 'CH', 'geneva': 'CH', 'basel': 'CH',
                        'eth': 'CH', 'epfl': 'CH', 'cern': 'CH',
                        
                        'russia': 'RU', 'moscow': 'RU', 'st petersburg': 'RU', 'saint petersburg': 'RU',
                        'novosibirsk': 'RU', 'msu': 'RU',
                        
                        # North America
                        'united states': 'US', 'usa': 'US', 'america': 'US', 'u.s.': 'US',
                        'california': 'US', 'texas': 'US', 'new york': 'US', 'florida': 'US',
                        'mit': 'US', 'stanford': 'US', 'harvard': 'US', 'berkeley': 'US',
                        'caltech': 'US', 'princeton': 'US', 'yale': 'US', 'columbia': 'US',
                        'university of': 'US', 'state university': 'US', 'tech': 'US',
                        
                        'canada': 'CA', 'toronto': 'CA', 'vancouver': 'CA', 'montreal': 'CA',
                        'calgary': 'CA', 'ottawa': 'CA', 'ubc': 'CA', 'mcgill': 'CA',
                        'waterloo': 'CA', 'uoft': 'CA',
                        
                        # Oceania
                        'australia': 'AU', 'sydney': 'AU', 'melbourne': 'AU', 'brisbane': 'AU',
                        'perth': 'AU', 'adelaide': 'AU', 'unsw': 'AU', 'usyd': 'AU',
                        'unimelb': 'AU', 'anu': 'AU', 'monash': 'AU',
                        
                        'new zealand': 'NZ', 'auckland': 'NZ', 'wellington': 'NZ', 'christchurch': 'NZ',
                        
                        # Africa
                        'south africa': 'ZA', 'cape town': 'ZA', 'johannesburg': 'ZA', 'durban': 'ZA',
                        'uct': 'ZA', 'wits': 'ZA', 'stellenbosch': 'ZA',
                        
                        'egypt': 'EG', 'cairo': 'EG', 'alexandria': 'EG', 'auc': 'EG',
                        
                        # Latin America
                        'brazil': 'BR', 'sao paulo': 'BR', 'rio de janeiro': 'BR', 'brasilia': 'BR',
                        'usp': 'BR', 'unicamp': 'BR',
                        
                        'mexico': 'MX', 'mexico city': 'MX', 'guadalajara': 'MX', 'monterrey': 'MX',
                        'unam': 'MX', 'itesm': 'MX',
                        
                        'argentina': 'AR', 'buenos aires': 'AR', 'cordoba': 'AR', 'uba': 'AR',
                        
                        'chile': 'CL', 'santiago': 'CL', 'valparaiso': 'CL', 'uc': 'CL',
                    }
                    for keyword, country_code in country_keywords.items():
                        if keyword in affiliation_name:
                            return country_code
        
        return None
    
    def get_author_country(self, title, authors_str):
        """Get country for first author using multiple methods"""
        authors = [author.strip() for author in authors_str.split(',')]
        first_author = authors[0] if authors else ""
        
        if not first_author:
            return None
        
        # Method 1: Try OpenAlex for the author
        country = self.get_country_from_openalex(first_author)
        if country:
            return country
        
        # Method 2: Try Semantic Scholar with paper title and authors
        safe_author = first_author.encode('ascii', 'ignore').decode('ascii')
        print(f"  Trying Semantic Scholar for: {safe_author}")
        paper = self.get_semantic_scholar_paper(title, authors)
        if paper:
            country = self.extract_country_from_semantic_scholar(paper)
            if country:
                # Cache this result for the author
                self.author_cache[first_author] = country
                return country
        
        return None

# Initialize the lookup class
lookup = AuthorCountryLookup()

# Country codes of interest
country_map = {
    "IN": "India",
    "PK": "Pakistan", 
    "BD": "Bangladesh",
    "CN": "China",
    "SG": "Singapore",
    "MY": "Malaysia",
    "KR": "South Korea",
    "US": "United States",
    "GB": "United Kingdom",
    "DE": "Germany",
    "LK": "Sri Lanka",
    "IR": "Iran",
    "TR": "Turkey",
    "AU": "Australia",
    "FR": "France"
}

# Process papers
print("Processing papers with dual API approach:")
print("="*60)

country_counter = Counter()
successful_lookups = 0
total_lookups = 0
no_country_found = []  # List to store authors with no country found

# Test with limited sample first
test_limit = 2782# Adjust as needed

for i, (title, authors_str) in enumerate(valid_papers[:test_limit]):
    # Safe handling of Unicode characters in titles
    safe_title = title.encode('ascii', 'ignore').decode('ascii') if title else "No title"
    print(f"\n{i+1}/{test_limit}. Processing paper: {safe_title[:50]}...")
    
    authors = [author.strip() for author in authors_str.split(',')]
    first_author = authors[0] if authors else ""
    
    if not first_author:
        continue
    
    # Safe handling of Unicode characters in author names    
    safe_author = first_author.encode('ascii', 'ignore').decode('ascii')
    print(f"  First author: {safe_author}")
    total_lookups += 1
    
    # Get country using combined approach
    country_code = lookup.get_author_country(title, authors_str)
    
    if country_code:
        successful_lookups += 1
        if country_code in country_map:
            country_counter[country_map[country_code]] += 1
            print(f"  Found country: {country_map[country_code]} ({country_code})")
        else:
            print(f"  Found country {country_code} (not in target list)")
    else:
        # Add to no_country_found list only if no country was found at all
        no_country_found.append({
            'Author Name': first_author,
            'Paper Title': title
        })
        print(f"  No country found")
    
    # Rate limiting - be nice to APIs
    if i % 10 == 0 and i > 0:
        print(f"  ... Processed {i} papers, taking a short break...")
        time.sleep(2)
    else:
        time.sleep(0.2)

# Results summary - Create lists for each country
print(f"\n" + "="*60)
print(f"RESULTS SUMMARY:")
print(f"Success rate: {successful_lookups}/{total_lookups} ({successful_lookups/total_lookups*100:.1f}%)")
print(f"\nCountry-wise Paper Counts:")
print("="*60)

# Create and display lists for each country
for country, count in country_counter.most_common():
    print(f"\n{country}:")
    print(f"  Total Papers: {count}")
    
    # Create a list of papers for this country
    country_papers = []
    for j, (title, authors_str) in enumerate(valid_papers[:test_limit]):
        authors = [author.strip() for author in authors_str.split(',')]
        first_author = authors[0] if authors else ""
        
        if first_author:
            country_code = lookup.get_author_country(title, authors_str)
            if country_code and country_code in country_map and country_map[country_code] == country:
                country_papers.append({
                    'paper_number': j + 1,
                    'title': title,
                    'first_author': first_author
                })
    
    # Display first few papers as examples
    print(f"  Sample papers (showing first 3):")
    for k, paper in enumerate(country_papers[:3]):
        safe_title = paper['title'].encode('ascii', 'ignore').decode('ascii')
        safe_author = paper['first_author'].encode('ascii', 'ignore').decode('ascii')
        print(f"    {k+1}. {safe_title[:50]}... (by {safe_author})")

# Create CSV for authors with no country found
if no_country_found:
    csv_filename = "authors_no_country_found.csv"
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Author Name', 'Paper Title']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for entry in no_country_found:
            writer.writerow(entry)
    
    print(f"\n" + "="*60)
    print(f"CSV FILE CREATED:")
    print(f"  Filename: {csv_filename}")
    print(f"  Authors with no country found: {len(no_country_found)}")
    print(f"  Sample entries:")
    for i, entry in enumerate(no_country_found[:5]):
        safe_author = entry['Author Name'].encode('ascii', 'ignore').decode('ascii')
        safe_title = entry['Paper Title'].encode('ascii', 'ignore').decode('ascii')
        print(f"    {i+1}. {safe_author} - {safe_title[:50]}...")
else:
    print(f"\nNo authors with missing country information found.")

# Debug: Show cache statistics
print(f"\nCache Statistics:")
print(f"Authors cached: {len(lookup.author_cache)}")
print(f"Papers cached: {len(lookup.paper_cache)}")

conn.close()