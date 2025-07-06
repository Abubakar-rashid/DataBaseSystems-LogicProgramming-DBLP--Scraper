import sqlite3
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import logging
import os 
from datetime import datetime
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def insert_conf_paper(paper_dict):
    """
    Insert a row into the conf_papers table in DBLP.db
    
    Args:
        paper_dict (dict): Dictionary containing paper information with keys:
            - name: Paper title
            - authors: Authors of the paper
            - conference_href: Conference URL
            - paper_href: Paper URL
            - year: Publication year
            - isbn: ISBN number
            - pages: Page numbers
            - conference_location: Conference location
            - created_at: Creation timestamp (if empty, will use current time)
            - conference_processed: Processed conference name
            - edition_name: Edition name
    
    Returns:
        int: The ROWID of the inserted row, or None if insertion failed
    """
    
    # Database file path (in current working directory)
    db_path = os.path.join(os.getcwd(), 'DBLP.db')
    
    # Check if database exists
    if not os.path.exists(db_path):
        print(f"Error: Database file {db_path} not found!")
        return None
    
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # If created_at is empty, use current timestamp
        if not paper_dict.get('created_at'):
            paper_dict['created_at'] = datetime.now().isoformat()
        
        # Prepare the INSERT statement
        insert_query = """
        INSERT INTO conf_papers (
            name, authors, conference_href, paper_href, year, 
            isbn, pages, conference_location, created_at, 
            conference_processed, edition_name
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Extract values from dictionary in the correct order
        values = (
            paper_dict.get('name', ''),
            paper_dict.get('authors', ''),
            paper_dict.get('conference_href', ''),
            paper_dict.get('paper_href', ''),
            paper_dict.get('year', ''),
            paper_dict.get('isbn', ''),
            paper_dict.get('pages', ''),
            paper_dict.get('conference_location', ''),
            paper_dict.get('created_at', ''),
            paper_dict.get('conference_processed', ''),
            paper_dict.get('edition_name', '')
        )
        
        # Execute the insert
        cursor.execute(insert_query, values)
        
        # Get the ROWID of the inserted row
        rowid = cursor.lastrowid
        
        # Commit the transaction
        conn.commit()
        
        print(f"Successfully inserted paper with ROWID: {rowid}")
        return rowid
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    
    except Exception as e:
        print(f"Error: {e}")
        return None
    
    finally:
        # Close the connection
        if conn:
            conn.close()

class DBLPScraper:
    def __init__(self, db_path='DBLP.db'):
        self.db_path = db_path
        self.driver = None
        self.setup_driver()
        
    def setup_driver(self):
        """Setup Chrome driver with appropriate options"""
        chrome_options = Options()
        # chrome_options.add_argument('--headless')  # Remove this line if you want to see the browser
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(10)
        
    def get_conference_hrefs(self):
        """Extract href links from Conference_href table"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name, href FROM Conferences_hrefs")
        conferences = cursor.fetchall()
        
        conn.close()
        return conferences
    
    def scrape_conference_content_links(self, conference_url):
        """Scrape all content links from a conference page using the actual DBLP structure"""
        try:
            logger.info(f"Navigating to conference page: {conference_url}")
            self.driver.get(conference_url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            content_links = []
            
            # Look for the main publication list
            try:
                publ_lists = self.driver.find_elements(By.CSS_SELECTOR, "ul.publ-list")
                logger.info(f"Found {len(publ_lists)} publication lists")
                
                for publ_list in publ_lists:
                    # Find all entry items within this list
                    entries = publ_list.find_elements(By.CSS_SELECTOR, "li.entry")
                    logger.info(f"Found {len(entries)} entries in publication list")
                    
                    for entry in entries:
                        try:
                            # Check if this entry has table of contents links
                            toc_links = self.extract_toc_links_from_entry(entry)
                            content_links.extend(toc_links)
                            
                            # Also check for direct conference/proceeding links
                            direct_links = self.extract_direct_links_from_entry(entry)
                            content_links.extend(direct_links)
                            
                        except Exception as e:
                            logger.debug(f"Error processing entry: {e}")
                            continue
                            
            except Exception as e:
                logger.error(f"Error finding publication lists: {e}")
            
            # Also look for direct links in the page that might contain papers
            additional_links = self.find_additional_content_links()
            content_links.extend(additional_links)
            
            # Remove duplicates
            unique_links = []
            seen_urls = set()
            for link in content_links:
                if link['url'] not in seen_urls:
                    unique_links.append(link)
                    seen_urls.add(link['url'])
            
            logger.info(f"Found {len(unique_links)} unique content links")
            return unique_links
            
        except Exception as e:
            logger.error(f"Error scraping conference content links: {e}")
            return []
    
    def extract_toc_links_from_entry(self, entry):
        """Extract table of contents links from a DBLP entry"""
        toc_links = []
        
        try:
            # Look for the table of contents link specifically
            toc_link_element = entry.find_element(By.CSS_SELECTOR, "a.toc-link")
            href = toc_link_element.get_attribute("href")
            
            if href and self.is_valid_content_link(href):
                # Extract title from the cite element
                title = "Unknown"
                try:
                    title_element = entry.find_element(By.CSS_SELECTOR, "span.title")
                    title = title_element.text.strip()
                except:
                    try:
                        cite_element = entry.find_element(By.CSS_SELECTOR, "cite.data")
                        title = cite_element.text.strip().split('\n')[0]  # First line usually contains title
                    except:
                        pass
                
                toc_links.append({
                    'url': href,
                    'title': title,
                    'section': 'toc-link',
                    'type': 'table_of_contents'
                })
                logger.debug(f"Found TOC link: {title} -> {href}")
                
        except NoSuchElementException:
            # No TOC link in this entry
            pass
        except Exception as e:
            logger.debug(f"Error extracting TOC links: {e}")
        
        return toc_links
    
    def extract_direct_links_from_entry(self, entry):
        """Extract direct conference/proceeding links from entry navigation"""
        direct_links = []
        
        try:
            # Look in the navigation menu for venue links
            nav_element = entry.find_element(By.CSS_SELECTOR, "nav.publ")
            
            # Find links that point to DBLP database pages
            nav_links = nav_element.find_elements(By.TAG_NAME, "a")
            
            for link in nav_links:
                href = link.get_attribute("href")
                if href and self.is_valid_content_link(href):
                    # Check if this is a database/venue link
                    if "/db/" in href and href.endswith(".html"):
                        title = link.get_attribute("title") or link.text.strip()
                        
                        direct_links.append({
                            'url': href,
                            'title': title,
                            'section': 'nav-link',
                            'type': 'database_page'
                        })
                        logger.debug(f"Found direct link: {title} -> {href}")
                        
        except NoSuchElementException:
            pass
        except Exception as e:
            logger.debug(f"Error extracting direct links: {e}")
        
        return direct_links
    
    def find_additional_content_links(self):
        """Find additional content links that might not be in the main publication list"""
        additional_links = []
        
        try:
            # Look for any links that contain "contents" or lead to paper listings
            all_links = self.driver.find_elements(By.TAG_NAME, "a")
            
            for link in all_links:
                href = link.get_attribute("href")
                text = link.text.strip().lower()
                
                if href and self.is_valid_content_link(href):
                    # Look for links that suggest they contain paper listings
                    if any(keyword in text for keyword in ['contents', 'proceedings', 'papers', 'table of contents']):
                        additional_links.append({
                            'url': href,
                            'title': link.text.strip(),
                            'section': 'additional',
                            'type': 'content_page'
                        })
                        
        except Exception as e:
            logger.debug(f"Error finding additional links: {e}")
        
        return additional_links
    
    def is_valid_content_link(self, url):
        """Check if URL is a valid content link to scrape"""
        if not url or not isinstance(url, str):
            return False
            
        # Must be DBLP URL
        if "dblp.org" not in url:
            return False
            
        # Skip certain types of links
        skip_patterns = [
            "bibtex", "ris", "rdf", "xml", "endnote", ".nt", ".ttl",
            "google.com", "scholar.google", "semanticscholar",
            "citeseer", "pubpeer", "reddit.com", "linkedin.com",
            "bibsonomy", "bluesky", "twitter.com", "doi.org",
            "usenix.org", "acm.org", "ieee.org"  # External sites
        ]
        
        for pattern in skip_patterns:
            if pattern in url.lower():
                return False
                
        # Look for patterns that indicate paper/content pages
        valid_patterns = [
            "/db/conf/", "/db/journals/", 
            "contents", "proceedings"
        ]
        
        return any(pattern in url for pattern in valid_patterns)
    
    def scrape_papers_from_content_page(self, content_url, conference_name):
        """Scrape papers and authors from a content page"""
        try:
            logger.info(f"Scraping papers from: {content_url}")
            self.driver.get(content_url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            papers = []
            
            # Look for the main content structure - DBLP uses specific classes
            # Use ONLY ONE selector - the most specific one that works
            try:
                # Try the most specific selector first
                paper_elements = self.driver.find_elements(By.CSS_SELECTOR, "ul.publ-list li.entry.inproceedings")
                
                # If no inproceedings found, try articles
                if not paper_elements:
                    paper_elements = self.driver.find_elements(By.CSS_SELECTOR, "ul.publ-list li.entry.article")
                
                # If still nothing, try generic entries
                if not paper_elements:
                    paper_elements = self.driver.find_elements(By.CSS_SELECTOR, "ul.publ-list li.entry")
                
                logger.info(f"Found {len(paper_elements)} paper elements to process")
                
                # Process each element ONCE
                for i, element in enumerate(paper_elements):
                    try:
                        if i % 50 == 0:  # Progress indicator
                            logger.info(f"Processing paper {i+1}/{len(paper_elements)}")
                        
                        if self.is_paper_entry(element):
                            paper_data = self.extract_paper_data(element, conference_name, content_url)
                            if paper_data and paper_data['name'].strip():
                                papers.append(paper_data)
                                print(paper_data)
                                insert_conf_paper(paper_data)
                                # print("papers appended to the the array")
                                
                    except Exception as e:
                        logger.debug(f"Error processing paper {i}: {e}")
                        continue
            except Exception as e:
                logger.error(f"Error scraping papers from {content_url}: {e}")
                return []
            # Remove duplicates based on paper title and authors
            unique_papers = []
            seen_papers = set()
            for paper in papers:
                paper_key = (paper['name'].lower().strip(), paper['authors'].lower().strip())
                if paper_key not in seen_papers and paper['name'].strip():
                    unique_papers.append(paper)
                    seen_papers.add(paper_key)
            
            logger.info(f"Found {len(unique_papers)} unique papers")
            return unique_papers
            
        except Exception as e:
            logger.error(f"Error scraping papers from {content_url}: {e}")
            return []
    
    def is_paper_entry(self, element):
        """Check if the entry represents an actual paper (not editorship, etc.)"""
        try:
            # Check for paper-specific classes
            classes = element.get_attribute("class") or ""
            
            # Skip editorial entries
            if "editor" in classes or "toc" in classes:
                return False
            
            # Look for paper indicators
            paper_indicators = ["inproceedings", "article", "incollection"]
            if any(indicator in classes for indicator in paper_indicators):
                return True
                
            # Check if it has author information (papers should have authors)
            try:
                authors = element.find_elements(By.CSS_SELECTOR, "span[itemprop='author']")
                if len(authors) > 0:
                    return True
            except:
                pass
                
            # Check for paper title patterns
            try:
                title_elem = element.find_element(By.CSS_SELECTOR, "span.title")
                title = title_elem.text.strip()
                # If it has a substantial title, it's likely a paper
                if len(title) > 10:
                    return True
            except:
                pass
                
            return False
            
        except Exception as e:
            logger.debug(f"Error checking if paper entry: {e}")
            return False
    
    def extract_paper_data(self, element, conference_name, page_url):
        """Extract paper data from a DBLP paper element"""
        try:
            paper_data = {
                'name': '',
                'authors': '',
                'conference_href': page_url,
                'paper_href': '',
                'year': '',
                'isbn': '',
                'pages': '',
                'conference_location': '',
                'created_at': '',
                'conference_processed': conference_name,
                'edition_name': ''
            }
            
            # Extract paper title - DBLP uses span.title
            try:
                title_elem = element.find_element(By.CSS_SELECTOR, "span.title")
                paper_data['name'] = title_elem.text.strip()
            except:
                # Fallback: try to get title from itemprop
                try:
                    title_elem = element.find_element(By.CSS_SELECTOR, "[itemprop='headline'] span.title")
                    paper_data['name'] = title_elem.text.strip()
                except:
                    # Last resort: try cite element text
                    try:
                        cite_elem = element.find_element(By.CSS_SELECTOR, "cite")
                        text = cite_elem.text.strip()
                        # Title is usually after the authors and before the venue
                        lines = text.split('\n')
                        for line in lines:
                            if len(line) > 20 and not line.startswith('http'):
                                paper_data['name'] = line.strip().rstrip('.')
                                break
                    except:
                        pass
            
            # Extract authors - DBLP uses span[itemprop='author']
            authors = []
            try:
                author_elements = element.find_elements(By.CSS_SELECTOR, "span[itemprop='author']")
                for auth_elem in author_elements:
                    try:
                        # Author name is in nested span[itemprop='name']
                        name_elem = auth_elem.find_element(By.CSS_SELECTOR, "span[itemprop='name']")
                        author_name = name_elem.get_attribute("title") or name_elem.text
                        if author_name and author_name.strip():
                            authors.append(author_name.strip())
                    except:
                        # Fallback to direct text
                        author_name = auth_elem.text.strip()
                        if author_name:
                            authors.append(author_name)
            except:
                pass
            
            if authors:
                paper_data['authors'] = ', '.join(authors)
            
            # Extract paper record link
            try:
                # Look for the DBLP record link
                rec_link = element.find_element(By.CSS_SELECTOR, "a[href*='/rec/']")
                paper_data['paper_href'] = rec_link.get_attribute("href")
            except:
                # Try other patterns
                try:
                    links = element.find_elements(By.TAG_NAME, "a")
                    for link in links:
                        href = link.get_attribute("href")
                        if href and "/rec/" in href:
                            paper_data['paper_href'] = href
                            break
                except:
                    pass
            
            # Extract year - look in the citation text
            try:
                cite_text = element.text
                year_match = re.search(r'\b(19|20)\d{2}\b', cite_text)
                if year_match:
                    paper_data['year'] = year_match.group()
            except:
                pass
            
            # Extract pages - look for page numbers
            try:
                cite_text = element.text
                pages_match = re.search(r'pp\.\s*(\d+(?:-\d+)?)', cite_text, re.IGNORECASE)
                if not pages_match:
                    pages_match = re.search(r'pages?\s+(\d+(?:-\d+)?)', cite_text, re.IGNORECASE)
                if pages_match:
                    paper_data['pages'] = pages_match.group(1)
            except:
                pass
            
            # Try to extract conference location and other details from text
            try:
                cite_text = element.text
                # Look for location patterns
                location_patterns = [
                    r'([A-Za-z\s]+),\s*([A-Z]{2,3}),\s*(USA|UK|Germany|France|Italy|Spain|Canada|Australia)',
                    r'([A-Za-z\s]+),\s*([A-Za-z\s]+),\s*\d{4}'
                ]
                
                for pattern in location_patterns:
                    match = re.search(pattern, cite_text)
                    if match:
                        paper_data['conference_location'] = match.group().strip()
                        break
            except:
                pass
            
            return paper_data
            
        except Exception as e:
            logger.debug(f"Error extracting paper data: {e}")
            return None
    
    def save_papers_to_db(self, papers):
        """Save scraped papers to the database - DEBUG VERSION"""
        print(f"DEBUG: save_papers_to_db called with {len(papers) if papers else 0} papers")
        
        if not papers:
            print("DEBUG: No papers to save, returning early")
            return
        
        try:
            print(f"DEBUG: Attempting to connect to database: {self.db_path}")
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='conf_papers'")
            table_exists = cursor.fetchone()
            print(f"DEBUG: conf_papers table exists: {table_exists is not None}")
            
            if not table_exists:
                print("ERROR: conf_papers table does not exist!")
                conn.close()
                return
            
            # Insert papers into conf_papers table
            insert_query = """
            INSERT OR REPLACE INTO conf_papers 
            (name, authors, conference_href, paper_href, year, isbn, pages, 
            conference_location, created_at, conference_processed, edition_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            successful_inserts = 0
            for i, paper in enumerate(papers):
                try:
                    cursor.execute(insert_query, (
                        paper['name'],
                        paper['authors'], 
                        paper['conference_href'],
                        paper['paper_href'],
                        paper['year'],
                        paper['isbn'],
                        paper['pages'],
                        paper['conference_location'],
                        paper['created_at'],
                        paper['conference_processed'],
                        paper['edition_name']
                    ))
                    successful_inserts += 1
                    
                    if i == 0:  # Log first paper details
                        print(f"DEBUG: First paper data - Name: '{paper['name'][:50]}...', Authors: '{paper['authors'][:50]}...'")
                        
                except Exception as e:
                    print(f"DEBUG: Error inserting paper {i}: {e}")
                    print(f"DEBUG: Paper data: {paper}")
            
            conn.commit()
            conn.close()
            
            print(f"DEBUG: Successfully inserted {successful_inserts}/{len(papers)} papers")
            logger.info(f"Saved {successful_inserts} papers to database")
            
        except Exception as e:
            print(f"ERROR: Database save failed: {e}")
            logger.error(f"Database save error: {e}")

    def run_scraper(self):
        """Main scraper function - DEBUG VERSION"""
        try:
            # Get conference URLs from database
            conferences = self.get_conference_hrefs()
            logger.info(f"Found {len(conferences)} conferences to process")
            
            for conf_name, conf_url in conferences:
                logger.info(f"Processing conference: {conf_name}")
                
                # Scrape content links from conference page
                content_links = self.scrape_conference_content_links(conf_url)
                
                if not content_links:
                    logger.warning(f"No content links found for {conf_name}")
                    continue
                
                logger.info(f"Found {len(content_links)} content links for {conf_name}")
                all_papers = []
                
                # Process each content link
                for i, link_data in enumerate(content_links):
                    try:
                        logger.info(f"Processing link {i+1}/{len(content_links)}: {link_data['title'][:60]}...")
                        papers = self.scrape_papers_from_content_page(
                            link_data['url'], 
                            conf_name
                        )
                        
                        if papers:
                            all_papers.extend(papers)
                            logger.info(f"Got {len(papers)} papers from this link. Total so far: {len(all_papers)}")
                            self.save_papers_to_db(all_papers)
                        else:
                            logger.warning(f"No papers found in link: {link_data['url']}")
                        
                        # Add delay between requests
                        time.sleep(2)
                        
                    except Exception as e:
                        logger.error(f"Error processing content link {link_data['url']}: {e}")
                        continue
                
                # DEBUG: Check what we have before saving
                print(f"\nDEBUG: About to save papers for {conf_name}")
                print(f"DEBUG: all_papers length: {len(all_papers)}")
                print(f"DEBUG: all_papers is None: {all_papers is None}")
                print(f"DEBUG: bool(all_papers): {bool(all_papers)}")
                
                if all_papers:
                    print(f"DEBUG: First paper name: '{all_papers[0]['name'][:50]}...'")
                    
                # Save papers to database
                if all_papers:
                    logger.info(f"Saving {len(all_papers)} papers to database for {conf_name}")
                    self.save_papers_to_db(all_papers)
                    logger.info(f" COMPLETED {conf_name}: {len(all_papers)} papers saved to database")
                else:
                    logger.warning(f" No papers found for {conf_name}")
                
                # Add delay between conferences
                time.sleep(3)
                
        except Exception as e:
            logger.error(f"Error in main scraper: {e}")
            import traceback
            print(f"DEBUG: Full traceback: {traceback.format_exc()}")
        finally:
            if self.driver:
                self.driver.quit()
                
def main():
    scraper = DBLPScraper('DBLP.db')  # Update path to your database
    scraper.run_scraper()

if __name__ == "__main__":
    main()