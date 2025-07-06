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

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DBLPJournalScraper:
    def __init__(self, db_path="DBLP.db"):
        self.db_path = db_path
        self.driver = None
        self.setup_driver()
        self.setup_database()
    
    def setup_driver(self):
        """Setup Chrome driver with options"""
        chrome_options = Options()
        # chrome_options.add_argument("--headless")  # Run in background
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.implicitly_wait(10)
            logger.info("Chrome driver initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {e}")
            raise
    
    def setup_database(self):
        """Create tables for storing scraped journal data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if journal_papers table exists and get its columns
        cursor.execute("PRAGMA table_info(journal_papers)")
        existing_columns = [column[1] for column in cursor.fetchall()]
        
        if existing_columns:
            logger.info(f"Found existing journal_papers table with columns: {existing_columns}")
            
            # Add missing columns to existing table
            additional_columns = {
                'journal_href': 'TEXT',
                'paper_href': 'TEXT',
                'year': 'TEXT',
                'volume': 'TEXT',
                'issue': 'TEXT',
                'pages': 'TEXT',
                'doi': 'TEXT',
                'journal_name': 'TEXT',
                'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
            }
            
            for column_name, column_type in additional_columns.items():
                if column_name not in existing_columns:
                    try:
                        cursor.execute(f'ALTER TABLE journal_papers ADD COLUMN {column_name} {column_type}')
                        logger.info(f"Added column {column_name} to journal_papers table")
                    except sqlite3.OperationalError as e:
                        logger.warning(f"Could not add column {column_name}: {e}")
        else:
            # Table doesn't exist, create it with complete structure
            cursor.execute('''
                CREATE TABLE journal_papers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    authors TEXT NOT NULL,
                    journal_href TEXT,
                    paper_href TEXT,
                    year TEXT,
                    volume TEXT,
                    issue TEXT,
                    pages TEXT,
                    doi TEXT,
                    journal_name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            logger.info("Created new journal_papers table")
        
        conn.commit()
        conn.close()
        logger.info("Database table created/verified for journals")
    
    def get_journal_hrefs(self):
        """Get all journal hrefs from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name, href FROM journals_hrefs")
        hrefs = cursor.fetchall()
        conn.close()
        
        logger.info(f"Retrieved {len(hrefs)} journal hrefs from database")
        return hrefs
    
    def extract_journal_info(self, page_source):
        """Extract journal information from the page"""
        journal_info = {}
        
        # Extract journal name
        try:
            journal_name_element = self.driver.find_element(By.CSS_SELECTOR, "h1")
            journal_info['name'] = journal_name_element.text.strip()
        except NoSuchElementException:
            journal_info['name'] = "Unknown Journal"
        
        # Extract additional metadata
        try:
            # Look for ISSN if present
            issn_match = re.search(r'ISSN\s+([\d-]+)', page_source)
            journal_info['issn'] = issn_match.group(1) if issn_match else ""
            
        except Exception as e:
            logger.warning(f"Error extracting journal metadata: {e}")
            journal_info['issn'] = ""
        
        return journal_info
    
    def find_volume_links(self):
        """Find all volume links from the journal main page"""
        try:
            volume_links = []
            
            # Look for volume links - these are typically in a list format
            volume_selectors = [
                "a[href*='Volume']",
                "a[href*='volume']",
                "a[href*='vol']",
                ".volume-link",
                "li a"  # Generic list links
            ]
            
            for selector in volume_selectors:
                try:
                    links = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for link in links:
                        link_text = link.text.strip()
                        link_href = link.get_attribute('href')
                        
                        # Check if this looks like a volume link
                        if (re.search(r'volume\s*\d+', link_text, re.IGNORECASE) or 
                            re.search(r'vol\.?\s*\d+', link_text, re.IGNORECASE) or
                            'volume' in link_href.lower()):
                            
                            volume_links.append({
                                'text': link_text,
                                'href': link_href
                            })
                    
                    if volume_links:
                        logger.info(f"Found {len(volume_links)} volume links using selector: {selector}")
                        break
                        
                except Exception:
                    continue
            
            # If no volume links found with CSS selectors, try XPath
            if not volume_links:
                try:
                    xpath_links = self.driver.find_elements(By.XPATH, "//a[contains(text(), 'Volume') or contains(text(), 'volume')]")
                    for link in xpath_links:
                        volume_links.append({
                            'text': link.text.strip(),
                            'href': link.get_attribute('href')
                        })
                    logger.info(f"Found {len(volume_links)} volume links using XPath")
                except Exception:
                    pass
            
            return volume_links
            
        except Exception as e:
            logger.error(f"Error finding volume links: {e}")
            return []
    
    def scrape_papers_from_volume(self, volume_url, journal_info, volume_info):
        """Scrape papers from a specific volume page"""
        try:
            self.driver.get(volume_url)
            time.sleep(2)
            
            papers = []
            
            # Look for paper entries - DBLP journals use specific classes
            paper_selectors = [
                ".entry.article",
                ".publ-list li",
                "li[class*='entry']",
                "cite.data",
                ".article-entry",
                ".paper-entry"
            ]
            
            paper_elements = []
            for selector in paper_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        paper_elements = elements
                        logger.info(f"Found {len(elements)} journal papers using selector: {selector}")
                        break
                except Exception:
                    continue
            
            if not paper_elements:
                logger.warning(f"No journal paper elements found in volume: {volume_info}")
                return papers
            
            for paper_element in paper_elements:
                try:
                    paper_data = self.extract_journal_paper_info(paper_element, journal_info, volume_info)
                    if paper_data:
                        papers.append(paper_data)
                except Exception as e:
                    logger.error(f"Error extracting journal paper info: {e}")
                    continue
            
            logger.info(f"Scraped {len(papers)} papers from volume: {volume_info}")
            return papers
            
        except Exception as e:
            logger.error(f"Error scraping papers from volume {volume_info}: {e}")
            return []
    
    def extract_journal_paper_info(self, paper_element, journal_info, volume_info=None):
        """Extract individual journal paper information"""
        paper_data = {}
        
        try:
            # Extract paper title
            title_selectors = [
                ".title",
                "span[itemprop='name']",
                "span.title",
                "cite span:first-child"
            ]
            
            paper_data['title'] = ""
            for selector in title_selectors:
                try:
                    title_element = paper_element.find_element(By.CSS_SELECTOR, selector)
                    paper_data['title'] = title_element.text.strip()
                    break
                except NoSuchElementException:
                    continue
            
            # If no title found with selectors, try to get first text content
            if not paper_data['title']:
                paper_data['title'] = paper_element.text.split('\n')[0].strip()
            
            # Extract authors
            authors = []
            author_selectors = [
                "span[itemprop='author']",
                ".author",
                "span.author",
                "a[href*='/pers/']"
            ]
            
            for selector in author_selectors:
                try:
                    author_elements = paper_element.find_elements(By.CSS_SELECTOR, selector)
                    for author_elem in author_elements:
                        author_name = author_elem.text.strip()
                        if author_name:
                            authors.append(author_name)
                    if authors:
                        break
                except NoSuchElementException:
                    continue
            
            paper_data['authors_text'] = ', '.join(authors)
            
            # Extract paper href
            try:
                paper_link = paper_element.find_element(By.CSS_SELECTOR, "a")
                paper_data['href'] = paper_link.get_attribute('href')
            except NoSuchElementException:
                paper_data['href'] = ""
            
            # Extract journal-specific information
            paper_text = paper_element.text
            
            # Extract year - try from volume_info first, then paper text
            if volume_info and 'year' in volume_info:
                paper_data['year'] = volume_info['year']
            else:
                year_match = re.search(r'\b(19|20)\d{2}\b', paper_text)
                paper_data['year'] = year_match.group() if year_match else ""
            
            # Extract volume - try from volume_info first, then paper text
            if volume_info and 'volume' in volume_info:
                paper_data['volume'] = volume_info['volume']
            else:
                volume_match = re.search(r'Vol\.?\s*(\d+)', paper_text, re.IGNORECASE)
                paper_data['volume'] = volume_match.group(1) if volume_match else ""
            
            # Extract issue/number
            issue_match = re.search(r'(?:No\.?|Issue)\s*(\d+)', paper_text, re.IGNORECASE)
            paper_data['issue'] = issue_match.group(1) if issue_match else ""
            
            # Extract pages
            pages_match = re.search(r'(\d+-\d+)', paper_text)
            paper_data['pages'] = pages_match.group(1) if pages_match else ""
            
            # Extract DOI if present
            doi_match = re.search(r'doi[:\s]*(10\.\d+/[^\s]+)', paper_text, re.IGNORECASE)
            paper_data['doi'] = doi_match.group(1) if doi_match else ""
            
            # Add journal info
            paper_data['journal_name'] = journal_info.get('name', '')
            
            return paper_data
            
        except Exception as e:
            logger.error(f"Error extracting journal paper info: {e}")
            return None
    
    def save_journal_papers_to_db(self, papers, journal_href):
        """Save scraped journal papers to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get existing columns to build dynamic query
        cursor.execute("PRAGMA table_info(journal_papers)")
        columns_info = cursor.fetchall()
        existing_columns = [column[1] for column in columns_info]
        
        logger.info(f"Available columns in journal_papers: {existing_columns}")
        
        for paper in papers:
            try:
                # Prepare data for insertion based on existing columns
                insert_data = {}
                
                # Map data to existing columns
                if 'name' in existing_columns:
                    insert_data['name'] = paper.get('title', '')[:500]  # Truncate if too long
                
                if 'authors' in existing_columns:
                    insert_data['authors'] = paper.get('authors_text', '')[:1000]  # Truncate if too long
                
                # Add additional columns if they exist
                if 'journal_href' in existing_columns:
                    insert_data['journal_href'] = journal_href
                
                if 'paper_href' in existing_columns:
                    insert_data['paper_href'] = paper.get('href', '')
                
                if 'year' in existing_columns:
                    insert_data['year'] = paper.get('year', '')
                
                if 'volume' in existing_columns:
                    insert_data['volume'] = paper.get('volume', '')
                
                if 'issue' in existing_columns:
                    insert_data['issue'] = paper.get('issue', '')
                
                if 'pages' in existing_columns:
                    insert_data['pages'] = paper.get('pages', '')
                
                if 'doi' in existing_columns:
                    insert_data['doi'] = paper.get('doi', '')
                
                if 'journal_name' in existing_columns:
                    insert_data['journal_name'] = paper.get('journal_name', '')
                
                # Build dynamic INSERT query
                if insert_data:
                    columns = ', '.join(insert_data.keys())
                    placeholders = ', '.join(['?' for _ in insert_data])
                    values = list(insert_data.values())
                    
                    query = f'INSERT INTO journal_papers ({columns}) VALUES ({placeholders})'
                    cursor.execute(query, values)
                
            except Exception as e:
                logger.error(f"Error saving journal paper '{paper.get('title', 'Unknown')}' to database: {e}")
                continue
        
        conn.commit()
        conn.close()
        logger.info(f"Saved {len(papers)} journal papers to database")
    
    def scrape_journal(self, journal_name, journal_href):
        """Scrape a single journal by going through all its volumes"""
        try:
            logger.info(f"Scraping journal: {journal_name}")
            logger.info(f"URL: {journal_href}")
            
            # Navigate to journal main page
            self.driver.get(journal_href)
            time.sleep(2)
            
            # Extract journal information
            journal_info = self.extract_journal_info(self.driver.page_source)
            
            # Find all volume links from the main page
            volume_links = self.find_volume_links()
            
            if not volume_links:
                logger.warning(f"No volume links found for {journal_name}")
                return
            
            logger.info(f"Found {len(volume_links)} volumes for {journal_name}")
            
            all_papers = []
            
            # Scrape each volume
            for i, volume_link in enumerate(volume_links, 1):
                try:
                    logger.info(f"Scraping volume {i}/{len(volume_links)}: {volume_link['text']}")
                    
                    # Extract volume information from the link text
                    volume_info = self.extract_volume_info(volume_link['text'])
                    
                    # Scrape papers from this volume
                    volume_papers = self.scrape_papers_from_volume(
                        volume_link['href'], 
                        journal_info, 
                        volume_info
                    )
                    
                    all_papers.extend(volume_papers)
                    
                    # Add delay between volume requests
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error scraping volume {volume_link['text']}: {e}")
                    continue
            
            if all_papers:
                # Save all papers to database
                self.save_journal_papers_to_db(all_papers, journal_href)
                logger.info(f"Successfully scraped {len(all_papers)} papers from {journal_name}")
            else:
                logger.warning(f"No papers found for {journal_name}")
                
        except Exception as e:
            logger.error(f"Error scraping journal {journal_name}: {e}")
    
    def extract_volume_info(self, volume_text):
        """Extract volume and year information from volume link text"""
        volume_info = {}
        
        # Extract volume number
        volume_match = re.search(r'volume\s*(\d+)', volume_text, re.IGNORECASE)
        if volume_match:
            volume_info['volume'] = volume_match.group(1)
        else:
            # Try alternative patterns
            vol_match = re.search(r'vol\.?\s*(\d+)', volume_text, re.IGNORECASE)
            volume_info['volume'] = vol_match.group(1) if vol_match else ""
        
        # Extract year
        year_match = re.search(r'\b(19|20)\d{2}\b', volume_text)
        if year_match:
            volume_info['year'] = year_match.group()
        else:
            # Try to extract year range (e.g., "2017-2019")
            year_range_match = re.search(r'\b(19|20)\d{2}-(19|20)?\d{2}\b', volume_text)
            if year_range_match:
                # Use the first year of the range
                volume_info['year'] = year_range_match.group().split('-')[0]
            else:
                volume_info['year'] = ""
        
        return volume_info
    
    def scrape_all_journals(self):
        """Main method to scrape all journals"""
        try:
            # Get all journal hrefs
            journal_hrefs = self.get_journal_hrefs()
            
            logger.info(f"Starting to scrape {len(journal_hrefs)} journals")
            
            for i, (journal_name, journal_href) in enumerate(journal_hrefs, 1):
                logger.info(f"Processing {i}/{len(journal_hrefs)}: {journal_name}")
                
                try:
                    self.scrape_journal(journal_name, journal_href)
                    
                    # Add delay between requests to be respectful
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Failed to scrape {journal_name}: {e}")
                    continue
            
            logger.info("Finished scraping all journals")
            
        except Exception as e:
            logger.error(f"Error in scrape_all_journals: {e}")
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("Driver closed")

def main():
    """Main function to run the journal scraper"""
    try:
        # Initialize scraper
        scraper = DBLPJournalScraper("DBLP.db")  # Replace with your database path
        
        # Start scraping
        scraper.scrape_all_journals()
        
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    main()