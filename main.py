import logging
import yaml
from src.scrapers.graphy.assignments import GraphyAssignmentScraper

def load_config():
    """Loads the .yml configuration file."""
    with open('config.yml', 'r') as file:
        return yaml.safe_load(file)

def run_graphy_assignment_scraper(email, password, assignment_ids):
    scraper = GraphyAssignmentScraper(email, password)
    scraper.login()
    
    if isinstance(assignment_ids, str) and assignment_ids == "meta":
        # Only scrape and save assignment metadata
        assignments = scraper.fetch_assignments()
        scraper.save_assignment_metadata(assignments)
        logging.info("Metadata scraping completed.")
    else:
        # Convert single assignment_id to list if it's not already
        if isinstance(assignment_ids, str):
            assignment_ids = [assignment_ids]
        
        # Scrape submissions for all given assignment ids
        scraper.run_multiple(assignment_ids)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = load_config()
    cfg = config['graphy_assignment_scraper']
    run_graphy_assignment_scraper(cfg['email'], cfg['password'], cfg['assignment_id'])
