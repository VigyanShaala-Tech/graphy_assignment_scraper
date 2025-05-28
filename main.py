import logging
import yaml
from src.scrapers.graphy.assignments import GraphyAssignmentScraper

def load_config():
    """Loads the .yml configuration file."""
    with open('config.yml', 'r') as file:
        return yaml.safe_load(file)

def run_graphy_assignment_scraper(email, password, assignment_id):
    scraper = GraphyAssignmentScraper(email, password)
    scraper.login()
    
    if assignment_id == "meta":
        # Only scrape and save assignment metadata
        assignments = scraper.fetch_assignments()
        scraper.save_assignment_metadata(assignments)
        logging.info("Metadata scraping completed.")
    else:
        # Scrape submissions for the given assignment id
        scraper.run(assignment_id)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = load_config()
    cfg = config['graphy_assignment_scraper']
    run_graphy_assignment_scraper(cfg['email'], cfg['password'], cfg['assignment_id'])
