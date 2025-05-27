import asyncio
import logging
import yaml
from src.scrapers.graphy.assignments import GraphyAssignmentScraper

def load_config():
    """Loads the .yml configuration file."""
    with open('config.yml', 'r') as file:
        return yaml.safe_load(file)

def run_graphy_assignment_scraper(email, password, assignment_id):
    """Run the Graphy Assignment scraper."""
    try:
        scraper = GraphyAssignmentScraper(email, password, assignment_id)
        scraper.run()
    except Exception as e:
        logging.error(f"Scraper terminated with error: {e}")

if __name__ == "__main__":
    # Load the configuration file
    config = load_config()

    scraper = 1  
    
    if scraper == 1:
        # Use Graphy Assignment Scraper
        email = config['graphy_assignment_scraper']['email']
        password = config['graphy_assignment_scraper']['password']
        assignment_id = config['graphy_assignment_scraper']['assignment_id']
        run_graphy_assignment_scraper(email, password, assignment_id)
    else:
        logging.error("Invalid scraper selection.")
