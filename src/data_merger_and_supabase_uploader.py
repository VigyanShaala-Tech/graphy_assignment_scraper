import pandas as pd
import logging
from datetime import datetime
import os
from src.supabase_integration import SupabaseUploader
import yaml
import glob

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/merge_scraper_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from config.yml"""
    with open('config.yml', 'r') as file:
        return yaml.safe_load(file)

def get_latest_file(pattern):
    """Get the most recent file matching the pattern"""
    files = glob.glob(pattern)
    if not files:
        logger.warning(f"No files found matching pattern: {pattern}")
        return None
    return max(files, key=os.path.getctime)

def merge_and_upload_data():
    try:
        # Load configuration
        config = load_config()
        mode = config.get('mode', 'scrape-and-upload')  # Default to scrape-and-upload if not specified
        
        # Get the latest files
        scraper_csv_path = get_latest_file("output/graphy/assignments/multiple_assignments_*.csv")
        metadata_csv_path = get_latest_file("output/graphy/assignments/all_assignments_metadata_*.csv")
        excel_path = "output/TSTT 7.0.xlsx"
        output_csv_path = "output/graphy/assignments/merged_assignment_submissions.csv"

        if not scraper_csv_path:
            raise FileNotFoundError("No scraper CSV files found. Please run the scraper first.")

        logger.info(f"Using scraper CSV: {scraper_csv_path}")
        logger.info(f"Using Excel file: {excel_path}")

        # If mode is scrape-only, exit after scraping
        if mode == 'scrape-only':
            logger.info("Running in scrape-only mode. Skipping merge and upload process.")
            return

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

        # Load data
        logger.info("Loading data from CSV and Excel files...")
        # Using keep_default_na=False to prevent pandas from interpreting empty strings as NaN
        scraper_df = pd.read_csv(scraper_csv_path, keep_default_na=False)
        excel_df = pd.read_excel(excel_path, keep_default_na=False)

        # If metadata file exists, use it for assignment names
        if metadata_csv_path:
            logger.info(f"Using metadata CSV: {metadata_csv_path}")
            metadata_df = pd.read_csv(metadata_csv_path, keep_default_na=False)
            
            # Rename _id to assignment_id in metadata for merging
            metadata_df = metadata_df.rename(columns={'_id': 'assignment_id'})
            
            # Merge to get assignment names
            scraper_df = pd.merge(
                scraper_df,
                metadata_df[['assignment_id', 'title']],
                on='assignment_id',
                how='left'
            )
            
            # Rename title to assignment_name
            scraper_df = scraper_df.rename(columns={'title': 'assignment_name'})
            
            # Log the total number of assignments mapped
            logger.info(f"Total assignments mapped: {len(metadata_df)}")
        else:
            logger.warning("No metadata file found. Assignment names will not be included.")
            scraper_df['assignment_name'] = 'Unknown'  # Add a default assignment name

        # Merge only the needed columns from Excel into the scraper data
        logger.info("Merging data...")
        merged_df = pd.merge(
            scraper_df,
            excel_df[['Email', 'Name of the Student', 'Name of the college']],
            on='Email',
            how='left'
        )

        # Save the result
        merged_df.to_csv(output_csv_path, index=False)
        logger.info(f"Merged file saved to: {output_csv_path}")

        # Rename columns to match Supabase table structure
        merged_df.rename(columns={
            "Email": "student_email",
            "Name of the Student": "student_name",
            "Name of the college": "college_name",
            "submitted_at": "submitted_at" 
        }, inplace=True)

        supabase_columns = [
            "id", 
            "assignment_id", 
            "student_email", 
            "student_name",
            "college_name", 
            "submission_status", 
            "feedback_comments",  
            "submitted_at",
            "assignment_name" 
        ]
        
        # Ensure all supabase_columns exist in merged_df before filtering
        cols_to_select = [col for col in supabase_columns if col in merged_df.columns]
        filtered_df = merged_df[cols_to_select]
    
        # Upload to Supabase
        logger.info("Uploading data to Supabase...")
        uploader = SupabaseUploader(config_path="config.yml")
        uploader.upload_dataframe(filtered_df, batch_size=50)
        logger.info("Data upload completed successfully")

    except Exception as e:
        logger.error(f"Error in merge_and_upload_data: {str(e)}")
        raise

if __name__ == "__main__":
    merge_and_upload_data()