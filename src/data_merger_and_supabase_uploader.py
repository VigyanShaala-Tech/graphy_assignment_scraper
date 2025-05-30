import pandas as pd
import logging
from datetime import datetime
import os
from supabase_integration import SupabaseUploader

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

def merge_and_upload_data():
    try:
        # File paths
        # NOTE: Update the scraper_csv_path with the correct timestamp after running the scraper
        scraper_csv_path = r"D:\graphy_assignment_scraper\output\graphy\assignments\multiple_assignments_20250530_111403.csv"
        excel_path = r"D:\graphy_assignment_scraper\output\TSTT 7.0.xlsx"
        output_csv_path = "output/graphy/assignments/merged_assignment_submissions.csv"

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

        # Load data
        logger.info("Loading data from CSV and Excel files...")
        # Using keep_default_na=False to prevent pandas from interpreting empty strings as NaN
        scraper_df = pd.read_csv(scraper_csv_path, keep_default_na=False)
        excel_df = pd.read_excel(excel_path, keep_default_na=False)

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