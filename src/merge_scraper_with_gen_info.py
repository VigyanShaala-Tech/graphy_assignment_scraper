import pandas as pd

# File paths
scraper_csv_path = r"output\graphy\assignments\multiple_assignments_20250529_214339.csv"
excel_path = r"D:\VigyanShaala\graphy_assignment_scraper\output\graphy\TSTT 7.0.xlsx"
output_csv_path = "output/graphy/assignments/merged_assignment_submissions.csv"

# Load data
scraper_df = pd.read_csv(scraper_csv_path)
excel_df = pd.read_excel(excel_path)

# Merge only the needed columns from Excel into the scraper data
merged_df = pd.merge(
    scraper_df,
    excel_df[['Email', 'Name of the Student', 'Name of the college']],
    on='Email',
    how='left'
)

# Save the result
merged_df.to_csv(output_csv_path, index=False)
print(f"Merged file saved to: {output_csv_path}")
