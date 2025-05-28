import os
import csv
import logging
import requests
from datetime import datetime

class GraphyAssignmentScraper:
    LOGIN_URL = "https://mytribe.vigyanshaala.com/s/authenticate"
    BASE_URL_TEMPLATE = "https://mytribe.vigyanshaala.com/s/assignments/{assignment_id}/submissions"

    def __init__(self, email: str, password: str, assignment_id: str):
        self.email = email
        self.password = password
        self.assignment_id = assignment_id
        self.session = requests.Session()
        self.output_dir = "output/graphy/assignments"
        self.output_file = f"{self.output_dir}/assignment_submissions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        os.makedirs("logs", exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler("logs/vigyanshaala_scraper.log"),
                logging.StreamHandler()
            ]
        )

    def login(self):
        payload = {
            "email": self.email,
            "password": self.password,
            "age": "",
            "url": "/t/public/login"
        }
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://mytribe.vigyanshaala.com",
            "Referer": "https://mytribe.vigyanshaala.com/t/public/login"
        }
        response = self.session.post(self.LOGIN_URL, headers=headers, data=payload)
        if response.status_code == 200:
            logging.info("Login successful.")
        else:
            logging.error(f"Login failed with status code {response.status_code}")
            raise Exception("Login failed")

    def fetch_submissions(self, start: int, length: int = 50):
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": "https://mytribe.vigyanshaala.com/s/assignments",
            "X-Requested-With": "XMLHttpRequest"
        }
        params = {
            "draw": 1,
            "start": start,
            "length": length,
            "search[value]": "",
            "search[regex]": "false",
            "queries": "{}"
        }
        url = self.BASE_URL_TEMPLATE.format(assignment_id=self.assignment_id)
        try:
            response = self.session.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json().get("data", [])
        except Exception as e:
            logging.error(f"Error fetching data for start={start}: {e}")
            return None

    def write_to_csv(self, writer, data):
        
        # Header row for your database-compatible CSV
        writer.writerow([
            'id','student_id','student_email','student_name', 'course_id', 'mentor_id', 'cohort_code',
            'submission_status', 'marks', 'feedback_comments',
            'submitted_at', 'file_name','assignment_file'
        ])

        for item in data:
            
            id = item.get('_id') 
            student_id = item.get('user', {}).get('_id') 
            student_email = item.get('user', {}).get('email') 
            student_name = item.get('user', {}).get('fname') 
            course_id = item.get('courseId')  # Assuming courseId maps to resource
            mentor_id = item.get('data', [{}])[-1].get('adminId')  # Latest adminId if present
            cohort_code = None  # Not in JSON, placeholder or look up elsewhere

            for submission in item.get('data', []):
                submission_status = submission.get('status', 'under-review')
                marks = None  # Not in JSON; could be filled later
                feedback = submission.get('message', '').replace('\n', ' ').strip()
                submitted_at = submission.get('date', {}).get('$date')
                file_name = submission.get('fileName', '')
                assignment_file = submission.get('filePath', '')

                writer.writerow([
                    id,
                    student_id,
                    student_email,
                    student_name,
                    course_id,
                    mentor_id,
                    cohort_code,
                    submission_status,
                    marks,
                    feedback,
                    submitted_at,
                    file_name,
                    assignment_file
                ])

    def run(self):
        self.login()
        start = 0
        length = 50

        with open(self.output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            
            while True:
                data = self.fetch_submissions(start, length)
                if data is None:
                    logging.error("Stopping due to fetch error.")
                    break
                if not data:
                    logging.info("No more data found. Scraping complete.")
                    break
                self.write_to_csv(writer, data)
                logging.info(f"Fetched and saved {len(data)} records from start={start}")
                start += length

        logging.info(f"All data saved to {self.output_file}")
