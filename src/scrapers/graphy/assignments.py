import os
import csv
import json
import logging
import requests
from datetime import datetime

class GraphyAssignmentScraper:
    LOGIN_URL = "https://mytribe.vigyanshaala.com/s/authenticate"
    COURSE_ASSETS_API = "https://mytribe.vigyanshaala.com/s/courseassets"
    BASE_URL_TEMPLATE = "https://mytribe.vigyanshaala.com/s/assignments/{assignment_id}/submissions"

    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password
        self.session = requests.Session()
        self.output_dir = "output/graphy/assignments"
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
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

    def fetch_assignments(self) -> list:
        """Fetch all assignment objects from the course assets table."""
        assignments = []
        start = 0
        length = 100
        while True:
            params = {
                "draw": 1,
                "start": start,
                "length": length,
                "search[value]": "",
                "search[regex]": "false",
                "queries": '{"spayee:resource.spayee:courseAssetType":"assignment","reviewCount":true}',
                "timezoneOffset": -330,
                "sortBy": "reviewCount.underreview",
                "sortDir": -1
            }
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
                "Referer": "https://mytribe.vigyanshaala.com/s/courseassets",
                "X-Requested-With": "XMLHttpRequest"
            }

            try:
                response = self.session.get(self.COURSE_ASSETS_API, headers=headers, params=params)
                response.raise_for_status()
                data = response.json().get("data", [])
                if not data:
                    break
                assignments.extend(data)
                start += length
            except Exception as e:
                logging.error(f"Failed to fetch assignments: {e}")
                break

        logging.info(f"Fetched {len(assignments)} total assignments.")
        return assignments

    def save_assignment_metadata(self, assignments: list):
        """Save all assignment metadata into a CSV file and raw JSON file."""
        output_file = os.path.join(self.output_dir, f"all_assignments_metadata_{self.timestamp}.csv")
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            headers = ['_id', 'title', 'courseId', 'courseTitle', 'courseAssetType', 'createdAt', 'updatedAt',
                    'createdById', 'createdByName', 'createdByEmail', 'reviewed', 'rejected', 'underReview']
            writer.writerow(headers)
            for item in assignments:
                writer.writerow([
                    item.get('_id', ''),
                    item.get('spayee:resource', {}).get('spayee:title', ''),
                    # courses is a list; take first course's _id and title if available
                    item.get('courses', [{}])[0].get('_id', ''),
                    item.get('courses', [{}])[0].get('spayee:resource', {}).get('spayee:title', ''),
                    item.get('spayee:resource', {}).get('spayee:courseAssetType', ''),
                    item.get('createdDate', {}).get('$date', ''),
                    item.get('modifiedDate', {}).get('$date', ''),
                    item.get('createdBy', {}).get('_id', ''),
                    item.get('createdBy', {}).get('fname', ''),
                    item.get('createdBy', {}).get('email', ''),
                    item.get('reviewCount', {}).get('reviewed', 0),
                    item.get('reviewCount', {}).get('rejected', 0),
                    item.get('reviewCount', {}).get('underreview', 0),
                ])
        logging.info(f"Assignment metadata saved to: {output_file}")

    def fetch_submissions(self, assignment_id: str, start: int, length: int = 50):
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
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
        url = self.BASE_URL_TEMPLATE.format(assignment_id=assignment_id)
        try:
            response = self.session.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json().get("data", [])
        except Exception as e:
            logging.error(f"Error fetching submissions for {assignment_id} (start={start}): {e}")
            return None

    def run(self, assignment_id=None):
        """Main execution method for a single assignment."""
        if not assignment_id:
            logging.error("No assignment_id provided to run method.")
            return

        output_file = os.path.join(self.output_dir, f"assignment_{assignment_id}_{self.timestamp}.csv")
        logging.info(f"Scraping submissions for assignment {assignment_id}")
        
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                'assignment_id', 'id', 'student_id', 'Email', 'student_name', 'course_id', 'mentor_id', 'cohort_code',
                'submission_status', 'marks', 'feedback_comments',
                'submitted_at', 'file_name', 'assignment_file'
            ])

            start = 0
            while True:
                data = self.fetch_submissions(assignment_id, start)
                if data is None or not data:
                    break
                self.write_to_csv(writer, data, assignment_id)
                logging.info(f"Fetched {len(data)} submissions for assignment {assignment_id} (start={start})")
                start += 50

        logging.info(f"Finished scraping assignment {assignment_id}. Saved to {output_file}")

    def run_multiple(self, assignment_ids):
        """Main execution method for multiple assignments."""
        if not assignment_ids:
            logging.error("No assignment_ids provided to run_multiple method.")
            return

        output_file = os.path.join(self.output_dir, f"multiple_assignments_{self.timestamp}.csv")
        logging.info(f"Scraping submissions for {len(assignment_ids)} assignments")
        
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                'assignment_id', 'id', 'student_id', 'Email', 'student_name', 'course_id', 'mentor_id', 'cohort_code',
                'submission_status', 'marks', 'feedback_comments',
                'submitted_at', 'file_name', 'assignment_file'
            ])

            for assignment_id in assignment_ids:
                logging.info(f"Processing assignment {assignment_id}")
                start = 0
                while True:
                    data = self.fetch_submissions(assignment_id, start)
                    if data is None or not data:
                        break
                    self.write_to_csv(writer, data, assignment_id)
                    logging.info(f"Fetched {len(data)} submissions for assignment {assignment_id} (start={start})")
                    start += 50

        logging.info(f"Finished scraping all assignments. Saved to {output_file}")

    def write_to_csv(self, writer, data, assignment_id):
        for item in data:
            id = item.get('_id')
            student_id = item.get('user', {}).get('_id')
            # Remove the prefix from email if it exists
            student_email = item.get('user', {}).get('email', '')
            if student_email and student_email.startswith('vigyanshaalainternational1617-'):
                student_email = student_email.replace('vigyanshaalainternational1617-', '')
            student_name = item.get('user', {}).get('fname')
            course_id = item.get('courseId')
            mentor_id = item.get('data', [{}])[-1].get('adminId') if item.get('data') else None
            cohort_code = None  # Placeholder

            for submission in item.get('data', []):
                submission_status = submission.get('status', '')
                marks = submission.get('marks', '')
                feedback = submission.get('message', '').replace('\n', ' ').strip()
                submitted_at = submission.get('date', {}).get('$date')
                file_name = submission.get('fileName', '')
                assignment_file = submission.get('filePath', '')

                writer.writerow([
                    assignment_id,
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
