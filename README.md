# 📘 Graphy Assignment Submissions Scraper

This Python scraper automates the process of logging into the **VigyanShaala (Graphy)** platform, fetching assignment submission records for a specific assignment, and saving them to a structured CSV file suitable for analysis or import into databases like PostgreSQL.

---

## 🚀 Features

- 🔐 Secure login to the Graphy platform
- 📥 Paginated fetching of assignment submissions
- 📄 CSV output with structured student data, submission status, feedback, and more
- 🧾 Logs activity and errors to both console and file
- 🗂️ Output stored with timestamps for version tracking

---

## 🏗️ Project Structure

```
project-root/
├── main.py                              # Entry point to run the scraper
├── config.yml                           # Configuration file for credentials and assignment ID
├── logs/
│   └── vigyanshaala_scraper.log         # Log file
├── output/
│   └── graphy/
│       └── assignments/
│           └── assignment_submissions_<timestamp>.csv
└── src/
    └── scrapers/
        └── graphy/
            └── assignments.py           # Core scraper logic
```

---

## ⚙️ Configuration

Create a `config.yml` in the root directory with your credentials and assignment ID:

```yaml
graphy_assignment_scraper:
  email: "your_email@example.com"
  password: "your_secure_password"
  assignment_id: "your_assignment_id_here"
```

> 🔒 **Do not share this file publicly.**

---

## ▶️ How to Run

### 🧱 Requirements

Install dependencies:

```bash
pip install -r requirements.txt
```

### ▶️ Run the scraper

```bash
python main.py
```

---

## 📤 Output CSV Format

The output CSV is saved inside `output/graphy/assignments/` and includes the following columns:

| Column             | Description                                      |
|--------------------|--------------------------------------------------|
| id                 | Auto-increment submission ID                     |
| student_id         | ID of the learner who submitted the assignment   |
| resource_id        | Associated course ID                             |
| mentor_id          | ID of last admin who reviewed                    |
| cohort_code        | Placeholder field (not extracted currently)      |
| submission_status  | Submission status (e.g., under-review)           |
| marks              | Placeholder field (not extracted currently)      |
| feedback_comments  | Admin feedback message                           |
| submitted_at       | Submission timestamp                             |
| assignment_file    | Link to uploaded file by the student             |

---

## 🪵 Logging

Logs are written to both console and file:

```
logs/vigyanshaala_scraper.log
```

Example:
```
2025-05-27 15:32:10 [INFO] Login successful.
2025-05-27 15:32:12 [INFO] Fetched and saved 50 records from start=0
```

---

## 🧪 Sample Usage in Code

```python
from src.scrapers.graphy.assignments import GraphyAssignmentScraper

scraper = GraphyAssignmentScraper("email", "password", "assignment_id")
scraper.run()
```

---

