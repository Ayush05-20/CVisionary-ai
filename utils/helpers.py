import os
import mysql.connector
import logging
from flask import flash
import json

logger = logging.getLogger(__name__)
ALLOWED_EXTENSIONS = {'pdf', 'docx', 'doc', 'rtf'}
def allowed_file(filename):
    """Checks if the uploaded file's extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password="my$qlayush1",
            database='jobs'
        )
        return conn
    except mysql.connector.Error as e:
        logger.error(f"Error connecting to MySQL database: {e}", exc_info=True)
        flash(f"Database connection error: {e}", 'error')
        return None

def fetch_jobs_from_db() -> list[dict]:
    """Fetches job listings from the MySQL database."""
    conn = None
    cur = None
    job_listings_data = []
    try:
        conn = get_db_connection()
        if not conn:
            return []

        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM jobs")
        jobs = cur.fetchall()
        seen_jobs = set()

        for job in jobs:
            def safe_json_load(field_name):
                try:
                    # Handle cases where the field might be None or an empty string
                    data = job.get(field_name)
                    return json.loads(data) if data else []
                except json.JSONDecodeError:
                    logger.warning(f"Failed to decode JSON for {field_name} (URL: {job.get('url')}). Setting to empty list.")
                    return []

            job_details = {
                "url": job.get('url', ''),
                "job_title": job.get('title', '').strip(),
                "job_cat": job.get('job_cat', '').strip(),
                "location": job.get('location', '').strip(),
                "company": job.get('company', '').strip(),
                "education": job.get('education', '').strip(),
                "experience": job.get('experience', '').strip(),
                "skills_required": safe_json_load('skills'),
                "general_requirements": safe_json_load('general_requirements'),
                "specific_requirements": safe_json_load('specific_requirements'),
                "job_description_duties": safe_json_load('dis'),
                "job_description_responsibilities": safe_json_load('responsibilities')
            }
            
            description_parts = []
            if job_details['job_description_duties']: description_parts.append("Duties:\n" + "\n".join(f"- {item}" for item in job_details['job_description_duties']))
            if job_details['job_description_responsibilities']: description_parts.append("Responsibilities:\n" + "\n".join(f"- {item}" for item in job_details['job_description_responsibilities']))
            if job_details['general_requirements']: description_parts.append("General Requirements:\n" + "\n".join(f"- {item}" for item in job_details['general_requirements']))
            if job_details['specific_requirements']: description_parts.append("Specific Requirements:\n" + "\n".join(f"- {item}" for item in job_details['specific_requirements']))
            job_details["job_description"] = "\n\n".join(description_parts).strip()

            exp_str = job_details["experience"].lower()
            experience_level = ""
            if "entry" in exp_str or "fresh" in exp_str or "0-" in exp_str or "less than 1" in exp_str: experience_level = "Entry Level"
            elif "mid" in exp_str or "2-" in exp_str or "3-" in exp_str: experience_level = "Mid Level"
            elif "senior" in exp_str or "5+" in exp_str or "lead" in exp_str: experience_level = "Senior Level"
            job_details["experience_level"] = experience_level

            job_key = (job_details["job_title"].lower(), job_details["company"].lower(), job_details["location"].lower())
            if job_key not in seen_jobs:
                seen_jobs.add(job_key)
                job_listings_data.append(job_details)
        
        logger.info(f"Successfully fetched {len(job_listings_data)} unique jobs from the MySQL database.")
        return job_listings_data
    except mysql.connector.Error as e:
        logger.error(f"Error fetching jobs from MySQL database: {e}", exc_info=True)
        return []
    finally:
        if cur: cur.close()
        if conn: conn.close()