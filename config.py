import os

class Config:

    UPLOAD_FOLDER = 'uploads'
    MAX_CONTENT_LENGTH = 5 * 1024 * 1024 # 5 MB limit
    ALLOWED_EXTENSIONS = {'pdf', 'docx', 'doc', 'rtf'}
    TOP_N_FOR_LLM = 30
    DEFAULT_JOB_SCRAPE_URL = "https://merojob.com/search/?q=&industry=12&industry=26&industry=40&industry=41&industry=42&industry=91"

    # Ensure upload folder exists
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)