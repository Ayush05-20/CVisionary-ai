from flask import Flask, render_template, redirect, url_for, request, session, flash, jsonify, send_file
from firebase_admin import auth, initialize_app, credentials
import os
import logging
from werkzeug.utils import secure_filename
import io
import json 
from weasyprint import HTML 
from flask_session import Session

# Configure logging for app.py
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import custom modules
# Adjust import paths based on your actual project structure if different
from resume_scraper.resume_parser import parse_resume_from_file, generate_resume_summary, infer_career_interests
from models.job_matcher import ResumeJobMatcher
from utils.helpers import allowed_file, fetch_jobs_from_db
from config import Config # Assuming Config is in config.py in the same directory or accessible via PYTHONPATH

# Initialize Firebase (assuming your credentials file is correctly placed)
try:
    cred = credentials.Certificate("cvisionary-d034a-firebase-adminsdk-fbsvc-dca53ec298.json")
    firebase_app = initialize_app(cred)
    logger.info("Firebase initialized successfully.")
except Exception as e:
    logger.error(f"Error initializing Firebase: {e}")
    firebase_app = None # Ensure firebase_app is None if initialization fails

app = Flask(__name__)
app.secret_key = 'your_super_secret_key_here' # **IMPORTANT**: Change this to a strong, random key in production!

# Configure upload settings from Config class
app.config['UPLOAD_FOLDER'] = Config.UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = Config.MAX_CONTENT_LENGTH

# Initialize ResumeJobMatcher globally (or per request, but globally is fine for now)
matcher = ResumeJobMatcher()

# Ensure the upload folder exists on startup
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Configure Flask-Session
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

# --- Existing Routes ---
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/index')
def index():
    return render_template('index.html')

@app.route('/features')
def features():
    return render_template('features.html')

@app.route('/login')
def login_page():
    return render_template('login-signup.html', tab='login')

@app.route('/signup')
def signup_page():
    return render_template('login-signup.html', tab='signup')

@app.route('/login-signup')
def login_signup():
    tab = request.args.get('tab', 'login')
    return render_template('login-signup.html', tab=tab)

@app.route('/howitworks')
def howitworks():
    return render_template('howitworks.html')

@app.route('/pricing')
def pricing():
    return render_template('pricing.html')

@app.route('/about-us')
def about_us():
    return render_template('about-us.html')

@app.route('/contact-us')
def contact_us():
    return render_template('contact-us.html')

@app.route('/activity2')
def activity2():
    return render_template('activity2.html')

# --- Modified /upload route for GET requests (serves the page) ---
@app.route('/upload', methods=['GET'])
def upload_page():
    return render_template('upload.html')

# --- New /process-resume route for POST requests (handles upload & AI processing) ---
@app.route('/process-resume', methods=['POST'])
def process_resume():
    logger.info("Received request to /process-resume")

    if 'cv-file' not in request.files:
        logger.warning("No file part in the request.")
        return jsonify({"error": "No file part"}), 400

    file = request.files['cv-file']

    if file.filename == '':
        logger.warning("No selected file.")
        return jsonify({"error": "No selected file"}), 400

    if file and allowed_file(file.filename):
        filename_secured = secure_filename(file.filename)
        # Create a temporary file path for processing
        filepath_temp = os.path.join(app.config['UPLOAD_FOLDER'], filename_secured)
        
        try:
            # Save the file temporarily
            file.save(filepath_temp)
            logger.info(f"Temporarily saved file: {filepath_temp}")

            # Open the saved file for parsing
            with open(filepath_temp, 'rb') as temp_file_obj:
                parsed_resume_data = parse_resume_from_file(temp_file_obj)

            if not parsed_resume_data or 'error' in parsed_resume_data:
                error_msg = parsed_resume_data.get('error', 'Unknown error parsing resume') if parsed_resume_data else 'No resume data parsed'
                logger.error(f"Resume parsing error: {error_msg}")
                return jsonify({"error": error_msg}), 500

            # Generate resume summary
            resume_summary = generate_resume_summary(parsed_resume_data)
            if not resume_summary:
                logger.warning("Failed to generate resume summary.")

            # Generate LLM-powered Job Recommendations (using the job_matcher instance)
            llm_recommended_jobs = matcher.generate_job_recommendations(parsed_resume_data)
            if not llm_recommended_jobs:
                logger.info('No suitable job recommendations found from AI.')

            # Fetch and Match against scraped jobs (if you want both types of recommendations)
            # You might want to run scrape_job_listings less frequently (e.g., as a scheduled task)
            # rather than on every resume upload for performance.
            job_listings = fetch_jobs_from_db()
            scraped_matched_jobs = []
            if job_listings:
                scraped_matched_jobs = matcher.match_resume_to_jobs(
                    parsed_resume_data, resume_summary, job_listings
                )
                if not scraped_matched_jobs:
                    logger.info('No suitable job matches found from scraped jobs.')
            else:
                logger.warning("No job listings found in DB for traditional matching.")

            # Store all processed data in session
            session['parsed_resume_data'] = parsed_resume_data
            session['resume_summary'] = resume_summary
            session['scraped_matched_jobs'] = scraped_matched_jobs
            session['llm_recommended_jobs'] = llm_recommended_jobs

            logger.info("Resume processed successfully. Data stored in session.")
            flash('Resume uploaded and processed successfully!', 'success')
            return jsonify({"message": "Resume processed successfully!", "redirect": url_for('show_results')}), 200

        except Exception as e:
            logger.exception("An unhandled error occurred during resume processing.")
            return jsonify({"error": f"An unexpected error occurred: {str(e)}."}), 500
        finally:
            if os.path.exists(filepath_temp):
                os.remove(filepath_temp)  # Clean up the temporary uploaded file
                logger.info(f"Cleaned up temporary uploaded file: {filepath_temp}")
    else:
        logger.warning(f"Disallowed file type uploaded for {file.filename}")
        return jsonify({"error": "Invalid file type. Supported formats: PDF, DOCX, DOC, RTF"}), 400

# --- New Route for displaying results ---
@app.route('/results', methods=['GET'])
def show_results():
    parsed_resume_data = session.get('parsed_resume_data')
    resume_summary = session.get('resume_summary')
    scraped_matched_jobs = session.get('scraped_matched_jobs', [])
    llm_recommended_jobs = session.get('llm_recommended_jobs', [])

    if not parsed_resume_data:
        flash('No resume data found. Please upload a resume first.', 'warning')
        return redirect(url_for('upload_page'))

    return render_template(
        'result.html',
        resume_data=parsed_resume_data,
        resume_summary=resume_summary,
        scraped_matched_jobs=scraped_matched_jobs,
        llm_recommended_jobs=llm_recommended_jobs
    )

# --- Modified Download Routes (fetching data from session) ---
@app.route('/download_parsed_resume_pdf')
def download_parsed_resume_pdf():
    parsed_resume_data = session.get('parsed_resume_data')
    if not parsed_resume_data:
        flash('No parsed resume data available for download. Please upload a resume first.', 'error')
        return redirect(url_for('upload_page'))

    # Render the HTML template specifically for PDF conversion
    # Ensure this template is in your 'templates' folder
    rendered_html = render_template('resume_pdf_template.html', resume_data=parsed_resume_data)

    # Convert HTML to PDF using WeasyPrint
    pdf_bytes = io.BytesIO()
    try:
        HTML(string=rendered_html).write_pdf(pdf_bytes)
        pdf_bytes.seek(0) # Rewind the BytesIO object to the beginning

        full_name = parsed_resume_data.get('Full Name', 'Parsed_Resume').replace(' ', '_')
        download_filename = f"{full_name}_CVisionary.pdf"

        return send_file(
            pdf_bytes,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=download_filename
        )
    except Exception as e:
        logger.exception("Error generating PDF for parsed resume.")
        flash(f"Error generating PDF: {str(e)}", 'error')
        return redirect(url_for('show_results')) # Redirect to results page on error

@app.route('/download_recommended_jobs_json')
def download_recommended_jobs_json():
    # This route will now download the LLM-powered recommendations
    llm_recommended_jobs = session.get('llm_recommended_jobs')
    if not llm_recommended_jobs:
        flash('No LLM job recommendations available for download.', 'error')
        return redirect(url_for('upload_page'))

    try:
        # Create a BytesIO object to store the JSON data
        json_bytes = io.BytesIO()
        json_bytes.write(json.dumps(llm_recommended_jobs, indent=2).encode('utf-8'))
        json_bytes.seek(0)

        return send_file(
            json_bytes,
            mimetype='application/json',
            as_attachment=True,
            download_name='llm_recommended_jobs.json'
        )
    except Exception as e:
        logger.exception("Error generating JSON file for LLM recommended jobs.")
        flash(f"Error generating JSON file: {str(e)}", 'error')
        return redirect(url_for('show_results')) # Redirect to results page on error


if __name__ == '__main__':
    app.run(debug=True) # Set debug=False for production
