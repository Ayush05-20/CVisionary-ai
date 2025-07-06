
# resume_praser.py
import google.generativeai as genai
import os
import json
import re
import logging
from pypdf import PdfReader
from typing import List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

api_key = os.getenv("GEMINI_API_KEY")

if not api_key:
    logger.error("API key is not found. Please set the GEMINI_API_KEY environment variable in your .env file or system environment.")
    # You might want to raise an exception or handle this more gracefully in production
    # For development, just logging might be enough, but the AI functions will fail.
    # raise ValueError("GEMINI_API_KEY is not set.")
else:
    genai.configure(api_key=api_key)

def clean_json_response(text):
    """
    Cleans the AI response to extract valid JSON content.
    Handles markdown code blocks and attempts to fix common JSON issues.
    """
    # Remove markdown code block fences
    text = text.replace("```json", "").replace("```", "").strip()

    # Remove invalid control characters that can break JSON parsing
    text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)

    # Find the outermost JSON object or array
    try:
        # Prioritize finding a complete JSON object or array
        match_obj = re.search(r'\{.*\}', text, re.DOTALL)
        match_arr = re.search(r'\[.*\]', text, re.DOTALL)

        json_str = ""
        if match_obj and match_arr:
            # Take the one that starts earlier
            if match_obj.start() < match_arr.start():
                json_str = match_obj.group(0)
            else:
                json_str = match_arr.group(0)
        elif match_obj:
            json_str = match_obj.group(0)
        elif match_arr:
            json_str = match_arr.group(0)
        else:
            logger.warning(f"No JSON object or array found in text: {text[:200]}...")
            return text # Return original if no JSON structure found

        # Attempt to balance braces/brackets - simple heuristic
        if json_str.startswith('{'):
            open_count = json_str.count('{')
            close_count = json_str.count('}')
            if open_count > close_count:
                json_str += '}' * (open_count - close_count)
        elif json_str.startswith('['):
            open_count = json_str.count('[')
            close_count = json_str.count(']')
            if open_count > close_count:
                json_str += ']' * (open_count - close_count)

        return json_str
    except Exception as e:
        logger.error(f"Error during JSON cleaning: {e}, text: {text[:200]}...")
        return text

def ats_extractor(resume_data_text):
    """
    Extracts ATS-friendly information from the resume data.
    
    Args:
        resume_data_text (str): The resume data in string format.
        
    Returns:
        dict: A dictionary containing extracted information.
    """
    prompt = """
    You are an ATS (Applicant Tracking System) that reads resumes and extracts relevant information.
    From the given resume data, extract the following information and return it in valid JSON format:
    {
        "Full Name": "",
        "Email Address": "",
        "Phone Number": "",
        "LinkedIn Profile URL": "",
        "Education": [
            {
                "Degree": "",
                "Major": "",
                "University": "",
                "Years": ""
            }
        ],
        "Work Experience": [
            {
                "Company": "",
                "Position": "",
                "Duration": "",
                "Description": ""
            }
        ],
        "Technical Skills": [],
        "Soft Skills": [],
        "Certifications": [],
        "Projects": [
            {
                "Name": "",
                "Description": "",
                "Technologies": [],
                "URL": ""
            }
        ],
        "Summary_or_Objective": ""
    }
    
    IMPORTANT:
    1. Return ONLY valid JSON format (no surrounding text or markdown, no introductory or concluding sentences).
    2. Ensure all strings are properly escaped (e.g., double quotes, backslashes).
    3. Education should be an array of objects, each with Degree, Major, University, and Years.
    4. Work Experience should include company, position, duration (e.g., "Jan 2020 - Dec 2022"), and a brief description of responsibilities/achievements.
    5. Projects should include name, description, technologies used (as a list), and URL (if available).
    6. "Technical Skills" and "Soft Skills" should be lists of individual skills.
    7. "Certifications" should be a list of certification names.
    8. "Summary_or_Objective" should capture the candidate's personal summary or objective statement.
    9. If information is missing, use empty arrays, empty strings, or null as appropriate.
    10. Pay special attention to extracting all projects mentioned in the resume.
    """
    
    model = genai.GenerativeModel("gemini-2.0-flash")
    
    try:
        response = model.generate_content([
            {"role": "user", 
             "parts": [f"{prompt} \n\n Resume Text:\n {resume_data_text}"]}
        ])
        
        cleaned_response_text = clean_json_response(response.text)
        parsed_data = json.loads(cleaned_response_text)
        return parsed_data
        
    except json.JSONDecodeError as je:
        logger.error(f"JSON Decode Error in ATS extractor: {je}")
        logger.error(f"Problematic JSON string: {cleaned_response_text[:500]}...")
        return {
            "error": f"JSON parsing failed: {je}",
            "raw_response": response.text if 'response' in locals() else None,
            "cleaned_response_attempt": cleaned_response_text if 'cleaned_response_text' in locals() else None
        }
    except Exception as e:
        logger.error(f"General error in AI processing for ATS extractor: {str(e)}")
        return {
            "error": str(e),
            "raw_response": response.text if 'response' in locals() else None
        }

def generate_resume_summary(parsed_resume_data: dict) -> str:
    """
    Generates a concise, human-readable summary from the parsed resume data.
    
    Args:
        parsed_resume_data (dict): The dictionary containing parsed resume information.
        
    Returns:
        str: A summary of the resume.
    """
    prompt = """
    Based on the following structured resume data, write a concise, professional summary (around 3-5 sentences) that highlights the candidate's key qualifications, experience, and skills relevant for job applications. Focus on their strongest assets and career focus.

    Resume Data (JSON):
    {resume_json}

    Summary:
    """
    
    model = genai.GenerativeModel("gemini-2.0-flash")
    
    try:
        # Convert the dictionary to a pretty-printed JSON string for the prompt
        resume_json_str = json.dumps(parsed_resume_data, indent=2)
        
        response = model.generate_content([
            {"role": "user", 
             "parts": [prompt.format(resume_json=resume_json_str)]}
        ])
        
        # The summary is expected to be plain text, not JSON
        summary_text = response.text.strip()
        
        # Remove any leading/trailing markdown characters if AI accidentally adds them
        if summary_text.startswith('```') and summary_text.endswith('```'):
            summary_text = summary_text[3:-3].strip()

        return summary_text
        
    except Exception as e:
        logger.error(f"Error generating resume summary: {str(e)}")
        return "Could not generate a summary for this resume."

def infer_career_interests(parsed_resume_data: dict) -> List[str]:
    """
    Infers broad career interests or job categories from the parsed resume data
    to guide job searching.
    
    Args:
        parsed_resume_data (dict): The dictionary containing parsed resume information.
        
    Returns:
        List[str]: A list of 3-5 general job search keywords/categories.
    """
    prompt = """
    Analyze the following structured resume data. Based on the candidate's education, work experience, technical skills, soft skills, and projects, infer 3-5 by key words(e.g., "Software Developer","Python", "Data", "A.I" "Markating", "Data Analysis", "Customer Service", "Education", "Healthcare", "Admin Support"). Return ONLY a valid JSON array of these keywords. Do not include any introductory or concluding text, or markdown code block fences.

    Resume Data (JSON):
    {resume_json}

    Return: []
    """
    
    model = genai.GenerativeModel("gemini-2.0-flash")
    
    try:
        resume_json_str = json.dumps(parsed_resume_data, indent=2)
        
        response = model.generate_content([
            {"role": "user", 
             "parts": [prompt.format(resume_json=resume_json_str)]}
        ])
        
        cleaned_response = clean_json_response(response.text)
        inferred_interests = json.loads(cleaned_response)
        
        if isinstance(inferred_interests, list) and all(isinstance(item, str) for item in inferred_interests):
            logger.info(f"Inferred career interests: {inferred_interests}")
            return inferred_interests
        else:
            logger.warning(f"Unexpected format for inferred career interests: {inferred_interests}. Returning default.")
            return ["IT", "Administration", "Sales", "Customer Service"] # Fallback generic interests
        
    except json.JSONDecodeError as je:
        logger.error(f"JSON Decode Error in infer_career_interests: {je}")
        logger.error(f"Problematic JSON string for career interests: {cleaned_response[:500]}...")
        return ["IT", "Administration", "Sales", "Customer Service"]
    except Exception as e:
        logger.error(f"General error in AI processing for infer_career_interests: {str(e)}")
        return ["IT", "Administration", "Sales", "Customer Service"] # Fallback

UPLOAD_PATH = "uploads" # Matches app.config['UPLOAD_FOLDER'] in cli4.py
os.makedirs(UPLOAD_PATH, exist_ok=True)

def save_file(file_object, filename="file.pdf"):
    """Save uploaded file to disk"""
    file_path = os.path.join(UPLOAD_PATH, filename)
    try:
        # Rewind file_object to the beginning if it has already been read
        file_object.seek(0)
        with open(file_path, "wb") as f:
            f.write(file_object.read())
        logger.info(f"File saved to: {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Error saving file {filename}: {str(e)}")
        return None

def extract_text_from_pdf(file_path):
    """Extracts all text from a PDF using pypdf"""
    if not file_path or not os.path.exists(file_path):
        logger.error(f"PDF file path is invalid or does not exist: {file_path}")
        return None
    try:
        reader = PdfReader(file_path)
        data = ""
        for page in reader.pages:
            try:
                data += page.extract_text() or ""
            except Exception as page_e:
                logger.warning(f"Could not extract text from a page: {page_e}")
        return data
    except Exception as e:
        logger.error(f"Error extracting text from PDF {file_path}: {str(e)}")
        return None

def parse_resume_from_file(file_object):
    """Parses a resume file and extracts structured data."""
    logger.info("Starting resume parsing process.")
    file_path = save_file(file_object)
    if not file_path:
        logger.error("Failed to save resume file for parsing.")
        return {"error": "Failed to save file for processing."}

    resume_text = extract_text_from_pdf(file_path)
    # Ensure the file is removed after text extraction
    try:
        os.remove(file_path)
        logger.info(f"Temporarily saved file removed: {file_path}")
    except OSError as e:
        logger.warning(f"Error removing temporary file {file_path}: {e}")

    if not resume_text:
        logger.error("Failed to extract text from resume.")
        return {"error": "Failed to extract text from your resume. Please ensure it's a readable PDF."}

    parsed_data = ats_extractor(resume_text)
    if "error" in parsed_data:
        logger.error(f"ATS extractor reported an error: {parsed_data['error']}")
        return parsed_data
    
    logger.info("Resume parsed successfully.")
    return parsed_data