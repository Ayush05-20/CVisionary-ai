import os
import json
import logging
import re
import subprocess
from typing import Dict, List, Optional
from langchain_core.prompts import PromptTemplate
from langchain_google_genai import GoogleGenerativeAI
from resume_scraper.resume_parser import parse_resume_from_file, generate_resume_summary # Assuming this is in your project path
from utils.helpers import fetch_jobs_from_db # Import the helper function
from config import Config

import re
logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {'pdf', 'docx', 'doc', 'rtf'}
TOP_N_FOR_LLM = 5

class ResumeJobMatcher:
    def __init__(self, model_name="gemini-2.0-flash"):
        """
        Initialize the ResumeJobMatcher with Gemini 2.0 Flash model.
        
        Args:
            model_name: The Gemini model to use (default: gemini-2.0-flash-exp)
        
        Environment Variables Required:
            GOOGLE_API_KEY: Your Google AI API key
        """
        try:
            # Get the API key from environment
            api_key = os.getenv('GEMINI_API_KEY')
            if not api_key:
                raise ValueError("GEMINI_API_KEY environment variable is required")
            
            # Initialize Gemini model
            self.llm = GoogleGenerativeAI(
                model=model_name,
                google_api_key=api_key,
                temperature=0.3,  # Lower temperature for more consistent responses
                max_output_tokens=4096,
                top_p=0.8,
                top_k=40
            )
            logger.info(f"Initialized Gemini model: {model_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini model: {e}")
            raise

    def scrape_job_listings(self, job_sites: List[str]) -> List[Dict]:
        scrapy_project_path = os.path.join(os.getcwd(), 'jobscraping')
        try:
            logger.info("Starting Scrapy spider to scrape job listings into the MySQL database...")
            cmd = ['scrapy', 'crawl', 'jobspider']
            result = subprocess.run(cmd, cwd=scrapy_project_path, capture_output=True, text=True, check=False)
            if result.returncode != 0:
                logger.error(f"Scrapy process failed with error code {result.returncode}")
                logger.error(f"Scrapy stdout:\n{result.stdout}")
                logger.error(f"Scrapy stderr:\n{result.stderr}")
                logger.error("Error scraping job listings: Scrapy process failed. Check logs for details.")
                return []
            logger.info("Scrapy spider finished and jobs should be in the MySQL database.")
            job_listings = self._fetch_jobs_from_db()
            return job_listings
        except FileNotFoundError:
            logger.error("Scrapy command not found. Make sure Scrapy is installed and in your PATH.")
            logger.error("Error: Scrapy command not found. Please ensure Scrapy is installed.")
            return []
        except Exception as e:
            logger.error(f"An unexpected error occurred during job scraping: {e}", exc_info=True)
            logger.error(f"An unexpected error occurred during job scraping: {str(e)}")
            return []

    def _fetch_jobs_from_db(self) -> List[Dict]:
        """Fetch jobs from database using the helper function"""
        try:
            return fetch_jobs_from_db()
        except Exception as e:
            logger.error(f"Error fetching jobs from database: {e}")
            return []

    def _clean_json_response(self, response: str, expect_array: bool = False) -> Dict | List:
        """Clean and parse JSON response from Gemini"""
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            pass
        
        original_response = response
        
        # Remove markdown code blocks
        response = response.replace("```json", "").replace("```", "").strip()
        
        # Remove control characters
        try:
            response = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', response)
        except Exception as e:
            logger.error(f"Error cleaning control characters: {e}")
        
        # Extract JSON pattern
        json_pattern = r'(\{(?:[^{}]|(?:\{.*?\}))*\})' if not expect_array else r'(\[(?:[^\[\]]|(?:\{.*?\}))*\])'
        matches = re.findall(json_pattern, response, re.DOTALL)
        
        if not matches:
            logger.error(f"No JSON object found in response: {original_response}")
            return [] if expect_array else {}
        
        # Try to parse each match
        for json_str in matches:
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                try:
                    # Fix common JSON issues
                    json_str = re.sub(r',\s*([}\]])', r'\1', json_str)
                    json_str = re.sub(r'([{,])\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', json_str)
                    return json.loads(json_str)
                except json.JSONDecodeError:
                    continue
        
        logger.error(f"All JSON extraction attempts failed on: {original_response}")
        return [] if expect_array else {}

    def extract_resume_keywords(self, resume_data: Dict) -> List[str]:
        """Extract keywords from resume using Gemini"""
        keyword_prompt = PromptTemplate(
            input_variables=["resume_data"],
            template="""Extract only the specific technical and soft skills, tools, and technologies explicitly mentioned in the provided resume data. Do not infer or add any skills, tools, or technologies that are not present in the resume.

Focus on extracting:
- Technical skills and programming languages
- Software tools and technologies
- Frameworks and libraries
- Certifications and qualifications
- Industry-specific terminology
- Soft skills explicitly mentioned
- Project technologies and methodologies

Return ONLY a valid JSON array of strings containing the exact keywords found in the resume data. 

Example format: ["Python", "Machine Learning", "SQL", "Project Management", "Communication"]

If no keywords are found, return an empty array: []

Resume Data:
{resume_data}

JSON Array:"""
        )
        
        try:
            logger.debug(f"Resume data for keyword extraction: {json.dumps(resume_data, indent=2)}")
            
            if not resume_data:
                logger.warning("Empty resume data provided for keyword extraction")
                return []
            
            # Invoke Gemini
            response = self.llm.invoke(keyword_prompt.format(resume_data=json.dumps(resume_data)))
            
            if not response or not response.strip():
                logger.warning("Gemini returned empty response for keywords")
                return []
            
            logger.debug(f"Raw Gemini response for keywords: {response}")
            
            # Clean and parse the response
            response_data = self._clean_json_response(response, expect_array=True)
            logger.debug(f"Cleaned extracted keywords: {response_data}")
            
            if isinstance(response_data, list):
                return response_data
            
            logger.warning("Unexpected response format for keywords")
            return []
            
        except Exception as e:
            logger.error(f"Error extracting resume keywords: {e}")
            return []

    def _fallback_scoring(self, resume_data, job):
        """Fallback scoring method when LLM fails"""
        resume_skills = set(resume_data.get("Technical Skills", []))
        job_skills = set(job.get("skills_required", []))
        common_skills = resume_skills & job_skills
        score = int(len(common_skills) / max(len(job_skills), 1) * 100) if job_skills else 0
        
        return {
            "match_score": score,
            "matched_skills": list(common_skills),
            "missing_skills": list(job_skills - resume_skills),
            "match_reasoning": "Fallback scoring used due to LLM processing error.",
            "job_fit": "Good Match" if score > 50 else "Poor Match"
        }

    def match_resume_to_jobs(self, resume_data: Dict, resume_summary: str, job_listings: List[Dict]) -> List[Dict]:
        """Match resume to job listings using Gemini"""
        if not resume_data:
            logger.error("No resume data provided for matching")
            return []
        
        if not job_listings:
            logger.warning("No job listings provided for matching")
            return []

        # Stage 1 - Pre-filtering (removed location/job_preference filtering)
        pre_filtered_jobs = job_listings

        # Stage 2 - Pre-scoring based on keyword similarity
        keywords = self.extract_resume_keywords(resume_data)
        logger.info(f"Extracted keywords from resume: {keywords}")
        resume_skills_set = set(keywords)

        for job in pre_filtered_jobs:
            job_skills_set = set(job.get('skills_required', []))
            intersection = len(resume_skills_set.intersection(job_skills_set))
            union = len(resume_skills_set.union(job_skills_set))
            jaccard_score = intersection / union if union > 0 else 0
            job['pre_score'] = jaccard_score
        
        # Sort by pre-score and take top N for detailed analysis
        pre_filtered_jobs.sort(key=lambda x: x.get('pre_score', 0), reverse=True)
        jobs_to_rank = pre_filtered_jobs[:TOP_N_FOR_LLM]
        logger.info(f"Selected top {len(jobs_to_rank)} jobs for detailed Gemini ranking.")

        # Detailed matching prompt for Gemini
        matching_prompt = PromptTemplate(
            input_variables=["resume_details", "resume_summary", "job_listing", "keywords"],
            template="""Analyze the compatibility between the resume and job listing. Focus on experience alignment, skill matches, and overall suitability.

Resume Summary:
{resume_summary}

Key Resume Keywords: {keywords}

Full Resume Details:
{resume_details}

Job Listing:
{job_listing}

Provide a detailed analysis and return ONLY a valid JSON object with this exact structure:

{{
    "match_score": [integer between 0-100],
    "matched_skills": [list of skills that match between resume and job],
    "missing_skills": [list of required skills not present in resume],
    "match_reasoning": "Detailed explanation of the match score focusing on experience alignment and skill compatibility",
    "job_fit": "[Excellent Match|Good Match|Moderate Match|Poor Match]"
}}

Consider:
- Work experience relevance (40% weight)
- Technical skills alignment (30% weight)
- Education/certifications (20% weight)
- Soft skills and cultural fit (10% weight)

JSON Response:"""
        )

        matched_jobs = []
        for i, job in enumerate(jobs_to_rank, 1):
            try:
                logger.info(f"Ranking job {i}/{len(jobs_to_rank)}: {job.get('job_title', 'Unknown Job')}")
                
                # Get match analysis from Gemini
                match_result = self.llm.invoke(
                    matching_prompt.format(
                        resume_details=json.dumps(resume_data, indent=2),
                        resume_summary=resume_summary,
                        job_listing=json.dumps(job, indent=2),
                        keywords=", ".join(keywords) if keywords else "None"
                    )
                )
                
                if not match_result or not match_result.strip():
                    logger.warning(f"Empty response from Gemini for job {i}, using fallback scoring")
                    match_data = self._fallback_scoring(resume_data, job)
                else:
                    match_data = self._clean_json_response(match_result, expect_array=False)
                    if not match_data:
                        logger.warning(f"Failed to parse Gemini response for job {i}, using fallback scoring")
                        match_data = self._fallback_scoring(resume_data, job)

                # Validate and fix match score
                match_score = match_data.get("match_score", 0)
                if not isinstance(match_score, (int, float)) or not (0 <= match_score <= 100):
                    matched_skills_count = len(match_data.get("matched_skills", []))
                    required_skills_count = len(job.get("skills_required", []))
                    match_score = int((matched_skills_count / required_skills_count) * 100) if required_skills_count > 0 else 0
                    match_data["match_score"] = match_score

                # Set job fit based on score
                if match_score >= 80:
                    match_data["job_fit"] = "Excellent Match"
                elif match_score >= 60:
                    match_data["job_fit"] = "Good Match"
                elif match_score >= 40:
                    match_data["job_fit"] = "Moderate Match"
                else:
                    match_data["job_fit"] = "Poor Match"
                
                # Ensure reasoning exists
                if not match_data.get("match_reasoning"):
                    match_data["match_reasoning"] = f"Score based on skill overlap and experience alignment."

                matched_job = {**job, "match_details": match_data}
                matched_jobs.append(matched_job)
                
            except Exception as e:
                logger.error(f"Error ranking job {i}: {e}")
                error_match_data = {
                    "match_score": 0,
                    "matched_skills": [],
                    "missing_skills": [],
                    "match_reasoning": f"Error during matching: {str(e)}",
                    "job_fit": "Error"
                }
                matched_jobs.append({**job, "match_details": error_match_data})
        
        # Sort by match score
        matched_jobs.sort(key=lambda x: x.get('match_details', {}).get('match_score', 0), reverse=True)
        logger.info(f"Completed ranking. Found {len(matched_jobs)} suitable jobs.")
        return matched_jobs

    def generate_job_recommendations(self, resume_data: Dict) -> List[Dict]:
        """Generate job recommendations based on resume data using Gemini"""
        if not resume_data:
            logger.error("No resume data provided for job recommendation.")
            return []

        # Extract keywords for better prompting
        keywords = self.extract_resume_keywords(resume_data)
        logger.info(f"Extracted keywords for job recommendation: {keywords}")

        recommendation_prompt = PromptTemplate(
            input_variables=["resume_details", "keywords"],
            template="""Based on the resume details and keywords provided, recommend the top 3 most suitable job roles for this candidate.

Resume Details:
{resume_details}

Key Resume Keywords: {keywords}

For each job role, provide:
1. job_title: A specific job title based on the resume content
2. match_score: Integer percentage (0-100) indicating profile fit
3. suitability_reasoning: 2-3 sentences explaining why this role fits, referencing specific resume elements
4. improvement_suggestions: 2-3 sentences of actionable advice to improve candidacy

Return ONLY a valid JSON array of exactly 3 job recommendation objects. No additional text or formatting.

Expected JSON format:
[
    {{
        "job_title": "Specific Job Title",
        "match_score": 85,
        "suitability_reasoning": "Candidate has relevant experience in X and skills in Y, making them well-suited for this role.",
        "improvement_suggestions": "Consider gaining experience in Z technology and developing stronger skills in W area."
    }},
    {{
        "job_title": "Another Job Title",
        "match_score": 78,
        "suitability_reasoning": "Strong background in A and demonstrated competency in B align with role requirements.",
        "improvement_suggestions": "Build portfolio projects showcasing C skills and pursue certification in D."
    }},
    {{
        "job_title": "Third Job Title",
        "match_score": 72,
        "suitability_reasoning": "Educational background and project experience provide foundation for this career path.",
        "improvement_suggestions": "Gain hands-on experience through internships and strengthen technical skills in E."
    }}
]

JSON Array:"""
        )

        try:
            logger.info("Generating job recommendations using Gemini...")
            
            response = self.llm.invoke(
                recommendation_prompt.format(
                    resume_details=json.dumps(resume_data, indent=2),
                    keywords=", ".join(keywords) if keywords else "None"
                )
            )
            
            if not response or not response.strip():
                logger.warning("Gemini returned empty response for job recommendations.")
                return []

            logger.debug(f"Raw Gemini response for recommendations: {response}")
            parsed_recommendations = self._clean_json_response(response, expect_array=True)
            
            if not isinstance(parsed_recommendations, list):
                logger.error(f"Expected a list of recommendations, got: {type(parsed_recommendations)}")
                return []

            # Validate and format the recommendations
            recommended_jobs = []
            for rec in parsed_recommendations:
                if (isinstance(rec, dict) and
                    "job_title" in rec and
                    "match_score" in rec and
                    "suitability_reasoning" in rec and
                    "improvement_suggestions" in rec):
                    
                    # Ensure match_score is an integer
                    try:
                        rec["match_score"] = int(rec["match_score"])
                    except (ValueError, TypeError):
                        rec["match_score"] = 50  # Default score if conversion fails
                    
                    recommended_jobs.append(rec)
                else:
                    logger.warning(f"Skipping malformed recommendation: {rec}")

            # Sort by match score and return top 3
            recommended_jobs.sort(key=lambda x: x.get('match_score', 0), reverse=True)
            final_recommendations = recommended_jobs[:3]
            
            logger.info(f"Generated {len(final_recommendations)} job recommendations.")
            return final_recommendations
            
        except Exception as e:
            logger.error(f"Error generating job recommendations: {e}", exc_info=True)
            return []