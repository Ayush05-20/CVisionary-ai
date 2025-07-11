import os
import json
import logging
import re
import subprocess
from typing import Dict, List, Optional
from langchain_core.prompts import PromptTemplate
from langchain_ollama import OllamaLLM
from resume_scraper.resume_parser import parse_resume_from_file, generate_resume_summary # Assuming this is in your project path
from utils.helpers import fetch_jobs_from_db # Import the helper function
from config import Config

import re
logger = logging.getLogger(__name__)



ALLOWED_EXTENSIONS = {'pdf', 'docx', 'doc', 'rtf'}
TOP_N_FOR_LLM = 5
class ResumeJobMatcher:
    def __init__(self, model_name="llama3.2"):
        try:
            self.llm = OllamaLLM(model=model_name)
            logger.info(f"Initialized LLM model: {model_name}")
        except Exception as e:
            logger.error(f"Failed to initialize LLM model: {e}")
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


    def _clean_json_response(self, response: str, expect_array: bool = False) -> Dict | List:
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            pass
        original_response = response
        response = response.replace("```json", "").replace("```", "").strip()
        try:
            response = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', response)
        except Exception as e:
            logger.error(f"Error cleaning control characters: {e}")
        json_pattern = r'(\{(?:[^{}]|(?:\{.*?\}))*\})' if not expect_array else r'(\[(?:[^\[\]]|(?:\{.*?\}))*\])'
        matches = re.findall(json_pattern, response, re.DOTALL)
        if not matches:
            logger.error(f"No JSON object found in response: {original_response}")
            return [] if expect_array else {}
        for json_str in matches:
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                try:
                    json_str = re.sub(r',\s*([}\]])', r'\1', json_str)
                    json_str = re.sub(r'([{,])\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', json_str)
                    return json.loads(json_str)
                except json.JSONDecodeError:
                    continue
        logger.error(f"All JSON extraction attempts failed on: {original_response}")
        return [] if expect_array else {}

    def extract_resume_keywords(self, resume_data: Dict) -> List[str]:
        keyword_prompt = PromptTemplate(
            input_variables=["resume_data"],
            template="""Extract only the specific technical and soft skills, tools, and technologies explicitly mentioned in the provided resume data. Do not infer or add any skills, tools, or technologies that are not present in the resume. Focus on:
                Focus on extracting:
                - Skills and technologies mentioned in the resume
                
                _ Skills and technologies used or learned in projects or workshops
                
                - Relevant projects and certifications
                
                - Technical skills 
                
                - Soft skills
            
                - Certifications
        
                - Industry-specific terminology
             
                - Key qualifications

                Return ONLY a valid JSON array of strings containing the exact keywords found in the resume data. Do not include:
                - Any explanatory text
                - Code block markers (```json)
                - Markdown formatting
                - Additional comments
                - Anything outside the JSON array

                If no keywords are found, return an empty array: []

                Resume Data:
                {resume_data}

                Return: []"""
        )
        try:
            logger.debug(f"Resume data for keyword extraction: {json.dumps(resume_data, indent=2)}")
            if not resume_data:
                logger.warning("Empty resume data provided for keyword extraction")
                return []
            response = self.llm.invoke(keyword_prompt.format(resume_data=json.dumps(resume_data)))
            if not response or not response.strip():
                logger.warning("LLM returned empty response for keywords")
                return []
            logger.debug(f"Raw LLM response for keywords: {response}")
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
        resume_skills = set(resume_data.get("Technical Skills", []))
        job_skills = set(job.get("skills_required", []))
        common_skills = resume_skills & job_skills
        score = int(len(common_skills) / max(len(job_skills), 1) * 100) if job_skills else 0
        return {"match_score": score, "matched_skills": list(common_skills), "missing_skills": list(job_skills - resume_skills), "match_reasoning": "Fallback scoring used.", "job_fit": "Good Match" if score > 50 else "Poor Match"}

    def match_resume_to_jobs(self, resume_data: Dict, resume_summary: str, job_listings: List[Dict]) -> List[Dict]:
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
        
        pre_filtered_jobs.sort(key=lambda x: x.get('pre_score', 0), reverse=True)
        jobs_to_rank = pre_filtered_jobs[:TOP_N_FOR_LLM]
        logger.info(f"Selected top {len(jobs_to_rank)} jobs for detailed LLM ranking.")

        matching_prompt = PromptTemplate(
            input_variables=["resume_details", "resume_summary", "job_listing", "keywords"],
            template="""Analyze the fit between the resume and the job listing. Prioritize alignment between the resume's work experience and the job's responsibilities. 
            Return ONLY a valid JSON object. Do not include any text outside the JSON object. The match_score must be an integer between 0 and 100.

Resume Summary:
{resume_summary}

Key Resume Keywords: {keywords}

Full Resume Details:
{resume_details}

Job Listing to Analyze:
{job_listing}

Return:
{{
    "match_score": 0,
    "matched_skills": [],
    "missing_skills": [],
    "match_reasoning": "Reasoning for the match score, focusing on experience alignment.",
    "job_fit": "e.g., 'Excellent Match', 'Good Match', 'Poor Match'"
}}"""
        )

        matched_jobs = []
        for i, job in enumerate(jobs_to_rank, 1):
            try:
                logger.info(f"Ranking job {i}/{len(jobs_to_rank)}: {job.get('job_title', 'Unknown Job')}")
                
                match_result = self.llm.invoke(
                    matching_prompt.format(
                        resume_details=json.dumps(resume_data),
                        resume_summary=resume_summary,
                        job_listing=json.dumps(job),
                        keywords=", ".join(keywords) if keywords else "None"
                    )
                )
                
                if not match_result or not match_result.strip():
                    match_data = self._fallback_scoring(resume_data, job)
                else:
                    match_data = self._clean_json_response(match_result, expect_array=False)
                    if not match_data:
                        match_data = self._fallback_scoring(resume_data, job)

                match_score = match_data.get("match_score", 0)
                if not isinstance(match_score, (int, float)) or not (0 <= match_score <= 100):
                    matched_skills_count = len(match_data.get("matched_skills", []))
                    required_skills_count = len(job.get("skills_required", []))
                    match_score = int((matched_skills_count / required_skills_count) * 100) if required_skills_count > 0 else 0
                    match_data["match_score"] = match_score

                if match_score >= 80: match_data["job_fit"] = "Excellent Match"
                elif match_score >= 60: match_data["job_fit"] = "Good Match"
                elif match_score >= 40: match_data["job_fit"] = "Moderate Match"
                else: match_data["job_fit"] = "Poor Match"
                
                if not match_data.get("match_reasoning"): match_data["match_reasoning"] = f"Score based on skill overlap."

                matched_job = {**job, "match_details": match_data}
                matched_jobs.append(matched_job)
            except Exception as e:
                logger.error(f"Error ranking job {i}: {e}")
                matched_jobs.append({**job, "match_details": {"match_score": 0, "match_reasoning": f"Error during matching: {e}", "job_fit": "Error"}})
                
        matched_jobs.sort(key=lambda x: x.get('match_details', {}).get('match_score', 0), reverse=True)
        logger.info(f"Completed ranking. Found {len(matched_jobs)} suitable jobs.")
        return matched_jobs

    def generate_job_recommendations(self, resume_data: Dict) -> List[Dict]:
        """
        Generates top 3 job recommendations based on resume data using LLM.
        Each recommendation includes job title, match score, suitability reasoning,
        and improvement suggestions.
        """
        if not resume_data:
            logger.error("No resume data provided for job recommendation.")
            return []

        # Extract keywords for better prompting
        keywords = self.extract_resume_keywords(resume_data)
        logger.info(f"Extracted keywords for job recommendation: {keywords}")

        recommendation_prompt = PromptTemplate(
            input_variables=["resume_details", "keywords"],
            template="""Based on the following resume details and extracted keywords, identify the top 3 most suitable job roles for this candidate. For each job role, provide:
            1.  **job_title**: A suitable job title make sure the given job title should be based on the resume (e.g., "Software Engineer", "Data Analyst", "Marketing Specialist" ).
            2.  **match_score**: An integer percentage (0-100) indicating how well the candidate's current profile matches this job role.
            3.  **suitability_reasoning**: A concise explanation (2-3 sentences) of why this job role is a good fit, referencing specific skills or experiences from the resume.
            4.  **improvement_suggestions**: Specific, actionable advice (2-3 sentences) on how the candidate can improve their profile to be an even better fit for this role (e.g., "Gain experience in X technology", "Develop Y soft skill", "Certify in Z").

            IMPORTANT: Return ONLY a valid JSON array of 3 objects. Do not include any explanatory text, code block markers, or other content outside the JSON array. Ensure your response is parseable by json.loads().

            If you cannot recommend 3 jobs, provide as many as you can, or an empty array if none are suitable.

Resume Details:
{resume_details}

Key Resume Keywords: {keywords}

Return Example Structure:
[
    {{
        "job_title": "Job Title 1",
        "match_score": "based on key resume matching",
        "suitability_reasoning": "Candidate has strong Python skills and experience in data visualization, making them suitable for this role.",
        "improvement_suggestions": "To improve, focus on gaining more experience with SQL databases and cloud platforms."
    }},
    {{
        "job_title": "Job Title 2",
        "match_score": "based on key resume matching",
        "suitability_reasoning": "Their project management experience and communication skills align well with this position.",
        "improvement_suggestions": "Consider a certification in Agile methodologies and build a portfolio of diverse projects."
    }},
    {{
        "job_title": "Job Title 3",
        "match_score": "based on key resume matching",
        "suitability_reasoning": "Relevant education background and entry-level experience in customer support.",
        "improvement_suggestions": "Develop advanced proficiency in CRM software and leadership skills for future growth."
    }}
]
""")
        recommended_jobs = []
        try:
            logger.info("Generating job recommendations using LLM...")
            
            response = self.llm.invoke(
                recommendation_prompt.format(
                    resume_details=json.dumps(resume_data, indent=2),
                    keywords=", ".join(keywords) if keywords else "None"
                )
            )
            
            if not response or not response.strip():
                logger.warning("LLM returned empty response for job recommendations.")
                return []

            logger.debug(f"Raw LLM response for recommendations: {response}")
            parsed_recommendations = self._clean_json_response(response, expect_array=True)
            
            if not isinstance(parsed_recommendations, list):
                logger.error(f"Expected a list of recommendations, got: {type(parsed_recommendations)}")
                return []

            # Validate and format the recommendations
            for rec in parsed_recommendations:
                if (isinstance(rec, dict) and
                    "job_title" in rec and
                    "match_score" in rec and
                    "suitability_reasoning" in rec and
                    "improvement_suggestions" in rec):
                    
                    rec["match_score"] = int(rec["match_score"]) # Ensure integer
                    recommended_jobs.append(rec)
                else:
                    logger.warning(f"Skipping malformed recommendation: {rec}")

            # Sort by match score in descending order
            recommended_jobs.sort(key=lambda x: x.get('match_score', 0), reverse=True)
            logger.info(f"Generated {len(recommended_jobs)} job recommendations.")
            return recommended_jobs[:3] # Ensure only top 3 are returned
            
        except Exception as e:
            logger.error(f"Error generating job recommendations: {e}", exc_info=True)
            logger.error(f"An error occurred while generating job recommendations: {str(e)}")
            return []
