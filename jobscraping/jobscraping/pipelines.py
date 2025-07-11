# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import os
import mysql.connector
import json
from itemadapter import ItemAdapter
from jobscraping.items import JobItem  # Ensure this import path is correct


class JobscrapingPipeline:
    def process_item(self, item, spider):
        return item


class SaveToMYSqlPipeLine:
    def __init__(self):
        # Use environment variables for database configuration
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', 'my$qlayush1'),
            'database': os.getenv('DB_NAME', 'jobs'),
            'port': int(os.getenv('DB_PORT', '3306')),
            'autocommit': False
        }
        
        self.conn = None
        self.cur = None
        self.setup_database()

    def setup_database(self):
        """Setup database connection with error handling"""
        try:
            # Establish database connection
            self.conn = mysql.connector.connect(**self.db_config)
            self.cur = self.conn.cursor()

            # Create table if it doesn't exist
            self.cur.execute('''
            CREATE TABLE IF NOT EXISTS jobs(
                id INT AUTO_INCREMENT PRIMARY KEY,
                url VARCHAR(255) UNIQUE,
                title VARCHAR(255),
                job_cat VARCHAR(255),
                location VARCHAR(255),
                company VARCHAR(255),
                education VARCHAR(255),
                experience VARCHAR(255),
                skills TEXT,
                general_requirements TEXT,
                specific_requirements TEXT,
                dis TEXT,
                responsibilities TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );''')
            
            self.conn.commit()
            print("Database connection established successfully")
            
        except mysql.connector.Error as e:
            print(f"Error connecting to MySQL database: {e}")
            print("Pipeline will continue without database storage")
            self.conn = None
            self.cur = None

    def process_item(self, item, spider):
        # If no database connection, just return the item
        if not self.conn or not self.cur:
            spider.logger.warning("No database connection available. Item not saved to database.")
            return item

        try:
            # Convert lists to JSON strings for storage in TEXT columns
            skills_json = json.dumps(item.get('skills', []))
            general_requirements_json = json.dumps(item.get('general_requirements', []))
            specific_requirements_json = json.dumps(item.get('specific_requirements', []))
            dis_json = json.dumps(item.get('dis', []))
            responsibilities_json = json.dumps(item.get('responsibilities', []))

            # Check if job already exists based on URL to prevent duplicates
            self.cur.execute("SELECT id FROM jobs WHERE url = %s", (str(item.get('url', '')),))
            existing_job_id = self.cur.fetchone()

            if existing_job_id:
                # If job exists, update its details
                self.cur.execute("""
                    UPDATE jobs SET
                        title = %s, job_cat = %s, location = %s, company = %s,
                        education = %s, experience = %s, skills = %s, 
                        general_requirements = %s, specific_requirements = %s,
                        dis = %s, responsibilities = %s
                    WHERE url = %s
                """, (
                    str(item.get('title', '')),
                    str(item.get('job_cat', '')),
                    str(item.get('location', '')),
                    str(item.get('company', '')),
                    str(item.get('education', '')),
                    str(item.get('experience', '')),
                    skills_json,
                    general_requirements_json,
                    specific_requirements_json,
                    dis_json,
                    responsibilities_json,
                    str(item.get('url', ''))
                ))
                spider.logger.info(f"Updated job: {item.get('title', 'No title')} - {item.get('company', 'No company')}")
            else:
                # If job does not exist, insert new record
                self.cur.execute("""
                    INSERT INTO jobs(
                        url, title, job_cat, location, company,
                        education, experience, skills, general_requirements,
                        specific_requirements, dis, responsibilities
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    str(item.get('url', '')),
                    str(item.get('title', '')),
                    str(item.get('job_cat', '')),
                    str(item.get('location', '')),
                    str(item.get('company', '')),
                    str(item.get('education', '')),
                    str(item.get('experience', '')),
                    skills_json,
                    general_requirements_json,
                    specific_requirements_json,
                    dis_json,
                    responsibilities_json
                ))
                spider.logger.info(f"Added new job: {item.get('title', 'No title')} - {item.get('company', 'No company')}")

            self.conn.commit()
            return item

        except mysql.connector.Error as e:
            if self.conn:
                self.conn.rollback()
            spider.logger.error(f"Database error while saving item: {e}")
            spider.logger.error(f"Item data: {dict(item)}")
            # Return item to continue processing other items
            return item
        except KeyError as e:
            spider.logger.error(f"Missing key in item: {e}")
            spider.logger.error(f"Available keys: {list(item.keys())}")
            spider.logger.error(f"Item data: {dict(item)}")
            return item
        except Exception as e:
            spider.logger.error(f"Unexpected error while processing item: {e}")
            return item

    def close_spider(self, spider):
        """Ensure cursor and connection are closed properly"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        spider.logger.info("MySQL connection closed by SaveToMYSqlPipeLine.")