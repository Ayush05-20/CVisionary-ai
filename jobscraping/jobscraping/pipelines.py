# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import os
import psycopg2
import psycopg2.extras
import json
from itemadapter import ItemAdapter
from jobscraping.items import JobItem  # Ensure this import path is correct


class JobscrapingPipeline:
    def process_item(self, item, spider):
        return item


class SaveToPostgreSQLPipeLine:
    def __init__(self):
        # Use environment variables for database configuration
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', 'jobs'),
            'port': int(os.getenv('DB_PORT', '5432'))
        }
        
        # Alternative: Use DATABASE_URL (Render provides this)
        self.database_url = os.getenv('DATABASE_URL')
        
        self.conn = None
        self.cur = None
        self.setup_database()

    def setup_database(self):
        """Setup database connection with error handling"""
        try:
            # Try DATABASE_URL first (Render's format), then individual config
            if self.database_url:
                self.conn = psycopg2.connect(self.database_url)
            else:
                self.conn = psycopg2.connect(**self.db_config)
            
            self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Create table if it doesn't exist (PostgreSQL syntax)
            self.cur.execute('''
            CREATE TABLE IF NOT EXISTS jobs(
                id SERIAL PRIMARY KEY,
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
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );''')
            
            # Create trigger for updated_at (PostgreSQL doesn't have ON UPDATE like MySQL)
            self.cur.execute('''
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ language 'plpgsql';
            ''')
            
            self.cur.execute('''
            DROP TRIGGER IF EXISTS update_jobs_updated_at ON jobs;
            CREATE TRIGGER update_jobs_updated_at
                BEFORE UPDATE ON jobs
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();
            ''')
            
            self.conn.commit()
            print("PostgreSQL database connection established successfully")
            
        except psycopg2.Error as e:
            print(f"Error connecting to PostgreSQL database: {e}")
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
            existing_job = self.cur.fetchone()

            if existing_job:
                # If job exists, update its details
                self.cur.execute("""
                    UPDATE jobs SET
                        title = %s, job_cat = %s, location = %s, company = %s,
                        education = %s, experience = %s, skills = %s, 
                        general_requirements = %s, specific_requirements = %s,
                        dis = %s, responsibilities = %s, updated_at = CURRENT_TIMESTAMP
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

        except psycopg2.Error as e:
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
        spider.logger.info("PostgreSQL connection closed by SaveToPostgreSQLPipeLine.")