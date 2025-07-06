# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class JobscrapingPipeline:
    def process_item(self, item, spider):
        return item

import mysql.connector

import json
from jobscraping.items import JobItem # Ensure this import path is correct

class SaveToMYSqlPipeLine:

    def __init__(self):
        # Establish database connection
        self.conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password="my$qlayush1", # Make sure this is correct for your MySQL setup
            database="jobs",
            # Optional: autocommit=False if you want to explicitly commit
        )
        self.cur = self.conn.cursor()

        # Create table if it doesn't exist
        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS jobs(
            id INT AUTO_INCREMENT PRIMARY KEY,
            url VARCHAR(255) UNIQUE, -- Changed to VARCHAR(255) and UNIQUE for better indexing and data integrity
            title VARCHAR(255),
            job_cat VARCHAR(255),
            location VARCHAR(255),
            company VARCHAR(255),
            education VARCHAR(255),
            experience VARCHAR(255),
            skills TEXT,              -- Storing lists as JSON strings
            general_requirements TEXT,
            specific_requirements TEXT,
            dis TEXT,                 -- Storing lists as JSON strings
            responsibilities TEXT     -- Storing lists as JSON strings
        );'''
        )
        # Add an index for faster lookups on the 'url' column
        # MySQL automatically creates an index for UNIQUE columns, but explicitly adding for clarity.


        self.conn.commit()


    def process_item(self, item, spider):
        try:
            # Convert lists to JSON strings for storage in TEXT columns
            # Ensure a list is always passed to json.dumps
            skills_json = json.dumps(item.get('skills', []))
            general_requirements_json = json.dumps(item.get('general_requirements', []))
            specific_requirements_json = json.dumps(item.get('specific_requirements', []))
            dis_json = json.dumps(item.get('dis', []))
            responsibilities_json = json.dumps(item.get('responsibilities', []))

            # Check if job already exists based on URL to prevent duplicates and enable updates
            self.cur.execute("SELECT id FROM jobs WHERE url = %s", (str(item.get('url', '')),))
            existing_job_id = self.cur.fetchone()

            if existing_job_id:
                # If job exists, update its details
                self.cur.execute("""
                    UPDATE jobs SET
                        title = %s, job_cat = %s, location = %s, company = %s,
                        education = %s, experience = %s, skills = %s, dis = %s,
                        responsibilities = %s
                    WHERE url = %s
                """, (
                    str(item.get('title', '')),
                    str(item.get('job_cat', '')),
                    str(item.get('location', '')),
                    str(item.get('company', '')),
                    str(item.get('education', '')),
                    str(item.get('experience', '')),
                    skills_json,              # Use JSON string
                    general_requirements_json,
                    specific_requirements_json,
                    dis_json,                 # Use JSON string
                    responsibilities_json,    # Use JSON string
                    str(item.get('url', ''))  # WHERE clause
                ))
                spider.logger.info(f"Updated job: {item.get('title', 'No title')} - {item.get('company', 'No company')}")
            else:
                # If job does not exist, insert new record
                self.cur.execute("""
                    INSERT INTO jobs(
                        url, title, job_cat, location, company,
                        education, experience, skills, general_requirements,specific_requirements,dis, responsibilities
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
                    skills_json,              # Use JSON string
                    general_requirements_json,
                    specific_requirements_json,
                    dis_json,                 # Use JSON string
                    responsibilities_json     # Use JSON string
                ))
                spider.logger.info(f"Added new job: {item.get('title', 'No title')} - {item.get('company', 'No company')}")

            self.conn.commit()
            return item

        except mysql.connector.Error as e:
            self.conn.rollback()
            spider.logger.error(f"Database error while saving item: {e}")
            spider.logger.error(f"Item data: {dict(item)}")
            # Re-raise or drop item if critical for integrity, otherwise return item to continue
            # For now, returning item to continue processing other items
            return item
        except KeyError as e:
            spider.logger.error(f"Missing key in item: {e}")
            spider.logger.error(f"Available keys: {list(item.keys())}")
            spider.logger.error(f"Item data: {dict(item)}")
            return item

    def close_spider(self, spider):
        # Ensure cursor and connection are closed properly
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        spider.logger.info("MySQL connection closed by SaveToMYSqlPipeLine.")