import os
import logging
import psycopg2
import psycopg2.extras
import logging
logger = logging.getLogger(__name__)
def allowed_file(filename):
    """Check if the uploaded file has an allowed extension."""
    ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'doc', 'docx', 'csv', 'xlsx'}
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_db_connection():
    """Get PostgreSQL database connection with error handling"""
    try:
        # Try DATABASE_URL first (Render provides this)
        database_url = os.getenv('DATABASE_URL')
        
        if database_url:
            conn = psycopg2.connect(database_url)
        else:
            # Fallback to individual environment variables
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', ''),
                database=os.getenv('DB_NAME', 'jobs'),
                port=int(os.getenv('DB_PORT', '5432'))
            )
        
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL database: {e}")
        return None

def get_jobs_from_db(limit=None):
    """Fetch jobs from PostgreSQL database"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        if limit:
            cur.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT %s", (limit,))
        else:
            cur.execute("SELECT * FROM jobs ORDER BY created_at DESC")
        
        jobs = cur.fetchall()
        
        # Convert RealDictRow to regular dict for easier handling
        jobs_list = [dict(job) for job in jobs]
        
        cur.close()
        conn.close()
        
        return jobs_list
    except psycopg2.Error as e:
        logging.error(f"Error fetching jobs from database: {e}")
        if conn:
            conn.close()
        return []

def fetch_jobs_from_db(limit=None):
    """Alias for get_jobs_from_db for backwards compatibility"""
    return get_jobs_from_db(limit)

def search_jobs_in_db(keywords, limit=10):
    """Search for jobs in PostgreSQL database based on keywords"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Create search query with ILIKE for case-insensitive search
        search_conditions = []
        params = []
        
        for keyword in keywords:
            search_conditions.append("""
                (title ILIKE %s OR 
                 skills ILIKE %s OR 
                 general_requirements ILIKE %s OR 
                 specific_requirements ILIKE %s OR
                 responsibilities ILIKE %s)
            """)
            keyword_param = f'%{keyword}%'
            params.extend([keyword_param] * 5)
        
        query = f"""
            SELECT * FROM jobs 
            WHERE {' OR '.join(search_conditions)}
            ORDER BY created_at DESC
            LIMIT %s
        """
        params.append(limit)
        
        cur.execute(query, params)
        jobs = cur.fetchall()
        
        # Convert RealDictRow to regular dict
        jobs_list = [dict(job) for job in jobs]
        
        cur.close()
        conn.close()
        
        return jobs_list
    except psycopg2.Error as e:
        logging.error(f"Error searching jobs in database: {e}")
        if conn:
            conn.close()
        return []