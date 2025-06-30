from pymongo import MongoClient
import streamlit as st
from pymongo.errors import PyMongoError, ConnectionFailure
import time

@st.cache_resource(show_spinner=False)
def get_mongo_client():
    """Create cached MongoDB client connection"""
    try:
        client = MongoClient(
            host=st.secrets["mongo"]["host"],
            username=st.secrets["mongo"]["username"],
            password=st.secrets["mongo"]["password"],
            authSource="admin",
            tls=True,
            tlsAllowInvalidCertificates=True,
            serverSelectionTimeoutMS=3000,
            connectTimeoutMS=3000
        )
        # Test connection
        client.server_info()
        return client
    except ConnectionFailure as e:
        st.error(f"❌ MongoDB connection failed: {str(e)}")
        return None
    except Exception as e:
        st.error(f"❌ Unexpected error: {str(e)}")
        return None

def get_collection(collection_name="students"):
    """Get MongoDB collection with retry logic"""
    max_retries = 2
    for attempt in range(max_retries):
        try:
            client = get_mongo_client()
            if client is None:
                return None
            db = client[st.secrets["mongo"]["database"]]
            return db[collection_name]
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            st.error(f"❌ Failed to get collection: {str(e)}")
            return None

def initialize_sample_data(collection_name="students"):
    """Initialize sample data if collection is empty with error handling"""
    try:
        collection = get_collection(collection_name)
        if collection is None:
            return False
            
        if collection.count_documents({}) > 0:
            return True
            
        sample_data = []
        
        if collection_name == "students":
            sample_data = [
                {"name": "Alice Johnson", "major": "Computer Science", 
                 "enrollment_year": 2020, "gpa": 3.8, "salary": 85000,
                 "courses": ["Data Structures", "Algorithms"]},
                {"name": "Bob Smith", "major": "Data Science", 
                 "enrollment_year": 2021, "gpa": 3.5, "salary": 92000,
                 "courses": ["Machine Learning", "Statistics"]},
                {"name": "Komal Patel", "major": "Business", 
                 "enrollment_year": 2019, "gpa": 3.9, "salary": 78000,
                 "courses": ["Economics", "Management"]},
                {"name": "Alice", "location": {"type": "Point", "coordinates": [-74.0059, 40.7128]}},
                {"name": "Bob", "location": {"type": "Point", "coordinates": [-74.0100, 40.7150]}}
            ]
            
            collection.create_index([("location", "2dsphere")])
            
        elif collection_name == "courses":
            sample_data = [
                {"title": "Data Structures", "department": "CS", 
                 "credits": 4, "instructor": "Dr. Smith"},
                {"title": "Machine Learning", "department": "DS", 
                 "credits": 3, "instructor": "Dr. Johnson"},
                {"title": "Machine Learning Fundamentals", "description": "Introduction to ML algorithms"},
            {"title": "Advanced Data Science", "description": "Deep learning and neural networks"},
            ]
            
            collection.create_index([("title", "text"), ("description", "text")])
            
        elif collection_name == "departments":
            sample_data = [
                {"name": "Computer Science", "code": "CS", "head": "Dr. Smith"},
                {"name": "Data Science", "code": "DS", "head": "Dr. Johnson"}
            ]
        elif collection_name == "faculty":
            sample_data = [
                {"name": "Dr. Smith", "department": "CS", "position": "Professor"},
                {"name": "Dr. Johnson", "department": "DS", "position": "Associate Professor"}
            ]
        elif collection_name == "enrollments":
            sample_data = [
                {"student": "Alice Johnson", "course": "Data Structures", "grade": "A"},
                {"student": "Bob Smith", "course": "Machine Learning", "grade": "B+"}
            ]
        
        if sample_data:
            collection.insert_many(sample_data)
            st.toast(f"✅ Sample data added to {collection_name}", icon="✅")
        
        return True
    except Exception as e:
        st.error(f"❌ Data initialization failed: {str(e)}")
        return False