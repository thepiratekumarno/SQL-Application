import streamlit as st
from pymongo import MongoClient
import pandas as pd
import re

def get_mongo_client():
    return MongoClient(
        host=st.secrets["mongo"]["host"],
        username=st.secrets["mongo"]["username"],
        password=st.secrets["mongo"]["password"],
        authSource="admin",
        tls=True,
        tlsAllowInvalidCertificates=False
    )

def get_collection():
    client = get_mongo_client()
    db = client[st.secrets["mongo"]["database"]]
    return db[st.secrets["mongo"]["collection"]]

def get_schema():
    return """
    MongoDB Collection: students
    Fields:
      - _id: ObjectId (auto-generated)
      - name: string
      - major: string
      - enrollment_year: integer
      - gpa: float
      - courses: array of strings
      - salary: integer
    """

def execute_mongo_query(query: dict):
    collection = get_collection()
    try:
        # Handle different query types
        if "find" in query:
            projection = query.get("projection", {})
            cursor = collection.find(query["find"], projection)
            return list(cursor)
        elif "insert" in query:
            result = collection.insert_one(query["insert"])
            return {"inserted_id": str(result.inserted_id)}
        elif "update" in query:
            result = collection.update_many(
                query["filter"], 
                query["update"]
            )
            return {"matched": result.matched_count, "modified": result.modified_count}
        elif "delete" in query:
            result = collection.delete_many(query["delete"])
            return {"deleted": result.deleted_count}
        else:
            return {"error": "Invalid query type"}
    except Exception as e:
        return {"error": str(e)}