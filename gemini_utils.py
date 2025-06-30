import os
import requests
import json
import re
from dotenv import load_dotenv
import streamlit as st

load_dotenv()

@st.cache_data(ttl=600, show_spinner=False)
def generate_mongo_query(user_prompt: str, collection_name: str) -> dict:
    """Generate MongoDB query from natural language with caching"""
    API_KEY = os.getenv("GOOGLE_API_KEY")
    if not API_KEY:
        return {"error": "API key missing"}
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={API_KEY}"
    headers = {'Content-Type': 'application/json'}
    
    # Enhanced prompt with schema awareness
    prompt = f"""
    You are an expert MongoDB developer. Convert this natural language command into a MongoDB query in JSON format.

Collection: {collection_name}
Important: 
- For bulk operations, use "bulk" operation with "operations" array
- For advanced operations, use "advanced" operation with specific parameters
- Use proper MongoDB syntax for complex queries
- Support transactions, geospatial queries, text search, etc.

# Advanced Operation Examples:

1. Bulk Insert:
Command: "Add three new students: Alice (GPA 3.8), Bob (GPA 3.5), Charlie (GPA 3.9)"
{{
  "operation": "bulk",
  "operations": [
    {{
      "operation": "insert",
      "collection": "students",
      "document": {{"name": "Alice", "gpa": 3.8}}
    }},
    {{
      "operation": "insert",
      "collection": "students",
      "document": {{"name": "Bob", "gpa": 3.5}}
    }},
    {{
      "operation": "insert",
      "collection": "students",
      "document": {{"name": "Charlie", "gpa": 3.9}}
    }}
  ]
}}

2. Transaction (Update and Delete):
Command: "Transfer 10000 salary from Alice to Bob and remove Charlie's enrollment"
{{
  "operation": "advanced",
  "advanced_operation": "transaction",
  "operations": [
    {{
      "operation": "update",
      "collection": "students",
      "filter": {{"name": "Alice"}},
      "update": {{"$inc": {{"salary": -10000}}}}
    }},
    {{
      "operation": "update",
      "collection": "students",
      "filter": {{"name": "Bob"}},
      "update": {{"$inc": {{"salary": 10000}}}}
    }},
    {{
      "operation": "delete",
      "collection": "enrollments",
      "filter": {{"student": "Charlie"}}
    }}
  ]
}}

3. Text Search:
Command: "Find courses related to machine learning"
{{
  "operation": "advanced",
  "advanced_operation": "text_search",
  "collection": "courses",
  "search_term": "machine learning"
}}

4. Geospatial Query:
Command: "Find students within 5km of New York"
{{
  "operation": "advanced",
  "advanced_operation": "geospatial",
  "collection": "students",
  "coordinates": [-74.0059, 40.7128],
  "max_distance": 5000
}}

5. Map-Reduce:
Command: "Calculate average GPA by department using map-reduce"
{{
  "operation": "advanced",
  "advanced_operation": "map_reduce",
  "collection": "students",
  "map": "function() {{ emit(this.major, this.gpa); }}",
  "reduce": "function(key, values) {{ return Array.avg(values); }}"
}}

6. Create Index:
Command: "Create text index on course titles"
{{
  "operation": "advanced",
  "advanced_operation": "create_index",
  "collection": "courses",
  "index": {{"title": "text"}}
}}

    
    Command: "{user_prompt}"
    """
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 1000,
            "topP": 0.8,
            "topK": 40
        },
        "safetySettings": [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
        ]
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        response_json = response.json()
        
        # Extract text response
        if 'candidates' in response_json and response_json['candidates']:
            query_text = response_json['candidates'][0]['content']['parts'][0]['text']
        elif 'content' in response_json and 'parts' in response_json['content']:
            query_text = response_json['content']['parts'][0]['text']
        else:
            return {"error": "Unexpected API response: " + json.dumps(response_json)}
        
        # Clean and parse JSON
        query_text = re.sub(r'^```json|```$', '', query_text, flags=re.IGNORECASE).strip()
        
        # Fix common JSON formatting issues
        query_text = query_text.replace("'", "\"")  # Replace single quotes
        query_text = re.sub(r'(\w+):', r'"\1":', query_text)  # Add quotes to keys
        query_text = re.sub(r':\s*([a-zA-Z_][\w]*)', r': "\1"', query_text)  # Add quotes to unquoted string values
        
        return json.loads(query_text)
    
    except requests.exceptions.RequestException as e:
        return {"error": f"Network error: {str(e)}"}
    except json.JSONDecodeError as e:
        return {"error": f"JSON parsing error: {str(e)} in: {query_text}"}
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}"}

@st.cache_data(ttl=600, show_spinner=False)
def explain_query(query):
    """Explain MongoDB query in simple terms with caching"""
    API_KEY = os.getenv("GOOGLE_API_KEY")
    if not API_KEY:
        return "API key missing"
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={API_KEY}"
    headers = {'Content-Type': 'application/json'}
    
    # Enhanced explanation prompt
    prompt = f"""
    Explain this MongoDB query in simple terms for a non-technical user:
    1. Describe the operation being performed
    2. Explain the filter criteria (if any)
    3. Explain the update/change (if any)
    4. Mention which collection is being accessed
    
    Query:
    {json.dumps(query, indent=2)}
    """
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 500}
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=20)
        response.raise_for_status()
        response_json = response.json()
        
        if 'candidates' in response_json and response_json['candidates']:
            return response_json['candidates'][0]['content']['parts'][0]['text']
        return "Failed to generate explanation"
    
    except Exception as e:
        return f"Explanation service unavailable: {str(e)}"