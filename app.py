from pymongo import DeleteMany, InsertOne, ReplaceOne, UpdateMany
import streamlit as st
from init_db import get_collection, initialize_sample_data
from gemini_utils import generate_mongo_query, explain_query
import pandas as pd
import plotly.express as px
import uuid
from datetime import datetime
import json
from bson import ObjectId
from mongo_utils import get_mongo_client

# Initialize page config
st.set_page_config(
    page_title="AI Database Assistant", 
    layout="wide", 
    page_icon="🤖",
    initial_sidebar_state="expanded"
)

# Custom CSS for UI enhancements
st.markdown("""
<style>
    /* Main container */
    .stApp {
        background-color: #0e1117;
    }
    
    /* Cards and containers */
    .card {
        background-color: #192841;
        border-radius: 10px;
        padding: 20px;
        margin-bottom: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    /* Buttons */
    .stButton>button {
        background-color: #9ef0f0;
        color: #0e1117;
        border-radius: 8px;
        font-weight: bold;
        transition: all 0.3s;
    }
    
    .stButton>button:hover {
        background-color: #6ae0e0;
        transform: scale(1.05);
    }
    
    /* Text areas */
    .stTextArea textarea {
        background-color: #1a1a2e !important;
        color: #fafafa !important;
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #9ef0f0 !important;
    }
    
    /* Status container */
    .stStatusContainer {
        border: 1px solid #4a90e2;
        border-radius: 8px;
        padding: 15px;
        background-color: #1a1a2e;
    }
    
    /* Error messages */
    .stAlert {
        border-radius: 8px;
    }
    
    /* Chatbot container */
    #pocketflow-chatbot-container {
        position: fixed;
        bottom: 20px;
        right: 20px;
        z-index: 1000;
    }
    
    #pocketflow-chatbot-icon {
        width: 60px;
        height: 60px;
        border-radius: 50%;
        background: linear-gradient(135deg, #6a11cb 0%, #2575fc 100%);
        display: flex;
        align-items: center;
        justify-content: center;
        color: white;
        font-size: 24px;
        cursor: pointer;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'current_query' not in st.session_state:
    st.session_state.current_query = ""
if 'explain_mode' not in st.session_state:
    st.session_state.explain_mode = True
if 'visualization_type' not in st.session_state:
    st.session_state.visualization_type = "Table"
if 'collection_name' not in st.session_state:
    st.session_state.collection_name = "students"
if 'schema_info' not in st.session_state:
    st.session_state.schema_info = {
        "students": ["name", "major", "enrollment_year", "gpa", "courses", "salary"],
        "courses": ["title", "department", "credits", "instructor"],
        "departments": ["name", "code", "head"],
        "faculty": ["name", "department", "position"],
        "enrollments": ["student", "course", "grade"]
    }

# --- UI Components ---
def collection_selector():
    collections = ["students", "courses", "departments", "faculty", "enrollments"]
    st.session_state.collection_name = st.sidebar.selectbox(
        "📁 Select Collection", 
        collections, 
        key="collection_selector"
    )
    return st.session_state.collection_name

def visualization_selector():
    options = ["Table", "Bar Chart", "Pie Chart", "Line Chart", "Scatter Plot", "Histogram"]
    return st.selectbox("📊 Visualization Type", options, key="viz_selector")

def show_query_history():
    with st.sidebar.expander("📜 Query History", expanded=False):
        if not st.session_state.query_history:
            st.info("No history yet")
            return
            
        for idx, entry in enumerate(st.session_state.query_history):
            col1, col2 = st.columns([3, 1])
            with col1:
                st.caption(f"{entry['timestamp'][11:19]}: {entry['query']}")
            with col2:
                if st.button("↻", key=f"history_btn_{idx}"):
                    st.session_state.current_query = entry['query']
                    st.session_state.explain_mode = False
                    st.rerun()

# --- Visualization Functions ---
def safe_convert(value):
    """Convert MongoDB values to Python types safe for DataFrame conversion"""
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, list):
        return ", ".join(map(str, value))
    if isinstance(value, dict):
        return json.dumps(value)
    return value

def visualize_data(data, viz_type):
    if not data or not isinstance(data, list) or len(data) == 0:
        return None
    
    try:
        # Convert MongoDB data to safe types
        safe_data = []
        for doc in data:
            safe_doc = {}
            for key, value in doc.items():
                safe_doc[key] = safe_convert(value)
            safe_data.append(safe_doc)
            
        df = pd.DataFrame(safe_data)
        
        if df.empty:
            return None
            
        # Auto-detect best visualization
        if viz_type == "Auto":
            numeric_cols = df.select_dtypes(include=['number']).columns
            
            if len(numeric_cols) >= 2:
                return px.scatter(df, x=numeric_cols[0], y=numeric_cols[1], title="Scatter Plot")
            elif len(df.columns) >= 2:
                return px.bar(df, x=df.columns[0], y=df.columns[1], title="Bar Chart")
            else:
                return df
                
        elif viz_type == "Table":
            return df
        
        elif viz_type == "Bar Chart":
            if len(df.columns) < 2:
                return df
            return px.bar(df, x=df.columns[0], y=df.columns[1], title="Bar Chart")
        
        elif viz_type == "Pie Chart":
            if len(df.columns) < 2:
                return df
            return px.pie(df, names=df.columns[0], values=df.columns[1], title="Pie Chart")
        
        elif viz_type == "Line Chart":
            if len(df.columns) < 2:
                return df
            return px.line(df, x=df.columns[0], y=df.columns[1], title="Line Chart")
        
        elif viz_type == "Scatter Plot":
            if len(df.columns) < 2:
                return df
            return px.scatter(df, x=df.columns[0], y=df.columns[1], title="Scatter Plot")
        
        elif viz_type == "Histogram":
            if len(df.columns) < 1:
                return df
            return px.histogram(df, x=df.columns[0], title="Histogram")
        
        return df
    except Exception as e:
        st.error(f"Visualization error: {str(e)}")
        return df  # Fallback to DataFrame

# --- Query Execution ---
def execute_mongo_query(query: dict):
    if "collection" not in query:
        return {"error": "Collection not specified"}
    
    try:
        collection = get_collection(query["collection"])
        if collection is None:
            return {"error": "Collection not found"}
        
        operation = query.get("operation", "").lower()
        
        if operation == "find":
            find_query = query.get("query", {})
            projection = query.get("projection", {"_id": 0})
            limit = query.get("limit", 100)  # Default limit for safety
            
            cursor = collection.find(find_query, projection)
            if limit > 0:
                cursor = cursor.limit(limit)
            return list(cursor)
        elif operation == "bulk":
            return execute_bulk_operations(query.get("operations", []))
    
        elif operation == "advanced":
            return execute_advanced_operation(query)
        elif operation == "insert":
            # Handle both single and bulk inserts
            if "document" in query:
                document = query["document"]
                result = collection.insert_one(document)
                return {"inserted_id": str(result.inserted_id)}
            elif "documents" in query:
                documents = query["documents"]
                result = collection.insert_many(documents)
                return {"inserted_ids": [str(id) for id in result.inserted_ids]}
            else:
                return {"error": "No document(s) provided for insert operation"}
        
        elif operation == "update":
            filter_query = query.get("filter", {})
            update_query = query.get("update", {})
            result = collection.update_many(filter_query, update_query)
            return {"matched": result.matched_count, "modified": result.modified_count}
        
        elif operation == "delete":
            # Handle both "filter" and "query" parameters
            filter_query = query.get("filter", query.get("query", {}))
            result = collection.delete_many(filter_query)
            return {"deleted": result.deleted_count}
        
        elif operation == "aggregate":
            pipeline = query.get("pipeline", [])
            return list(collection.aggregate(pipeline))
        
        elif operation == "count":
            count_query = query.get("query", {})
            return {"count": collection.count_documents(count_query)}
        
        else:
            return {"error": f"Invalid operation: {operation}"}
    
    except Exception as e:
        return {"error": f"Execution error: {str(e)}"}


def execute_bulk_operations(operations: list):
    """Execute bulk write operations"""
    try:
        if not operations:
            return {"error": "No operations provided"}
            
        collection_name = operations[0].get("collection")
        collection = get_collection(collection_name)
        if collection is None:
            return {"error": "Collection not found"}
        
        bulk_ops = []
        for op in operations:
            op_type = op["operation"]
            
            if op_type == "insert":
                bulk_ops.append(InsertOne(op["document"]))
                
            elif op_type == "update":
                bulk_ops.append(UpdateMany(
                    op["filter"],
                    op["update"]
                ))
                
            elif op_type == "delete":
                bulk_ops.append(DeleteMany(op["filter"]))
                
            elif op_type == "replace":
                bulk_ops.append(ReplaceOne(
                    op["filter"],
                    op["replacement"]
                ))
        
        result = collection.bulk_write(bulk_ops)
        return {
            "inserted": result.inserted_count,
            "modified": result.modified_count,
            "deleted": result.deleted_count,
            "upserted": result.upserted_count
        }
    
    except Exception as e:
        return {"error": f"Bulk operation failed: {str(e)}"}

def execute_advanced_operation(query: dict):
    """Execute advanced MongoDB operations"""
    try:
        collection = get_collection(query["collection"])
        if collection is None:
            return {"error": "Collection not found"}
        
        operation = query["operation"]
        
        if operation == "text_search":
            return list(collection.find(
                {"$text": {"$search": query["search_term"]}},
                {"score": {"$meta": "textScore"}}
            ).sort([("score", {"$meta": "textScore"})]))
        
        elif operation == "geospatial":
            return list(collection.find({
                "location": {
                    "$near": {
                        "$geometry": {
                            "type": "Point",
                            "coordinates": query["coordinates"]
                        },
                        "$maxDistance": query.get("max_distance", 1000)
                    }
                }
            }))
        
        elif operation == "transaction":
            with get_mongo_client().start_session() as session:
                with session.start_transaction():
                    results = []
                    for op in query["operations"]:
                        results.append(execute_mongo_query(op))
                    return results
        
        elif operation == "create_index":
            index_spec = query["index"]
            collection.create_index(index_spec)
            return {"status": f"Index created: {index_spec}"}
        
        elif operation == "map_reduce":
            result = collection.map_reduce(
                query["map"],
                query["reduce"],
                query.get("out", "results"),
                query.get("query", {})
            )
            return list(result.find())
        
        else:
            return {"error": f"Unsupported advanced operation: {operation}"}
    
    except Exception as e:
        return {"error": f"Advanced operation failed: {str(e)}"}

# Update the validation function
def validate_query(query: dict) -> tuple:
    """Validate MongoDB query structure"""
    required_keys = ["collection", "operation"]
    
    # Check required keys
    if not all(key in query for key in required_keys):
        return False, "Missing required keys (collection or operation)"
    
    # Validate operation types
    valid_operations = ["find", "insert", "update", "delete", "aggregate", "count", "bulk", "advanced"]
    if query["operation"] not in valid_operations:
        return False, f"Invalid operation: {query['operation']}"
    
    # Operation-specific validation
    operation = query["operation"]
    
    if operation == "insert":
        if "document" not in query and "documents" not in query:
            return False, "Insert operation requires 'document' or 'documents' parameter"
    
    if operation == "update" and ("filter" not in query or "update" not in query):
        return False, "Update operation requires 'filter' and 'update'"
    
    if operation == "delete":
        if "filter" not in query and "query" not in query:
            return False, "Delete operation requires 'filter' or 'query' parameter"
    
    if operation == "aggregate" and "pipeline" not in query:
        return False, "Aggregate operation requires 'pipeline'"
    
    if operation == "bulk" and "operations" not in query:
        return False, "Bulk operation requires 'operations' array"
    
    if operation == "advanced" and "advanced_operation" not in query:
        return False, "Advanced operation requires 'advanced_operation' type"
    
    return True, "Valid query"

# --- Query History Management ---
def save_query_to_history(query, result):
    try:
        history_entry = {
            "id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "query": query,
            "result_summary": f"{len(result)} items" if isinstance(result, list) else "Operation",
            "result": result
        }
        st.session_state.query_history.insert(0, history_entry)
        
        # Keep only last 20 entries
        if len(st.session_state.query_history) > 20:
            st.session_state.query_history = st.session_state.query_history[:20]
    except Exception as e:
        st.error(f"History save error: {str(e)}")

# --- Chatbot Embed Function ---
def embed_chatbot():
    """Embed PocketFlow Chatbot in the application"""
    chatbot_html = """
    <!-- PocketFlow Chatbot - Start -->
    <div id="pocketflow-chatbot-container">
        <div id="pocketflow-chatbot-icon">🤖</div>
    </div>
    <script>
    (function() {
        var script = document.createElement("script");
        script.src = "https://askthispage.com/embed/chatbot.js";
        script.onload = function() {
            initializeChatbot({
                extra_urls: [],
                prefixes: [],
                chatbotName: 'Database Assistant',
                wsUrl: 'wss://askthispage.com/api/ws/chat',
                instruction: 'You are an expert MongoDB assistant. Help users with database queries and operations.',
                isOpen: false
            });
        };
        document.head.appendChild(script);
    })();
    </script>
    <!-- PocketFlow Chatbot - End -->
    """
    st.components.v1.html(chatbot_html, height=0)

# --- Main Application ---
def main():
    # Header with gradient
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #6a11cb 0%, #2575fc 100%);
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 2rem;
    ">
        <h1 style="color:white;text-align:center;">🤖 AI-Powered Database Assistant</h1>
        <p style="color:white;text-align:center;">Interact with your MongoDB using natural language commands</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize MongoDB
    collection_name = collection_selector()
    if not initialize_sample_data(collection_name):
        st.error("Failed to initialize sample data. Check database connection.")
    
    # Sidebar controls
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        with st.expander("📋 Database Schema", expanded=False):
            if collection_name in st.session_state.schema_info:
                st.write(f"**Collection:** `{collection_name}`")
                st.write("**Fields:**")
                for field in st.session_state.schema_info[collection_name]:
                    st.code(field, language="plaintext")
            else:
                st.info("No schema info available")
        
        st.session_state.explain_mode = st.checkbox("Explain queries before execution", value=True)
        debug_mode = st.checkbox("Enable Debug Mode", value=False)
        show_query_history()
        
        st.divider()
        st.info("💡 **Tips & Examples:**")
        st.caption("""
        **Find:** "Show students with GPA > 3.5"  
        **Insert:** "Add new student: John, CS, 2023"  
        **Update:** "Increase salary by 10% for Data Scientists"  
        **Delete Field:** "Remove salary for Komal"  
        **Delete Docs:** "Remove students enrolled before 2020"  
        **Aggregate:** "Show average salary by major"
        """)
        
        # Chatbot info in sidebar
        with st.expander("💬 AI Chatbot Help", expanded=False):
            st.write("""
            **The AI chatbot is available in the bottom-right corner!**  
            Click the 🤖 icon to get help with:
            - Database queries
            - MongoDB syntax
            - Query optimization
            - Data visualization tips
            """)
            st.info("The chatbot understands your database schema and can answer questions about your data!")

    # Command input area
    st.subheader("💬 Enter Your Command")
    user_input = st.text_area(
        label="Database command:", 
        value=st.session_state.current_query,
        placeholder="e.g., 'Remove salary field for Komal' or 'Show CS students with GPA > 3.7'",
        height=100,
        label_visibility="collapsed",
        key="command_input"
    )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        execute_btn = st.button("🚀 Execute Command", type="primary", use_container_width=True)
    with col2:
        st.caption("💡 Tip: Be specific - mention field names and values")

    # Variable to store execution result
    execution_result = None
    query_executed = False

    if execute_btn and user_input:
        st.session_state.current_query = user_input
        query_executed = True
        
        # Step 1: Generate query
        with st.status("🔍 Generating database query...", expanded=True) as status:
            st.write("Processing your natural language command...")
            try:
                mongo_query = generate_mongo_query(user_input, collection_name)
            except Exception as e:
                st.error(f"❌ Generation Error: {str(e)}")
                status.update(label="Query generation failed", state="error")
                return
            
            if "error" in mongo_query:
                st.error(f"❌ Generation Error: {mongo_query['error']}")
                status.update(label="Query generation failed", state="error")
                return
            
            st.subheader("Generated Query")
            st.json(mongo_query)
            
            # Validate query
            is_valid, validation_msg = validate_query(mongo_query)
            if not is_valid:
                st.error(f"❌ Invalid Query: {validation_msg}")
                status.update(label="Query validation failed", state="error")
                return
            
            # Step 2: Explain query
            if st.session_state.explain_mode:
                st.subheader("Explanation")
                try:
                    explanation = explain_query(mongo_query)
                    st.write(explanation)
                except Exception as e:
                    st.warning(f"Explanation failed: {str(e)}")
                
                if not st.checkbox("✅ Execute this query?", value=True):
                    status.update(label="Query ready", state="complete")
                    query_executed = False
                    return
            
            # Step 3: Execute query
            st.write("Executing query against database...")
            try:
                execution_result = execute_mongo_query(mongo_query)
            except Exception as e:
                st.error(f"❌ Execution Error: {str(e)}")
                status.update(label="Execution failed", state="error")
                return
            
            if "error" in execution_result:
                st.error(f"❌ Execution Error: {execution_result['error']}")
                status.update(label="Execution failed", state="error")
                return
            
            # Step 4: Handle results
            status.update(label="✅ Operation completed!", state="complete", expanded=False)
            st.success("Command executed successfully")
            
            # Save to history
            save_query_to_history(user_input, execution_result)

    # Display results OUTSIDE the status container
    if query_executed and execution_result:
        st.subheader("📊 Results")
        
        # Add export option
        if isinstance(execution_result, list) and execution_result:
            export_col1, export_col2 = st.columns(2)
            with export_col1:
                # Convert to safe types for CSV export
                safe_data = []
                for doc in execution_result:
                    safe_doc = {}
                    for key, value in doc.items():
                        safe_doc[key] = safe_convert(value)
                    safe_data.append(safe_doc)
                    
                csv = pd.DataFrame(safe_data).to_csv(index=False).encode('utf-8')
                st.download_button(
                    "💾 Export as CSV",
                    csv,
                    f"{collection_name}_results.csv",
                    "text/csv",
                    key='download-csv'
                )
            with export_col2:
                st.session_state.visualization_type = visualization_selector()
        
        # Visualize results
        if isinstance(execution_result, list) and execution_result:
            viz_result = visualize_data(execution_result, st.session_state.visualization_type)
            
            if viz_result is not None:
                if isinstance(viz_result, pd.DataFrame):
                    st.dataframe(viz_result, use_container_width=True)
                else:
                    st.plotly_chart(viz_result, use_container_width=True)
            else:
                st.info("No data to visualize")
            
            # Show raw data in expander (now outside status container)
            with st.expander("🔍 Raw Data"):
                st.json(execution_result)
        else:
            st.subheader("Operation Result")
            st.json(execution_result)
            
        if debug_mode:
            with st.expander("🚧 Debug Information"):
                st.subheader("Execution Details")
                st.json({
                    "collection": collection_name,
                    "query": user_input,
                    "result_type": type(execution_result).__name__,
                    "result_size": len(execution_result) if isinstance(execution_result, list) else 1
                })
    
    # Embed the chatbot at the end of the main content
    embed_chatbot()

if __name__ == "__main__":
    main()
