import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# --- Page Config & Styling ---
st.set_page_config(
    page_title="Distributed ML Pipeline | Monitoring",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Terminal-Style UI
st.markdown("""
<style>
    :root {
        --bg-color: #0d1117;
        --secondary-bg: #161b22;
        --accent-green: #2ecc71;
        --accent-amber: #f1c40f;
        --text-color: #e6edf3;
    }
    .stApp {
        background-color: var(--bg-color);
        color: var(--text-color);
    }
    [data-testid="stSidebar"] {
        background-color: var(--secondary-bg);
    }
    h1, h2, h3 {
        color: var(--accent-green) !important;
        font-family: 'Courier New', Courier, monospace;
    }
    .stMetric {
        background-color: var(--secondary-bg);
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #30363d;
    }
    .stDataFrame {
        border: 1px solid #30363d;
    }
</style>
""", unsafe_allow_html=True)

# --- Sidebar Navigation ---
st.sidebar.title("🤖 ML Data Control")
page = st.sidebar.radio("Navigate", [
    "Pipeline Runs", 
    "Dataset Versions", 
    "Quality Reports", 
    "Deduplication Stats", 
    "Source Analytics"
])

st.sidebar.markdown("---")
st.sidebar.info("Pipeline Status: **RUNNING**")
st.sidebar.text(f"Last Sync: {datetime.now().strftime('%H:%M:%S')}")

# --- Helper Functions for Mock Data ---
def get_pipeline_history():
    return pd.DataFrame({
        "DAG ID": ["daily_ingestion", "weekly_dedupe", "weekly_quality", "daily_ingestion"],
        "Status": ["Success", "Success", "Running", "Success"],
        "Duration": ["12m 30s", "45m 12s", "18m 05s", "10m 55s"],
        "Records": [1500200, 4800000, 3200100, 1499500],
        "Timestamp": [datetime.now() - timedelta(hours=i*24) for i in range(4)]
    })

# --- Main Pages ---

if page == "Pipeline Runs":
    st.title("> PIPELINE_RUN_HISTORY")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Documents", "5,000,210", "+152k")
    col2.metric("Avg Quality Score", "0.92", "+0.03")
    col3.metric("Storage Used", "42.5 GB", "S3/MinIO")
    col4.metric("Uptime", "148h", "99.9%")

    df_runs = get_pipeline_history()
    st.table(df_runs)
    
    st.subheader("Runtime Trend")
    chart_data = pd.DataFrame(np.random.randn(20, 2), columns=['Ingestion', 'Dedupe'])
    st.line_chart(chart_data)

elif page == "Dataset Versions":
    st.title("> DATASET_VERSIONING_LOG")
    
    versions = [
        {"version": "v20240610_a7b2", "records": "4.2M", "size": "38GB", "status": "Current"},
        {"version": "v20240520_b3d9", "records": "3.8M", "size": "35GB", "status": "Archived"},
        {"version": "v20240415_f1e8", "records": "3.5M", "size": "32GB", "status": "Archived"}
    ]
    st.dataframe(pd.DataFrame(versions), use_container_width=True)
    
    st.subheader("Record Growth")
    st.area_chart(pd.DataFrame([3.2, 3.5, 3.8, 4.2], columns=["Records (Millions)"]))

elif page == "Quality Reports":
    st.title("> DATA_QUALITY_ANALYTICS")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Filter Pass Rates")
        stages = ["Language", "Length", "Toxicity", "Perplexity"]
        pass_rates = [98.5, 95.2, 88.0, 91.5]
        fig = px.bar(x=stages, y=pass_rates, color=pass_rates, 
                     color_continuous_scale="Viridis", labels={'x': 'Stage', 'y': '% Pass'})
        st.plotly_chart(fig, use_container_width=True)
        
    with col2:
        st.subheader("Rejection Reasons")
        labels = ['Toxic', 'Low Word Count', 'Non-English', 'Near Duplicate']
        values = [4500, 2500, 10500, 15000]
        fig = px.pie(values=values, names=labels, hole=.3)
        st.plotly_chart(fig, use_container_width=True)

elif page == "Deduplication Stats":
    st.title("> DEDUPLICATION_METRICS")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Jaccard Similarity Distribution")
        hist_data = np.random.normal(0.85, 0.1, 1000)
        fig = px.histogram(x=hist_data, nbins=50, title="Candidate Pair Similarity")
        st.plotly_chart(fig, use_container_width=True)
        
    with col2:
        st.subheader("Source Overlap Heatmap")
        z = [[10, 20, 30, 40], [20, 10, 10, 20], [30, 10, 50, 10], [40, 20, 10, 90]]
        fig = px.imshow(z, x=['Web', 'DB', 'API', 'Int'], y=['Web', 'DB', 'API', 'Int'], 
                        color_continuous_scale='GnBu')
        st.plotly_chart(fig, use_container_width=True)

elif page == "Source Analytics":
    st.title("> SOURCE_INSIGHTS")
    
    source_df = pd.DataFrame({
        'Source': ['Web Crawl', 'Structured DB', 'API Export', 'Internal Docs'],
        'Count': [2000000, 1000000, 1000000, 1000000],
        'Avg Word Count': [820, 310, 290, 1100]
    })
    
    st.subheader("Distribution by Source")
    fig = px.bar(source_df, x='Source', y='Count', color='Source')
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Word Count Profile")
    fig = px.box(source_df, x='Source', y='Avg Word Count')
    st.plotly_chart(fig, use_container_width=True)

st.sidebar.markdown("---")
st.sidebar.write("System: AMD 32-Core / 64GB RAM")
st.sidebar.write("Workspace: `/mnt/data/ml-pipeline`")
