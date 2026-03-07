import requests
import pandas as pd
from datetime import datetime
import os

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

os.makedirs("/opt/airflow/output", exist_ok=True)

RAW_FILE = "/opt/airflow/output/playlist_raw.csv"
TRANSFORM_FILE = "/opt/airflow/output/playlist_transformed.csv"
SUMMARY_FILE = "/opt/airflow/output/summary_report.txt"


# -----------------------------
# Fetch Data
# -----------------------------
def fetch_playlist():

    url = "https://raw.githubusercontent.com/rushi4git/spotify-playlist-data/main/spotify_playlist.json"

    response = requests.get(url)

    data = response.json()

    df = pd.DataFrame(data["tracks"])

    df.to_csv(RAW_FILE, index=False)

    print("Raw playlist saved")


# -----------------------------
# Transform Data
# -----------------------------
def transform_playlist():

    df = pd.read_csv(RAW_FILE)

    df["duration_minutes"] = df["duration_ms"] / 60000
    df["release_year"] = df["release_date"].astype(str).str[:4]

    def category(pop):
        if pop <= 40:
            return "Low"
        elif pop <= 70:
            return "Medium"
        else:
            return "High"

    df["popularity_category"] = df["popularity"].apply(category)

    df = df.drop_duplicates()

    df.to_csv(TRANSFORM_FILE, index=False)

    print("Transformed file created")


# -----------------------------
# Generate Summary Report
# -----------------------------
def generate_summary():

    df = pd.read_csv(TRANSFORM_FILE)

    total_tracks = len(df)
    avg_duration = df["duration_minutes"].mean()

    top_tracks = df.sort_values(by="popularity", ascending=False).head(5)

    artist_counts = df["artist_name"].value_counts()

    with open(SUMMARY_FILE, "w") as f:

        f.write("Spotify Playlist Analysis Report\n")
        f.write("---------------------------------\n\n")

        f.write(f"Total Tracks: {total_tracks}\n")
        f.write(f"Average Duration (minutes): {avg_duration:.2f}\n\n")

        f.write("Top 5 Most Popular Tracks:\n")
        f.write(top_tracks[["track_name", "popularity"]].to_string(index=False))
        f.write("\n\n")

        f.write("Tracks per Artist:\n")
        f.write(artist_counts.to_string())


# -----------------------------
# DAG Definition
# -----------------------------
default_args = {
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id="spotify_playlist_pipeline",
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["spotify", "data_pipeline"]
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_playlist",
        python_callable=fetch_playlist
    )

    transform_task = PythonOperator(
        task_id="transform_playlist",
        python_callable=transform_playlist
    )

    summary_task = PythonOperator(
        task_id="generate_summary",
        python_callable=generate_summary
    )

    fetch_task >> transform_task >> summary_task