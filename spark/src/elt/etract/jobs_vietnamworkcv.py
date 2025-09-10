import pandas as pd

def extract_vienamwork_jobs():
    source_json_path = "/opt/data/bronze/vietnamworks_jobs.json"
    df = pd.read_json(source_json_path)
    return df

