import pandas as pd

def extract_topcv_jobs():
    source_json_path = "/opt/data/bronze/topcv_jobs.json"
    df = pd.read_json(source_json_path)
    return df
