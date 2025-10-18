import pandas as pd

def fetch_attribution_data(project_id, dataset, table,client):
    query = f"""
        SELECT
            DATE(conversion_at) AS conversion_date,
            first_click_channel,
            last_click_channel,
            purchase_value
        FROM `{project_id}.{dataset}.{table}`
        WHERE conversion_at BETWEEN TIMESTAMP('2020-11-15') AND TIMESTAMP('2020-11-30')
    """
    return client.query(query).to_dataframe()

def load_live_panel_data(project_id, dataset, table,client):
    
    query = f"""
        SELECT *
        FROM `{project_id}.{dataset}.{table}`
        ORDER BY conversion_at DESC
        LIMIT 50
    """
    return client.query(query).to_dataframe()