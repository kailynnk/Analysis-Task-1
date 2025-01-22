import gzip
import time
import pandas as pd
from datetime import datetime

def run_pandas_analysis(path, start_date, end_date):

    # start time for performance
    start_time_performance = time.time()

    # ensure that script validates that the end hour is after the start hour
    if end_date <= start_date:
        raise ValueError("End time must be after start time")
    
    df = pd.read_csv(path)

    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
    df['timestamp'] = df['timestamp'].dt.tz_convert(None)
    
    filtered_df = df[(df['timestamp'] >= start_date) & (df['timestamp'] < end_date)]
    
    color_counts = filtered_df['pixel_color'].value_counts()
    coordinate_counts = filtered_df['coordinate'].value_counts()

    most_frequent_color = color_counts.idxmax() if not color_counts.empty else None
    most_frequent_coordinate = coordinate_counts.idxmax() if not coordinate_counts.empty else None

    end_time_performance = time.time()

    execution_time = (end_time_performance - start_time_performance) * 1000

    results = {
        "Most Placed Color": most_frequent_color,
        "Most Placed Pixel Location": most_frequent_coordinate,
        "Execution Time (ms)": execution_time
    }

    return results

filepath = '/Users/kkragas/Documents/CSC 369/Assignments/Assignment 1/2022_place_canvas_history.csv'
start_time = datetime.strptime("2022-04-01 12", "%Y-%m-%d %H") 
end_time = datetime.strptime("2022-04-01 18", "%Y-%m-%d %H")
results_1 = run_pandas_analysis(filepath, start_time, end_time)
print(results_1)
