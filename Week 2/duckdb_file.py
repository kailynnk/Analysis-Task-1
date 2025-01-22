import duckdb
import time
from datetime import datetime

def run_analysis_duckdb(path, start_date, end_date):
    # start time for performance tracking
    start_time_performance = time.time()

    # ensure that script validates that the end hour is after the start hour
    if end_date <= start_date:
        raise ValueError("End time must be after start time")

    # DuckDB connection
    con = duckdb.connect()

    # read_csv_auto('{path}', gzip=True)
    query = f"""
        SELECT pixel_color, coordinate
        FROM read_csv_auto('{path}', compression='gzip')
        WHERE timestamp >= '{start_date}' AND timestamp < '{end_date}'
    """

    df = con.execute(query).fetchdf()

    most_frequent_color = df['pixel_color'].value_counts().idxmax() if not df.empty else None
    most_frequent_coordinate = df['coordinate'].value_counts().idxmax() if not df.empty else None

    end_time_performance = time.time()

    execution_time = (end_time_performance - start_time_performance) * 1000

    results = {
        "Most Placed Color": most_frequent_color,
        "Most Placed Pixel Location": most_frequent_coordinate,
        "Execution Time (ms)": execution_time
    }

    return results


filepath = '/Users/kkragas/Documents/CSC 369/Assignments/Assignment 1/2022_place_canvas_history.csv.gzip'
# 1-hour time frame
start_time = datetime.strptime("2022-04-01 12", "%Y-%m-%d %H") 
end_time = datetime.strptime("2022-04-01 15", "%Y-%m-%d %H")
results_1 = run_analysis_duckdb(filepath, start_time, end_time)
print(results_1)
