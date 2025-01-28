import time
import duckdb
from datetime import datetime

def preprocess_data(path, start_date, end_date):
    # start time for performance tracking
    start_time_performance = time.time()

    # ensure that script validates that the end hour is after the start hour
    if end_date <= start_date:
        raise ValueError("End time must be after start time")

    # DuckDB connection
    con = duckdb.connect()

    # read_csv_auto('{path}', gzip=True)
    query = f"""
        SELECT timestamp, user_id, pixel_color
        FROM read_csv_auto('{path}', ignore_errors=true)
        WHERE timestamp >= '{start_date}' AND timestamp < '{end_date}'
    """

    df = con.execute(query).fetchdf()

    end_time_performance = time.time()

    execution_time = (end_time_performance - start_time_performance) * 1000

    print(execution_time)

    return df

# filepath = '/Users/kkragas/Documents/CSC 369/Assignments/Assignment 1/2022_place_canvas_history.csv'
# 1-hour time frame
# start_time = datetime.strptime("2022-04-01 12", "%Y-%m-%d %H") 
# end_time = datetime.strptime("2022-04-01 18", "%Y-%m-%d %H")
# results_1 = preprocess_data(filepath, start_time, end_time)
# print(results_1)

# time: 0.2 mins
# analysis_path = '/Users/kkragas/Documents/CSC 369/Assignments/Assignment 1/analysis_file.csv'
# results_1.to_csv(analysis_path, index=False)

def analyze_preprocessed_data(preprocessed_data_path):

    start_time_performance = time.time()

    con = duckdb.connect()

    query = f"""
        SELECT timestamp, user_id, pixel_color
        FROM read_csv_auto('{preprocessed_data_path}', ignore_errors=true)
    """

    df = con.execute(query).fetchdf()

    # Rank Colors by Distinct Users
    color_ranking = df.groupby('pixel_color')['user_id'].nunique().sort_values(ascending=False).head(5)

    # Calculate Average Session Length
    df = df.sort_values(by=['user_id', 'timestamp'])
    # time differences between consecutive rows for each user
    df['time_diff'] = df.groupby('user_id')['timestamp'].diff().dt.total_seconds().fillna(0)
    # 15-minute = 900 seconds of inactivity 
    df['session_change'] = (df['time_diff'] > 900).astype(int)
    session_lengths = df.groupby(['user_id', df['session_change'].cumsum()])['time_diff'].sum()
    mean_session_length = session_lengths[session_lengths > 0].mean() / 60

    # Pixel Placement Percentiles
    pixel_counts_by_user = df.groupby('user_id').size()
    p50th = pixel_counts_by_user.quantile(0.50).item()
    p75th = pixel_counts_by_user.quantile(0.75).item()
    p90th = pixel_counts_by_user.quantile(0.90).item()
    p99th = pixel_counts_by_user.quantile(0.99).item()
    

    # Count First-Time Users
    first_time_users = df.groupby('user_id').first().shape[0]

    end_time_performance = time.time()

    execution_time = (end_time_performance - start_time_performance) * 1000

    results = {
        "Color Ranking by Distinct Users": color_ranking,
        "Average Session Length (mins)": mean_session_length,
        "50th Percentile": p50th,
        "75th Percentile": p75th,
        "90th Percentile": p90th,
        "99th Percentile": p99th,
        "Count of First-Time Users": first_time_users,
        "Execution Time (ms)": execution_time
    }

    return results

final_results = analyze_preprocessed_data("/Users/kkragas/Documents/CSC 369/Assignments/Assignment 1/analysis_file.csv")
print(final_results)