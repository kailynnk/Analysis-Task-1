import time
import polars as pl
from datetime import datetime

def run_analysis_polars(path, start_date, end_date):

    # start time for performance tracking
    start_time_performance = time.time()

    # ensure that script validates that the end hour is after the start hour
    if end_date <= start_date:
        raise ValueError("End time must be after start time")
    
    df = pl.read_csv(path, columns=['timestamp', 'pixel_color', 'coordinate'], rechunk=False)

    start_date_pl = pl.Series([start_date]).cast(pl.Datetime)
    end_date_pl = pl.Series([end_date]).cast(pl.Datetime)

    filtered_df = df.filter((df["timestamp"] >= start_date_pl) & (df["timestamp"] < end_date_pl))

    most_frequent_color = (
        filtered_df["pixel_color"]
        .value_counts(sort=True, name="counts")
        .get_column("pixel_color")[0]
        if not filtered_df.is_empty()
        else None
        )

    most_frequent_coordinate = (
        filtered_df["coordinate"]
        .value_counts(sort=True, name="counts")
        .get_column("coordinate")[0]
        if not filtered_df.is_empty()
        else None
        )

    end_time_performance = time.time()

    execution_time = (end_time_performance - start_time_performance) * 1000

    results = {
        "Most Placed Color": most_frequent_color,
        "Most Placed Pixel Location": most_frequent_coordinate,
        "Execution Time (ms)": execution_time
    }

    return results

filepath = '/Users/kkragas/Documents/CSC 369/Assignments/Assignment 1/2022_place_canvas_history.csv'
# 1-hour time frame
start_time = datetime.strptime("2022-04-01 12", "%Y-%m-%d %H") 
end_time = datetime.strptime("2022-04-01 18", "%Y-%m-%d %H")
results_1 = run_analysis_polars(filepath, start_time, end_time)
print(results_1)

