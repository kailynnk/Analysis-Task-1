import csv
import gzip
import time
from datetime import datetime
from collections import Counter

def run_analysis(path, start_date, end_date):

    # start time for performance
    start_time_performance = time.time()

    # ensure that script validates that the end hour is after the start hour
    if end_date <= start_date:
        raise ValueError("End time must be after start time")

    # counter keeps track of the frequencies of each color and location
    color_counter = Counter()
    coordinate_counter = Counter()

    with gzip.open(path, mode="rt") as file:
        reader = csv.DictReader(file)

        for row in reader:
            timestamp = row["timestamp"]
            color = row["pixel_color"]
            coordinate = row["coordinate"]

            # print(f"Processing row: {timestamp}")

            # convert for comparison
            try: 
                data_timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f %Z")

            except ValueError:
                data_timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S %Z")

            # if the current timestamp is within specified range
            if start_date <= data_timestamp < end_date:
                color_counter[color] += 1
                coordinate_counter[coordinate] += 1

    # most frequent color and coordinate in the specified time range
    most_frequent_color = color_counter.most_common(1)
    most_frequent_coordinate = coordinate_counter.most_common(1)

    # execution time
    end_time_performance = time.time()
    # convert to execution time milliseconds
    execution_time = (end_time_performance - start_time_performance) * 1000 

    results = {
        "Most Placed Color": most_frequent_color[0][0] if most_frequent_color else None,
        "Most Placed Pixel Location": most_frequent_coordinate[0][0] if most_frequent_coordinate else None,
        "Execution Time (ms)": execution_time
    }

    return results


filepath = '/Users/kkragas/Documents/CSC 369/Assignments/Assignment 1/2022_place_canvas_history.csv.gzip'

# 1 hr time frame
start_time = datetime.strptime("2022-04-01 12", "%Y-%m-%d %H") 
end_time = datetime.strptime("2022-04-01 13", "%Y-%m-%d %H")
results_1 = run_analysis(filepath, start_time, end_time)
print(results_1)
