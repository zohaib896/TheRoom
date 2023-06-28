from flask import Flask, jsonify, request
import os
import csv
import pandas as pd
import sqlite3
import time

app = Flask(__name__)

# Folder path containing the JSON and CSV files
folder_path = 'C:/Users/M-ZOH/Desktop/old pc/Interviews and Material/TheRoom/archive'

# Specify the time delay (in seconds) between processing each record
time_delay = 0

# Process CSV files
for file_name in os.listdir(folder_path):
    if file_name.endswith('.csv'):
        # Get the table name from the file name
        table_name = os.path.splitext(file_name)[0]

        # Open the CSV file with the correct encoding
        with open(os.path.join(folder_path, file_name), 'r', encoding='utf-8-sig', errors='replace') as csv_file:
            csv_reader = csv.reader(csv_file)

            # Read the header row to get column names
            header = next(csv_reader)

            # Connect to the SQLite database
            connection = sqlite3.connect('youtube_data.db')
            cursor = connection.cursor()

            # Create the table in SQLite using the column names
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(header)});"
            cursor.execute(create_table_query)

            # Process and insert rows into the table
            for row in csv_reader:
                # Simulate the time delay
                time.sleep(time_delay)

                # Insert the record into the table
                insert_query = f"INSERT INTO {table_name} VALUES ({','.join(['?'] * len(row))});"
                cursor.execute(insert_query, tuple(row))

            # Commit the changes to the database
            connection.commit()

            # Close the database connection
            connection.close()

# Process JSON files
for file_name in os.listdir(folder_path):
    if file_name.endswith('.json'):
        file_path = os.path.join(folder_path, file_name)

        # Load the dataset from the file
        df = pd.read_json(file_path)

        # Get the column headers from the JSON file
        columns = list(df.columns)

        # Connect to the SQLite database
        connection = sqlite3.connect('youtube_data.db')
        cursor = connection.cursor()

        # Create a table in the database dynamically
        table_name = file_name.split('.')[0]  # Use the file name as the table name
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(columns)})"
        cursor.execute(create_table_query)

        # Stream the records sequentially with a time delay
        for index, row in df.iterrows():
            # Convert values to string format
            row_values = [str(value) for value in row]

            # Insert the record into the respective table
            insert_query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({','.join(['?'] * len(columns))})"
            cursor.execute(insert_query, tuple(row_values))

            # Commit the changes to the database
            connection.commit()

            # Add a time delay
            time.sleep(time_delay)

        # Close the database connection
        connection.close()
# Function to query individual records by attributes
@app.route('/records', methods=['GET'])
def query_records():
    table_name = request.args.get('table_name')
    column_name = request.args.get('column_name')

    if not column_name:
        return jsonify({"error": "Column name not provided"})

    query = f"SELECT {column_name} FROM {table_name};"
    with sqlite3.connect('youtube_data.db') as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

    records = [row[0] for row in results]

    return jsonify(records)

###  example : http://127.0.0.1:5000/records?table_name=CAvideos&column_name=channel_title

# API endpoint to query aggregate statistics by channel
@app.route('/statistics', methods=['GET'])
def query_statistics():
    with sqlite3.connect('youtube_data.db') as connection:
        cursor = connection.cursor()

        aggregate_query = """
        SELECT channel_title, COUNT(*) as video_count, SUM(views) as total_views, AVG(likes) as average_likes
        FROM (
            SELECT channel_title, views, likes
            FROM CAvideos
            UNION ALL
            SELECT channel_title, views, likes
            FROM FRvideos
            UNION ALL
            SELECT channel_title, views, likes
            FROM INvideos
            UNION ALL
            SELECT channel_title, views, likes
            FROM KRvideos
            UNION ALL
            SELECT channel_title, views, likes
            FROM RUvideos
            UNION ALL
            SELECT channel_title, views, likes
            FROM DEvideos
            UNION ALL
            SELECT channel_title, views, likes
            FROM GBvideos
            UNION ALL
            SELECT channel_title, views, likes
            FROM JPvideos
            UNION ALL
            SELECT channel_title, views, likes
            FROM MXvideos
            UNION ALL
            SELECT channel_title, views, likes
            FROM USvideos
        )
        GROUP BY channel_title;
        """
        cursor.execute(aggregate_query)
        results = cursor.fetchall()

        statistics = []
        for row in results:
            channel_title, video_count, total_views, average_likes = row
            statistic = {
                'channel_title': channel_title,
                'video_count': video_count,
                'total_views': total_views,
                'average_likes': average_likes
            }
            statistics.append(statistic)

    return jsonify(statistics)
## example http://127.0.0.1:5000/statistics

if __name__ == '__main__':
    app.run()
