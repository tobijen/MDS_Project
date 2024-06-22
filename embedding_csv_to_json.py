import pandas as pd
import json

# Load the CSV files
results_df = pd.read_csv('./csv_data/results.csv')
drivers_df = pd.read_csv('./csv_data/drivers.csv')
races_df = pd.read_csv('./csv_data/races.csv')
constructors_df = pd.read_csv('./csv_data/constructors.csv')
pitstop_df = pd.read_csv('./csv_data/pit_stops.csv')
status_df = pd.read_csv('./csv_data/status.csv')

# Merge the DataFrames on the driverId column
merged_driver_df = results_df.merge(drivers_df, on='driverId', how='left')
merged_races_df = merged_driver_df.merge(races_df, on='raceId', how='left')
merged_constructors_df = merged_races_df.merge(constructors_df, on='constructorId', how='left')
#merged_pitstops_df = merged_constructors_df.merge(drivers_df, on=['driverId', 'raceId'], how='left')
merged_status_df = merged_constructors_df.merge(status_df, on='statusId', how='left')

print(list(merged_constructors_df.columns))

# Function to convert merged DataFrame to the desired JSON structure
def convert_to_json(row):
    driver_data = {
        "code": row.get('code', "\\N"),
        "dob": row.get('dob', "\\N"),
        "driverId": row.get('driverId', "\\N"),
        "driverRef": row.get('driverRef', "\\N"),
        "forename": row.get('forename', "\\N"),
        "nationality": row.get('nationality_x', "\\N"),
        "number": row.get('number_y', "\\N"),  # Use 'number_y' for drivers
        "surname": row.get('surname', "\\N"),
        "url": row.get('url', "\\N")
    }

    race_data = {
        "year": row['year'],
        "race_name": row['name_x'],
        "date": row['date'],
    }

    constructor_data = {
        "constructor_name": row['name_y'],
        "constructor_url": row['url'],
        "nationality": row['nationality_y'],
    }

    status_data = row.get('status', "\\N")
    
    result_data = {
        "fastestLap": row.get('fastestLap', "\\N"),
        "fastestLapSpeed": row.get('fastestLapSpeed', "\\N"),
        "fastestLapTime": row.get('fastestLapTime', "\\N"),
        "grid": row.get('grid', "\\N"),
        "laps": row.get('laps', "\\N"),
        "milliseconds": row.get('milliseconds', "\\N"),
        "number": row.get('number_x', "\\N"),  # Use 'number_x' for results
        "points": row.get('points', "\\N"),
        "position": row.get('position', "\\N"),
        "positionOrder": row.get('positionOrder', "\\N"),
        "positionText": row.get('positionText', "\\N"),
        "rank": row.get('rank', "\\N"),
        "resultId": row.get('resultId', "\\N"),
        "time": row.get('time', "\\N"),
        "status": status_data,
        "driver": driver_data,
        "race": race_data,
        "constructor": constructor_data,
        "pitStops": []
    }
    
    return result_data

# Join pit stops data with the merged DataFrame based on raceId and driverId
merged_df_with_pitstops = merged_status_df.merge(pitstop_df, on=['raceId', 'driverId'], how='left', suffixes=('', '_pit'))

# Create a dictionary of pit stops by a combination of raceId and driverId
pit_stops_dict = pitstop_df.groupby(['raceId', 'driverId']).apply(lambda x: x.to_dict(orient='records')).to_dict()

# Embed pit stops into the JSON structure
json_data = []
for _, row in merged_status_df.iterrows():
    result = convert_to_json(row)
    race_id = row.get('raceId', None)
    driver_id = row.get('driverId', None)
    pit_stops_key = (race_id, driver_id)
    if pit_stops_key in pit_stops_dict:
        result['pitStops'] = pit_stops_dict[pit_stops_key]
    json_data.append(result)

# Save to JSON file
with open('./json_data/output.json', 'w') as json_file:
    json.dump(json_data, json_file, indent=4)

# Print the JSON data (optional)
print(json.dumps(json_data, indent=4))
