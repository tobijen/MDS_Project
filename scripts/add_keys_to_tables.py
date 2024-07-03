import pandas as pd

# Load the CSV file
file_paths = ['./csv_data/pit_stops.csv', './csv_data/results.csv']

for file_path in file_paths:
    df = pd.read_csv(file_path)

    # Create the new pitstopid column by combining driver_id and race_id
    # Assuming both driver_id and race_id are columns in the CSV
    df['pitstopId'] = df['driverId'].astype(str) + '_' + df['raceId'].astype(str)

    df.to_csv(file_path, index=False)