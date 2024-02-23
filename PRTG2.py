import os
import requests
import pandas as pd
from io import StringIO
import warnings
from datetime import datetime
import re
from tqdm import tqdm  # Import tqdm library

warnings.filterwarnings("ignore", category=DeprecationWarning)

# Read parameters from file
with open("server_address.txt", "r") as file:
    server_parameters = dict(line.strip().split("=") for line in file)

server_address = server_parameters.get("server")

# Read flags from the "min_max_flags.txt" file
flags = {}
id_prefix = 'id'
id_values = []

with open("min_max_flags.txt", "r") as file:
    for line in file:
        line = line.strip()
        if "=" in line:
            key, value = line.split("=")
            if key.startswith(id_prefix):
                id_values.append(value)
            else:
                flags[key] = value

# Construct API endpoints for upper error limit and warning limit for all IDs
api_endpoint_warning = f'https://{server_address}/api/getobjectproperty.htm?subtype=channel&subid=-1&name=limitmaxwarning&show=nohtmlencode&username=Ashwin.Gedekar&passhash=1815236212'
api_endpoint_error = f'https://{server_address}/api/getobjectproperty.htm?subtype=channel&subid=-1&name=limitmaxerror&show=nohtmlencode&username=Ashwin.Gedekar&passhash=1815236212'

# Create dictionaries to store upper error and warning limits for each ID
upper_warning_limits = {}
upper_error_limits = {}

# Make the API requests for each ID to get the upper error and warning limits
for id_value in id_values:
    response_warning = requests.get(f"{api_endpoint_warning}&id={id_value}")
    response_error = requests.get(f"{api_endpoint_error}&id={id_value}")

    # Check if the request was successful (status code 200)
    if response_warning.status_code == 200:
        match_warning = re.search(r'<result>(\d+)</result>', response_warning.text)
        if match_warning:
            upper_warning_limits[id_value] = int(match_warning.group(1)) * 8 / 1000000  # Convert bytes to megabits
    if response_error.status_code == 200:
        match_error = re.search(r'<result>(\d+)</result>', response_error.text)
        if match_error:
            upper_error_limits[id_value] = int(match_error.group(1)) * 8 / 1000000  # Convert bytes to megabits

# Create a string to store the formatted output
output_text = ""

# Construct API requests for each ID
for id_value in tqdm(id_values, desc="Processing IDs"):  # Use tqdm for progress bar
    # Construct the API endpoint URL using the extracted parameters
    api_endpoint = f'https://{server_address}/api/historicdata.csv?id={id_value}&avg={flags.get("avg")}&sdate={flags.get("sdate")}&edate={flags.get("edate")}&username={server_parameters.get("username")}&passhash={server_parameters.get("passhash")}'

    # Make the API request
    response = requests.get(api_endpoint)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        output_text += f"ID {id_value}:\n{'-' * len('ID ' + id_value + ':')}\n"

        try:
            # Use pandas to read the CSV data
            df = pd.read_csv(StringIO(response.text))

            # Clean up the column names (remove leading and trailing spaces)
            df.columns = df.columns.str.strip()

            # Extract specified columns along with "Date Time"
            selected_columns = ["Date Time", "Traffic Total (Speed)", "Traffic Total (Speed)(RAW)"]
            selected_data = df[selected_columns]

            # Convert "Traffic Total (Speed)(RAW)" to numeric type
            selected_data.loc[:, "Traffic Total (Speed)(RAW)"] = pd.to_numeric(selected_data["Traffic Total (Speed)(RAW)"], errors='coerce')

            # Drop rows with NaN values in "Traffic Total (Speed)(RAW)"
            selected_data = selected_data.dropna(subset=["Traffic Total (Speed)(RAW)"])

            # Check if the DataFrame is not empty
            if not selected_data.empty:
                if flags.get("max") == '1':
                    # Find the row with the maximum "Traffic Total (Speed)(RAW)"
                    max_raw_speed_row = selected_data.loc[selected_data["Traffic Total (Speed)(RAW)"].idxmax()]
                    output_text += f"MAX SPEED{' ' * (25 - len('MAX SPEED'))}{max_raw_speed_row['Traffic Total (Speed)']}\n"
                    output_text += f"MAX SPEED(RAW){' ' * (25 - len('MAX SPEED(RAW)'))}{max_raw_speed_row['Traffic Total (Speed)(RAW)']}\n"
                    output_text += f"Date Time{' ' * (25 - len('Date Time'))}{max_raw_speed_row['Date Time']}\n\n"

                    # Check if thr=1 and upper error limit and warning limit are available for the current ID
                    if flags.get("thr") == '1' and id_value in upper_error_limits and id_value in upper_warning_limits:
                        max_speed_value = float(max_raw_speed_row['Traffic Total (Speed)'].split()[0])

                        upper_error_limit = upper_error_limits[id_value]
                        upper_warning_limit = upper_warning_limits[id_value]

                        if max_speed_value > upper_error_limit and max_speed_value <= upper_warning_limit:
                            output_text += f"MAX SPEED for ID {id_value} is within Upper Error Limit({upper_error_limit} Mbit/s) and Upper Warning Limit({upper_warning_limit} Mbit/s)\n\n"
                        elif max_speed_value <= upper_error_limit and max_speed_value > upper_warning_limit:
                            output_text += f"MAX SPEED for ID {id_value} crosses Upper Warning Limit({upper_warning_limit} Mbit/s) but is within Upper Error Limit({upper_error_limit} Mbit/s)\n\n"
                        elif max_speed_value <= upper_error_limit and max_speed_value <= upper_warning_limit:
                            output_text += f"MAX SPEED for ID {id_value} is within both Upper Error Limit({upper_error_limit} Mbit/s) and Upper Warning Limit({upper_warning_limit} Mbit/s)\n\n"
                        else:
                            output_text += f"MAX SPEED for ID {id_value} is above both Upper Error Limit({upper_error_limit} Mbit/s) and Upper Warning Limit({upper_warning_limit} Mbit/s)\n\n"
                       
                     
                        

                # Find the row with the minimum "Traffic Total (Speed)(RAW)"
                min_raw_speed_row = selected_data.loc[selected_data["Traffic Total (Speed)(RAW)"].idxmin()]
                output_text += f"MIN SPEED{' ' * (25 - len('MIN SPEED'))}{min_raw_speed_row['Traffic Total (Speed)']}\n"
                output_text += f"MIN SPEED(RAW){' ' * (25 - len('MIN SPEED(RAW)'))}{min_raw_speed_row['Traffic Total (Speed)(RAW)']}\n"
                output_text += f"Date Time{' ' * (25 - len('Date Time'))}{min_raw_speed_row['Date Time']}\n\n"

            else:
                output_text += f"No non-NaN values found in 'Traffic Total (Speed)(RAW)' for ID {id_value}\n\n"

        except Exception as e:
            output_text += f"Error processing CSV data for ID {id_value}: {e}\n\n"
    else:
        output_text += f"Error: {response.status_code} - {response.text}\n\n"
    output_text += "#" * 55 + "\n\n"

# Create the output directory if it doesn't exist
output_directory = "output"
os.makedirs(output_directory, exist_ok=True)

# Get the current date and time
current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Construct the full file path for the output file
output_file_path = os.path.join(output_directory, f"output_{current_datetime}.txt")

# Write the formatted output to the specified file path
with open(output_file_path, "w") as output_file:
    output_file.write(output_text)

# Print the output file path to the terminal
print(output_text)
print(f"Output has been saved to {output_file_path}")
