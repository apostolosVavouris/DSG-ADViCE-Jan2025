import os
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import dask.dataframe as dd
import matplotlib.pyplot as plt

def convert_csv_to_parquet(input_folder, output_folder):
    """
    Converts all CSV files in the input folder to Parquet files and saves them in the output folder.

    Parameters:
        input_folder (str): Path to the folder containing CSV files.
        output_folder (str): Path to the folder where Parquet files will be saved.
"""
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # List all CSV files in the input folder
    for file_name in os.listdir(input_folder):
        if file_name.endswith('.csv'):
            # Construct full file paths
            csv_path = os.path.join(input_folder, file_name)

            property_id = f"{os.path.splitext(file_name)[0]}"

            parquet_path = os.path.join(output_folder, f"{property_id}.parquet")

            try:
                # Read CSV and convert to Parquet
                print(f"Converting {file_name} to Parquet...")

                df = pd.read_csv(csv_path)

                # Downcast all numerical values except for timestamp to float 32
                for col in df.select_dtypes(include=[np.number]).columns:
                    if col != "Timestamp":
                        df[col] = df[col].astype(np.float32)
                
                # Reshape to long format
                df_long = df.melt(
                    id_vars=["Timestamp"],
                    var_name="variable_name",
                    value_name="value"
                )

                df_long["Property_ID"] = property_id 

                # Save data partitioned by variable and heat pump ID
                for variable_name, variable_data in df_long.groupby("variable_name"):
                    output_path = os.path.join(
                        output_folder, 
                        f"variable_name={variable_name}", 
                        f"{property_id}"
                    )
                    
                    os.makedirs(output_path, exist_ok=True)
                
                    variable_data.to_parquet(
                        os.path.join(output_path, "part-0000.parquet"),
                        index=False
                    )
                    print(f"Saved Parquet file: {os.path.join(output_path, "part-0000.parquet")}")
            
            except Exception as e:
                print(f"Failed to convert {file_name} to Parquet. Error: {e}")

def read_heat_pump_data ():
   #data = pd.read_parquet("/Users/fgomezmedina/Library/CloudStorage/OneDrive-TheAlanTuringInstitute/DCE Documentation/ADViCE/ADViCE January DSG/Data/2025_Full_Dataset/parquet_trial/variable_name=External_Air_Temperature/Property_ID=EOH0749/part-0000.parquet", engine="pyarrow")
   #print(data)
   
   # Read all long-form time series datasets for a particular variable
   data = dd.read_parquet("/Users/fgomezmedina/Library/CloudStorage/OneDrive-TheAlanTuringInstitute/DCE Documentation/ADViCE/ADViCE January DSG/Data/2025_Full_Dataset/parquet_trial/variable_name=External_Air_Temperature/*/*", engine="pyarrow")
   
   # Compute the result
   result = data.compute()
   print(result)

   plt.figure()
   plt.scatter(data["Timestamp"],data["value"])
   plt.show()

# Example usage
input_folder = "/Users/fgomezmedina/Library/CloudStorage/OneDrive-TheAlanTuringInstitute/DCE Documentation/ADViCE/ADViCE January DSG/Data/2025_Full_Dataset/uncompressed_all_together/time_series"
output_folder = "/Users/fgomezmedina/Library/CloudStorage/OneDrive-TheAlanTuringInstitute/DCE Documentation/ADViCE/ADViCE January DSG/Data/2025_Full_Dataset/uncompressed_all_together/time_series_parquet"
print(f'Number of files in input folder:{len(os.listdir(input_folder))}. Number of files in output folder: {len(os.listdir(output_folder))}')

convert_csv_to_parquet(input_folder, output_folder)

# read_heat_pump_data()


