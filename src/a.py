import pandas as pd
import glob
import os

# Directory containing _aviation_outputs files
input_dir = r'C:\Users\andre\Downloads\Datarush 2025'
output_file = os.path.join(input_dir, 'eth_filtered_aviation_output.csv')

# Find all files ending with _aviation_outputs.csv
aviation_files = glob.glob(os.path.join(input_dir, '*_aviation_outputs.csv'))

# Read all files, skipping headers except for the first file
dfs = []
for i, file in enumerate(aviation_files):
    if i == 0:
        df = pd.read_csv(file)
    else:
        df = pd.read_csv(file, header=0)
    dfs.append(df)

# Concatenate all dataframes
result = pd.concat(dfs, ignore_index=True)

# Save to output file
result.to_csv(output_file, index=False)