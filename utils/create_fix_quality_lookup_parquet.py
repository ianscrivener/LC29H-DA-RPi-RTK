import pandas as pd
import fastparquet

# Define the fix quality enum mapping
fix_quality_data = {
   'code': [0, 1, 2, 3, 4, 5, 6, 7, 8],
   'description': [
       'No Fix',
       'GPS Fix', 
       'DGPS Fix',
       'PPS Fix',
       'RTK Fix',
       'RTK Float',
       'Dead Reckoning',
       'Manual Input',
       'Simulation'
   ]
}

# Create DataFrame
df = pd.DataFrame(fix_quality_data)

# Save as Parquet using fastparquet
fastparquet.write('fix_quality_lookup.parquet', df)

print("Created fix_quality_lookup.parquet")
print(df)