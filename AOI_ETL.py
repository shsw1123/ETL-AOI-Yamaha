import pandas as pd
import glob
import os
from datetime import datetime
rawdata_path = "C:/your desire raw data directory/*.csv"
target_directory = "C:/your desire finish good data directory"

# Extract csv file type function
def extract_csv(filetoprocess):
    extract_data = pd.read_csv(filetoprocess,low_memory=False,on_bad_lines='skip') # extract csv with prevent low_memory, and bad line error
    return extract_data

# Extract all the file in directory
def extracted():
    extracted = pd.DataFrame()
    for csv_file in glob.glob(rawdata_path):
        filename = os.path.basename(csv_file)
        extracted = pd.concat([extracted,extract_csv(csv_file)],ignore_index=True)
        print("successfully to extract:"+ filename)
    # Drop useless columns
    extracted.drop(
    ["Group Insp","Detect PosY ","Step Angle","Mark1 No","Distance L Result","Mark1 PosX","Mark1 PosY","Result",
    "Mark1 Block No","Mark1 Result","Mark1 Detect PosX","Mark1 Detect PosY","Mark2 No","Mark2 PosX","Mark2 PosY",
    "Mark2 Block No","Mark2 Result","Mark2 Detect PosX","Mark2 Detect PosY","Mark3 No","Mark3 PosX","Mark3 PosY",
    "Mark3 Block No","Mark3 Result","Mark3 Detect PosX","Mark3 Detect PosY","Target No","Mount No","Block No","Partst No",
    "Partial Type","Step No","Step PosX","Step PosY","Detect PosX","Group No",
    "Detect height","Linearity Result","Linearity Tol","Linearity Value","Distance L Base","Distance L Tol", "Distance L Value",
    "Angle Result","Angle Tol","Angle Value","Distance X Result","Distance X Base","Distance X Tol",
    "Distance Y Result","Distance Y Base","Distance Y Tol","Gap Result","Gap Base","Gap Tol",
    "Gap Value","Gap Target No","Gap X Result","Gap X Base","Gap X Tol","Gap X Value","Gap X Target No","Gap Y Result","Gap Y Base",
    "Gap Y Tol","Gap Y Value","Gap Y Target No","DefCalc Result","DefCalc Formula","DefCalc Base","DefCalc Tol","DefCalc Value"
    ], axis=1,inplace=True)
    return extracted


# Transform function to process
def transform(df):
    for j in range(len(df.index)): # repeat = number of row (**len(df.index))
        if (j) != 0: # First row is blank cell
            if (j%2) == 1: # Find odd number of row then perform the task (For copy values in X row and paste to Y row)
                Value = df.iloc[j,2] # Locate the cell that contain value
                df.iloc[j-1,2] = abs(Value) # Copy the number to your wish cell
    df["Group Name"] = df["Group Name"].replace({'_Y':''}, regex=True) # Replace "_Y" in group name

    df = df.iloc[::2] # Partition the row we needed. 
    df = df.reset_index(drop=True) 
    df["Distance Y Value"] = df["Distance Y Value"].abs() # Change the value to abs
    return df
# Load FG file
def load(Data):
    DT = datetime.now()
    dt = DT.strftime("%m%d%y%H%M%S")
    output_filename = input("Please enter the output file name:\n>")
    output_filename = output_filename+"@"+dt
    Targetfile = "{target_path}/{file_name}.csv".format(file_name = output_filename,target_path = target_directory)
    if os.path.exists(Targetfile):
        print("The file exists")
    else:
        Data.to_csv(Targetfile)

DATA = extracted()
TRANSFORMED = transform(DATA)
load(TRANSFORMED)
print("Process Completed!")

