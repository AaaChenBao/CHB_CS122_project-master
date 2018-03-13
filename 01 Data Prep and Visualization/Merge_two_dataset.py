# Merge data output from Modify_csv_data.py and Get_Security_Alert.py

import pandas as pd

# import data into pandas data frame
hyde_park = pd.read_csv('hyde_park_crime.csv')
security_alert_df = pd.read_csv('security_alert_data.csv')

# make all letters in the security alert data frame upper case
security_alert_df = security_alert_df.apply(lambda x: x.astype(str).str.upper())

# drop empty data in the security alert data frame
data_filter = (security_alert_df.PrimaryType != "NAN") & \
                (security_alert_df.Date != "NAN") & \
                (security_alert_df.Time != "NAN") & \
                (security_alert_df.Location != "NAN")

security_alert_df = security_alert_df[data_filter]

# take out latitude and longitude from the security alert data frame
security_alert_df['Latitude'] = security_alert_df['Location'].str[1:10]
security_alert_df['Longitude'] = security_alert_df['Location'].str[12:22]

# append two data frames
final_df = hyde_park.append(security_alert_df, ignore_index=True)

# convert string into integer
final_df["Date"] = final_df["Date"].astype(int)

# define useful columns
useful_col = ['Date', 'Time', 'PrimaryType', 'Latitude', 'Longitude', 'Location']
final_df = final_df[useful_col]

# sort values by ('Date', 'Time')
final_df = final_df.sort_values(by=['Date', 'Time'], ascending=True)

# export to csv
final_df.to_csv("Final_data.csv", index=False)


