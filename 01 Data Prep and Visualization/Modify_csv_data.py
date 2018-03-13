# Read in the original data file and format the data for future use

import pandas as pd
import FBI_code

# import data into pandas data frame
crime_df = pd.read_csv('Crimes_2001_to_present.csv')

# remove all the white spaces in the column names
crime_df.columns = crime_df.columns.str.replace('\s+', '')

# Spilt time column into Date, Time, and AM/PM
date_df = pd.DataFrame(crime_df.Date.str.split(' ', 2).tolist(),columns = ['Date', 'Time', 'AM/PM'])

# list unique values in certain columns
PrimaryType = crime_df.PrimaryType.unique().tolist()
LocationDescription = crime_df.LocationDescription.unique().tolist()

# create filter for hyde park longitude and latitude
latitude_filter = (crime_df.Latitude >= 41.78013) & \
                  (crime_df.Latitude <= 41.809241)
longitude_filter = (crime_df.Longitude >= -87.615546) & \
                   (crime_df.Longitude <= -87.575706)

# create filter for data crime year
year_filter = (crime_df.Year >= 2013)

# create filter for crime primary type, only happen outdoors
type_filter = (crime_df.PrimaryType != "CRIMINAL TRESPASS") & \
              (crime_df.PrimaryType != "GAMBLING") & \
              (crime_df.PrimaryType != "PROSTITUTION") & \
              (crime_df.PrimaryType != "RITUALISM")

# create filter for crime location description, only happen outdoors
location_description_filter = (crime_df.LocationDescription != "Apartment")

# import the dictionary from FBI_code
FBI_code_dict = FBI_code.get_FBI_code()


def rename_column_name(data, column, new_column):
    '''
    Rename data frame column

    Inputs:
        data: pandas data frame
        column: string, old name
        new_column: string, new name

    Returns: data frame
    '''
    temp = data.rename(index=str, columns={column: new_column})
    return temp


def filter(data, filter):
    '''
    Take values in the data frame by applying filter

    Inputs:
        data: pandas data frame
        filter: pre defined filter

    Returns: data frame
    '''
    temp = data[filter]
    return temp


def map_to_dictionary(data, new_column, column, dict_name):
    '''
    Add a new columns to data frame based on the dictionary value

    Inputs:
        data: pandas data frame
        new_column: string, new name
        column: string, old name
        dict_name: dict, match with dataframe

    Returns: data frame
    '''
    data[new_column] = data[column].map(dict_name)
    return data


def crime_data(data):
    '''
    Apply filters, change column name, and match with dictionary to main_data

    Inputs:
        main_data: pandas data frame

    Returns: data frame
    '''
    temp = filter(data, latitude_filter)
    temp = filter(temp, longitude_filter)
    temp = filter(temp, year_filter)
    temp = filter(temp, type_filter)
    temp = filter(temp, location_description_filter)
    temp = rename_column_name(temp, "Date", "DateString")
    temp = temp.reset_index(drop=True)
    temp = map_to_dictionary(temp, "AggregateType", "FBICode", FBI_code_dict)
    return temp


def transfer_date_to_int(date_str):
    '''
    Transfer date into integer

    Inputs:
        date_str: date data

    Returns: int
    '''
    month, day, year = date_str.split('/')
    return int(year)*10000 + int(month)*100 + int(day)


def transfer_time_to_int(time_str):
    '''
    Transfer time into integer

    Inputs:
        time_str: time data

    Returns: int
    '''
    hour, minute, second = time_str.split(':')
    return int(hour) * 100 + int(float(minute))


def transfer_date_time_type(time_df):
    '''
    Format time_df

    Inputs:
        time_df: pandas data frame
        name: string, csv file name

    Returns: data frame
    '''
    for i in range(len(time_df)):
        date = transfer_date_to_int(time_df.Date[i])
        time_df.Date[i] = date
        time = transfer_time_to_int(time_df.Time[i])

        if time_df['AM/PM'][i] == 'PM':
            time_df.Time[i] = time + 1200
        else:
            time_df.Time[i] = time
    return time_df


def fix_time():
    '''
    Fix in time, transfer 12:00pm to 1200, transfer 12:00 am to 0;
    find time bigger than 2400 and minus 1200
    find time between 1200 to 1300 and minus 1200

    Inputs:

    Returns: data frame
    '''
    time_df = transfer_date_time_type(date_df)
    for i in range(len(time_df)):
        if time_df["Time"][i] >= 2400:
            time_df["Time"][i] = time_df["Time"][i] - 1200
        if (1200 <= time_df["Time"][i]) & (time_df["Time"][i] <= 1300) & \
                (time_df["AM/PM"][i] == "AM"):
            time_df["Time"][i] = time_df["Time"][i] - 1200
    return time_df


def hype_park_crime():
    '''
    Join two data frame, crime_df and adjusted_date_df

    Inputs:

    Returns: data frame
    '''
    adjusted_date_df = fix_time()
    crime = crime_data(crime_df)
    final_crime_df = pd.concat([crime, adjusted_date_df], axis=1, join='inner')
    return final_crime_df


# Export as CSV
hype_park_crime().to_csv("Hyde_park_crime.csv", index=False)

# if __name__ == "__main__":
#     hype_park_crime()