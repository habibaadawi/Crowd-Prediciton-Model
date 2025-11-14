from config import *


string_nan_values = [
    "",               # empty string
    " ",              # single space
    "na",
    "n/a",
    "na/",
    "n.a.",
    "nan",
    "none",
    "null",
    "nul",
    "nil",
    "missing",
    "unknown",
    "not known",
    "not applicable",
    "not available",
    "no data",
    "no info",
    "undefined",
    "undetermined",
    "?",              # placeholder
    "-",              # placeholder
    "--",
    "tbd",            # to be determined
    "tba",            # to be announced
    "pending"
]


# 1️⃣ Routes --done
def clean_routes_data(df):
    df = df.copy()

    def map_colors_in_route_color(df):
        # Simple conversion: HEX code to Color Name
        color_conversion = {
            'EE352E': 'Red',
            '00933C': 'Green', 
            'B933AD': 'Purple',
            '2850AD': 'Blue',
            'FF6319': 'Orange',
            '6CBE45': 'Lime Green',
            '6D6E71': 'Dark Gray',
            '996633': 'Brown',
            'A7A9AC': 'Light Gray',
            'FCCC0A': 'Yellow'
        }

    
        # Map the color codes to color names
        color_names_series = df['route_color'].map(color_conversion)

            

            
        return color_names_series
    
    df = df.replace(string_nan_values, pd.NA)
    df = df.drop_duplicates(subset=["route_id"])
    df["route_id"] = df["route_id"].astype(str)
    df["route_short_name"] = df["route_short_name"].str.strip().str.upper()
    df["route_long_name"] = df["route_long_name"].str.strip().str.title()
    valid_types = [0, 1, 2, 3, 4, 5, 6, 7]
    df = df[df["route_type"].isin(valid_types)]
    df['route_color'] = map_colors_in_route_color(df)
    



    return df 


# 2️⃣ Stop Times
def clean_stop_times_data(df):
    df = df.copy()
    df = df.replace(string_nan_values, pd.NA)
    # Convert times to timedelta (HH:MM:SS)
    for col in ["arrival_time", "departure_time"]:
        df[col] = pd.to_timedelta(df[col], errors="coerce")
    df = df.dropna(subset=["trip_id", "stop_id"])
    df["stop_sequence"] = df["stop_sequence"].astype(int)

    df['arrival_time'] = pd.to_timedelta(df['arrival_time'])
    df['departure_time'] = pd.to_timedelta(df['departure_time'])
    
    df = df.drop_duplicates(subset=["trip_id", "stop_id"])
    return df


# 3️⃣ Stops
def clean_stops_data(df):
    df = df.copy()
    df = df.replace(string_nan_values, pd.NA)
    df = df.drop_duplicates(subset=["stop_id"])
    df["stop_id"] = df["stop_id"].astype(str)
    df = df.dropna(subset=["stop_lat", "stop_lon"])
    df = df[(df["stop_lat"].between(-90, 90)) & (df["stop_lon"].between(-180, 180))]
    
    return df


# 4️⃣ Taxi / Mobility Data
def clean_taxi_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean NYC Taxi dataset for anomaly detection tasks.
    Keeps all columns but fixes invalid values, types, and timestamps.
    """

    df = df.copy()

    # --- 1️⃣ Handle string-based missing values ---
    string_nan_values = ["", " ", "NA", "NaN", "nan", "None", "null", "NULL"]
    df = df.replace(string_nan_values, np.nan)

    # --- 2️⃣ Convert datetime columns ---
    for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Drop rows with invalid timestamps (required for duration)
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

    # --- 3️⃣ Basic numeric sanity checks ---
    # Passenger count: must be >= 1 and reasonable (<= 6)
    if "passenger_count" in df.columns:
        df["passenger_count"] = pd.to_numeric(df["passenger_count"], errors="coerce")
        df.loc[(df["passenger_count"] <= 0) | (df["passenger_count"] > 6), "passenger_count"] = np.nan

    # Trip distance: must be positive and not absurd
    if "trip_distance" in df.columns:
        df["trip_distance"] = pd.to_numeric(df["trip_distance"], errors="coerce")
        df.loc[(df["trip_distance"] <= 0) | (df["trip_distance"] > 100), "trip_distance"] = np.nan

    # Fare amount and total amount sanity check
    if "fare_amount" in df.columns:
        df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce")
        df.loc[(df["fare_amount"] < 0) | (df["fare_amount"] > 1000), "fare_amount"] = np.nan

    if "total_amount" in df.columns:
        df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
        df.loc[(df["total_amount"] < 0) | (df["total_amount"] > 2000), "total_amount"] = np.nan

    # --- 4️⃣ Fix categorical columns ---
    categorical_cols = ["VendorID", "store_and_fwd_flag", "payment_type", "RatecodeID"]
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].astype("category")

    # --- 5️⃣ Compute derived values (for time-based analysis) ---
    df["trip_duration_min"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60
    )

    # Remove impossible durations (<=0 or > 180 minutes)
    df.loc[(df["trip_duration_min"] <= 0) | (df["trip_duration_min"] > 180), "trip_duration_min"] = np.nan

    # --- 6️⃣ Handle impossible or missing zone IDs ---
    if "PULocationID" in df.columns:
        df["PULocationID"] = pd.to_numeric(df["PULocationID"], errors="coerce").astype("Int64")

    if "DOLocationID" in df.columns:
        df["DOLocationID"] = pd.to_numeric(df["DOLocationID"], errors="coerce").astype("Int64")

    # --- 7️⃣ Drop rows where both key mobility signals are missing ---
    df = df.dropna(subset=["trip_distance", "trip_duration_min"], how="all")

    # --- 8️⃣ Reset index after cleaning ---
    df = df.reset_index(drop=True)

    return df



# 5️⃣ Trips
def clean_trips_data(df):
    df = df.copy()
    df = df.replace(string_nan_values, pd.NA)
    df = df.dropna(subset=["route_id", "trip_id"])
    df["trip_id"] = df["trip_id"].astype(str)
    df["route_id"] = df["route_id"].astype(str)
    df["service_id"] = df["service_id"].astype(str)
    df = df.drop_duplicates(subset=["trip_id"])
    df["direction_id"] = df["direction_id"].fillna(0).astype(int)

    return df


# 6️⃣ Weather
def clean_weather_data(df):
    df = df.copy()
    df = df.replace(string_nan_values, pd.NA)
    # Convert datetime column
    datetime_col = None
    for col in ["datetime", "timestamp", "date"]:
        if col in df.columns:
            datetime_col = col
            df[col] = pd.to_datetime(df[col], errors="coerce")
            break
    if datetime_col:
        df = df.dropna(subset=[datetime_col])
        
    #i will add these features since the stop_times df has only day name , so will use the day column to merge the 2 dfs 
    df['day_name'] = df.date.dt.day_name()
    df['month'] = df.date.dt.month
    df['year'] = df.date.dt.year

    # Fill missing numerical values (temperature, humidity, etc.)
    num_cols = df.select_dtypes(include=[np.number]).columns
    df[num_cols] = df[num_cols].fillna(method="ffill").fillna(method="bfill")
    # Drop duplicates
    df = df.drop_duplicates()
    return df

