from config import * 
def extract_features_from_route_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts useful structured features from 'route_long_name' and 'route_desc' columns
    of a GTFS routes DataFrame.
    
    Parameters
    ----------
    df : pd.DataFrame
        The GTFS routes DataFrame containing at least 'route_long_name' and 'route_desc'.
    
    Returns
    -------
    pd.DataFrame
        The same DataFrame with new extracted features:
        - origin
        - destination
        - is_express
        - is_shuttle
        - corridor_count
        - main_corridors (as list)
        - main_corridor_first
        - main_corridor_last
    """

    # Clean NaNs to avoid regex errors
    df = df.copy()
    df["route_long_name"] = df["route_long_name"].fillna("").astype(str)
    df["route_desc"] = df["route_desc"].fillna("").astype(str)
    
    # 1️⃣ Extract origin and destination (e.g., "Bay Ridge - Manhattan Beach")
    def extract_origin_destination(name):
        match = re.split(r"\s*-\s*", name)
        if len(match) >= 2:
            return match[0].strip(), match[-1].strip()
        return None, None
    
    df[["origin", "destination"]] = df["route_long_name"].apply(
        lambda x: pd.Series(extract_origin_destination(x))
    )
    
    # 2️⃣ Detect express and shuttle routes
    df["is_express"] = df["route_long_name"].str.contains("express", case=False, na=False).astype(int)
    df["is_shuttle"] = df["route_long_name"].str.contains("shuttle", case=False, na=False).astype(int)
    
    # 3️⃣ Extract main corridors from route_desc ("via X / Y / Z")
    def extract_corridors(desc):
        desc = desc.lower().strip()
        if desc.startswith("via "):
            desc = desc[4:]
        corridors = [c.strip().title() for c in re.split(r"/|,| and ", desc) if c.strip()]
        return corridors if corridors else None

    df["main_corridors"] = df["route_desc"].apply(extract_corridors)
    
    # 4️⃣ Count how many corridors are listed
    df["corridor_count"] = df["main_corridors"].apply(lambda x: len(x) if x else 0)
    
    # 5️⃣ Extract first and last corridor names
    df["main_corridor_first"] = df["main_corridors"].apply(lambda x: x[0] if x else None)
    df["main_corridor_last"] = df["main_corridors"].apply(lambda x: x[-1] if x else None)

    df = df.drop(columns=['route_long_name', 'route_desc', 'route_short_name', 'main_corridors'])
    
    return df


def calculate_ets(df):
    # arrival_time_real and departure_time_real are already datetime64[ns]
    # No need to convert them
    
    # Sort to ensure proper sequencing
    df = df.sort_values(['trip_id', 'stop_sequence']).reset_index(drop=True)

    # Initialize empty columns
    df['travel_time_seconds'] = np.nan
    df['distance_km'] = np.nan
    df['speed_kmh'] = np.nan

    # Group by trip and compute between-stop metrics
    for trip_id, group in df.groupby('trip_id'):
        idx = group.index

        # Compute time difference (arrival_i+1 - departure_i)
        travel_times = (
            group['arrival_time_real'].iloc[1:].values -
            group['departure_time_real'].iloc[:-1].values
        ) / np.timedelta64(1, 's')  # Convert to seconds
        
        df.loc[idx[1:], 'travel_time_seconds'] = travel_times

        # Compute distance between consecutive stops using lat/lon
        coords = list(zip(group['stop_lat'], group['stop_lon']))
        distances = [geodesic(coords[i], coords[i+1]).km for i in range(len(coords)-1)]
        df.loc[idx[1:], 'distance_km'] = distances

        # Compute speed (km/h)
        df.loc[idx[1:], 'speed_kmh'] = (
            df.loc[idx[1:], 'distance_km'] / 
            (df.loc[idx[1:], 'travel_time_seconds'] / 3600)
        )
        
        df['travel_time_seconds'] = df['travel_time_seconds'].fillna(0)
        df['distance_km'] = df['distance_km'].fillna(0)
        df['speed_kmh'] = df['speed_kmh'].fillna(0)

        # Option 2: Replace infinite/null speeds with 0 or NaN
        df['speed_kmh'] = df['speed_kmh'].replace([np.inf, -np.inf], 0)

        # Option 3: Filter out nulls when analyzing
        df_valid_segments = df.dropna(subset=['speed_kmh'])

    return df