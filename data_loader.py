import os
import requests
import wget
from config import *
def download_GTF_data_v2(base_dir):   
        # -----------------------------
        # Helper function
        # -----------------------------
        def ensure_dir(path):
            if not os.path.exists(path):
                os.makedirs(path)

        # Base data folder

    
        ensure_dir(base_dir)


        # Example: New York MTA GTFS
        gtfs_static_url = "https://transitfeeds.com/p/mta/79/latest/download"  # GTFS static
        gtfs_rt_vehicle_positions_url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"  # Real-time (requires API key for full use)
        taxi_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
        weather_url = "https://github.com/vega/vega-datasets/raw/main/data/weather.csv"


        import zipfile

        # -----------------------------
        # 1. GTFS Transit Data (Static + Real-Time)
        # -----------------------------
        gtfs_dir = os.path.join(base_dir, "gtfs")
        ensure_dir(gtfs_dir)

        gtfs_path = os.path.join(gtfs_dir, "gtfs_static.zip")
        gtfs_static_dir = os.path.join(gtfs_dir, "gtfs_static")
        ensure_dir(gtfs_static_dir)

        if not os.listdir(gtfs_static_dir):  # check if extracted folder is empty
            if not os.path.exists(gtfs_path):
                print("Downloading GTFS static data...")
                wget.download(gtfs_static_url, gtfs_path)
                print("\nGTFS static data downloaded.")
            else:
                print("GTFS zip already exists.")

            # ✅ Extract the zip file
            print("Extracting GTFS static data...")
            with zipfile.ZipFile(gtfs_path, "r") as zip_ref:
                zip_ref.extractall(gtfs_static_dir)
            print("GTFS data extracted successfully.")
        else:
            print("\nGTFS static data already extracted.")


        # -----------------------------
        # 2. NYC Taxi Trip Data (Traffic / Mobility)
        # -----------------------------
        traffic_dir = os.path.join( "traffic")
        ensure_dir(traffic_dir)

        taxi_trip_path = os.path.join(traffic_dir, "nyc_taxi_2024_01.parquet")
        if not os.listdir(traffic_dir)  : 
            print("Downloading NYC Taxi trip data (January 2024)...")
            wget.download(taxi_url, taxi_trip_path)
            print("\nTaxi trip data downloaded.")

            # For real-time, we’ll just note it (API access required)
            with open(os.path.join(gtfs_dir, "README.txt"), "w") as f:
                f.write("To access real-time GTFS feeds, register for an MTA API key at:\n")
                f.write("https://api.mta.info/#/AccessKey\n")
                
        else:
            print("\nTaxi trip data already downloaded")



        # -----------------------------
        # 3. Historical Hourly Weather Data
        # -----------------------------
        # -----------------------------
        # 3. Historical Hourly Weather Data (Open-Meteo Kaggle dataset)
        # -----------------------------
        weather_dir = os.path.join(base_dir, "weather")
        ensure_dir(weather_dir)
        weather_path = os.path.join(weather_dir, "weather.csv")
        # New working dataset (from Kaggle public source)
        if not os.listdir(weather_dir):
                
            try:
                print("Downloading historical weather dataset...")
                wget.download(weather_url, weather_path)
                print("\nWeather data downloaded successfully.")
            except Exception as e:
                print(f"❌ Failed to download weather dataset: {e}")
                print("You can manually download from: https://github.com/vega/vega-datasets/blob/main/data/weather.csv")
        else : 
            print("\nweather data already downloaded")
        
        return 

def load_GTF_static_data_v2(base_dir: str, traffic_data=False, weather_data=False):
    """
    Load the GTFS, taxi, and weather datasets from the provided base_dir.
    Returns a dictionary of pandas DataFrames.

    Parameters 
    ----------
    base_dir : enter the directory you want to load the data to 
    
    traffic_data : if traffic data exists  (by default false)

    weather_data : if weather data exists  (by default false)

    """
    taxi_df = None
    weather_df = None
    if traffic_data: 
      traffic_dir = os.path.join(base_dir, "traffic")
      taxi_df = pd.read_parquet(os.path.join(traffic_dir, "nyc_taxi_2024_01.parquet"))
    if weather_data : 
      weather_dir = os.path.join(base_dir, "weather")
      weather_df = pd.read_csv(os.path.join(weather_dir, "weather.csv"), parse_dates=["date"])

    stops_df = pd.read_csv(os.path.join(base_dir, "stops.txt"))
    routes_df = pd.read_csv(os.path.join(base_dir, "routes.txt"))
    stop_times_df = pd.read_csv(os.path.join(base_dir, "stop_times.txt"))
    trips_df = pd.read_csv(os.path.join(base_dir, "trips.txt"))
    

    return {
        "stops": stops_df,
        "routes": routes_df,
        "stop_times": stop_times_df,
        "trips": trips_df,
        "taxi": taxi_df,
        "weather": weather_df,
    }

def convert_GTF_realtime_data_to_df(base_dir):
    feed = gtfs_realtime_pb2.FeedMessage()
    with open(base_dir, 'rb') as f : 
        feed.ParseFromString(f.read())


    # Extract trip updates into a DataFrame
    data = []
    for entity in feed.entity:
        if entity.trip_update:
            trip = entity.trip_update.trip
            for stu in entity.trip_update.stop_time_update:
                data.append({
                    "trip_id": trip.trip_id,
                    "route_id": trip.route_id,
                    "stop_id": stu.stop_id,
                    "arrival_time": stu.arrival.time if stu.HasField("arrival") else None,
                    "departure_time": stu.departure.time if stu.HasField("departure") else None,
                    "delay": stu.arrival.delay if stu.HasField("arrival") and stu.arrival.HasField("delay") else None
                })

    trip_updates_df = pd.DataFrame(data)



    return trip_updates_df



def collect_realtime_gtfs_data_v1(api_key: str, duration_minutes: int = 10, interval_seconds: int = 30, output_filename: str = "mta_realtime_data.csv"):
    """
    Collect real-time GTFS data from the MTA API for a specified duration.
    
    Parameters:
    -----------
    api_key : str
        Your MTA API key for authentication
    duration_minutes : int, optional
        Total collection duration in minutes (default: 10)
    interval_seconds : int, optional
        Time between API calls in seconds (default: 30)
    output_filename : str, optional
        Name of the output CSV file (default: "mta_realtime_data.csv")
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing all collected real-time trip updates
    
    Example:
    --------
    df = collect_realtime_gtfs_data(
        api_key="YOUR_MTA_API_KEY",
        duration_minutes=60,
        interval_seconds=30,
        output_filename="my_realtime_data.csv"
    )
    """
    REALTIME_URL = f"https://gtfsrt.prod.obanyc.com/tripUpdates?key={api_key}"
    
    all_records = []
    start_time = time.time()
    collection_end_time = start_time + (duration_minutes * 60)
    
    print(f"🚀 Starting real-time data collection for {duration_minutes} minutes...")
    print(f"📡 Fetching data every {interval_seconds} seconds")
    print(f"💾 Output file: {output_filename}")
    
    while time.time() < collection_end_time:
        try:
            feed = FeedMessage()
            response = requests.get(REALTIME_URL)
            response.raise_for_status()  # Raise exception for bad status codes
            feed.ParseFromString(response.content)

            batch_records = []
            current_timestamp = datetime.datetime.now()
            
            for entity in feed.entity:
                if not entity.trip_update:
                    continue
                    
                trip_id = entity.trip_update.trip.trip_id
                route_id = entity.trip_update.trip.route_id
                
                for stu in entity.trip_update.stop_time_update:
                    record = {
                        "timestamp": current_timestamp,
                        "trip_id": trip_id,
                        "route_id": route_id,
                        "stop_id": stu.stop_id,
                        "arrival_time": datetime.datetime.fromtimestamp(stu.arrival.time) if stu.arrival.HasField('time') else None,
                        "departure_time": datetime.datetime.fromtimestamp(stu.departure.time) if stu.departure.HasField('time') else None,
                        "arrival_delay": stu.arrival.delay if stu.arrival.HasField('delay') else None,
                        "departure_delay": stu.departure.delay if stu.departure.HasField('delay') else None
                    }
                    batch_records.append(record)

            if batch_records:
                all_records.extend(batch_records)
                elapsed_time = time.time() - start_time
                print(f"✅ Fetched {len(batch_records):,} records | Total: {len(all_records):,} | Elapsed: {elapsed_time/60:.1f} min")
            else:
                print(f"⚠️  No records in current batch")
                
            # Calculate remaining time and sleep
            remaining_time = interval_seconds - (time.time() % interval_seconds)
            time.sleep(remaining_time)

        except requests.exceptions.RequestException as e:
            print(f"🔴 Network error: {e}")
            time.sleep(60)  # Wait longer for network issues
        except Exception as e:
            print(f"🔴 Unexpected error: {e}")
            time.sleep(60)  # Wait longer for other errors

    # Convert to DataFrame and save
    if all_records:
        rt_df = pd.DataFrame(all_records)
        rt_df.to_csv(output_filename, index=False)
        print(f"\n🎉 Collection complete!")
        print(f"💾 Saved {len(rt_df):,} records to {output_filename}")
        return rt_df
    else:
        print(f"\n⚠️  No data collected during the specified period")
        return pd.DataFrame()



def collect_realtime_gtfs_data(
    api_key: str, 
    duration_minutes: int = 10, 
    interval_seconds: int = 30, 
    output_filename: str = "mta_realtime_data.csv"
):
    """
    Collect real-time GTFS data from the MTA API for a specified duration.
    
    Parameters:
    -----------
    api_key : str
        Your MTA API key for authentication
    duration_minutes : int, optional
        Total collection duration in minutes (default: 10)
    interval_seconds : int, optional
        Time between API calls in seconds (default: 30)
    output_filename : str, optional
        Name of the output CSV file (default: "mta_realtime_data.csv")
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing all collected real-time trip updates
    """
    REALTIME_URL = f"https://gtfsrt.prod.obanyc.com/tripUpdates?key={api_key}"
    
    all_records = []
    start_time = time.time()
    collection_end_time = start_time + (duration_minutes * 60)
    
    print(f"🚀 Starting real-time data collection for {duration_minutes} minutes...")
    print(f"📡 Fetching data every {interval_seconds} seconds")
    print(f"💾 Output file: {output_filename}")
    
    while time.time() < collection_end_time:
        try:
            feed = FeedMessage()
            response = requests.get(REALTIME_URL)
            response.raise_for_status()
            feed.ParseFromString(response.content)

            batch_records = []
            current_timestamp = datetime.datetime.now()
            
            for entity in feed.entity:
                if not entity.trip_update:
                    continue
                    
                trip_id = entity.trip_update.trip.trip_id
                route_id = entity.trip_update.trip.route_id
                
                for stu in entity.trip_update.stop_time_update:
                    record = {
                        "timestamp": current_timestamp,
                        "trip_id": trip_id,
                        "route_id": route_id,
                        "stop_id": stu.stop_id,
                        "arrival_time": datetime.datetime.fromtimestamp(stu.arrival.time) if stu.arrival.HasField('time') else None,
                        "departure_time": datetime.datetime.fromtimestamp(stu.departure.time) if stu.departure.HasField('time') else None,
                        "arrival_delay": stu.arrival.delay if stu.arrival.HasField('delay') else None,
                        "departure_delay": stu.departure.delay if stu.departure.HasField('delay') else None
                    }
                    batch_records.append(record)

            if batch_records:
                all_records.extend(batch_records)
                elapsed_time = time.time() - start_time
                print(f"✅ Fetched {len(batch_records):,} records | Total: {len(all_records):,} | Elapsed: {elapsed_time/60:.1f} min")
            else:
                print(f"⚠️  No records in current batch")
                
            remaining_time = interval_seconds - (time.time() % interval_seconds)
            time.sleep(remaining_time)

        except requests.exceptions.RequestException as e:
            print(f"🔴 Network error: {e}")
            time.sleep(60)
        except Exception as e:
            print(f"🔴 Unexpected error: {e}")
            time.sleep(60)

    if all_records:
        rt_df = pd.DataFrame(all_records)
        rt_df.to_csv(output_filename, index=False)
        print(f"\n🎉 Collection complete!")
        print(f"💾 Saved {len(rt_df):,} records to {output_filename}")
        return rt_df
    else:
        print(f"\n⚠️  No data collected during the specified period")
        return pd.DataFrame()


def generate_collection_schedule(
    years=[2024, 2025],
    days_per_month=3,
    day_selection_strategy='distributed'
):
    """
    Generate a schedule of dates for data collection.
    
    Parameters:
    -----------
    years : list, optional
        List of years to collect data from (default: [2024, 2025])
    days_per_month : int, optional
        Number of days to collect per month (default: 3)
    day_selection_strategy : str, optional
        Strategy for selecting days: 'distributed', 'random', 'weekdays', 'weekends'
        (default: 'distributed')
    
    Returns:
    --------
    list of datetime.date
        List of dates for data collection
    """
    import random
    from calendar import monthrange
    
    collection_dates = []
    
    for year in years:
        for month in range(1, 13):
            # Get number of days in the month
            _, days_in_month = monthrange(year, month)
            
            if day_selection_strategy == 'distributed':
                # Evenly distribute days throughout the month
                step = days_in_month / (days_per_month + 1)
                selected_days = [int(step * (i + 1)) for i in range(days_per_month)]
            
            elif day_selection_strategy == 'random':
                # Randomly select days
                selected_days = random.sample(range(1, days_in_month + 1), 
                                             min(days_per_month, days_in_month))
                selected_days.sort()
            
            elif day_selection_strategy == 'weekdays':
                # Select weekdays only (Monday-Friday)
                weekdays = []
                for day in range(1, days_in_month + 1):
                    date = datetime.date(year, month, day)
                    if date.weekday() < 5:  # 0-4 are Monday-Friday
                        weekdays.append(day)
                selected_days = random.sample(weekdays, 
                                             min(days_per_month, len(weekdays)))
                selected_days.sort()
            
            elif day_selection_strategy == 'weekends':
                # Select weekend days only (Saturday-Sunday)
                weekends = []
                for day in range(1, days_in_month + 1):
                    date = datetime.date(year, month, day)
                    if date.weekday() >= 5:  # 5-6 are Saturday-Sunday
                        weekends.append(day)
                selected_days = random.sample(weekends, 
                                             min(days_per_month, len(weekends)))
                selected_days.sort()
            
            else:
                raise ValueError(f"Unknown strategy: {day_selection_strategy}")
            
            # Create date objects
            for day in selected_days:
                collection_dates.append(datetime.date(year, month, day))
    
    return collection_dates


def collect_multi_day_data(
    api_key: str,
    years=[2024, 2025],
    days_per_month=3,
    day_selection_strategy='distributed',
    duration_minutes_per_day=60,
    interval_seconds=30,
    output_dir="gtfs_data",
    combine_all=True
):
    """
    Collect GTFS data from multiple days across multiple months and years.
    
    Parameters:
    -----------
    api_key : str
        Your MTA API key
    years : list, optional
        Years to collect data from (default: [2024, 2025])
    days_per_month : int, optional
        Number of days to collect per month (default: 3)
    day_selection_strategy : str, optional
        'distributed', 'random', 'weekdays', or 'weekends' (default: 'distributed')
    duration_minutes_per_day : int, optional
        Minutes to collect per day (default: 60)
    interval_seconds : int, optional
        Seconds between API calls (default: 30)
    output_dir : str, optional
        Directory to save output files (default: "gtfs_data")
    combine_all : bool, optional
        Whether to combine all data into one file (default: True)
    
    Returns:
    --------
    pandas.DataFrame or None
        Combined DataFrame if combine_all=True, else None
    """
    import os
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate collection schedule
    collection_dates = generate_collection_schedule(
        years=years,
        days_per_month=days_per_month,
        day_selection_strategy=day_selection_strategy
    )
    
    print(f"\n📅 Collection Schedule Generated:")
    print(f"   Total dates: {len(collection_dates)}")
    print(f"   Years: {years}")
    print(f"   Strategy: {day_selection_strategy}")
    print(f"   Days per month: {days_per_month}")
    print(f"\n📋 Scheduled dates:")
    for date in collection_dates:
        print(f"   - {date.strftime('%Y-%m-%d (%A)')}")
    
    # Check which dates have already passed (can collect now)
    today = datetime.date.today()
    past_dates = [d for d in collection_dates if d <= today]
    future_dates = [d for d in collection_dates if d > today]
    
    print(f"\n📊 Date Analysis:")
    print(f"   Past/Today dates (can collect now): {len(past_dates)}")
    print(f"   Future dates (need scheduling): {len(future_dates)}")
    
    if future_dates:
        print(f"\n⏰ Future collection dates:")
        for date in future_dates[:5]:  # Show first 5
            print(f"   - {date.strftime('%Y-%m-%d (%A)')}")
        if len(future_dates) > 5:
            print(f"   ... and {len(future_dates) - 5} more")
    
    # Collect data for past dates (simulation - in reality you'd schedule this)
    print(f"\n" + "="*60)
    print("NOTE: This will collect data NOW for demonstration purposes.")
    print("In production, you would schedule this to run on each target date.")
    print("="*60)
    
    user_input = input("\nDo you want to start collecting data now? (yes/no): ")
    if user_input.lower() != 'yes':
        print("Collection cancelled.")
        return None
    
    all_dataframes = []
    
    for i, date in enumerate(past_dates, 1):
        print(f"\n{'='*60}")
        print(f"📅 Collection {i}/{len(past_dates)}: {date.strftime('%Y-%m-%d (%A)')}")
        print(f"{'='*60}")
        
        filename = f"mta_data_{date.strftime('%Y%m%d')}.csv"
        filepath = os.path.join(output_dir, filename)
        
        df = collect_realtime_gtfs_data(
            api_key=api_key,
            duration_minutes=duration_minutes_per_day,
            interval_seconds=interval_seconds,
            output_filename=filepath
        )
        
        if not df.empty:
            df['collection_date'] = date
            all_dataframes.append(df)
    
    # Combine all data if requested
    if combine_all and all_dataframes:
        print(f"\n{'='*60}")
        print("🔄 Combining all collected data...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        combined_filepath = os.path.join(output_dir, "mta_data_combined.csv")
        combined_df.to_csv(combined_filepath, index=False)
        print(f"💾 Combined data saved to: {combined_filepath}")
        print(f"📊 Total records: {len(combined_df):,}")
        print(f"📅 Date range: {combined_df['collection_date'].min()} to {combined_df['collection_date'].max()}")
        print(f"{'='*60}")
        return combined_df
    
    return None


# Example usage:
if __name__ == "__main__":
    # Replace with your actual MTA API key
    API_KEY = "YOUR_MTA_API_KEY"
    
    # Example 1: Collect from 3 distributed days per month for 2024 and 2025
    df = collect_multi_day_data(
        api_key=API_KEY,
        years=[2024, 2025],
        days_per_month=3,
        day_selection_strategy='distributed',
        duration_minutes_per_day=60,
        interval_seconds=30,
        output_dir="gtfs_data_2024_2025",
        combine_all=True
    )
    
    # Example 2: Collect only weekdays
    # df = collect_multi_day_data(
    #     api_key=API_KEY,
    #     years=[2024, 2025],
    #     days_per_month=2,
    #     day_selection_strategy='weekdays',
    #     duration_minutes_per_day=120,
    #     output_dir="gtfs_weekdays"
    # )
    
    # Example 3: View the schedule without collecting
    # schedule = generate_collection_schedule(
    #     years=[2024, 2025],
    #     days_per_month=3,
    #     day_selection_strategy='distributed'
    # )
    # for date in schedule:
    #     print(date.strftime('%Y-%m-%d (%A)'))