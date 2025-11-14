from config import *

class FeatureEngineeringRouteDf():
   
    
    def feature_engineering_for_route_id(self, df, route_id_column='route_id'):
        """
        Optimized feature engineering for MTA route_id column
        Removed redundant features to reduce multicollinearity
        """
        
        # Create a copy to avoid modifying original dataframe
        df_eng = df.copy()
        
        # 🔤 Basic Character-Based Features (KEEP - unique structural features)
        df_eng['route_id_length'] = df_eng[route_id_column].str.len()
        df_eng['is_single_char'] = (df_eng[route_id_column].str.len() == 1).fillna(False).astype(int)
        
        # 🚇 Service Pattern Features (KEEP - unique grouping feature)
        df_eng['main_route_group'] = df_eng[route_id_column].str.extract('(\d+|[A-Z])')[0]
        
        # 🎨 Color Family Features (KEEP - unique visual branding)
        def get_color_family(route_id):
            route_str = str(route_id)
            if route_str in ['1', '2', '3']:
                return 'red_family'
            elif route_str in ['4', '5', '5X', '6', '6X']:
                return 'green_family'
            elif route_str in ['7', '7X']:
                return 'purple_family'
            elif route_str in ['A', 'C', 'E']:
                return 'blue_family'
            elif route_str in ['B', 'D', 'F', 'FX', 'M']:
                return 'orange_family'
            elif route_str == 'G':
                return 'lime_family'
            elif route_str in ['J', 'Z']:
                return 'brown_family'
            elif route_str == 'L':
                return 'gray_family'
            elif route_str in ['N', 'Q', 'R', 'W']:
                return 'yellow_family'
            else:
                return 'other_family'
        
        df_eng['predicted_color_family'] = df_eng[route_id_column].apply(get_color_family)
        
        # 📊 Numeric & Ranking Features (KEEP - unique importance metric)
        def route_importance(route_id):
            route_str = str(route_id)
            base_id = route_str[0]  # Take first character
            if base_id.isdigit():
                return 1 / int(base_id)  # Lower numbers = more important
            elif base_id.isalpha():
                return ord('Z') - ord(base_id)  # Earlier letters = more important
            else:
                return 0
        
        df_eng['route_importance_score'] = df_eng[route_id_column].apply(route_importance)
        
        # Manhattan Centrality Indicator (KEEP - unique geographic feature)
        manhattan_core_routes = ['1', '2', '3', '4', '5', '6', '7', 'A', 'C', 'E', 'N', 'Q', 'R', 'W']
        df_eng['is_manhattan_core'] = df_eng[route_id_column].isin(manhattan_core_routes).astype(int)
        
        # Peak Hour Service Indicator (KEEP - unique temporal feature)
        peak_hour_routes = ['5X', '6X', '7X', 'FX', 'Z']
        df_eng['is_peak_hour_only'] = df_eng[route_id_column].isin(peak_hour_routes).astype(int)
        
        # Route Age Estimation (KEEP - unique historical feature)
        df_eng['estimated_route_age'] = df_eng[route_id_column].apply(
            lambda x: 'historic' if str(x).isdigit() and int(x) <= 3 else
                    'classic' if str(x).isdigit() or (str(x).isalpha() and str(x) in 'ABCDEFG') else
                    'modern'
        )
        
        print(f"✅ Created {len([col for col in df_eng.columns if col not in df.columns])} optimized features from {route_id_column}")
        print(f"📊 New features: {[col for col in df_eng.columns if col not in df.columns]}")
        
        return df_eng
    

    def feature_engineering_for_route_long_name(self, df, route_long_name_column='route_long_name'):
        """
        Optimized feature engineering for MTA route_long_name column
        Removed redundant and low-value features
        """
        
        # Create a copy to avoid modifying original dataframe
        df_eng = df.copy()
        
        # Ensure we're working with strings and handle NaN values
        df_eng[route_long_name_column] = df_eng[route_long_name_column].fillna('')
        
        # 🗺️ Geographic & Corridor Features (KEEP - unique geographic intelligence)
        def extract_geographic_features(name):
            name = str(name).upper()
            features = {}
            
            # Main corridors/avenues
            corridors = ['BROADWAY', '7 AVENUE', 'LEXINGTON AVENUE', '8 AVENUE', 
                        '6 AVENUE', 'FLUSHING', '14 ST', '42 ST', 'NASSAU ST',
                        'QUEENS BOULEVARD', 'BROOKLYN-QUEENS', 'FRANKLIN AVENUE',
                        'ROCKAWAY', 'PELHAM', 'JAMAICA', 'CANARSIE']
            
            for corridor in corridors:
                if corridor in name:
                    features['main_corridor'] = corridor.lower().replace(' ', '_')
                    break
            else:
                features['main_corridor'] = 'other'
            
            # Borough coverage
            boroughs = ['MANHATTAN', 'BROOKLYN', 'QUEENS', 'BRONX', 'STATEN ISLAND']
            boroughs_in_name = [borough for borough in boroughs if borough in name]
            features['boroughs_mentioned_count'] = len(boroughs_in_name)
            features['boroughs_list'] = ', '.join(boroughs_in_name) if boroughs_in_name else 'none'
            
            # Specific neighborhoods/areas mentioned
            areas = ['ASTORIA', 'FLUSHING', 'JAMAICA', 'CANARSIE', 'PELHAM', 'ROCKAWAY',
                    'INWOOD', 'WAKEFIELD', 'WOODLAWN', 'MIDDLE VILLAGE', 'FOREST HILLS',
                    'CONEY ISLAND', 'BRIGHTON', 'BAY RIDGE']
            
            areas_found = [area for area in areas if area in name]
            features['areas_mentioned_count'] = len(areas_found)
            features['areas_list'] = ', '.join(areas_found) if areas_found else 'none'
            
            return features
        
        # Apply geographic feature extraction
        geo_features = df_eng[route_long_name_column].apply(extract_geographic_features)
        df_eng['main_corridor'] = geo_features.apply(lambda x: x['main_corridor'])
        df_eng['boroughs_mentioned_count'] = geo_features.apply(lambda x: x['boroughs_mentioned_count'])
        df_eng['boroughs_mentioned'] = geo_features.apply(lambda x: x['boroughs_list'])
        df_eng['areas_mentioned_count'] = geo_features.apply(lambda x: x['areas_mentioned_count'])
        df_eng['areas_mentioned'] = geo_features.apply(lambda x: x['areas_list'])
        
        # 🚇 Service Type & Operational Features (KEEP - comprehensive service classification)
        def extract_service_features(name):
            name = str(name).upper()
            features = {}
            
            # Service type (KEEP - replaces is_express, is_shuttle from route_id)
            if 'LOCAL' in name:
                features['service_type'] = 'local'
            elif 'EXPRESS' in name:
                features['service_type'] = 'express'
            elif 'SHUTTLE' in name:
                features['service_type'] = 'shuttle'
            elif 'CROSSTOWN' in name:
                features['service_type'] = 'crosstown'
            else:
                features['service_type'] = 'other'
            
            # Service pattern complexity (KEEP - replaces complexity_level from route_id)
            if 'LOCAL' in name and 'EXPRESS' in name:
                features['service_pattern'] = 'mixed'
            elif '/' in name or '&' in name:
                features['service_pattern'] = 'combined'
            else:
                features['service_pattern'] = 'simple'
            
            # Time restrictions (KEEP - unique operational feature)
            features['has_time_restriction'] = int(any(word in name for word in 
                                                      ['WEEKDAYS', 'WEEKENDS', 'RUSH', 'DAYTIME', 'NIGHTS']))
            
            # Route characteristics (KEEP - unique orientation features)
            features['is_crosstown'] = int('CROSSTOWN' in name)
            features['is_avenue_based'] = int(any(word in name for word in 
                                                ['AVENUE', 'AV', 'BOULEVARD', 'BLVD']))
            features['is_street_based'] = int(any(word in name for word in 
                                                ['STREET', 'ST', 'PLACE', 'PL']))
            features['has_multiple_services'] = int(name.count('Local') + name.count('Express') > 1)
            
            return features
        
        # Apply service feature extraction
        service_features = df_eng[route_long_name_column].apply(extract_service_features)
        df_eng['service_type'] = service_features.apply(lambda x: x['service_type'])
        df_eng['service_pattern'] = service_features.apply(lambda x: x['service_pattern'])
        df_eng['has_time_restriction'] = service_features.apply(lambda x: x['has_time_restriction'])
        df_eng['is_crosstown'] = service_features.apply(lambda x: x['is_crosstown'])
        df_eng['is_avenue_based'] = service_features.apply(lambda x: x['is_avenue_based'])
        df_eng['is_street_based'] = service_features.apply(lambda x: x['is_street_based'])
        df_eng['has_multiple_services'] = service_features.apply(lambda x: x['has_multiple_services'])
        
        # 📊 Text-Based & Complexity Features (KEEP only high-value text features)
        def extract_text_features(name):
            name = str(name)
            features = {}
            
            # Basic text properties (KEEP - unique complexity metrics)
            features['long_name_length'] = len(name)
            features['long_name_word_count'] = len(name.split())
            
            # Geographic scope indicator (KEEP - unique geographic feature)
            features['contains_borough_name'] = int(any(borough in name.upper() for borough in 
                                                      ['MANHATTAN', 'BROOKLYN', 'QUEENS', 'BRONX']))
            
            return features
        
        # Apply text feature extraction
        text_features = df_eng[route_long_name_column].apply(extract_text_features)
        df_eng['long_name_length'] = text_features.apply(lambda x: x['long_name_length'])
        df_eng['long_name_word_count'] = text_features.apply(lambda x: x['long_name_word_count'])
        df_eng['contains_borough_name'] = text_features.apply(lambda x: x['contains_borough_name'])
        
        # 🎯 Advanced Derived Features (KEEP - unique demographic and network features)
        def extract_advanced_features(name):
            name = str(name).upper()
            features = {}
            
            # Route importance indicators
            features['serves_manhattan_cbd'] = int(any(term in name for term in 
                                                     ['BROADWAY', 'LEXINGTON', '7 AV', '8 AV', '6 AV']))
            
            # Service complexity (KEEP - replaces complexity_level from route_id)
            word_count = len(str(name).split())
            if word_count <= 3:
                features['name_complexity'] = 'simple'
            elif word_count <= 5:
                features['name_complexity'] = 'medium'
            else:
                features['name_complexity'] = 'complex'
            
            # Tourist route indicator (KEEP - replaces tourist_friendly from route_id)
            features['likely_tourist_route'] = int(any(term in name for term in
                                                     ['BROADWAY', 'TIMES SQUARE', '42 ST', 'CENTRAL PARK']))
            
            # Commuter route indicator (KEEP - unique demographic feature)
            features['likely_commuter_route'] = int(any(term in name for term in
                                                      ['EXPRESS', 'QUEENS', 'BROOKLYN', 'BRONX']))
            
            return features
        
        # Apply advanced feature extraction
        advanced_features = df_eng[route_long_name_column].apply(extract_advanced_features)
        df_eng['serves_manhattan_cbd'] = advanced_features.apply(lambda x: x['serves_manhattan_cbd'])
        df_eng['name_complexity'] = advanced_features.apply(lambda x: x['name_complexity'])
        df_eng['likely_tourist_route'] = advanced_features.apply(lambda x: x['likely_tourist_route'])
        df_eng['likely_commuter_route'] = advanced_features.apply(lambda x: x['likely_commuter_route'])
        
        # 🌐 Network Position Features (KEEP - unique network intelligence)
        def extract_network_features(name):
            name = str(name).upper()
            features = {}
            
            # Route position in network (KEEP - replaces route_type_category from route_id)
            if 'SHUTTLE' in name:
                features['network_role'] = 'connector'
            elif 'CROSSTOWN' in name:
                features['network_role'] = 'crosstown'
            elif 'EXPRESS' in name and 'LOCAL' in name:
                features['network_role'] = 'hybrid'
            elif 'EXPRESS' in name:
                features['network_role'] = 'trunk'
            elif 'LOCAL' in name:
                features['network_role'] = 'local'
            else:
                features['network_role'] = 'other'
            
            # Coverage breadth (KEEP - replaces borough_coverage from route_id)
            borough_count = sum(1 for borough in ['MANHATTAN', 'BROOKLYN', 'QUEENS', 'BRONX'] 
                              if borough in name)
            if borough_count >= 3:
                features['coverage_breadth'] = 'regional'
            elif borough_count == 2:
                features['coverage_breadth'] = 'interborough'
            else:
                features['coverage_breadth'] = 'local'
            
            return features
        
        # Apply network feature extraction
        network_features = df_eng[route_long_name_column].apply(extract_network_features)
        df_eng['network_role'] = network_features.apply(lambda x: x['network_role'])
        df_eng['coverage_breadth'] = network_features.apply(lambda x: x['coverage_breadth'])
        
        print(f"✅ Created {len([col for col in df_eng.columns if col not in df.columns])} optimized features from {route_long_name_column}")
        print(f"📊 New features: {[col for col in df_eng.columns if col not in df.columns]}")
        
        return df_eng

    
    def apply_all_feature_engineering(self, df):
        """
        Apply all optimized feature engineering to the routes dataframe
        """
        print("🚀 Starting optimized feature engineering for routes dataframe...")
        
        # Apply route_id feature engineering
        df_with_features = self.feature_engineering_for_route_id(df)
        
        # Apply route_long_name feature engineering
        df_with_features = self.feature_engineering_for_route_long_name(df_with_features)
        
        total_new_features = len([col for col in df_with_features.columns if col not in df.columns])
        print(f"🎉 Total new features created: {total_new_features}")
        
        return df_with_features
    

def extract_stop_times_features(stop_times_df):
    """
    Extracts key engineered features from a GTFS stop_times DataFrame.
    Includes trip duration, anomaly flags, and parsed trip_id structure.

    Parameters
    ----------
    stop_times_df : pandas.DataFrame
        Must contain 'trip_id', 'arrival_time', 'departure_time', and 'stop_sequence'.

    Returns
    -------
    df : pandas.DataFrame
        DataFrame with new core feature columns added.
    """

    df = stop_times_df.copy()

    
    # ---------------------------------------------------------
    # 🚌 2. Trip ID decomposition (keep core identifiers)
    # ---------------------------------------------------------
    parts = df['trip_id'].str.split('-', expand=True)
    df['day_type'] = parts[3]

    # Extract post-day structure (block, trip, direction)
    temp = parts[4].str.split('_', n=2, expand=True)
    df['direction'] = temp[2].str.extract(r'\.\.(\w+)$')

    # Keep only direction flags
    df['is_northbound'] = df['direction'].str.contains('N', na=False).astype(int)
    df['is_southbound'] = df['direction'].str.contains('S', na=False).astype(int)


    # ---------------------------------------------------------
    # 🧹 4. Cleanup and return
    # ---------------------------------------------------------
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Drop redundant intermediate parsing columns
    df.drop(
        columns=[
            'direction',  # encoded already by north/south flags
        ],
        inplace=True,
        errors='ignore'
    )

    return df

def extract_stops_features(stops_df):
    df = stops_df.copy()
    
    df['hierarchy_level'] = 0 
    # Main stations (highest level)
    df.loc[(df['location_type'] == 1.0) & (df['parent_station'].isna()), 'hierarchy_level'] = 2
    
    # Platforms within stations (middle level)  
    df.loc[(df['location_type'].isna()) & (df['parent_station'].notna()), 'hierarchy_level'] = 1
    
    
    #from stop name column 
    df['stop_name_clean'] = df['stop_name'].str.replace(r'\(.*?\)', '', regex=True).str.strip().str.lower()

    df['is_terminal_stop'] = df['stop_name_clean'].str.contains(
        r'(college|park|bay|stillwell|tottenville|st george|beach 116)', regex=True
    ).astype(int)

    df['is_interchange_stop'] = df['stop_name_clean'].str.contains(
        r'(times sq|grand central|union sq|atlantic av|barclays|court sq|fulton|brooklyn bridge)',
        regex=True
    ).astype(int)

    df['borough_hint'] = np.select([
        df['stop_name_clean'].str.contains('bronx'),
        df['stop_name_clean'].str.contains('brooklyn'),
        df['stop_name_clean'].str.contains('queens'),
        df['stop_name_clean'].str.contains('staten'),
        df['stop_name_clean'].str.contains('manhattan'),
    ], ['Bronx', 'Brooklyn', 'Queens', 'Staten Island', 'Manhattan'], default='Unknown')

    df['has_direction_in_name'] = df['stop_name_clean'].str.contains(
        r'(east|west|north|south)'
    ).astype(int)

    df['is_airport_related'] = df['stop_name_clean'].str.contains(
        r'(airport|jfk)'
    ).astype(int)

    df['stop_freq_rank'] = df['stop_name'].map(df['stop_name'].value_counts(normalize=True))

    df.drop(columns=['stop_name_clean'], inplace=True)

    #from stop id column 
    df['base_stop_id'] = df['stop_id'].str.extract(r'(\d+|[A-Z]\d+)')
    df['direction'] = df['stop_id'].str.extract(r'([NSEW])$')

    df['is_northbound'] = (df['direction'] == 'N').astype(int)
    df['is_southbound'] = (df['direction'] == 'S').astype(int)
    df['is_eastbound'] = (df['direction'] == 'E').astype(int)
    df['is_westbound'] = (df['direction'] == 'W').astype(int)

    
    return df

def extract_trip_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts structured features from the 'trip_id' column.

    Parameters:
        df (pd.DataFrame): DataFrame containing a 'trip_id' column.

    Returns:
        pd.DataFrame: Original DataFrame with new extracted feature columns.
    """
    if 'trip_id' not in df.columns:
        raise KeyError("The DataFrame must contain a 'trip_id' column.")

    # Split trip_id by '-'
    split_cols = df["trip_id"].str.split('-', expand=True)

    # Assign meaningful column names     #
    df["route_code"] = split_cols[1]       # e.g., 1038 / R052
    df["day_name"] = split_cols[2]         # e.g., Sunday / Saturday
    df["trip_info"] = split_cols[3]        # e.g., 00_000600_1..S03R

    # Extract sub-features from trip_info
    df["trip_time"] = df["trip_info"].str.extract(r"(\d{2}_\d{6})")   # e.g., 00_000600
    df["direction"] = df["trip_info"].str.extract(r"([NS]\d{2}R)")    # e.g., S03R / N27R
    df["dir_letter"] = df["direction"].str[0]                         # N or S
    df["route_num"] = df["direction"].str[1:3]                        # 03 or 27

    df = df.drop(columns=['route_code', 'trip_info', 'direction'])


    #from trip_headsign column 
    split_cols = df["trip_id"].str.split('', expand=True)
    df["trip_info"] = split_cols[3]      # Example: 00_000600_1..S03R

    # --- 2. Extract details from trip_info ---
    df["trip_time"] = df["trip_info"].str.extract(r"(\d{2}_\d{6})")   # 00_000600
    df["direction"] = df["trip_info"].str.extract(r"([NS]\d{2}R)")    # S03R / N27R
    df["dir_letter"] = df["direction"].str[0]                         # N or S
    df["route_num"] = df["direction"].str[1:3]                        # 03 or 27

    # --- 3. Convert binary direction_id to a label ---
    if "direction_id" in df.columns:
        df["direction_label"] = df["direction_id"].map({0: "Northbound", 1: "Southbound"})

    # --- 4. Clean and encode the 'trip' column (train line) ---
    if "trip" in df.columns:
        df["trip"] = df["trip"].astype(str)
        # Identify if route is express (X suffix) or local
        df["is_express"] = df["trip"].str.endswith("X").astype(int)
        # Clean route names (remove X)
        df["trip_clean"] = df["trip"].str.replace("X", "", regex=False)

    # --- 5. Extract geographic hints from stop_name (station names) ---
    if "stop_name" in df.columns:
        df["is_brooklyn"] = df["stop_name"].str.contains("Brooklyn", case=False, na=False).astype(int)
        df["is_manhattan"] = df["stop_name"].str.contains("Manhattan|42 St|Broadway|Times Sq", case=False, na=False).astype(int)
        df["is_queens"] = df["stop_name"].str.contains("Queens|Jamaica", case=False, na=False).astype(int)
        df["is_bronx"] = df["stop_name"].str.contains("Bronx|Woodlawn|Wakefield", case=False, na=False).astype(int)
        df["is_stat_island"] = df["stop_name"].str.contains("St George|Tottenville", case=False, na=False).astype(int)

    # --- 6. Cleanup temporary columns ---
    df.drop(columns=["trip_info"], inplace=True, errors="ignore")

    return df


    return df





