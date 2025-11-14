# Routes_df 
## route_id 
🔤 Structural Features

    route_id_length - Character length of route ID
    is_single_char - 1 if route ID is a single character
    main_route_group - Main route identifier (first number or letter)

🎨 Visual & Branding Features

    predicted_color_family - Color family (red/green/blue/orange/etc.)

⭐ Importance & Temporal Features

    route_importance_score - Numeric importance ranking
    is_manhattan_core - 1 if route serves Manhattan core
    is_peak_hour_only - 1 if route runs only during peak hours
    estimated_route_age - historic/classic/modern age classification

## route_long_name 
Features Created by This Function

Geographic Features (8 features):

    main_corridor - Primary street/avenue (broadway, 7_avenue, lexington_avenue, etc.)
    boroughs_mentioned_count - Number of boroughs mentioned
    boroughs_mentioned - List of boroughs mentioned
    areas_mentioned_count - Number of neighborhoods/areas mentioned
    areas_mentioned - List of areas mentioned
    contains_borough_name - 1 if any borough name appears
    serves_manhattan_cbd - 1 if serves Manhattan central business district
    coverage_breadth - local/interborough/regional coverage
    Service Type Features (9 features):

    service_type - local/express/shuttle/crosstown/other
    service_pattern - simple/combined/mixed service pattern
    has_time_restriction - 1 if mentions time restrictions
    is_crosstown - 1 if crosstown route
    is_shuttle - 1 if shuttle service
    is_avenue_based - 1 if route follows an avenue
    is_street_based - 1 if route follows a street
    network_role - connector/crosstown/hybrid/trunk/local/other
    has_multiple_services - 1 if mentions multiple service types
    Text & Complexity Features (8 features):

    long_name_length - Character length of route name
    long_name_word_count - Number of words in route name
    has_hyphen - 1 if contains hyphen
    has_slash - 1 if contains slash
    has_ampersand - 1 if contains ampersand
    has_parentheses - 1 if contains parentheses
    has_dash_separator - 1 if uses " - " as separator
    name_complexity - simple/medium/complex based on word count
    Usage & Demographic Features (3 features):

    likely_tourist_route - 1 if likely serves tourist areas
    likely_commuter_route - 1 if likely serves commuters
    network_role - Route's role in the transit network
    Total: 28 new features extracted from the route long name!


🌍 Geographic Features (6 features)

    main_corridor - Primary street/avenue (broadway/7_avenue/lexington_avenue/etc.)
    boroughs_mentioned_count - Number of boroughs mentioned
    boroughs_mentioned - List of boroughs mentioned
    areas_mentioned_count - Number of neighborhoods/areas mentioned
    areas_mentioned - List of areas mentioned
    contains_borough_name - 1 if any borough name appears explicitly
🚇 Service Type Features (7 features)

    service_type - local/express/shuttle/crosstown/other
    service_pattern - simple/combined/mixed service pattern
    has_time_restriction - 1 if mentions time restrictions
    is_crosstown - 1 if crosstown route
    is_avenue_based - 1 if route follows an avenue
    is_street_based - 1 if route follows a street
    has_multiple_services - 1 if mentions multiple service types
    
📊 Text & Complexity Features (3 features)

    long_name_length - Character length of route name
    long_name_word_count - Number of words in route name
    name_complexity - simple/medium/complex based on word count

👥 Usage & Network Features (4 features)

    serves_manhattan_cbd - 1 if serves Manhattan central business district
    likely_tourist_route - 1 if likely serves tourist areas
    likely_commuter_route - 1 if likely serves commuters
    network_role - connector/crosstown/hybrid/trunk/local/other
    coverage_breadth - local/interborough/regional coverage

## stop times df
🕒 Duration & Timing Features (4 features)

    trip_duration_min - Trip duration in minutes
    same_time_flag - 1 if arrival_time = departure_time
    zero_duration_many_stops - 1 if zero duration but >21 stops (anomaly)
    avg_time_per_stop - Average minutes per stop
    stops_per_minute - Stops per minute (inverse of time per stop)
🚌 Trip ID Decomposition Features (4 features)

    day_type - Service day type (e.g., "Sunday", "Weekday")
    direction - Extracted direction code (e.g., "S03R")
    is_northbound - 1 if direction contains 'N'
    is_southbound - 1 if direction contains 'S'

## stops df 
 - location_type and parent station will be merged into **hierarchy_level** column with 3 unique values [1, 2, 3] 
 

    1. **`stop_name_clean`**
    → Cleaned version of the stop name, with any text in parentheses removed and extra spaces stripped.
    *(Example: "Times Sq (1,2,3)" → "Times Sq")*

    2. **`stop_name_lower`** *(temporary)*
    → Lowercase version of the stop name used for text matching.
    *(Dropped at the end; not a final feature.)*

    3. **`is_terminal_stop`**
    → **1 if the stop is a terminal/end station** (detected by keywords like “Tottenville”, “Stillwell”, “College”, etc.), else 0.
    *(Useful for identifying start/end points of routes.)*

    4. **`is_interchange_stop`**
    → **1 if the stop is a major interchange/hub** (e.g., “Times Sq”, “Grand Central”, “Union Sq”), else 0.
    *(Marks high-traffic or connection points.)*

    5. **`borough_hint`**
    → Categorical feature giving a **hint of which NYC borough** the stop name refers to.
    Values: `"Bronx"`, `"Brooklyn"`, `"Queens"`, `"Staten Island"`, `"Manhattan"`, or `"Unknown"`.
    *(Adds rough geographical context.)*

    6. **`has_direction_in_name`**
    → **1 if the stop name contains a direction word** (e.g., “East”, “North”, “South”), else 0.
    *(Captures directional clues in station naming.)*

    7. **`is_airport_related`**
    → **1 if the stop name mentions “airport” or “JFK”**, else 0.
    *(Useful for detecting airport-linked routes.)*

    8. **`stop_freq_rank`**
    → Normalized **frequency of how often each stop name appears** in the dataset.
    *(Helps identify common vs rare stops — values between 0 and 1.)*

## trips df 
    day_name → Extracted from trip_id; indicates the day the trip runs (e.g., "Sunday", "Saturday", "Weekday"). Useful for distinguishing weekend vs. weekday service.
    trip_time → Extracted from the trip_info segment of trip_id; represents the scheduled time of the trip in "HH_MMMMMM" format, which can be converted into minutes or hours.
    dir_letter → The first letter of the direction code ("N" or "S"), representing northbound or southbound travel.
    route_num → The numeric portion of the direction code (e.g., "03" or "27"), identifying a specific route branch or line variant.


## real time and static time data sources 
    Yes — the static and real-time GTFS feeds you downloaded are compatible.
    Both the static feed from TransitFeeds (MTA, ID 81) and the real-time feed from 
      gtfsrt.prod.obanyc.com/tripUpdates?key= belong to the same MTA NYC Bus system, and are designed to 
    work together.