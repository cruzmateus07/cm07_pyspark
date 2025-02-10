Project consists in:

1. Authenticate with Google's Youtube API
    a. files: youtube_auth.py
2. Extract data into a Bronze Layer/Landing Zone
    a. files: source_to_bronze.py
3. Refine data into a 3NF Silver Layer
    a. files: bronze_to_silver.py
4. Aggregate Business logics and build Silver data into Dimension-Fact Model in Gold Layer
    a. files: silver_to_gold.py