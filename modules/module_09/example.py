#!/usr/bin/env python3
"""
Module 09: PostGIS Spatial Property Search
Advanced PostgreSQL with PostGIS for property analysis
"""

import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

class PropertySearchModule:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            database=os.getenv('DB_NAME', 'property_search'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'password'),
            cursor_factory=RealDictCursor
        )

    def setup_postgis_tables(self):
        """Create PostGIS enabled property tables"""
        cursor = self.conn.cursor()
        
        # Enable PostGIS extension
        cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        
        # Create properties table with spatial data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS properties (
                id SERIAL PRIMARY KEY,
                address TEXT NOT NULL,
                price DECIMAL(12,2),
                bedrooms INTEGER,
                bathrooms DECIMAL(3,1),
                square_feet INTEGER,
                property_type VARCHAR(50),
                listing_status VARCHAR(20) DEFAULT 'active',
                location GEOMETRY(POINT, 4326),
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        
        # Create spatial index for fast geospatial queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_properties_location 
            ON properties USING GIST (location);
        """)
        
        self.conn.commit()
        cursor.close()
        print("✓ PostGIS tables and indexes created")

    def find_nearby_properties(self, lat, lng, radius_km=5):
        """Find properties within radius using spatial queries"""
        cursor = self.conn.cursor()
        
        query = """
            SELECT 
                id, address, price, bedrooms, bathrooms, 
                property_type, square_feet,
                ST_Distance(location, ST_GeomFromText('POINT(%s %s)', 4326)) * 111320 as distance_meters
            FROM properties 
            WHERE ST_DWithin(location, ST_GeomFromText('POINT(%s %s)', 4326), %s / 111.32)
            AND listing_status = 'active'
            ORDER BY distance_meters
            LIMIT 20;
        """
        
        cursor.execute(query, (lng, lat, lng, lat, radius_km))
        results = cursor.fetchall()
        
        print(f"Found {len(results)} properties within {radius_km}km:")
        for prop in results[:5]:
            distance_m = prop['distance_meters'] or 0
            price = prop['price'] or 0
            address = prop['address'] or 'Unknown Address'
            print(f"  {address} - Price: {price:,} ({distance_m:.0f}m away)")
        
        cursor.close()
        return results

    def analyze_market_density(self):
        """Analyze property market density using spatial aggregation"""
        cursor = self.conn.cursor()
        
        query = """
            WITH grid_analysis AS (
                SELECT 
                    ST_SnapToGrid(location, 0.01) as grid_cell,
                    COUNT(*) as property_count,
                    AVG(price) as avg_price,
                    property_type
                FROM properties 
                WHERE location IS NOT NULL 
                AND listing_status = 'active'
                AND price IS NOT NULL
                GROUP BY ST_SnapToGrid(location, 0.01), property_type
                HAVING COUNT(*) >= 2
            )
            SELECT 
                property_type,
                property_count,
                ROUND(avg_price::numeric, 0) as average_price,
                ST_X(grid_cell) as center_lng,
                ST_Y(grid_cell) as center_lat
            FROM grid_analysis
            ORDER BY avg_price DESC
            LIMIT 10;
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        print("Market density analysis:")
        for row in results:
            prop_type = row['property_type'] or 'Unknown'
            avg_price = row['average_price'] or 0
            count = row['property_count'] or 0
            print(f"  {prop_type}: {avg_price:,.0f} avg, {count} properties")
        
        cursor.close()
        return results

    def run_spatial_demo(self):
        """Run complete spatial analysis demonstration"""
        print(f"Running Module 09 - PostGIS Spatial Analysis")
        self.setup_postgis_tables()
        
        # Demo with San Francisco coordinates
        sf_lat, sf_lng = 37.7749, -122.4194
        self.find_nearby_properties(sf_lat, sf_lng, radius_km=3)
        self.analyze_market_density()
        
        print(f"Module 09 spatial analysis completed!")

if __name__ == "__main__":
    demo = PropertySearchModule()
    demo.run_spatial_demo()
