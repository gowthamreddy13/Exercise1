import duckdb


def main():
    pass
import duckdb
import pyarrow.parquet as pq

def create_table():
    con = duckdb.connect(database=':memory:', read_only=False)
    con.execute('CREATE TABLE electric_cars ('
                'vin VARCHAR(10),'
                'county VARCHAR,'
                'city VARCHAR,'
                'state VARCHAR,'
                'postal_code VARCHAR,'
                'model_year INTEGER,'
                'make VARCHAR,'
                'model VARCHAR,'
                'electric_vehicle_type VARCHAR,'
                'cafv_eligibility VARCHAR,'
                'electric_range INTEGER,'
                'base_msrp FLOAT,'
                'legislative_district INTEGER,'
                'dol_vehicle_id INTEGER,'
                'vehicle_location VARCHAR,'
                'electric_utility VARCHAR,'
                'census_tract BIGINT'
                ')'
                )
    return con

def import_csv(con):
    con.execute(' C:\Users\saigo\Downloads\data-engineering-practice\Exercises\Exercise-8\data' (HEADER TRUE, DELIMITER \',\')')

def count_cars_per_city(con):
    result = con.execute('SELECT city, COUNT(*) AS car_count FROM electric_cars GROUP BY city')
    return result.fetch_all()

def find_top_3_vehicles(con):
    result = con.execute('SELECT make, model, COUNT(*) AS car_count FROM electric_cars '
                         'GROUP BY make, model ORDER BY car_count DESC LIMIT 3')
    return result.fetch_all()

def find_most_popular_vehicle_by_postal_code(con):
    result = con.execute('SELECT postal_code, make, model, COUNT(*) AS car_count '
                         'FROM electric_cars '
                         'GROUP BY postal_code, make, model '
                         'HAVING COUNT(*) = (SELECT MAX(count) '
                         '                   FROM (SELECT COUNT(*) AS count '
                         '                         FROM electric_cars '
                         '                         GROUP BY postal_code, make, model) subquery)')

    return result.fetch_all()

def count_cars_by_model_year(con):
    result = con.execute('SELECT model_year, COUNT(*) AS car_count '
                         'FROM electric_cars '
                         'GROUP BY model_year')

    table = result.fetch_arrow_table()
    pq.write_to_dataset(table, 'car_count_by_model_year.parquet', partition_cols=['model_year'])

# Testing function for count_cars_per_city
def test_count_cars_per_city():
    con = create_table()
    import_csv(con)
    result = count_cars_per_city(con)
    assert result == [('Yakima', 1), ('San Diego', 1), ('Eugene', 1)]

# Main execution
def main():
    con = create_table()
    import_csv(con)

    # Task 1: Count the number of electric cars per city
    result_per_city = count_cars_per_city(con)
    print("Electric cars per city:")
    for city, count in result_per_city:
        print(f"{city}: {count}")

    # Task 2: Find the top 3 most popular electric vehicles
    top_vehicles = find_top_3_vehicles(con)
    print("\nTop 3 most popular electric vehicles:")
    for make, model, count in top_vehicles:
        print(f"{make} {model}: {count}")

    


if __name__ == "__main__":
    main()
