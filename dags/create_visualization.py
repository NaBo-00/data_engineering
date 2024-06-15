import requests
import json
import sys

from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

def get_access_token():
    login_url = "http://superset:8088/api/v1/security/login"
    login_data = {
        "username": os.getenv('ADMIN_USERNAME'), #"admin",
        "password": os.getenv('ADMIN_PASSWORD'), # "admin", 
        "provider": "db",
        "refresh": True
    }
    response = requests.post(login_url, json=login_data)
    
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        print(f"Failed to obtain access token: {response.status_code} {response.text}")
        raise ValueError(f"Failed to obtain access token: {response.text}")
    
def get_csrf_token(session, headers):
    csrf_url = "http://superset:8088/api/v1/security/csrf_token/"
    csrf_response = session.get(csrf_url, headers=headers)
    csrf_token = csrf_response.json().get("result")
    return csrf_token

def update_session_with_csrf_token(session, csrf_token):
    session.headers.update({
        "X-CSRFToken": csrf_token
    })

def check_database_exists(database_name, headers):
    database_list_url = "http://superset:8088/api/v1/database/"
    response = requests.get(database_list_url, headers=headers)
    if response.status_code == 200:
        databases = response.json().get("result")
        for database in databases:
            if database["database_name"] == database_name:
                return True, database["id"]
    return False, None

def create_database(database_name, sqlalchemy_uri, headers, session):
    csrf_token = get_csrf_token(session, headers)
    update_session_with_csrf_token(session, csrf_token)

    database_url = "http://superset:8088/api/v1/database/"
    database_data = {
        "database_name": database_name,
        "sqlalchemy_uri": sqlalchemy_uri,
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "extra": json.dumps({
            "engine_params": {
                "connect_args": {
                    "application_name": "Superset"
                }
            },
            "metadata_params": {},
            "metadata_cache_timeout": {},
            "schemas_allowed_for_csv_upload": []
        }),
        "impersonate_user": False,
        "external_metadata": [],
        "is_managed_externally": False,
        "allow_csv_upload": True
    }
    response = session.post(database_url, headers=headers, json=database_data)
    if response.status_code == 201:
        return response.json().get("id")
    else:
        raise ValueError(f"Failed to create database: {response.text}")

def check_dataset_exists(dataset_name, headers):
    dataset_list_url = "http://superset:8088/api/v1/dataset/"
    response = requests.get(dataset_list_url, headers=headers)
    if response.status_code == 200:
        datasets = response.json().get("result")
        for dataset in datasets:
            if dataset["table_name"] == dataset_name:
                return True
    return False

def create_dataset(dataset_name, headers, session, database_id):
    csrf_token = get_csrf_token(session, headers)
    update_session_with_csrf_token(session, csrf_token)

    dataset_url = "http://superset:8088/api/v1/dataset/"
    dataset_data = {
        "database": database_id,
        "schema": "public",
        "table_name": dataset_name,
        "sql": ""
    }
    response = session.post(dataset_url, headers=headers, json=dataset_data)
    if response.status_code == 201:
        return response.json().get("id")
    else:
        raise ValueError(f"Failed to create dataset: {response.text}")

def check_dashboard_exists(dashboard_title, headers):
    dashboard_list_url = "http://superset:8088/api/v1/dashboard/"
    response = requests.get(dashboard_list_url, headers=headers)
    if response.status_code == 200:
        dashboards = response.json().get("result")
        for dashboard in dashboards:
            if dashboard["dashboard_title"] == dashboard_title:
                return True
    return False

def create_dashboard(dataset_name, headers, session):
    csrf_token = get_csrf_token(session, headers)
    update_session_with_csrf_token(session, csrf_token)

    dashboard_url = "http://superset:8088/api/v1/dashboard/"
    dashboard_data = {
        "dashboard_title": dataset_name,
        "css": "",
        "json_metadata": "{}",
        "slug": dataset_name,
        "owners": [1],
        "published": True,
        "position_json": "{}",
    }
    response = session.post(dashboard_url, json=dashboard_data)
    if response.status_code == 201:
        return response.json().get("id")
    else:
        raise ValueError(f"Failed to create dashboard: {response.text}")

def get_dataset_id_by_name(dataset_name, headers):
    dataset_list_url = "http://superset:8088/api/v1/dataset/"
    response = requests.get(dataset_list_url, headers=headers)
    if response.status_code == 200:
        datasets = response.json().get("result")
        for dataset in datasets:
            if dataset["table_name"] == dataset_name:
                return dataset["id"]
    return None

def get_dashboard_id_by_name(dashboard_name, headers):
    dashboard_list_url = "http://superset:8088/api/v1/dashboard/"
    response = requests.get(dashboard_list_url, headers=headers)
    if response.status_code == 200:
        dashboards = response.json().get("result")
        for dashboard in dashboards:
            if dashboard["dashboard_title"] == dashboard_name:
                return dashboard["id"]
    return None

def create_number_chart(dataset_id, dashboard_id, session, headers, slice_name, label, aggregate, column_name, currency):
    csrf_token = get_csrf_token(session, headers)
    update_session_with_csrf_token(session, csrf_token)

    chart_url = "http://superset:8088/api/v1/chart/"
    chart_data = {
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "slice_name": slice_name,
        "viz_type": "big_number_total",
        "params": json.dumps({
            "adhoc_filters": [],
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "y_axis_format": "SMART_NUMBER",
            "currency_format": currency,
            "time_format": None,
            "python_date_format": None,
            "conditional_formatting": [],
            "extra_form_data": {},
            "dashboards": [dashboard_id],
            "metric": {
                "label": label,
                "expressionType": "SIMPLE",
                "aggregate": aggregate,
                "column": {
                    "column_name": column_name
                }
            }
        })
    }
    response = session.post(chart_url, headers=headers, json=chart_data)
    if response.status_code == 201:
        print("Chart created successfully.")
        return response.json().get("id")
    else:
        raise ValueError(f"Failed to create chart: {response.text}")

def create_pie_chart(dataset_id, dashboard_id, session, headers, slice_name, groupby, label, aggregate, column_name ):
    csrf_token = get_csrf_token(session, headers)
    update_session_with_csrf_token(session, csrf_token)

    chart_url = "http://superset:8088/api/v1/chart/"
    chart_data = {
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "slice_name": slice_name,
        "viz_type": "pie",
        "params": json.dumps({
            "adhoc_filters": [],
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,          
            "groupby": [groupby],
            "metric": {
                "label": label,
                "expressionType": "SIMPLE",
                "aggregate": aggregate,
                "column": {
                    "column_name": column_name
                }
            },
            "row_limit": 500,
            "time_format": "smart_date",
            "date_format": {},
            "conditional_formatting": [],
            "extra_form_data": {},
            "dashboards": [dashboard_id],
            "legend": True,
        })
    }
    response = session.post(chart_url, headers=headers, json=chart_data)
    if response.status_code == 201:
        print("Pie chart created successfully .")
        return response.json().get("id")
    else:
        raise ValueError(f"Failed to create pie chart: {response.text}")

def create_bar_chart(dataset_id, dashboard_id, session, headers, slice_name, x_axis, label, aggregate, column_name, time_grain, x_axis_title, y_axis_title, axis_title_margin, time_filter):
    csrf_token = get_csrf_token(session, headers)
    update_session_with_csrf_token(session, csrf_token)

    chart_url = "http://superset:8088/api/v1/chart/"
    chart_data = {
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "slice_name": slice_name,
        "viz_type": "echarts_timeseries_bar",
        "params": json.dumps({
            "adhoc_filters": [{"clause":"WHERE","comparator":f"{time_filter}","datasourceWarning":False,"expressionType":"SIMPLE","filterOptionName":"filter_uwi5zcpi44a_kxaqzewvms","isExtra":False,"isNew":False,"operator":"TEMPORAL_RANGE","sqlExpression":None,"subject":"tpep_pickup_datetime"}],
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "x_axis": x_axis,
            "time_format": "smart_date",
            "date_format": {},
            "conditional_formatting": [],
            "extra_form_data": {},
            "dashboards": [dashboard_id],
            "metrics": [{"label": label, "expressionType": "SIMPLE", "aggregate": aggregate, "column": {"column_name": column_name}}],
            "x_axis_title": x_axis_title,
            "y_axis_title": y_axis_title,
            "x_axis_title_margin": axis_title_margin,
            "y_axis_title_margin": axis_title_margin,
            "zoomable": True,
            "xAxisForceCategorical": True,
            "x_axis_sort":column_name,
            "x_axis_sort_asc": True,
            "show_value": True,
            "granularity_sqla": x_axis,
            "time_grain_sqla": time_grain
        })
    }
    response = session.post(chart_url, headers=headers, json=chart_data)
    if response.status_code == 201:
        print("Bar chart created successfully.")
        return response.json().get("id")
    else:
        raise ValueError(f"Failed to create bar chart: {response.text}")
    
def create_grouped_bar_chart(dataset_id, dashboard_id, session, headers, slice_name, x_axis, label, aggregate, column_name, x_axis_title, y_axis_title, axis_title_margin):
    csrf_token = get_csrf_token(session, headers)
    update_session_with_csrf_token(session, csrf_token)

    chart_url = "http://superset:8088/api/v1/chart/"
    chart_data = {
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "slice_name": slice_name,
        "viz_type": "echarts_timeseries_bar",
        "params": json.dumps({
            "adhoc_filters": [],
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "x_axis": x_axis,
            "time_format": "smart_date",
            "date_format": {},
            "conditional_formatting": [],
            "extra_form_data": {},
            "dashboards": [dashboard_id],
            "metrics": [{"label": label, "expressionType": "SIMPLE", "aggregate": aggregate, "column": {"column_name": column_name}}],
            "x_axis_title": x_axis_title,
            "y_axis_title": y_axis_title,
            "x_axis_title_margin": axis_title_margin,
            "y_axis_title_margin": axis_title_margin,
            "zoomable": True,
            "xAxisForceCategorical": True,
            "x_axis_sort":column_name,
            "x_axis_sort_asc": True,
            "show_value": True
        })
    }
    response = session.post(chart_url, headers=headers, json=chart_data)
    if response.status_code == 201:
        print("Bar chart created successfully.")
        return response.json().get("id")
    else:
        raise ValueError(f"Failed to create bar chart: {response.text}")

def extract_year_month(input_string):
    parts = input_string.split('_')
    if len(parts) >= 3:
        year_month = '_'.join(parts[-3:-1])
        year = parts[-3]
        month = parts[-2]
        if int(month) == 12:
            year_plus = str(int(year) + 1)
            month_plus = '01'
        else:
            year_plus = str(int(year))
            month_plus = '{:02d}'.format(int(month) + 1)
        return year_month, year, month, year_plus, month_plus
    else:
        return None

def main(file_name):
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    database_name = "PostgreSQLTaxi"
    sqlalchemy_uri = os.getenv('SUPERSET_DATABASE_URI') #"postgresql+psycopg2://airflow:airflow@postgres/taxi_data_processed"
    
    dataset_name = f"{file_name}_processed"
    print(f"Dataset Name: {dataset_name}")
    
    year_month_tuple = extract_year_month(dataset_name)

    year_month = year_month_tuple[0]
    year = year_month_tuple[1]
    month = year_month_tuple[2]
    year_plus = year_month_tuple[3]
    month_plus = year_month_tuple[4]

    session = requests.Session()
    session.headers.update(headers)

    exists, database_id = check_database_exists(database_name, headers)
    if exists:
        print("Database already exists.")
    else:
        database_id = create_database(database_name, sqlalchemy_uri, headers, session)
        print("Database created successfully.")
    
    print(f"Database ID: {database_id}")

    if not check_dataset_exists(dataset_name, headers):
        dataset_id = create_dataset(dataset_name, headers, session, database_id)
        print("Dataset created successfully.")
    else:
        print("Dataset already exists.")
        dataset_id = get_dataset_id_by_name(dataset_name, headers)

    print(f"Dataset ID: {dataset_id}")

    if not check_dashboard_exists(dataset_name, headers):
        dashboard_id = create_dashboard(dataset_name, headers, session)
        print("Dashboard created successfully.")
    else:
        print("Dashboard already exists.")
        dashboard_id = get_dashboard_id_by_name(dataset_name, headers)

    print(f"Dashboard ID: {dashboard_id}")

     # Create Number Charts
    create_number_chart(dataset_id, dashboard_id, session, headers, f"Total_Amount_{year_month}", "SUM(total_amount)", "SUM", "total_amount", {"symbol": "USD", "symbolPosition": "prefix"})
    create_number_chart(dataset_id, dashboard_id, session, headers, f"AVG_Total_Amount_{year_month}", "AVG(total_amount)", "AVG", "total_amount", {"symbol": "USD", "symbolPosition": "prefix"})
    create_number_chart(dataset_id, dashboard_id, session, headers, f"MAX_Total_Amount_{year_month}", "MAX(total_amount)", "MAX", "total_amount", {"symbol": "USD", "symbolPosition": "prefix"})
    create_number_chart(dataset_id, dashboard_id, session, headers, f"Total_Records_{year_month}", "COUNT(tpep_pickup_datetime)", "COUNT", "tpep_pickup_datetime", '')
    create_number_chart(dataset_id, dashboard_id, session, headers, f"Total_Trip_Distance_miles_{year_month}", "SUM(trip_distance)", "SUM", "trip_distance", '')
    create_number_chart(dataset_id, dashboard_id, session, headers, f"AVG_Trip_Distance_miles_{year_month}", "AVG(trip_distance)", "AVG", "trip_distance", '')
    
    # Create Pie Charts
    create_pie_chart(dataset_id, dashboard_id, session, headers, f"Pie_DO_Location_{year_month}", "DOLocationID", "COUNT(DOLocationID)", "COUNT", "DOLocationID" )
    create_pie_chart(dataset_id, dashboard_id, session, headers, f"Pie_PU_Location_{year_month}", "PULocationID", "COUNT(PULocationID)", "COUNT", "PULocationID" )
    
    # Create Bar Charts
    create_bar_chart(dataset_id, dashboard_id, session, headers, f"Total_PU_per_Hour_{year_month}", "tpep_pickup_datetime", "COUNT(tpep_pickup_datetime)", "COUNT", "tpep_pickup_datetime", "PT1H", "PU Time Per Hour", "Total PUs", 30, {"clause":"WHERE","comparator":f"{year}-{month}-01T00:00:00 : {year_plus}-{month_plus}-01T00:00:00","datasourceWarning":False,"expressionType":"SIMPLE","filterOptionName":"filter_uwi5zcpi44a_kxaqzewvms","isExtra":False,"isNew":False,"operator":"TEMPORAL_RANGE","sqlExpression":None,"subject":"tpep_pickup_datetime"})
    create_bar_chart(dataset_id, dashboard_id, session, headers, f"Total_DO_per_Hour_{year_month}", "tpep_dropoff_datetime", "COUNT(tpep_dropoff_datetime)", "COUNT", "tpep_dropoff_datetime", "PT1H", "DO Time Per Hour", "Total DOs", 30, {"clause":"WHERE","comparator":f"{year}-{month}-01T00:00:00 : {year_plus}-{month_plus}-01T00:00:00","datasourceWarning":False,"expressionType":"SIMPLE","filterOptionName":"filter_uwi5zcpi44a_kxaqzewvms","isExtra":False,"isNew":False,"operator":"TEMPORAL_RANGE","sqlExpression":None,"subject":"tpep_dropoff_datetime"})
    create_grouped_bar_chart(dataset_id, dashboard_id, session, headers, f"Payment_Types_{year_month}", "payment_type", "COUNT(payment_type)", "COUNT", "payment_type", "Payment Type (1. Credit Card; 2. Cash; 3. No Charge; 4. Dispute)", "Amount of Payments", 40)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: main.py {file_name}")
        sys.exit(-1)
    file_name = sys.argv[1]
    main(file_name)
