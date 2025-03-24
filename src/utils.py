from datetime import datetime, timedelta
import pandas as pd

def generate_date_dict(start_year, start_month, end_year, end_month):
    date_dict = {}
    current_date = datetime(start_year, start_month, 1)

    while current_date <= datetime(end_year, end_month, 1):
        key = current_date.strftime("%Y-%m")  # Format as "YEAR-Month"

        date_dict[key] = {
            "startDate": current_date.strftime("%Y-%m-%d"),
            "endDate": (current_date.replace(day=28)).strftime("%Y-%m-%d")
        }

        # Move to the next month
        next_month = current_date.month + 1
        next_year = current_date.year + (1 if next_month > 12 else 0)
        current_date = datetime(next_year, next_month if next_month <= 12 else 1, 1)

    return date_dict

