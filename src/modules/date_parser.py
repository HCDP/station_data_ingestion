
from datetime import datetime
import re

class DateParser:
    # XYYYY.MM.DD
    def __init__(self, date, period):
        if period == "day":
            self.datetime = datetime.strptime(date, "X%Y.%m.%d")
            self.isoString = self.datetime.strftime("%Y-%m-%d")
        elif period == "month":
            self.datetime = datetime.strptime(date, "X%Y.%m")
            self.isoString = self.datetime.strftime("%Y-%m")
        else:
            raise ValueError("Unknown period.")

    def getISOString(self):
        return self.isoString 

    def getDatetime(self):
        return self.datetime


def isoToDate(iso_string, period):
    date = ""
    if period == "day":
        date = datetime.strptime(iso_string, "%Y-%m-%d")
    elif period == "month":
        date = datetime.strptime(iso_string, "%Y-%m")
    else:
        raise ValueError("Unknown period.")

    return date


class DateParser2:
    def __init__(self, period):
        if period != "day" and period != "month":
            raise ValueError("Invalid period.")
        self.period = period
        self.header_regex = r"^X[0-9]{4}\.[0-9]{2}"
        self.header_format = "X%Y.%m"
        self.value_format = "%Y-%m"
        if period == "day":
            self.header_regex += r"\.[0-9]{2}"
            self.header_format += ".%d"
            self.value_format += "-%d"
        self.header_regex += r"$"
        
    
    def match(self, date: str):
        matches = False
        match = re.match(self.header_regex, date)
        if match is not None:
            matches = True
        return matches
    
    def header2date(self, date: str):
        dt = datetime.strptime(date, self.header_format)
        return dt
    
    def date2value(self, date: datetime):
        value = date.strftime(self.value_format)
        return value
        