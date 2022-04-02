
from datetime import datetime

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