from datetime import datetime

def parse_date(x: str) -> datetime: ...  # function

class ParseError(Exception):
    pass
