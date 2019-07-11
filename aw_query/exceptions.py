class QueryException(Exception):
    pass


class QueryFunctionException(QueryException):
    pass


class QueryParseException(QueryException):
    pass


class QueryInterpretException(QueryException):
    pass
