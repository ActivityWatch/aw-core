import logging
from typing import Union, List, Callable, Dict
from datetime import datetime, timedelta, timezone
from copy import deepcopy
import iso8601

from aw_core.models import Event
from aw_datastore import Datastore

from . import transforms

from .query2_functions import query2_functions
from .cached_queries import cache_query, get_cached_query

logger = logging.getLogger(__name__)

class QueryException(Exception):
    pass

class Token:
    def interpret(self, datastore: Datastore, namespace: dict):
        raise NotImplementedError

    def parse(string: str, namespace: dict):
        raise NotImplementedError

    def check(string: str):
        raise NotImplementedError

class Integer(Token):
    def __init__(self, value):
        self.value = value

    def interpret(self, datastore: Datastore, namespace: dict):
        return self.value

    def parse(string: str, namespace: dict):
        return Integer(int(string))

    def check(string: str):
        token = ""
        for char in string:
            if char.isdigit():
                token += char
            else:
                break
        return token, string[len(token):]

class Variable(Token):
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def interpret(self, datastore: Datastore, namespace: dict):
        namespace[self.name] = self.value
        return self.value

    def parse(string: str, namespace: dict):
        val = None
        if string in namespace:
            val = namespace[string]
        return Variable(string, val)

    def check(string: str):
        token = ""
        for i, char in enumerate(string):
            if char.isalpha() or char == '_':
                token += char
            elif i != 0 and char.isdigit():
                token += char
            else:
                break
        return token, string[len(token):]

class String(Token):
    def __init__(self, value):
        self.value = value

    def interpret(self, datastore: Datastore, namespace: dict):
        return self.value

    def parse(string: str, namespace: dict):
        string = string[1:-1]
        return String(string)

    def check(string: str):
        token = ""
        if string[0] != '"':
            return token, string
        token += '"'
        for char in string[1:]:
            token += char
            if char == '"':
                break
        if token[-1] != '"' or len(token) < 2:
            # Unclosed string?
            raise QueryException("Failed to parse string")
        return token, string[len(token):]

class Function(Token):
    def __init__(self, name, args):
        self.name = name
        self.args = args

    def interpret(self, datastore: Datastore, namespace: dict):
        if not self.name in query2_functions:
            raise QueryException("There's no function named {}".format(self.name))
        call_args = [datastore, namespace]
        for arg in self.args:
            call_args.append(arg.interpret(datastore, namespace))
        logger.debug("Arguments for functioncall to {} is {}".format(self.name, call_args))
        if self.name not in query2_functions:
            raise QueryException("Tried to call function '{}' which doesn't exist".format(self.name))
        try:
            result = query2_functions[self.name](*call_args)
        except TypeError:
            raise QueryException("Tried to call function {} with invalid amount of arguments".format(self.name))
        return result

    def parse(string: str, namespace: dict):
        arg_start = 0
        arg_end = len(string)
        # Find opening bracket
        for char in string:
            if char == '(':
                break
            arg_start = arg_start + 1
        for char in string[::-1]:
            if char == ')':
                break
            elif char != ' ':
                raise QueryException("asd")
            arg_end = arg_end - 1
        # Parse name
        name = string[:arg_start]
        # Parse arguments
        args = []
        args_str = string[arg_start+1:arg_end-1]
        while args_str:
            (arg_t, arg), args_str = _parse_token(args_str, namespace)
            comma = args_str.find(",")
            if comma != -1:
                args_str = args_str[comma+1:]
            args.append(arg_t.parse(arg, namespace))
        return Function(name, args)


    def check(string: str):
        i = 0
        # Find opening bracket
        found = False
        for char in string:
            if char.isalpha() or char == "_":
                i = i+1
            elif i != 0 and char.isdigit():
                i = i+1
            elif char == '(':
                i = i+1
                found = True
                break
            else:
                break
        if not found:
            return None, string
        to_consume = 1
        for char in string[i:]:
            i += 1
            if char == ')':
                to_consume = to_consume - 1
            elif char == '(':
                to_consume = to_consume + 1
            if to_consume == 0:
                break
        if to_consume != 0:
            raise QueryException("Unclosed function")
        return string[:i], string[i+1:]

def _parse_token(string: str, namespace: dict):
    # TODO: The whole parsing thing is shoddily written, needs a rewrite from ground-up
    if not isinstance(string, str):
        raise QueryException("Reached unreachable, cannot parse something that isn't a string")
    if len(string) == 0:
        return None
    string = string.strip()
    types = [String, Integer, Function, Variable]
    token = None
    t = None # Declare so we can return it
    for t in types:
        token, string = t.check(string)
        if token:
            break
    if not token:
        raise QueryException("Syntax error: {}".format(string))
    return (t, token), string

def create_namespace() -> dict:
    namespace = {
        "CACHE": False,
        "RETURN": None,
        "TRUE": 1,
        "FALSE": 0,
    }
    return namespace

def parse(line, namespace):
    separator_i = line.find("=")
    var_str = line[:separator_i]
    val_str = line[separator_i+1:]
    if not val_str:
        # TODO: Proper message
        raise QueryException("Nothing to assign")
    (var_t, var), var_str = _parse_token(var_str, namespace)
    var_str = var_str.strip()
    if var_str: # Didn't consume whole var string
        raise QueryException("Invalid syntax for assignment variable")
    if var_t is not Variable:
        raise QueryException("Cannot assign to a non-variable")
    (val_t, val), var_str = _parse_token(val_str, namespace)
    if var_str: # Didn't consume whole val string
        raise QueryException("Invalid syntax for value to assign")
    # Parse token
    var = var_t.parse(var, namespace)
    val = val_t.parse(val, namespace)
    return var, val

def interpret(var, val, namespace, datastore):
    if not isinstance(var, Variable):
        raise QueryException("Cannot assign to something that isn't an variable!")
    namespace[var.name] = val.interpret(datastore, namespace)
    logger.debug("Set {} to {}".format(var.name, namespace[var.name]))

def get_return(namespace):
    if "RETURN" not in namespace:
        raise QueryException("Query doesn't assign the RETURN variable, nothing to respond")
    return namespace["RETURN"]

def parse_metadata(query: str):
    namespace = create_namespace()
    query = query.split("\n")
    for line in query:
        line = line.strip()
        if line:
            logger.debug("Parsing: "+line)
            var, val = parse(line, namespace)
            if not isinstance(var, Variable):
                raise QueryException("Cannot assign to something that isn't a variable")
            if var.name.isupper() and var.name != "RETURN":
                interpret(var, val, namespace, None)

    if "NAME" not in namespace:
        raise QueryException("Query needs a NAME")
    if not isinstance(namespace["NAME"], str):
        raise QueryException("NAME is not of type string")
    if "STARTTIME" not in namespace:
        raise QueryException("Query needs a STARTTIME")
    if not isinstance(namespace["STARTTIME"], str):
        raise QueryException("STARTTIME is not of type string")
    if "ENDTIME" not in namespace:
        raise QueryException("Query needs a ENDTIME")
    if not isinstance(namespace["ENDTIME"], str):
        raise QueryException("ENDTIME is not of type string")

    namespace["STARTTIME"] = iso8601.parse_date(namespace["STARTTIME"])
    namespace["ENDTIME"] = iso8601.parse_date(namespace["ENDTIME"])

    return namespace

def query(query: str, datastore: Datastore) -> None:
    meta = parse_metadata(query)
    if meta["CACHE"]:
        cached_result = get_cached_query(meta["NAME"], datastore, meta["STARTTIME"], meta["ENDTIME"])
        if cached_result:
            return cached_result

    namespace = create_namespace()
    query = query.split("\n")
    for line in query:
        line = line.strip()
        if line:
            logger.debug("Parsing: "+line)
            var, val = parse(line, namespace)
            interpret(var, val, namespace, datastore)

    result = get_return(namespace)
    if isinstance(result, list):
        result = [Event(**e) for e in result]
    # Cache result
    if meta["CACHE"]:
        if meta["ENDTIME"] < datetime.now(timezone.utc):
            cache_query(deepcopy(result), meta["NAME"], datastore, meta["STARTTIME"], meta["ENDTIME"])
    return result
