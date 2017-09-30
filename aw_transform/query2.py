import logging
from typing import Union, List, Callable, Dict
from datetime import datetime, timedelta

from aw_core.models import Event
from aw_datastore import Datastore

from . import transforms

# FIXME: We'd want to use Datastore as a type annotations in several places within this file,
#        but import fails due to mutually-recursive imports

from .transforms import filter_period_intersect, filter_keyvals

def q2_query_bucket(datastore: Datastore, namespace: dict, bucketname: str):
    return datastore[bucketname].get()

def q2_filter_keyvals(datastore: Datastore, namespace: dict, events: list, key: str, vals: list, exclude: bool):
    # TODO: Implement
    pass

def q2_filter_period_intersect(datastore: Datastore, namespace: dict, events: list, filterevents: list):
    if type(events) != list:
        logging.debug(events)
        raise QueryException("1")
    if type(filterevents) != list:
        logging.debug(filterevents)
        raise QueryException("2")
    return filter_period_intersect(events, filterevents)

# TODO: Type checking
query2_functions = {
    "filter_period_intersect": q2_filter_period_intersect,
    "filter_keyvals": q2_filter_keyvals,
    "query_bucket": q2_query_bucket,
}

class QueryException(Exception):
    pass

class Token:
    # TODO: Proper token parsing instead of "verify"
    def verify(string: str, namespace: dict):
        raise NotImplementedError

class Integer(Token):
    def __init__(self, value):
        self.value = value

    def verify(string: str, namespace: dict):
        for char in string:
            if not char.isdigit():
                return False
        return Integer(int(string))

class Variable(Token):
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def verify(string: str, namespace: dict):
        for char in string:
            if char.isalpha():
                pass
            elif char == '_':
                pass
            else:
                return False
        val = None
        if string in namespace:
            val = namespace[string]
        return Variable(string, val)

class String(Token):
    def __init__(self, value):
        self.value = value

    def verify(string: str, namespace: dict):
        if len(string) < 2:
            return None
        if string[0] != '"':
            return None
        if string[-1] != '"':
            return None
        string_content = string[1:-1]
        for char in string_content:
            if char == '"':
                return None
        return String(string_content)

class Function(Token):
    def __init__(self, name, arguments):
        self.name = name
        self.arguments = arguments

    def execute(self, namespace, datastore):
        # FIXME: Comma in strings will break stuff
        args = self.arguments.split(',')
        if args == None:
            args = []
        parsed_args = [datastore, namespace]
        for arg in args:
            arg = _parse_token(arg, namespace)
            if type(arg) is Function:
                raise QueryException("Having a function as a arg is not supported")
            elif type(arg) is Variable or type(arg) is Integer or type(arg) is String:
                parsed_args.append(arg.value)
            else:
                raise QueryException("Reached unreachable, please contact a developer")
        logging.debug("Arguments for functioncall to {} is {}".format(self.name, parsed_args))
        if not self.name in query2_functions:
            raise QueryException("There's no function named {}".format(self.name))
        result = query2_functions[self.name](*parsed_args)
        return result


    def verify(string: str, namespace: dict):
        i = 0
        # Parse name until opening bracket
        for char in string:
            if char == '(':
                break
            i = i+1
        name = string[:i]
        if string[-1] != ')':
            return None
        arg_str = string[i+1:-1]
        return Function(name, arg_str)

def _parse_token(string: str, namespace: dict):
    # TODO: The whole parsing thing is shoddily written, needs a rewrite from ground-up
    if type(string) != str:
        raise QueryException("Reached unreachable, cannot parse something that isn't a string")
    if len(string) == 0:
        return None
    string = string.strip()
    types = [String, Integer, Variable, Function]
    token = None
    for t in types:
        token = t.verify(string, namespace)
        if token:
            break
    if not token:
        raise QueryException("Unable to parse token: {}".format(string))
    return token

def create_namespace() -> dict:
    namespace = {
        "NAME": None,
        "CACHE": False,
        "RETURN": None,
        "True": 1,
        "False": 0,
        "fetch_bucket": None, #TODO: Implement me
        # start_time
        # end_time
    }
    return namespace

def parse(line, namespace):
    var = line.split("=")[0]
    val = "".join(line.split("=")[1:])
    if not val:
        # TODO: Proper message
        raise QueryException("Nothing to assign")
    var = _parse_token(var, namespace)
    if type(var) is not Variable:
        # TODO: Proper message
        raise QueryException("Cannot assign to a non-variable")
    val = _parse_token(val, namespace)
    return val, var

def interpret(var, val, namespace, datastore):
    if type(var) is Variable:
        if type(val) is Integer or type(val) is String or type(val) is Variable:
            namespace[var.name] = val.value
        if type(val) is Function:
            namespace[var.name] = val.execute(namespace, datastore)
        logging.debug("Set {} to {}".format(var.name, namespace[var.name]))
    else:
        # TODO: Proper exception messages
        raise QueryException("asd")

def get_return(namespace):
    return namespace["RETURN"]

def query(query: str, datastore: Datastore) -> None:
    namespace = create_namespace()
    query = query.split("\n")
    for line in query:
        line = line.strip()
        if line:
            logging.debug("Parsing: "+line)
            val, var = parse(line, namespace)
            interpret(var, val, namespace, datastore)
    return get_return(namespace)
