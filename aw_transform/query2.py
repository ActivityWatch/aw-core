import logging
from typing import Union, List, Callable, Dict
from datetime import datetime, timedelta

from aw_core.models import Event
from aw_datastore import Datastore

from . import transforms

from .query2_functions import query2_functions

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
        if token[-1] != '"':
            # Unclosed string?
            raise QueryException("Failed to parse string")
        if len(token) < 2:
            raise
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
        logging.debug("Arguments for functioncall to {} is {}".format(self.name, call_args))
        result = query2_functions[self.name](*call_args)
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
            arg, args_str = _parse_token(args_str, namespace)
            comma = args_str.find(",")
            if comma != -1:
                args_str = args_str[comma+1:]
            args.append(arg[0].parse(arg[1], namespace))
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
    if type(string) != str:
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
    print(t)
    return (t, token), string

def create_namespace() -> dict:
    namespace = {
        "NAME": None,
        "CACHE": False,
        "RETURN": None,
        "TRUE": 1,
        "FALSE": 0,
        # start_time
        # end_time
    }
    return namespace

def parse(line, namespace):
    separator_i = line.find("=")
    var = line[:separator_i]
    val = line[separator_i+1:]
    if not val:
        # TODO: Proper message
        raise QueryException("Nothing to assign")
    var, rest = _parse_token(var, namespace)
    if var[0] is not Variable:
        # TODO: Proper message
        raise QueryException("Cannot assign to a non-variable")
    val, rest = _parse_token(val, namespace)
    # Parse token
    var = var[0].parse(var[1], namespace)
    val = val[0].parse(val[1], namespace)
    return val, var

def interpret(var, val, namespace, datastore):
    if type(var) is Variable:
        namespace[var.name] = val.interpret(datastore, namespace)
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
