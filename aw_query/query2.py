import logging
from typing import Union, List, Dict, Sequence, Callable, Type, Any, Tuple
from datetime import datetime

from aw_core.models import Event
from aw_datastore import Datastore

from .exceptions import QueryException, QueryParseException, QueryInterpretException
from .functions import functions

logger = logging.getLogger(__name__)


class QToken:
    def interpret(self, datastore: Datastore, namespace: dict):
        raise NotImplementedError

    @staticmethod
    def parse(string: str, namespace: dict):
        raise NotImplementedError

    @staticmethod
    def check(string: str) -> Tuple[str, str]:
        raise NotImplementedError


class QInteger(QToken):
    def __init__(self, value) -> None:
        self.value = value

    def interpret(self, datastore: Datastore, namespace: dict):
        return self.value

    @staticmethod
    def parse(string: str, namespace: dict = {}) -> QToken:
        return QInteger(int(string))

    @staticmethod
    def check(string: str):
        token = ""
        for char in string:
            if char.isdigit():
                token += char
            else:
                break
        return token, string[len(token) :]


class QVariable(QToken):
    def __init__(self, name, value) -> None:
        self.name = name
        self.value = value

    def interpret(self, datastore: Datastore, namespace: dict):
        if self.name not in namespace:
            raise QueryInterpretException(
                "Tried to reference variable '{}' which is not defined".format(
                    self.name
                )
            )
        namespace[self.name] = self.value
        return self.value

    @staticmethod
    def parse(string: str, namespace: dict) -> QToken:
        val = None
        if string in namespace:
            val = namespace[string]
        return QVariable(string, val)

    @staticmethod
    def check(string: str):
        token = ""
        for i, char in enumerate(string):
            if char.isalpha() or char == "_":
                token += char
            elif i != 0 and char.isdigit():
                token += char
            else:
                break
        return token, string[len(token) :]


class QString(QToken):
    def __init__(self, value):
        self.value = value

    def interpret(self, datastore: Datastore, namespace: dict):
        return self.value

    @staticmethod
    def parse(string: str, namespace: dict = {}) -> QToken:
        quotes_type = string[0]
        string = string.replace("\\" + quotes_type, quotes_type)
        string = string[1:-1]
        return QString(string)

    @staticmethod
    def check(string: str):
        token = ""
        quotes_type = string[0]
        if quotes_type != '"' and quotes_type != "'":
            return token, string
        token += quotes_type
        prev_char = None
        for char in string[1:]:
            token += char
            if (
                char == quotes_type and prev_char != "\\"
            ):  # escape quote_type with backslash
                break
            prev_char = char
        if token[-1] != quotes_type or len(token) < 2:
            # Unclosed string?
            raise QueryParseException("Failed to parse string")
        return token, string[len(token) :]


class QFunction(QToken):
    def __init__(self, name, args):
        self.name = name
        self.args = args

    def interpret(self, datastore: Datastore, namespace: dict):
        if self.name not in functions:
            raise QueryInterpretException(
                "Tried to call function '{}' which doesn't exist".format(self.name)
            )
        call_args = [datastore, namespace]
        for arg in self.args:
            call_args.append(arg.interpret(datastore, namespace))
        # logger.debug("Arguments for functioncall to {} is {}".format(self.name, call_args))
        try:
            result = functions[self.name](*call_args)  # type: ignore
        except TypeError:
            raise QueryInterpretException(
                "Tried to call function {} with invalid amount of arguments".format(
                    self.name
                )
            )
        return result

    @staticmethod
    def parse(string: str, namespace: dict) -> QToken:
        arg_start = 0
        arg_end = len(string) - 1
        # Find opening bracket
        for char in string:
            if char == "(":
                break
            arg_start = arg_start + 1
        # Parse name
        name = string[:arg_start]
        # Parse arguments
        args = []
        args_str = string[arg_start + 1 : arg_end]
        while args_str:
            (arg_t, arg), args_str = _parse_token(args_str, namespace)
            comma = args_str.find(",")
            if comma != -1:
                args_str = args_str[comma + 1 :]
            args.append(arg_t.parse(arg, namespace))
        return QFunction(name, args)

    @staticmethod
    def check(string: str):
        i = 0
        # Find opening bracket
        found = False
        for char in string:
            if char.isalpha() or char == "_":
                i = i + 1
            elif i != 0 and char.isdigit():
                i = i + 1
            elif char == "(":
                i = i + 1
                found = True
                break
            else:
                break
        if not found:
            return None, string
        to_consume = 1
        single_quote = False
        double_quote = False
        prev_char = None
        for char in string[i:]:
            i = i + 1
            if char == "'" and prev_char != "\\" and not double_quote:
                single_quote = not single_quote
            elif char == '"' and prev_char != "\\" and not single_quote:
                double_quote = not double_quote
            elif single_quote or double_quote:
                pass
            elif i != 0 and char.isdigit():
                pass
            elif char == "(":
                to_consume += 1
            elif char == ")":
                to_consume -= 1
            if to_consume == 0:
                break
            prev_char = char
        if to_consume != 0:
            return None, string
        return string[:i], string[i + 1 :]


class QDict(QToken):
    def __init__(self, value: dict) -> None:
        self.value = value

    def interpret(self, datastore: Datastore, namespace: dict):
        expanded_dict = {}
        for key, value in self.value.items():
            expanded_dict[key] = value.interpret(datastore, namespace)
        return expanded_dict

    @staticmethod
    def parse(string: str, namespace: dict) -> QToken:
        entries_str = string[1:-1]
        d: Dict[str, QToken] = {}
        while len(entries_str) > 0:
            entries_str = entries_str.strip()
            if len(d) > 0 and entries_str[0] == ",":
                entries_str = entries_str[1:]
            # parse key
            (key_t, key_str), entries_str = _parse_token(entries_str, namespace)
            if key_t != QString:
                raise QueryParseException("Key in dict is not a str")
            key = QString.parse(key_str).value  # type: ignore
            entries_str = entries_str.strip()
            # Remove :
            if entries_str[0] != ":":
                raise QueryParseException("Key in dict is not followed by a :")
            entries_str = entries_str[1:]
            # parse val
            (val_t, val_str), entries_str = _parse_token(entries_str, namespace)
            if not val_t:
                raise QueryParseException("Dict expected a value, got nothing")
            val = val_t.parse(val_str, namespace)
            # set
            d[key] = val
        return QDict(d)

    @staticmethod
    def check(string: str):
        if string[0] != "{":
            return None, string
        # Find closing bracket
        i = 1
        to_consume = 1
        single_quote = False
        double_quote = False
        prev_char = None
        for char in string[i:]:
            i += 1
            if char == "'" and prev_char != "\\" and not double_quote:
                single_quote = not single_quote
            elif char == '"' and prev_char != "\\" and not single_quote:
                double_quote = not double_quote
            elif single_quote or double_quote:
                pass
            elif char == "}":
                to_consume = to_consume - 1
            elif char == "{":
                to_consume = to_consume + 1
            if to_consume == 0:
                break
            prev_char = char
        return string[:i], string[i + 1 :]


class QList(QToken):
    def __init__(self, value: list) -> None:
        self.value = value

    def interpret(self, datastore: Datastore, namespace: dict):
        expanded_list = []
        for value in self.value:
            expanded_list.append(value.interpret(datastore, namespace))
        return expanded_list

    @staticmethod
    def parse(string: str, namespace: dict) -> QToken:
        entries_str = string[1:-1]
        l: List[QToken] = []
        while len(entries_str) > 0:
            entries_str = entries_str.strip()
            if len(l) > 0 and entries_str[0] == ",":
                entries_str = entries_str[1:]
            # parse
            (val_t, val_str), entries_str = _parse_token(entries_str, namespace)
            if not val_t:
                raise QueryParseException("List expected a value, got nothing")
            val = val_t.parse(val_str, namespace)
            # set
            l.append(val)
        return QList(l)

    @staticmethod
    def check(string: str):
        if string[0] != "[":
            return None, string
        # Find closing bracket
        i = 1
        to_consume = 1
        single_quote = False
        double_quote = False
        prev_char = None
        for char in string[i:]:
            i += 1
            if char == "'" and prev_char != "\\" and not double_quote:
                single_quote = not single_quote
            elif char == '"' and prev_char != "\\" and not single_quote:
                double_quote = not double_quote
            elif double_quote or single_quote:
                pass
            elif char == "]":
                to_consume = to_consume - 1
            elif char == "[":
                to_consume = to_consume + 1
            if to_consume == 0:
                break
            prev_char = char
        return string[:i], string[i + 1 :]


qtypes: Sequence[Type[QToken]] = [QString, QInteger, QFunction, QDict, QList, QVariable]


def _parse_token(string: str, namespace: dict) -> Tuple[Tuple[Any, str], str]:
    # TODO: The whole parsing thing is shoddily written, needs a rewrite from ground-up
    if not isinstance(string, str):
        raise QueryParseException(
            "Reached unreachable, cannot parse something that isn't a string"
        )
    if len(string) == 0:
        return (None, ""), string
    string = string.strip()
    token = None
    t = None  # Declare so we can return it
    for t in qtypes:
        token, string = t.check(string)
        if token:
            break
    if not token:
        raise QueryParseException("Syntax error: {}".format(string))
    return (t, token), string


def create_namespace() -> dict:
    namespace = {
        "True": True,
        "False": False,
        "true": True,
        "false": False,
    }
    return namespace


def parse(line, namespace):
    separator_i = line.find("=")
    var_str = line[:separator_i]
    val_str = line[separator_i + 1 :]
    if not val_str:
        # TODO: Proper message
        raise QueryParseException("Nothing to assign")
    (var_t, var), var_str = _parse_token(var_str, namespace)
    var_str = var_str.strip()
    if var_str:  # Didn't consume whole var string
        raise QueryParseException("Invalid syntax for assignment variable")
    if var_t is not QVariable:
        raise QueryParseException("Cannot assign to a non-variable")
    (val_t, val), var_str = _parse_token(val_str, namespace)
    if var_str:  # Didn't consume whole val string
        raise QueryParseException("Invalid syntax for value to assign")
    # Parse token
    var = var_t.parse(var, namespace)
    val = val_t.parse(val, namespace)
    return var, val


def interpret(var, val, namespace, datastore):
    namespace[var.name] = val.interpret(datastore, namespace)
    # logger.debug("Set {} to {}".format(var.name, namespace[var.name]))


def get_return(namespace):
    if "RETURN" not in namespace:
        raise QueryParseException(
            "Query doesn't assign the RETURN variable, nothing to respond"
        )
    return namespace["RETURN"]


def query(
    name: str, query: str, starttime: datetime, endtime: datetime, datastore: Datastore
) -> None:
    namespace = create_namespace()
    namespace["NAME"] = name
    namespace["STARTTIME"] = starttime.isoformat()
    namespace["ENDTIME"] = endtime.isoformat()

    query_stmts = query.split(";")
    for statement in query_stmts:
        statement = statement.strip()
        if statement:
            logger.debug("Parsing: " + statement)
            var, val = parse(statement, namespace)
            interpret(var, val, namespace, datastore)

    result = get_return(namespace)
    return result
