import json


class WrongCursorsFormatException(Exception):
    pass


class RequiredParameterNotFoundException(Exception):
    def __init__(self, parameter):
        self.parameter = parameter


class NotIntegerParameterException(Exception):
    def __init__(self, parameter):
        self.parameter = parameter


def get_int_parameter(parameter_name, request, required = False, default_value = None):
    parameter = request.args.get(parameter_name)

    if parameter is None:
        if required:
            raise RequiredParameterNotFoundException(parameter_name)
        else:
            return default_value

    if not parameter.isdigit():
        raise NotIntegerParameterException(parameter_name)
    else:
        return int(parameter)


def get_cursors(cursors_str):
    try:
        cursors = json.loads(cursors_str)
    except:
        raise WrongCursorsFormatException()
    try:
        for cursor in cursors:
            if not validate_cursor(cursor):
                raise WrongCursorsFormatException()
    except TypeError:
        raise WrongCursorsFormatException
    return cursors


def validate_cursor(cursor):
    return 'partition' in cursor and \
        'offset' in cursor and \
        cursor['partition'].isdigit() and \
        cursor['offset'].isdigit()
