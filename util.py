from datetime import datetime, date

def json_serial(obj):
    """
    https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable
    JSON serializer for objects not serializable by default json code
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    raise TypeError ("Type %s not serializable" % type(obj))
