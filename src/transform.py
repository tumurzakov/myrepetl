def uppercase(value):
    if value is None:
        return None
    elif isinstance(value, str):
        return value.upper()
    else:
        return value