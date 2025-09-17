def uppercase(value, row_data):
    if value is None:
        return None
    elif isinstance(value, str):
        return value.upper()
    else:
        return value