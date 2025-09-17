def uppercase(value, row_data, source_table):
    if value is None:
        return None
    elif isinstance(value, str):
        return value.upper()
    else:
        return value