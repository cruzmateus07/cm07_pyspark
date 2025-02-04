import regex

## Function to compare given email to regexp pattern

def is_valid_email(email):

    pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'

    return regex.match(pattern, email) is not None
