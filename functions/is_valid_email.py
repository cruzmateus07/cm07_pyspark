import regex

## Function to compare given email to regexp pattern

def is_valid_email(email):

    pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'

    return regex.match(pattern, email) is not None


## Function to compare list of emails to regexp pattern

def is_valid_email_list(emails: list):

    pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'

    invalid_email = []
    is_list_valid = True

    for email in emails:
        
        if regex.match(pattern, email) is not None:
            invalid_emails = [email].append()
            is_list_valid = False
    
    invalid_emails_qty = invalid_emails.count()

    return is_list_valid, invalid_emails, invalid_emails_qty
