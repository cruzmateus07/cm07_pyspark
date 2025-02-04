def is_palindrome(string):
    
    string = string.lower()

    return string == string[::-1]
