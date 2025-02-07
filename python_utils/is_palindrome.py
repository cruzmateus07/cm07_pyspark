## Simple function to analyze if a string is a palindrome

def is_palindrome(string):
    
    string = string.lower()

    return string == string[::-1]
