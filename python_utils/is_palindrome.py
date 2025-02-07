def is_palindrome(string):
    """
        Function to analyze if a string is a palindrome
    """

    string = string.lower()

    return string == string[::-1]
