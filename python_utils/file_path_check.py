import os


def file_path_check(file_path: str) -> None:
    """
        Verifies if the file path provided does exists
        Raise a FileNotFoundError if not and it to the log file
    """

    fp_check = os.path.exists(file_path)

    if not fp_check:
        raise FileNotFoundError("The dataset does not exists")
