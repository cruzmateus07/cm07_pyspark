import zipfile


def unzip(zip_path: str, file_to_unzip: str, extract_path: str):
    """
        Funciton to unzip any file
    """
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extract(file_to_unzip, extract_path)
