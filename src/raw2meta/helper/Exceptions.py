class HandlingCorruptFileError(Exception):
    "raised when file is not readable with .NET functions"
    pass


class HandlingEmptyFileError(Exception):
    "raised when file does not contain any scans"
    pass


class NoFittingProjectFound(Exception):
    "raised when a json could not be inserted into db because there was not fitting project ID"
    pass

class SafedAsJsonTempFile(Exception):
    "not an error, but message to avoid that it looks like the file has been written into db when it is only stored as Json"
    pass
