from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


def gauth():
    # GoogleAuth() returns a class that contains Google authentication and authorization related properties and methods using OAuth 2.0
    gauth = GoogleAuth()

    # Try to load saved client credentials (if available)
    gauth.LoadCredentialsFile("./credential_module.json")

    if gauth.credentials is None:
        try:
            gauth.LocalWebserverAuth()
        except (gauth.AuthenticationRejected, gauth.AuthenticationError):
            raise Exception("Authentication failed")
    elif gauth.access_token_expired:
        gauth.Refresh()
        gauth.SaveCredentialsFile("./credential_module.json")
    else:
        gauth.Authorize()

    # GoogleDrive() returns a class with methods to interact with the drive associated to the provided credentials
    return GoogleDrive(gauth)
