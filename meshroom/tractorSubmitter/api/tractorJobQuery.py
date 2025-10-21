#!/usr/bin/env python

from tractorSubmitter.api.base import TractorJob


def getQuery():
    try:
        # tractorLoginManager is a private python package
        # that handles authentification to Tractor
        # If not available, we offer a fallback method where you need to provide
        # credentials so that Meshroom can connect to tractor
        from tractorLoginManager import TractorLoginManager
        tlm = TractorLoginManager()
        return tlm.start_query()
    except ImportError:
        from submitterCredentialUi import getCredentials
        print("No TractorLoginManager found. Getting credentials...")
        credentials = getCredentials()
        if credentials:
            print(f"Username: {credentials['username']}")
            print(f"Password: {'*' * len(credentials['password'])}")
        else:
            print("Dialog cancelled")
        # TODO
        raise NotImplementedError
        return None
