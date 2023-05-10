#!/usr/bin/env python

import argparse
import getpass
import multiprocessing
import requests
import json
import os
import datetime
from builtins import input


###########################################
#                                         #
#           Global Variables              #
#                                         #
###########################################

# Global variable for the username to be used for the API login
username = ""
# Global variable for the password to be used for the API login
password = ""
# Global variable for the hostname of the service to be used
hostname = "https://registeredpreventad.loris.ca"
# Global variable for the API version to be used
api_version = "/api/v0.0.3-dev"
# Global variable for the storage location to be used
base_directory = ""
# Global variable for the imaging standard to be used
imaging_format = ""
# Global variable for the JSON Web token to be used for the API requests
token = ""
# Global variable for the list of specified candidates
specified_candidates = ""
# Global variable for the verbosity of the script
verbosity = False
# Global variable for the PREVENT-AD logo
preventad_logo = """                                                                                                                                                                                                                                                  
  _____  _____  ________      ________ _   _ _______            _____     
 |  __ \|  __ \|  ____\ \    / |  ____| \ | |__   __|     /\   |  __ \    
 | |__) | |__) | |__   \ \  / /| |__  |  \| |  | |______ /  \  | |  | |   
 |  ___/|  _  /|  __|   \ \/ / |  __| | . ` |  | |______/ /\ \ | |  | |   
 | |    | | \ \| |____   \  /  | |____| |\  |  | |     / ____ \| |__| |   
 |_____ |_|  \_|______|   \/   |______|_| \_| _____   /_/    \_|_____/    
 |  __ \                            | |      / ____|                      
 | |__) |___ ___  ___  __ _ _ __ ___| |__   | |  __ _ __ ___  _   _ _ __  
 |  _  // _ / __|/ _ \/ _` | '__/ __| '_ \  | | |_ | '__/ _ \| | | | '_ \ 
 | | \ |  __\__ |  __| (_| | | | (__| | | | | |__| | | | (_) | |_| | |_) |
 |_|  \_\___|___/\___|\__,_|_|  \___|_| |_|  \_____|_|  \___/ \__,_| .__/ 
                                                                   | |    
                                                                   |_|"""


def initialize_jwt(username, password, hostname, api_version):
    # type: (str, str, str, str) -> bool
    """

    Function which takes the appropriate credentials, the specific domain and the API version and uses a POST
    request through the Requests module to obtain a JSON web token to be used as identification for the other
    requests of the download script.

    :param username:        str:    username to be used for authentication with the host (i.e. john@email.com)
    :param password:        str:    password linked to the username used for the account
    :param hostname:        str:    address of the host to be queried through the API
                                    (i.e. https://registeredpreventad.loris.ca)
                                    Note that the https:// prefix should be included and that there shouldn't be a
                                    trailing slash
    :param api_version:     str:    version of the API to be used for the requests (i.e. /api/v0.0.3-dev)
                                    Note that there shouldn't be a trailing slash
    :return:                bool:   Status of the completion of request. The token itself is passed to the global
                                    variable of the same name
    """
    # Global variable to which the token obtained should be passed
    global token
    # Global variable for the verbosity of the script
    global verbosity

    # Try block to catch possible connection errors from the server
    try:
        # Crafting and sending a POST using the credentials, API version and hostname provided
        # Note the usage of the verify parameter which means the TLS certificate of the server
        # will be checked and the JSON made to respect the expected parameters of the API request
        response = requests.post(
                                    url=hostname + api_version + '/login',
                                    json={'username': username,
                                          'password': password},
                                    verify=True
                                 )
    # Catching timeout errors if the requests times out
    except requests.ConnectTimeout as TimeoutError:
        print("The login request to the server produced a timeout error. Check your connection and try again.")
        # If verbose, provide the error message
        if verbosity:
            print(TimeoutError)
        return False
    # Catching connection errors if the server closes the connection or some other issue comes up
    except requests.ConnectionError as ConnectionError:
        print("The login request to the server produced a connection error. Check your connection and try again.")
        # If verbose, provide the error message
        if verbosity:
            print(ConnectionError)
        return False

    # Any response other than a code 200 means the token was not obtained. Since both error code categories,
    # (400s and 500s) are valid, as opposed to empty, responses, they're not caught in the try block but should
    # still exit the function. The most likely problem for a non 200 status code is bad credentials
    if response.status_code != 200:
        print("The login request to the server returned an error. Please check your credentials.")
        return False
    else:
        # Extracting the content of the response, decoding it to ascii, parsing it as a JSON then extracting
        # the value of the property 'token'. The resulting value is assigned to the global variable of the same
        # name
        token = json.loads(response.content.decode('ascii'))['token']

    # Validation message
    print("Login successful. Token obtained.")
    # Returning the status of the completion of the login request
    return True


def get_json(api_request, session_object):
    # type: (str, requests.Session) -> dict
    """

    Function which takes an API GET request, parses the response through the JSON module and returns a dictionary
    of that value.

    :param api_request:     str:                    string of the GET request to be sent to the API
    :param session_object:  requests.Session:       handle of the session object to be used to launch the GET request
    :return:                dict:                   dictionary of the JSON parsing of the response of the server
    """
    # Global variable containing the token obtained through the initialize_jwt() function
    global token
    # Global variable containing the hostname of the server (i.e. https://registeredpreventad.loris.ca)
    global hostname
    # Global variable containing the API's version to be used to process the request (i.e. /api/v0.0.3-dev)
    global api_version
    # Global variable containing the verbosity of the script
    global verbosity

    # Processing the API GET request
    server_response = process_api_get(hostname + api_version + api_request,
                                      {},
                                      api_request,
                                      [200],
                                      session_object)

    # If the script is verbose
    if verbosity:
        # Print the success message for the request
        print("Success: " + api_request)
    # Returning the JSON of the response
    return json.loads(server_response.content.decode('ascii'))


def get_file(api_request, download_location, filename, etag_checking, session_object):
    # type: (str, str, str, bool, requests.Session) -> str
    """

    Download function which takes an API request to be passed through GET, saves the file to the download location, if
    necessary, and returns a value representing whether the file was already present. The downloaded status of
    the file is determined through a comparison of the ETag value contained in a file of the same name as the requested
    file but with a period prefix and an etag suffix (.filename.etag).

    :param api_request:         str:                API request to be passed through GET
    :param download_location:   str:                download location of the file
    :param filename:            str:                filename of the file
    :param etag_checking:       bool:               should the file be checked for download status or just downloaded
    :param session_object:      requests.Session:   handle of the session object to be used to launch the GET request
    :return:                    str:                returns "New" or "Current" depending on the status of the
                                                    file requested
    """
    # Global variable containing the token to be used for authentication for the API requests
    global token
    # Global variable containing the hostname of the server hosting the API
    global hostname
    # Global variable containing the version of the API to be used to process the request
    global api_version
    # Global variable for the verbosity of the script
    global verbosity

    # Local variable holding the server's response
    server_response = ""

    # If checking the ETag and a file of the appropriate name exists
    if etag_checking and os.path.isfile(download_location + "." + filename + ".etag"):
        # Open the file as read, load the value in the variable etag and close the file
        with open(download_location + "." + filename + ".etag", "r") as etag_file:
            etag = etag_file.read()

        # Launching the API request through the appropriate function
        # Note the usage of the second header parameter for ETag checking and that both 200
        # (mismatched ETag and existing file) and 304 (matching ETag and existing file) can
        # be considered as valid responses.
        server_response = process_api_get(hostname + api_version + api_request,
                                          {'If-None-Match': etag},
                                          api_request,
                                          [200, 304],
                                          session_object
                                          )

        # The 304 status code means the cache is current, which is the response if the ETag value provided matches
        # the ETag value on the server sie.
        if server_response.status_code == 304:
            # Given the matching ETag value, the file is assumed to be downloaded.
            if verbosity:
                # Printing the successful download message
                print("File: %s was already downloaded" % filename)
            # Returning the value that the file is downloaded and current
            return "Current"
        # For a 200 status code, it means the ETag didn't match, but we successfully downloaded the file
        # requested
        else:
            # Removing the ETag file
            os.remove(download_location + "." + filename + ".etag")

    # Launch the query if there's no ETag checking or no ETag file while keeping the response if there was
    # an ETag mismatch to avoid duplicating the request
    if not server_response:
        server_response = process_api_get(hostname + api_version + api_request,
                                          {},
                                          api_request,
                                          [200],
                                          session_object
                                          )

    # Writing the content downloaded as binary in the file specified then closing the file
    with open(download_location + filename, "w+b") as file_output:
        file_output.write(bytes(server_response.content))

    # if there was ETag checking
    if etag_checking:
        # Writing the value of ETag to the ETag file following the established pattern
        with open(download_location + "." + filename + ".etag", "w") as etag_file:
            etag_file.write(server_response.headers['ETag'])

    # If the script is verbose
    if verbosity:
        # Printing the success message
        print("Success: " + api_request)
    # Returning the value associated with a downloaded file
    return "New"


def process_api_get(url, headers, api_request, expected_codes, session_object):
    # type: (str, dict, str, list, requests.Session) -> requests.Response
    """

    Function which takes url and header information and passes them as parameter to the Requests.GET function
    to process the API request. This processing is done in an infinite loop to compensate for server issues
    and will continue, with error signaling, until a request has an appropriate status_code response.

    :param url:             str:                    URL component of the GET request
    :param headers:         dict:                   headers component of the GET request
    :param api_request:     str:                    API request component of the URL used
    :param expected_codes:  list:                   List of the acceptable status codes of the server response
    :param session_object:  requests.Session:       handle of the session object to be used to launch the GET request
    :return:                requests.Response:      Response object of the GET request
    """

    # Global variable for verbosity
    global verbosity

    # Local variable for the server response
    server_response = ""

    # Infinite loop to compensate for the recurring problem of the server closing connections remotely
    # without any message or warning. Could be simplified when the problem is isolated
    while True:
        # Try block to catch the connection errors
        try:
            # Launching the get request through the session object
            server_response = session_object.get(url=url,
                                                 headers=headers
                                                 )
        # Catching the connection errors mentioned above
        except requests.exceptions.ConnectTimeout as ConnectionTimeout:
            # If verbose
            if verbosity:
                # Printing error message
                print("The request %s done at %s raised a timeout error.\nThe error message follows. Retrying."
                      % (api_request, datetime.datetime.now()))
                print(ConnectionTimeout)
            else:
                print("Connection timeout for the download of %s. Retrying." % os.path.basename(api_request))
            # Iterating to the next step of the loop to retry the request
            continue
        except requests.exceptions.ConnectionError as ConnectionError:
            # If verbose
            if verbosity:
                # Printing error message
                print("The request %s done at %s raised a connection error.\nThe error message follows. Retrying."
                      % (api_request, datetime.datetime.now()))
                print(ConnectionError)
            else:
                print("Connection error for the download of %s. Retrying." % os.path.basename(api_request))
            # Iterating to the next step of the loop to retry the request
            continue
        # If the status code is within the expected values
        if server_response.status_code in expected_codes:
            # Exiting the loop
            break
    # Return the response object
    return server_response


def process_minc_func(candidate):
    # type: (dict) -> dict
    """

    Function which takes a CandID number, queries the API to construct the relevant folder structure, download
    the relevant files and then return a dictionary object with the image links and folder structure to be
    used as a reference.

    :param candidate:       str:        CandID number of the Candidate provided (i.e. 1234567)
    :return:                dict:       Dictionary with the CandID and API requests for each file under their
                                        proper visit labels
    """
    # Global variable to be used for authentication
    global token
    # Global variable of the hostname of the service
    global hostname
    # Global variable of the API version to be used
    global api_version
    # Global variable of the base directory to be used
    global base_directory

    # Create a Session object for the download of the files relevant to the candidate
    session_object = requests.Session()
    # Adding the header information for the authentication of the requests
    session_object.headers = {'Authorization': 'Bearer %s' % token}

    # Getting the visits' list from the API using the CandID
    visits = get_json("/candidates/%s" % candidate['CandID'],
                      session_object)['Visits']
    # Assigning empty dictionaries to each visit in the key 'Visits'
    candidate['Visits'] = {visit: {} for visit in visits}

    # For each visit
    for visit in visits:
        # Get the session JSON from the API using both the visit name and the CandID
        session_json = get_json("/candidates/%s/%s" % (candidate['CandID'], visit),
                                session_object)
        # Assigning the session JSON to the json key of the visit
        candidate['Visits'][visit]['json'] = session_json
        # Getting the images JSON from the API using the visit name and the CandID
        images_json = get_json("/candidates/%s/%s/images" % (candidate['CandID'], visit),
                               session_object)
        # Assigning an empty list to the Filenames key of the visit
        candidate['Visits'][visit]['Filenames'] = []
        # For each filename in the images JSON
        for visit_file in images_json['Files']:
            # Append to the list
            candidate['Visits'][visit]['Filenames'].append(visit_file['Filename'])

    # Removing visits where the Filenames list is empty (no images associated)
    candidate['Visits'] = {key: val for key, val in candidate['Visits'].items() if val['Filenames']}

    # If the folder for the candidate doesn't exist
    if not os.path.exists(base_directory + "/%s" % candidate['CandID']):
        # Create the folder for the candidate
        os.makedirs(base_directory + '/%s' % candidate['CandID'])

    # Write the candidate's information to the appropriate file
    with open(base_directory + "/%s/candidate.json" % candidate['CandID'], "w") as candidate_json:
        # Only write the properties expected by the current schema for the candidate's JSON
        candidate_json.write(str({property_key: candidate[property_key] for property_key in ("CandID",
                                                                                             "PSCID",
                                                                                             "Site",
                                                                                             "DoB",
                                                                                             "Gender",
                                                                                             "Language",
                                                                                             "Project")
                                  })
                             )
    # For each visit of the candidate
    for visit in candidate['Visits']:
        # if the folder for the visit doesn't exist
        if not os.path.exists(base_directory + "/%s/%s" % (candidate['CandID'], visit)):
            # Make the folder for the visit
            os.makedirs(base_directory + '/%s/%s' % (candidate['CandID'], visit))
        # Write the JSON for the session to the appropriate file
        with open(base_directory + "/%s/%s/%s" % (candidate['CandID'], visit, "session.json"), "w") as session_json:
            # Only write the value of the Meta property from the JSON obtained from the API
            session_json.write(str(candidate['Visits'][visit]['json']['Meta']))

    # For each visit of the candidate
    for visit in candidate['Visits']:
        # For each file in the Filenames list
        for session_file in candidate['Visits'][visit]['Filenames']:
            # If the file wasn't already downloaded, note that the image download uses the ETag checking
            if get_file('/candidates/%s/%s/images/%s' % (candidate['CandID'], visit, session_file),
                        base_directory + "%s/%s/" % (candidate['CandID'], visit),
                        session_file,
                        True,
                        session_object) == "New":
                # Download the QC for the image, note that the QC doesn't support the ETag checking in the current
                # version of the API
                get_file('/candidates/%s/%s/images/%s/qc' % (candidate['CandID'], visit, session_file),
                         base_directory + "%s/%s/" % (candidate['CandID'], visit),
                         session_file + ".qc.json",
                         False,
                         session_object)

    # Close the session object to free the connection pool
    session_object.close()

    # Return the candidate dictionary object
    return candidate


def process_bids_func(candidate):
    # type: (dict) -> dict
    """

    Function which takes the candidate dictionary object produced by the create_bids_candidates_list() function
    and creates the appropriate folder structure and download the relevant files to their proper locations.

    :param candidate:       dict:       dictionary object created by the create_bids_candidates_list() function
    :return:                dict:       same object as the input to be used for reference
    """

    # Global variable of the token to be used for authentication
    global token
    # Global variable of the hostname of the server to be queried
    global hostname
    # Global variable of the API version to be used
    global api_version
    # Global variable of the location to be used for storage
    global base_directory

    # Create a Session object for the download of the files relevant to the candidate
    session_object = requests.Session()
    # Adding the header information for the authentication of the requests
    session_object.headers = {'Authorization': 'Bearer %s' % token}

    # If the folder for the candidate doesn't exist, note the syntax to respect the BIDS schema
    if not os.path.exists(base_directory + "/sub-%s" % candidate['CandID']):
        # Create the folder for the candidate
        os.makedirs(base_directory + '/sub-%s' % candidate['CandID'])

    # For each visit of the candidate
    for visit in candidate['Visits']:
        # If the folder for the visit doesn't exist, note the syntax to respect the BIDS schema
        if not os.path.exists(base_directory + "/sub-%s/ses-%s" % (candidate['CandID'], visit)):
            # Create the folder for the visit
            os.makedirs(base_directory + '/sub-%s/ses-%s' % (candidate['CandID'], visit))
        # For each category of the visit
        for category in candidate['Visits'][visit]['Folders']:
            # If the folder for the category doesn't exist, note the syntax to respect the BIDS schema
            if not os.path.exists(base_directory + "/sub-%s/ses-%s/%s" % (candidate['CandID'], visit, category)):
                # Create the folder for the category
                os.makedirs(base_directory + '/sub-%s/ses-%s/%s' % (candidate['CandID'], visit, category))

    # For each visit of the candidate
    for visit in candidate['Visits']:
        # For each session file
        for session_file in candidate['Visits'][visit]['Files']:
            # Download the file with ETag checking
            get_file(session_file,
                     base_directory + "sub-%s/ses-%s/" % (candidate['CandID'], visit),
                     os.path.basename(session_file),
                     True,
                     session_object)

        # For each session's category
        for category in candidate['Visits'][visit]['Folders']:
            # For each file of the category
            for file_request in candidate['Visits'][visit]['Folders'][category]:
                # Download the file with ETag checking
                get_file(file_request,
                         base_directory + "sub-%s/ses-%s/%s/" % (candidate['CandID'], visit, category),
                         os.path.basename(file_request),
                         True,
                         session_object)

    # Close the session object to clear the connection pool associated
    session_object.close()

    # Return the candidate object
    return candidate


def create_bids_candidates_list():
    # type: () -> list
    """

    Function which calculates a candidate list based on the JSON obtained from the API.

    :return:    list:       List of candidates keyed on the CandID number
    """

    # Global variable for the token used for authentication
    global token

    # Creating the session object to be used for the GET requests
    session_object = requests.Session()
    # Adding the header information for authentication to the session object
    session_object.headers = {'Authorization': 'Bearer %s' % token}

    # Getting a JSON of all the files using the global values for hostname, api version and authentication token
    file_manifest = get_json('/projects/loris/bids/', session_object)

    # The dataset files are hardcoded in the JSON and can be obtained directly by using the API
    # request provided in the Link property of their names. A better organization of the JSON
    # would allow for all the downloads to be done within a loop instead of individually
    get_file(file_manifest['DatasetDescription']['Link'],
             base_directory,
             'dataset_description.json',
             True,
             session_object)
    get_file(file_manifest['README']['Link'],
             base_directory,
             'README',
             True,
             session_object)
    get_file(file_manifest['BidsValidatorConfig']['Link'],
             base_directory,
             '.bids-validator-config.json',
             True,
             session_object)
    get_file(file_manifest['Participants']['TsvLink'],
             base_directory,
             'participants.tsv',
             True,
             session_object)
    get_file(file_manifest['Participants']['JsonLink'],
             base_directory,
             'participants.json',
             True,
             session_object)

    # Creating a dictionary with a property of Candidates with a value of an empty dictionary
    candidates = {"Candidates": {}}

    # For each session in the JSON key SessionFiles
    for session in file_manifest['SessionFiles']:
        # If the candidate of the session is not in the Candidates dictionary
        if session['Candidate'] not in candidates['Candidates']:
            # Add a property in Candidates with the key CandID and a value of a dictionary with a CandID property
            # and a Visits property with a key of the visit's name and properties from the session files and the
            # category folders
            candidates['Candidates'][session['Candidate']] = {"CandID": session['Candidate'],
                                                              "Visits": {
                session['Visit']: {"Files": [session['TsvLink'], session['JsonLink']], "Folders": {}}
                                                              }}
        # If the candidate of the session already exists
        else:
            # Add the visit's name and information to the Visits property
            candidates['Candidates'][session['Candidate']]['Visits'][session['Visit']] = {"Files": [session['TsvLink'],
                                                                                                    session['JsonLink']
                                                                                                    ],
                                                                                          "Folders": {}
                                                                                          }

    # For each image in the JSON key Images
    for image in file_manifest['Images']:
        # If the category of the image doesn't exist in the categories for that candidate's visit
        if image['Subfolder'] not in candidates['Candidates'][image['Candidate']]['Visits'][image['Visit']]['Folders']:
            # Add the category and link it to an empty list
            candidates['Candidates'][image['Candidate']]['Visits'][image['Visit']]['Folders'][image['Subfolder']] = []
        # For each Link-type property of the image item
        # Note that the comprehension used here could be simplified if the JSON properly segregated the metadata from
        # the links. In the current API, given the variable number of links, it's simpler to remove the common metadata
        # and simply take the list of links than doing regular expressions on properties names
        for link in [property for property in image.keys() if property not in ["Candidate",
                                                                               "PSCID",
                                                                               "Visit",
                                                                               "LorisScanType",
                                                                               "Subfolder"]]:
            # Append the link info to the list of the images of the category of the visit of the candidate
            candidates['Candidates'][image['Candidate']]['Visits'][image['Visit']]['Folders'][image['Subfolder']].append(image[link])

    # Closing the session object for the candidate list to free the connection pool
    session_object.close()

    # Return a list of candidate objects keyed on the CandID property
    return list(candidates['Candidates'].values())


def candidate_argument_checker(candID):
    # type: (int) -> int
    """

    Function which takes a CandID number provided as argument and tests that it is convertible to an integer
    and then that that integer respects the range for a CandID

    :param candID:      int:           CandID of the candidate
    :return:            int:           CandID of the candidate
    """
    try:
        candID = int(candID)
    except ValueError:
        raise argparse.ArgumentTypeError('Invalid %s type for the CandID number. Use strings or ints without commas.'
                                         % type(candID))

    if candID >= 9999999 or candID <= 1000000:
        raise argparse.ArgumentTypeError('%s: Invalid CandID number.' % candID)
    return candID


def create_minc_candidates_list():
    # type: () -> dict
    """

    Function which creates a Session object which is then used to get the list of candidates for the MINC format.

    :return:    dict:       Dictionary value of the JSON-parsed response of the server to the API request
    """

    # Global variable for the token used for authentication
    global token

    # Creating the session object to be used for the GET requests
    session_object = requests.Session()
    # Adding the header information for authentication to the session object
    session_object.headers = {'Authorization': 'Bearer %s' % token}

    # Get the JSON for the candidates' list
    candidates = get_json('/candidates/', session_object)['Candidates']

    # Close the session object to free the connections' pool
    session_object.close()

    # Return the JSON for the candidates
    return candidates


# Setting the script level description
parser = argparse.ArgumentParser(prog="BHD.py",
                                 description="""This script allows for the download of the Registered PREVENT-AD 
                                 Dataset's imagery. The imagery is organized following the BIDS standard or simply
                                 provided as MINC files. By default, the BIDS version will be selected.""")
# Setting the output argument with a proper default value and help message
parser.add_argument("-o", "--output",
                    action="store",
                    type=str,
                    metavar="",
                    dest="output",
                    default=os.getcwd() + "/",
                    help="Absolute path for the output with a trailing slash. Default is the CWD of the script.")
# Setting the verbosity argument with default false value and help message
parser.add_argument("-v", "--verbose",
                    action="store_true",
                    dest="verbose",
                    help="Flag for verbose debugging messages.")
# Setting the format argument with forced choices between the two available choices and help message
parser.add_argument("-f", "--format",
                    action="store",
                    choices=["bids", "minc"],
                    metavar="",
                    dest="imaging_format",
                    default="bids",
                    help="Format of the imagery to be downloaded. Default is BIDS schema.")
# Setting the candidate argument with setting to support a list of unknown length and help message
parser.add_argument("-c", "--candidate",
                    action="store",
                    nargs="+",
                    metavar="",
                    dest="candidates",
                    type=candidate_argument_checker,
                    default=[],
                    help="CandIDs that should be downloaded. Default is to download every candidate available.")

# Parsing the command-line arguments collected through the argparse module
arguments = parser.parse_args()

# Assigning the arguments values to their global variables
base_directory = arguments.output
imaging_format = arguments.imaging_format
verbosity = arguments.verbose
specified_candidates = arguments.candidates

# Printing the PREVENT-AD Logo
print(preventad_logo)
# Print the Welcome message
print("Welcome to the PREVENT-AD Download script.")
# Get the username
username = input("Please enter your username: ")
# Get the password using getpass
password = getpass.getpass('Please enter your password: ')

# If a token is successfully obtained using the credentials, hostname and API version specified
if initialize_jwt(username, password, hostname, api_version):
    # If the imaging format specified is bids
    if imaging_format == "bids":
        # Create the candidate list
        candidates = create_bids_candidates_list()
        # Bind the generic processing function to the bids processing function
        process_function = process_bids_func
    # If the imaging format specified is minc
    elif imaging_format == "minc":
        # Create the candidate list
        candidates = create_minc_candidates_list()
        # Bind the generic processing function to the minc processing function
        process_function = process_minc_func
    # If the imaging format specified is not one of the two supported
    else:
        # Print the error message
        print("Invalid Imaging format provided. Exiting the script.")
        # Exit the script with an error code
        exit(1)

    # Create a pool of worker processes proportional to the number of core available
    worker_pool = multiprocessing.Pool()

    # Create an empty list for the completed candidates
    candidates_list = []

    # If some candidates are specified
    if specified_candidates:
        # Only keep the candidates specified
        candidates = [candidate for candidate in candidates if int(candidate['CandID']) in specified_candidates]

    # If candidates were specified and none where found
    if not candidates and specified_candidates:
        # Print the list submitted and exit
        print("None of the CandID numbers provided (%s) are valid." % ", ".join([str(i) for i in specified_candidates]))
        print("Exiting the script.")
        exit(2)

    # Map the processing function to each candidate and dispatch the match to a worker
    # process
    for result in worker_pool.map(process_function, candidates):
        # Append the return value of the processing function of each worker process to the completed candidates
        # list
        candidates_list.append(result)

    # Close the worker pool
    worker_pool.close()

    # Output the completed candidates list
    print("\nThe script completed successfully and downloaded %s/%s candidates.\n" % (len(candidates_list),
                                                                                      len(candidates)))
    # Exiting the script successfully
    exit(0)
# If the request for the token failed
else:
    # Print the exit message
    print("Exiting the script.")
    # Exit the script with an error code
    exit(3)
