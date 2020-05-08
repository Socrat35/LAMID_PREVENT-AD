#!/usr/bin/env python
# coding: utf-8

# # LORIS API MRI images downloader for PREVENT-AD 

# In[ ]:


import sys, getopt  # For script options
import os           # Operating System library to create directories and files
import errno        # For python 2.7 compatibility
import getpass      # For input prompt not to show what is entered
import json         # Provide convenient functions to handle JSON objects 
import requests     # To handle HTTP requests
import re           # Handle regular expression comparisons

# Python 2.7 compatibility
try:
    input = raw_input
except NameError:
    pass

hostname = 'openpreventad.loris.ca'
baseurl = 'https://' + hostname + '/api/v0.0.3-dev'


# ### Script options handling
# This ask the user to specify a directory where files should be downloaded

# In[ ]:

downloadtype = 'bids'
loris_scan_types = [
    'asl',
    'bold',
    'dwi65',
    'qT2star',
    'fieldmap',
    'FLAIR',
    'MP2RAGE',
    't1w',
    'T2star',
    't2w',
    'task-encoding-bold',
    'task-retrieval-bold'
]
requested_modalities = False

if '-f' in sys.argv:
    outputdir = input("Download directory absolute path :\n(press ENTER for current directory)") or os.getcwd()

else:

    description = '\nThis tool facilitates the download of the open PREVENT-AD dataset. Data are provided under two different formats:\n' \
                  '\t - data organized according to the BIDS standard or \n' \
                  '\t - data available under the MINC format.\n' \
                  'By default, the data will be downloaded according to the BIDS standard.\n'
    usage = (
        '\n'
        'usage  : ' + __file__ + ' -o <outputdir> -t <bids/minc> \n\n'
        'options: \n'
        '\t-o, --outputdir : path to the directory where the downloaded files will go \n'
        '\t-t, --type      : data organization - available options: <bids> or <minc>, default to <bids>\n'
        '\t-m, --modalities: comma-separated list of modalities to download. By default all modalities will be downloaded. '
                                 'Available modalities are: ' + ','.join(loris_scan_types) + '\n'
    )

    outputdir = os.getcwd()
    try:
      opts, args = getopt.getopt(sys.argv[1:], "ho:t:")
    except getopt.GetoptError:
      sys.exit(2)
    for opt, arg in opts:
      if opt == '-h':
         print(description + usage)
         sys.exit()
      elif opt == '-o':
         outputdir = arg
      elif opt == '-t':
          downloadtype = arg
      elif opt == '-m':
          requested_modalities = arg

if not os.path.isdir(outputdir):
    print('outputdir ' + outputdir + ' is not writable')
    exit()

print("\n*******************************************")
print('Files will be downloaded in ' + outputdir + '/')
print("*******************************************\n")

if downloadtype not in ('bids', 'minc'):
    print('Invalid option for -t, --type. Valid options for -t, --type are <bids> or <minc> ')
    exit(2)

if requested_modalities:
    requested_modalities_list = str(requested_modalities).split(',')
    for requested_modality in requested_modalities_list:
        if requested_modality not in loris_scan_types:
            print(requested_modality + 'is not a valid PREVENT-AD modality')
            exit(2)

# ### Login procedure  
# This will ask for your username and password and print the login result

# In[ ]:


print('Login on ' + hostname)

# Prepare the credentials using prompt
payload = {
    'username': input('username: '), 
    'password': getpass.getpass('password: ')
}

# Send an HTTP POST request to the /login endpoint
response = requests.post(
    url=baseurl + '/login',
    json=payload,
    verify=True
)

text = response.content.decode('ascii')

# If the response is successful (HTTP 200), extract the JWT token 
if response.status_code == 200:
    token = json.loads(text)['token']
    print('login successfull')
else:
    print(text)
    exit()


def download_file(file_link, local_directory):
    """
    Download a file through the LORIS API.

    Caveat: It wont download files that already exists. This validation is based on filename solely and not on its content... yet

    :param file_link      : API URL to use to download the file (from /candidates/ or /projects/ part)
    :param download_count : the download count to be updated
    :param local_directory: local directory where the file will be downloaded
    """

    basename = os.path.basename(file_link)
    etag_full_path = local_directory + '/.' + basename + '.etag'

    etag = ''
    # Check if an ETag file is present
    if os.path.isfile(etag_full_path):
        etag_file = open(etag_full_path, "r")
        etag = etag_file.read()
        etag_file.close()

    # Download the image if it doesn't already exist
    file_response = requests.get(
        url=baseurl + file_link,
        headers={'Authorization': 'Bearer %s' % token, 'If-None-Match': etag}
    )

    # Saving the file only if transferred.
    # Requests for unmodified files will be answered by HTTP 304 Not Modified
    download_status = False
    if file_response.status_code == 200:
        download_status = True
        file_path = local_directory + '/' + basename
        file_handle = open(file_path, "w+b")
        file_handle.write(bytes(file_response.content))

    etag_file = open(etag_full_path, "w")
    etag_file.write(file_response.headers['ETag'])
    etag_file.close()

    return True if download_status else False


def is_modality_in_the_requested_list(modality):

    for requested_scan_type in requested_modalities_list:
        pattern = '/-|_' + modality + '-|_/'
        if re.search(pattern, requested_scan_type):
            return True

    return False


# ### Data download

if downloadtype == 'minc':
    # For each visit of each candidate this will create a directory `/<CandID>/<VisitLabel>` & download all files and their QC info into it.

    # Get the list of all the candidates
    candidates = json.loads(requests.get(
        url = baseurl + '/candidates/',
        headers = {'Authorization': 'Bearer %s' % token}
    ).content.decode('ascii'))
    
    candidatetotal = len(candidates['Candidates'])
    print(str(candidatetotal) + ' candidates found')
    print("-------------------------------------------\n")
    processedcandidates = 0
    
    for candidate in candidates['Candidates']:
        candid = candidate['CandID']
    
        print('Processing candidate #' + candid + "\n")
    
        # Create the a directory for the candidate if it doesn't already exists
        directory = outputdir + '/' + candid
        try:
            os.makedirs(directory)
        except OSError as e:
            if e.errno == errno.EEXIST:
                pass
            else:
                print(errno.EEXIST)
                print(e.errno)
                raise
    
        # Write the candidate information into a JSON file
        candidatemetafile = open(directory + '/candidate.json', "w")
        candidatemetafile.write(str(candidate))
        candidatemetafile.close()
    
        # Get that candidate's list of sessions
        sessions = json.loads(requests.get(
            url = baseurl + '/candidates/' + candid,
            headers = {'Authorization': 'Bearer %s' % token}
        ).content.decode('ascii'))
    
        print(str(len(sessions['Visits'])) + " sessions found\n")
    
        for visit in sessions['Visits']:

            print('\tProcessing visit ' + visit + '\n')
    
            # Create the directory for that visit if it doesn't already exists
            directory = outputdir + '/' + candid + '/' + visit
            try:
                os.makedirs(directory)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    pass
                else:
                    print(errno.EEXIST)
                    print(e.errno)
                    raise
    
            # Get the session information
            session = json.loads(requests.get(
                url = baseurl + '/candidates/' + candid + '/' + visit,
                headers = {'Authorization': 'Bearer %s' % token}
            ).content.decode('ascii'))
    
            # Write the session information into a JSON file
            sessionmetafile = open(directory + '/session.json', "w")
            sessionmetafile.write(str(session['Meta']))
            sessionmetafile.close()
    
            # Get a list of all the images for the session
            files = json.loads(requests.get(
                url = baseurl + '/candidates/' + candid + '/' + visit + '/images',
                headers = {'Authorization': 'Bearer %s' % token}
            ).content.decode('ascii'))
    
            sessionfilecount = len(files['Files'])
            sys.stdout.write(str(sessionfilecount) + ' files found for session ' + visit)
    
            downloadcount = 0
            for file in files['Files']:
                filename = file['Filename']
                filelink = '/candidates/' + candid + '/' + visit + '/images/' + filename

                download_success = download_file(filelink, directory)
                if download_success:
                    downloadcount += 1
    
                # Download the QC information of the image if it doesn't already exist
                qcfullpath = directory + '/' + filename + '.qc.json'
                if not os.path.isfile(qcfullpath):
                    qc = requests.get(
                        url = baseurl + '/candidates/' + candid + '/' + visit + '/images/' + filename + '/qc',
                        headers = {'Authorization': 'Bearer %s' % token}
                    )
                    qcfile = open(qcfullpath, "w+b")
                    qcfile.write(bytes(qc.content))
    
            unmodified = sessionfilecount - downloadcount
            print(' - ' + str(downloadcount) + ' downloaded, ' + str(unmodified) + ' unmodified')
    
        processedcandidates += 1
        print("\n-------------------------------------------")
        print(str(processedcandidates) + ' out of ' + str(candidatetotal) + ' candidates processed')
        print("-------------------------------------------\n")
                
elif downloadtype == 'bids':

    download_count = 0

    # Get the list of BIDS endpoints
    bids_endpoints = json.loads(requests.get(
        url=baseurl + '/projects/loris/bids/',
        headers={'Authorization': 'Bearer %s' % token}
    ).content.decode('ascii'))

    # Download README, dataset_description.json, participants.tsv, participants.json and .bids-validator-config.json
    print("-------------------------------------------\n")
    print('Downloading study level BIDS files...\n')
    print("-------------------------------------------\n")
    download_count = 0
    for filetype in ['README', 'DatasetDescription', 'Participants', 'BidsValidatorConfig']:
        if filetype == 'Participants':
            download_success = download_file(bids_endpoints['Participants']['TsvLink'], outputdir)
            if download_success:
                download_count += 1
            download_success = download_file(bids_endpoints['Participants']['JsonLink'], outputdir)
            if download_success:
                download_count += 1
        else:
            download_success = download_file(bids_endpoints[filetype]['Link'], outputdir)
            if download_success:
                download_count += 1
    print('\t=> Downloaded ' + str(download_count) + ' new files\n\n')

    # Download the *scans.tsv visit files
    scans_total = len(bids_endpoints['SessionFiles'])
    print("-------------------------------------------\n")
    print('Downloading visit level *scans.tsv and *scans.json files...\n')
    print(str(scans_total) + ' visit level *_scans.* BIDS files found\n')
    print("-------------------------------------------\n")
    download_count = 0
    for file_dict in bids_endpoints['SessionFiles']:
        visit_directory = outputdir + '/' + file_dict['Candidate'] + '/' + file_dict['Visit']
        try:
            os.makedirs(visit_directory)
        except OSError as e:
            if e.errno == errno.EEXIST:
                pass
            else:
                print(errno.EEXIST)
                print(e.errno)
                raise
        download_success = download_file(file_dict['TsvLink'], visit_directory)
        if download_success:
            download_count += 1
        download_success = download_file(file_dict['JsonLink'], visit_directory)
        break
    print('\t=> Downloaded ' + str(download_count) + ' new files\n\n')

    # Download the images and their related files
    images_total = len(bids_endpoints['Images'])
    print("-------------------------------------------\n")
    print('Downloading images and their related files...\n')
    print(str(images_total) + ' image NIfTI BIDS files found\n')
    print("-------------------------------------------\n")
    download_count = 0
    for file_dict in bids_endpoints['Images']:
        image_modality = file_dict['LorisScanType']
        # skip if the user provided modalities to download and the file's scan type is not included in the modalities requested by the user
        if requested_modalities and not is_modality_in_the_requested_list(image_modality):
            continue
        image_directory = outputdir + '/' + file_dict['Candidate'] + '/' + file_dict['Visit'] + '/' + file_dict['Subfolder']
        try:
            os.makedirs(image_directory)
        except OSError as e:
            if e.errno == errno.EEXIST:
                pass
            else:
                print(errno.EEXIST)
                print(e.errno)
                raise
        download_success = download_file(file_dict['NiftiLink'], image_directory)
        if download_success:
            download_count += 1
        download_success = download_file(file_dict['JsonLink'], image_directory)
        if 'BvalLink' in file_dict.keys():
            download_success = download_file(file_dict['BvalLink'], image_directory)
        if 'BvecLink' in file_dict.keys():
            download_success = download_file(file_dict['BvecLink'], image_directory)
        if 'EventLink' in file_dict.keys():
            print(file_dict['EventLink'])
            download_success = download_file(file_dict['EventLink'], image_directory)
    print('\t=> Downloaded ' + str(download_count) + ' new files\n\n')

    print('\n-------------------------------------------')
    print('\nFinished downloading the open PREVENT-AD BIDS dataset')
    print('\n-------------------------------------------\n')
