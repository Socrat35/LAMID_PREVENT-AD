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

# Python 2.7 compatibility
try:
    input = raw_input
except NameError:
    pass

hostname = 'openpreventad.loris.ca'
baseurl = 'https://' + hostname + '/api/v0.0.3-dev'


# ### Script options handling
# This ask the user to specify a directory where files shoudl be downloaded

# In[ ]:



if '-f' in sys.argv:
    outputdir = input("Download directory absolute path :\n(press ENTER for current directory)") or os.getcwd()

else:    
    outputdir = os.getcwd()
    try:
      opts, args = getopt.getopt(sys.argv[1:],"ho:")
    except getopt.GetoptError:
      sys.exit(2)
    for opt, arg in opts:
      if opt == '-h':
         print(__file__ + ' -o <outputdir>')
         sys.exit()
      elif opt == '-o':
         outputdir = arg

if not os.path.isdir(outputdir):
    print('outputdir not writable')
    exit()

print("\n*******************************************")
print('Files will be downloaded in ' + outputdir + '/')
print("*******************************************\n")


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
    url = baseurl + '/login',
    json = payload,
    verify = True
)

text = response.content.decode('ascii')

# If the response is successful (HTTP 200), extract the JWT token 
if (response.status_code == 200):
    token = json.loads(text)['token']
    print('login successfull')
else:
    print(text)
    exit()


# ### Extraction  
# For each visits of each candidates this will create a directory `/<CandID>/<VisitLable>` and download all this files and their qc info into it.  
# 
# It wont download files that already exists. This validation is based on filename solely and not on it content... yet

# In[ ]:


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
            basename = os.path.basename(filename)
            etagfullpath = directory + '/.' + basename + '.etag'
            
            etag = ''
            # Check if an ETag file is present
            if os.path.isfile(etagfullpath):
                etagfile = open(etagfullpath, "r")
                etag = etagfile.read()
                etagfile.close()
            
            # Download the image if it doesn't already exist
            image = requests.get(
                url = baseurl + '/candidates/' + candid + '/' + visit + '/images/' + filename,
                headers = {'Authorization': 'Bearer %s' % token, 'If-None-Match': etag}
            )
            
            # Saving the file only if transfered. 
            # Requests for unmodified files will be answered by HTTP 304 Not Modified
            if image.status_code == 200:
                downloadcount +=1
                imagefilename = directory + '/' + filename
                mincfile = open(imagefilename, "w+b")
                mincfile.write(bytes(image.content))
                
            etagfile = open(etagfullpath, "w")
            etagfile.write(image.headers['ETag'])
            etagfile.close()
                
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
            

