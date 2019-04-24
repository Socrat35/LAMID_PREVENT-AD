
# PREVENT-AD MRI images downloader


```python
import getpass  # For input prompt not to show what is entered
import json     # Provide convenient functions to handle JSON objects 
import requests # To handle HTTP requests
import os       # Operating System library to create directories and files

hostname = 'openpreventad.loris.ca'
baseurl = 'https://' + hostname + '/api/v0.0.3-dev'
```

### Login procedure  
This will ask for your username and password and print the login result


```python
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

```

### Extraction  
For each visits of each candidates this will create a directory `/<CandID>/<VisitLable>` and download all this files and their qc info into it.  

It wont download files that already exists. This validation is based on filename solely and not on it content... yet


```python
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
    
    # Get that candidate's list of sessions
    sessions = json.loads(requests.get(
        url = baseurl + '/candidates/' + candid,
        headers = {'Authorization': 'Bearer %s' % token}
    ).content.decode('ascii'))
    
    print(str(len(sessions['Visits'])) + " sessions found\n")
    
    for visit in sessions['Visits']:
        # Create the directory for that visit if it doesn't already exists
        directory = candid + '/' + visit
        try:
            os.makedirs(directory)
        except FileExistsError:
            pass
        
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
        
        print(str(len(files['Files'])) + ' files found for session ' + visit)
        
        for file in files['Files']:
            filename = file['Filename']
            
            # Download the image if it doesn't already exist
            relativepath = directory + '/' + filename
            if not os.path.isfile(relativepath):
                image = requests.get(
                    url = baseurl + '/candidates/' + candid + '/' + visit + '/images/' + filename,
                    headers = {'Authorization': 'Bearer %s' % token}
                )
                mincfile = open(relativepath, "w+b")
                mincfile.write(bytes(image.content))
                
            # Download the QC information of the image if it doesn't already exist
            relativepath = directory + '/' + filename + '.qc.json'
            if not os.path.isfile(relativepath):
                qc = requests.get(
                    url = baseurl + '/candidates/' + candid + '/' + visit + '/images/' + filename + '/qc',
                    headers = {'Authorization': 'Bearer %s' % token}
                )
                qcfile = open(relativepath, "w+b")
                qcfile.write(bytes(qc.content))
              
    processedcandidates += 1
    print("\n-------------------------------------------")
    print(str(processedcandidates) + ' out of ' + str(candidatetotal) + ' candidates processed')
    print("-------------------------------------------\n")
            
```


```python

```
