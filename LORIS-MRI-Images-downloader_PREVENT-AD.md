
# PREVENT-AD MRI images downloader


```python
import getpass  # For input prompt not to show what is entered
import json     # Provide convenient functions to handle JSON objects 
import requests # To handle HTTP requests
import os       # Operating System library to create directories and files
import errno    # For python 2.7 compatibility

# Pyhton 2.7 compatibility
try:
    input = raw_input
except NameError:
    pass

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
    exit()

```

    Login on openpreventad.loris.ca
    username: xavier
    password: ········
    login successfull


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

    233 candidates found
    -------------------------------------------
    
    Processing candidate #1004359
    
    5 sessions found
    
    18 files found for session PREBL00
    9 files found for session PREEN00
    18 files found for session PREFU12
    11 files found for session PREFU24
    19 files found for session PREFU36
    
    -------------------------------------------
    1 out of 233 candidates processed
    -------------------------------------------
    
    Processing candidate #1016072
    
    4 sessions found
    
    18 files found for session PREBL00
    9 files found for session PREEN00



    ---------------------------------------------------------------------------

    TypeError                                 Traceback (most recent call last)

    /usr/local/lib/python3.7/site-packages/urllib3/connectionpool.py in _make_request(self, conn, method, url, timeout, chunked, **httplib_request_kw)
        376             try:  # Python 2.7, use buffering of HTTP responses
    --> 377                 httplib_response = conn.getresponse(buffering=True)
        378             except TypeError:  # Python 3


    TypeError: getresponse() got an unexpected keyword argument 'buffering'

    
    During handling of the above exception, another exception occurred:


    KeyboardInterrupt                         Traceback (most recent call last)

    <ipython-input-7-4904cea9d86b> in <module>
         73                 qc = requests.get(
         74                     url = baseurl + '/candidates/' + candid + '/' + visit + '/images/' + filename + '/qc',
    ---> 75                     headers = {'Authorization': 'Bearer %s' % token}
         76                 )
         77                 qcfile = open(relativepath, "w+b")


    /usr/local/lib/python3.7/site-packages/requests/api.py in get(url, params, **kwargs)
         73 
         74     kwargs.setdefault('allow_redirects', True)
    ---> 75     return request('get', url, params=params, **kwargs)
         76 
         77 


    /usr/local/lib/python3.7/site-packages/requests/api.py in request(method, url, **kwargs)
         58     # cases, and look like a memory leak in others.
         59     with sessions.Session() as session:
    ---> 60         return session.request(method=method, url=url, **kwargs)
         61 
         62 


    /usr/local/lib/python3.7/site-packages/requests/sessions.py in request(self, method, url, params, data, headers, cookies, files, auth, timeout, allow_redirects, proxies, hooks, stream, verify, cert, json)
        531         }
        532         send_kwargs.update(settings)
    --> 533         resp = self.send(prep, **send_kwargs)
        534 
        535         return resp


    /usr/local/lib/python3.7/site-packages/requests/sessions.py in send(self, request, **kwargs)
        644 
        645         # Send the request
    --> 646         r = adapter.send(request, **kwargs)
        647 
        648         # Total elapsed time of the request (approximately)


    /usr/local/lib/python3.7/site-packages/requests/adapters.py in send(self, request, stream, timeout, verify, cert, proxies)
        447                     decode_content=False,
        448                     retries=self.max_retries,
    --> 449                     timeout=timeout
        450                 )
        451 


    /usr/local/lib/python3.7/site-packages/urllib3/connectionpool.py in urlopen(self, method, url, body, headers, retries, redirect, assert_same_host, timeout, pool_timeout, release_conn, chunked, body_pos, **response_kw)
        598                                                   timeout=timeout_obj,
        599                                                   body=body, headers=headers,
    --> 600                                                   chunked=chunked)
        601 
        602             # If we're going to release the connection in ``finally:``, then


    /usr/local/lib/python3.7/site-packages/urllib3/connectionpool.py in _make_request(self, conn, method, url, timeout, chunked, **httplib_request_kw)
        378             except TypeError:  # Python 3
        379                 try:
    --> 380                     httplib_response = conn.getresponse()
        381                 except Exception as e:
        382                     # Remove the TypeError from the exception chain in Python 3;


    /usr/local/Cellar/python/3.7.2_2/Frameworks/Python.framework/Versions/3.7/lib/python3.7/http/client.py in getresponse(self)
       1319         try:
       1320             try:
    -> 1321                 response.begin()
       1322             except ConnectionError:
       1323                 self.close()


    /usr/local/Cellar/python/3.7.2_2/Frameworks/Python.framework/Versions/3.7/lib/python3.7/http/client.py in begin(self)
        294         # read until we get a non-100 response
        295         while True:
    --> 296             version, status, reason = self._read_status()
        297             if status != CONTINUE:
        298                 break


    /usr/local/Cellar/python/3.7.2_2/Frameworks/Python.framework/Versions/3.7/lib/python3.7/http/client.py in _read_status(self)
        255 
        256     def _read_status(self):
    --> 257         line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
        258         if len(line) > _MAXLINE:
        259             raise LineTooLong("status line")


    /usr/local/Cellar/python/3.7.2_2/Frameworks/Python.framework/Versions/3.7/lib/python3.7/socket.py in readinto(self, b)
        587         while True:
        588             try:
    --> 589                 return self._sock.recv_into(b)
        590             except timeout:
        591                 self._timeout_occurred = True


    /usr/local/Cellar/python/3.7.2_2/Frameworks/Python.framework/Versions/3.7/lib/python3.7/ssl.py in recv_into(self, buffer, nbytes, flags)
       1050                   "non-zero flags not allowed in calls to recv_into() on %s" %
       1051                   self.__class__)
    -> 1052             return self.read(nbytes, buffer)
       1053         else:
       1054             return super().recv_into(buffer, nbytes, flags)


    /usr/local/Cellar/python/3.7.2_2/Frameworks/Python.framework/Versions/3.7/lib/python3.7/ssl.py in read(self, len, buffer)
        909         try:
        910             if buffer is not None:
    --> 911                 return self._sslobj.read(len, buffer)
        912             else:
        913                 return self._sslobj.read(len)


    KeyboardInterrupt: 



```python

```
