# Installation Doc
### Prepare a venv with the specified requirement.txt. 
### Run the main.py. It will start a test server at port 9001. It can be changed later.
#### gunicorn -b 0.0.0.0:9001 -w 10   main:app --log-file log.txt --log-level DEBUG  will start gunicorn server with 10 workers 