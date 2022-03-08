Steps to run the benchmark.py
1. Edit the benchmark.py to match the mount_dir, server and cache directories.
2. Edit the todo variable to vary the range of size of files. Starts from 2k and currently goes to 1GB
3. Run the script with "python benchmark.py"
4. Outputs for variable file sizes would be written in csv files.

Steps for multiple Clients test
Case 1 where clients creates and writes the file
1. Edit the paths in clients.py
2. Edit the client variable to desirable clients
3. Mount the client with " tee output.txt"
4. Run the scraper.py on output.txt to get output.csv

Case 2 where clients read the files present at server
Perform the same steps with readclients.py

Steps to create random files with gradually increasing size
1. Make a directory where the files need to present 
2. Edit the variables in the script to desired number of files
3. File size increases in power of 2 
