from threading import Thread
import os
import datetime
client=16

mountdir="/home/hemalkumar/kalpit/mount_dir/"
buff = ""
for i in range(1024*1024):
    buff=buff+"a"

def task(i):
    temp = open(mountdir+str(i),"r")
    t1=datetime.datetime.now()
    read=temp.read()
    t2=datetime.datetime.now()
    temp.close()


if __name__ == "__main__":
	os.system('rm -rf /home/hemalkumar/kalpit/cache/*')
        threads=[]
	for i in range(client):
    	    threads.append(Thread(target=task,args=[i]))

	for thread in threads:
	    thread.start()
	for thread in threads:
	    thread.join()	
