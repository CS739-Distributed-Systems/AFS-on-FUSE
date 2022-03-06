import os
import datetime

mountdir="/home/hemalkumar/kalpit/mount_dir/"

def makefiles(f):
    x=1
    name='a'
    for i in range(1,20):
        x=x*2
        t1=datetime.datetime.now()
        temp = open(mountdir+chr(ord(name) + i-1),"w+")
        t2=datetime.datetime.now()
        for j in range(x):
            temp.write("a")
        t3=datetime.datetime.now()
        temp.close()
        t4=datetime.datetime.now()
        diff1=(t2-t1).microseconds
        diff2=(t3-t2).microseconds
        diff3=(t4-t3).microseconds
        diff4=(t4-t1).microseconds
        print(str(x)+" bytes,time to open,"+str(diff1)+",time to write,"+str(diff2)+",time to close,"+str(diff3)+",total time,"+str(diff4))
        f.write(str(x)+" bytes,time to open,"+str(diff1)+",time to write,"+str(diff2)+",time to close,"+str(diff3)+",total time,"+str(diff4)+"\n")
    os.system('rm -rf /home/hemalkumar/kalpit/cache/*')
  #  os.system('rm -rf /home/hemalkumar/kalpit/server/*')

    

def read(f):
    x=1
    name='a'
    for i in range(1,20):
        x=x*2
        t1=datetime.datetime.now()
        temp = open(mountdir+chr(ord(name) + i-1),"w+")
        temp.close()
        t2=datetime.datetime.now()
        diff=(t2-t1).microseconds
        
        print(str(x)+" bytes,First time to open and close"+str(diff))
        avgt=0
        for j in range(1,1000):
            t1=datetime.datetime.now()
            temp = open(mountdir+chr(ord(name) + i-1),"w+")
            temp.close()
            t2=datetime.datetime.now()
            diff=(t2-t1).microseconds
            avgt=avgt+diff
        avgt=avgt/1000
        print(str(x)+" bytes,Avg time to open and close"+str(avgt))
        f.write(str(x)+" bytes,First time to open and close"+str(diff)+"\n")
        f.write(str(x)+" bytes,Avg time to open and close"+str(avgt)+"\n")
#    os.system('rm -rf /home/hemalkumar/kalpit/cache/*')
    



if __name__ == "__main__":
    f = open("makefiles.csv","w+")
    makefiles(f)
    f.close()
    f = open("readspeeds.csv","w+")
    read(f)
    f.close()
#    os.system('rm -rf /home/hemalkumar/kalpit/server/*')
