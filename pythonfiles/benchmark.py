import os
import datetime
import time
mountdir="/home/hemalkumar/kalpit/mount_dir/"
todo=24

def makefiles(f):
    x=1024
    name='a'
    buff=""
    for i in range(2048):
        buff=buff+"a"
    count=1
    for i in range(1,todo):
        if i == todo-1:
           break
        x=x*2
        time.sleep(2*i)
        t1=datetime.datetime.now()
        temp = open(mountdir+chr(ord(name) + i-1),"w+")
        t2=datetime.datetime.now()
        for j in range(count):
            temp.write(buff)
        t3=datetime.datetime.now()
        temp.close()
        t4=datetime.datetime.now()
        count=count*2
        diff1=(t2-t1).microseconds
        diff2=(t3-t2).microseconds
        diff3=(t4-t3).microseconds
        diff4=(t4-t1).microseconds
        print(str(x)+" bytes,time to open,"+str(diff1)+",time to write,"+str(diff2)+",time to close,"+str(diff3)+",total time,"+str(diff4))
        f.write(str(x)+" bytes,time to open,"+str(diff1)+",time to write,"+str(diff2)+",time to close,"+str(diff3)+",total time,"+str(diff4)+"\n")
    os.system('rm -rf /home/hemalkumar/kalpit/cache/*')
  #  os.system('rm -rf /home/hemalkumar/kalpit/server/*')


def read(f):
    x=1024
    name='a'
    print("start read")
    for i in range(1,todo):
        if i==todo-1:
            break
        x=x*2
        time.sleep(1)
        t1=datetime.datetime.now()
        temp = open(mountdir+chr(ord(name) + i-1),"r+")
        temp.close()
        t2=datetime.datetime.now()
        diff1=(t2-t1).microseconds
        time.sleep(25)
#        print(str(x)+" bytes,First time to open and close"+str(diff1))
        avgt=0
        arr=[]
        for j in range(1,10):
            time.sleep(5)
            t1=datetime.datetime.now()
            temp = open(mountdir+chr(ord(name) + i-1),"r+")
            temp.close()
            t2=datetime.datetime.now()
            diff=(t2-t1).microseconds
            arr.append(diff)
            avgt=avgt+diff
        avgt=avgt/10
        arr.sort()
        print(str(x)+" bytes,First time,"+str(diff1)+",Avg time,"+str(avgt)+",Median time,"+str(arr[5]))
        f.write(str(x)+" bytes,First time,"+str(diff1)+",Avg time,"+str(avgt)+",Median time,"+str(arr[5])+"\n")
    os.system('rm -rf /home/hemalkumar/kalpit/cache/*')

def rununittests():
    os.system("mkdir "+mountdir+"abc")
    stream=os.popen("ls "+mountdir)
    output = stream.read()
    stream.close()
    if "abc" not in output:
        print("Unit test for mkdir failed")
    else:
        print("Unit test for mkdir passed")
    os.system("rmdir "+mountdir+"abc")
    stream=os.popen("ls "+mountdir)
    output = stream.read()
    if "abc" not in output:
        print("Unit test for rmdir passed")
    else:
        print("Unit test for rmdir failed")


def lastwriter():
    temp = open(mountdir+"a","a+")
    temp2= open(mountdir+"a","a+")
    temp.write("k")
    temp.close()
    time.sleep(10)
    temp2.write("m")
    temp2.close()
    time.sleep(10)
    temp=open(mountdir+"a","r")
    line=temp.readline()
    print(line)
    temp.close()
    


if __name__ == "__main__":
    rununittests()
    f = open("makefiles.csv","w+")
    makefiles(f)
    f.close()
    f = open("readspeeds.csv","w+")
    read(f)
    f.close()
    lastwriter()
    os.system('rm -rf /home/hemalkumar/kalpit/cache/*')
    os.system('rm -rf /home/hemalkumar/kalpit/server/*')
