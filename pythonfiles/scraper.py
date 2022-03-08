f = open("output.txt","r+")
create=[]
close=[]
for line in f:
    n=line.strip().split(',')
    if n[0]=="xmp_create":
        create.append(long(n[1]))
    else:
        close.append(long(n[1]))
f.close()
f = open("output.csv","w+")
x=1024
todo=21
num=1024*1024
f.write(",create,close\n")
for i in range(todo):
    x=x*2
    f.write(str(float(x)/float(num))+","+str(float(create[i])/float(1000))+","+str(float(close[i])/float(1000))+"\n")
f.close()
