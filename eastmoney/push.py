with open('c:\Users\cytz1\Desktop\gubalist.txt','w') as f:
    for i in list(range(100000,0,-1)):
        f.write('lpush GuBaPost:start_urls http://guba.eastmoney.com/default_'+str(i)+'.html'+'\n')