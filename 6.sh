#！bin/sh

excuteFun(){
      user=$1
      password=$2
      ip=$3 
      a=$4
      printf "\e[32m%s\e[0m \n" "excuteFun is Running:" 
      echo "mvFun Parameters:"
   
      printf "\e[31m%-10s %-10s\n" "user:" $user
      printf "%-10s %-10s\n" "ip:"  $ip
    
      expect -d -c"
                set timeout 300               
             spawn scp -3 -r ./mycode6.0 ${user}@$ip:/home/wanru
             
            expect {
            \"*(yes/no)?\"  { send \"yes\r\"; exp_continue } 
            \"*password:\"   { send \"$password\r\"; exp_continue }
            \"*Last login:*\"   {  }
            }
            
            expect eof"
       expect -d -c"
                set timeout 300               
             spawn ssh ${user}@$ip
             
            expect {
            \"*(yes/no)?\"  { send \"yes\r\"; exp_continue } 
            \"*password:\"   { send \"$password\r\"; exp_continue }
            \"*Last login:*\"   {  }
            }
            send \"cd ~/mycode6.0/\r\"
            send \"sed -i 's/#define HASH_MIN 500/#define HASH_MIN $a/g' nvm_engine.h\r\"
            send \"rm -f db1&&rm -f core*&&make clean&&make\r\"         
            send \"exit\r\"
            expect eof"

              printf "\e[32m%s\e[0m \n\n" "excuteFun End"
} 

    
#sudo chmod 777 /usr/local/hadoop/zookeeper-3.4.12/conf/zoo.cfg
#sudo chmod 777 /usr/local/hadoop/hbase-2.0.5/conf/hbase-site.xml
cd ./mycode6.0
# touch test.log
make clean
make
# kill -9 `ps -ef |grep server|awk '{print $2}' `
rm -f db1
rm -f core*
cd ..
let a=0
let i=40
# for((i=40;i<=40;i++));
# do
    ip=192.168.98.$i
   
   #  sh_path=
   #  opera='chmod 777 /usr/local/hadoop/zookeeper-3.4.12/conf/'
    user=wanru
    password=1111   
    #echo $opera
    #let a=($i-40)*500
    let a=0
    excuteFun $user $password $ip $a
    # pssh -H ${user}@$ip -P  'cd ~/mycode6.0/ && ./server \r'
 
 
# done
# cd ./mycode6.0

# ./server

       # send \"kill -9 `ps -ef |grep server|awk '{print $2}' `\r\"           
            # send \"nohup ./server > test.log 2>&1 &\r\"