#！bin/sh

excuteFun(){
      user=$1
      password=$2
      ip=$3 
      a=$4
    #   printf "\e[32m%s\e[0m \n" "excuteFun is Running:" 
    #   echo "mvFun Parameters:"
   
    #   printf "\e[31m%-10s %-10s\n" "user:" $user
    #   printf "%-10s %-10s\n" "ip:"  $ip
    
      expect -d -c"
                set timeout 300               
             spawn scp -3 -r ./mycode8.0 ${user}@$ip:/home/wanru
             
             
            expect {
            \"*(yes/no)?\"  { send \"yes\r\"; exp_continue } 
            \"*password:\"   { send \"$password\r\"; exp_continue }
            \"*Last login:*\"   {  }
            }
            
            expect eof"

       expect -d -c"
                set timeout 300               
             spawn scp -3 -r ./9.sh ${user}@$ip:/home/wanru
             
             
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
            send \"cd ~/mycode8.0/\r\"
            send \"ps -ef | grep './server -p' | grep -v 'grep' | cut -c 9-15 | xargs kill -9\r\"
            send \"ps -ef | grep './client*' | grep -v 'grep' | cut -c 9-15 | xargs kill -9\r\"
            send \"ps -ef | grep './slave -p' | grep -v 'grep' | cut -c 9-15 | xargs kill -9\r\"
            send \"sed -i 's/#define HASH_MIN 500/#define HASH_MIN $a/g' nvm_engine.h\r\"
            send \"rm -f *.log*&&rm -f ../nvm/db1&&rm -f ../nvm/log.txt&&rm -f db1&&rm -f core*&&make clean&&make\r\"         
            send \"exit\r\"
            expect eof"

              printf "\e[32m%s\e[0m \n\n" "excuteFun End"
} 

cd ./mycode8.0
ps -ef | grep './server -p' | grep -v 'grep' | cut -c 9-15 | xargs kill -9
# touch test.log
make clean
make
# kill -9 `ps -ef |grep server|awk '{print $2}' `
rm -f ../nvm/db1
rm -f core*
rm -f ../nvm/log.txt
rm -f *.log*
user=wanru
password=1111
cd ..

ip=192.168.98.40
let a=0
excuteFun $user $password $ip $a
   
ip=192.168.98.50
let a=0
excuteFun $user $password $ip $a

ip=192.168.98.52
let a=500
excuteFun $user $password $ip $a




# read -p "client num" client_num
# cd ./mycode8.0
# for((i=0;i<$client_num;i++));
# do
#     nohup ./client -l $value_len -i $iter -v $verbose > client$i.log 2>&1 &
#     echo "cd ./mycode8.0&&./client -l $value_len -i $iter -v $verbose > client$i.log 2>&1 &"
# done
# ./server

       # send \"kill -9 `ps -ef |grep server|awk '{print $2}' `\r\"           
            # send \"nohup ./server > test.log 2>&1 &\r\"