 # muti-client muti-server muti-slave kv-store system based on rdma and nvm with lock and MultiThread on server ##
## Quick Start: ##
on server 192.168.98.42

run ./8.sh to distribute mycode6.0 to 192.168.98.40，42 and modify some  parameters and make

next,
run slave on 192.168.98.50 and 192.168.98.52
run server on 192.168.98.40 and 192.168.98.42
run client on 192.168.98.40 and 192.168.98.42

### Slave: ###    
    - cd mycode8.0/
    - ./slave


### Server: ###    
    - cd mycode8.0/
    - ./server  

### Client: ###    
    - cd mycode8.0/
    - ./client -v 0 -i 5000 -l 1024


### details: ###   
    - 详见 命令.txt

