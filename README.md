 ## muti-client muti-server kv-store system based on rdma and nvm with lock and MultiThread on server ##
# Quick Start: ##
on server 192.168.98.42

run ./6.sh to distribute mycode6.0 to 192.168.98.40，42 and modify some  parameters and make

next,run server and client on 192.168.98.40 and 192.168.98.42


### Server: ###    
    - cd mycode6.0/
    - ./server  

### Client: ###    
    - cd mycode6.0/
    - ./client
