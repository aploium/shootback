# PyReverseTcpTunnel
A fast and reliable reverse TCP tunnel (support Gzip and AES in transmission).  
It also can be used as reverse socks/http/ftp/smtp... proxy, anything as long as is use TCP!
Help you to bypass firewall or NAT to access the LAN.  
Python反向TCP隧道代理, 用于NAT或防火墙后的内网机器与外网建立TCP隧道.   
(Only support Python 3, for now)  
 
  
### Brief explain about how the reverse tcp proxy works
In some situations, Machine Slaver can connect to Machine Master, but Master cannot connect to Slaver directly, by reusing the slaver's connection, programs in the master machine can connect to the slaver's machine directly(in those programs' sight)

In default config,  
Master will listen `127.0.0.1:1088` and `0.0.0.0:2082`  
Slaver will connect `master_ip:20822` and `target_ip:80` (in default cfg, they are all 127.0.0.1)  
  
Working stage data stream (in default config):  
`BobSoft<-->master_machine:1088<-->master_machine:20822<--[Gzip/AES]-->slaver_machine<-->target_machine:80`  
  
The proxy it self is transparent to the BobSoft  
Which means, as the view of the BobSoft, it is equal to:  
`BobSoft<-->target_machine`  
  
## Simple Usage
1. download and unzip, make sure you have python3.x (2.x support will be added later)  
2. copy or rename the `config.sample.py` to `config.py`  
3. `nc -l -p 80` (Listen localhost's 80 port, slaver will then connect this. nc for netcat)  
4. (new terminal) `python3 master.py`  (the master will listen `localhost:80` for your other programs and `0.0.0.0:20822` for slavers)  
5. (new terminal) `python3 slaver.py`  (the slaver will connect to `master_ip:20822` and `target_ip:80`, in this case, they are both localhost)  
6. (new terminal) `nc 127.0.0.1 1088`  (connect the tunnel)  
7. type and send something like `I love luciaz` in the nc of step 6#  
8. now you will see an `I love luciaz` output in the nc of step 3#, and try to send something back!  

## Another Example (SSH)
1. execute the slaver in another Linux machine (maybe VMare)  
2. set `slaver_target_addr = ('localhost', 22)` and `master_addr = ('master_ip', 20822)` , others remain default
3. use an ssh client(such as putty) to connect localhost:1088 in the MASTER machine
4. now you connected to the slaver machine's ssh.

#### Tips1:  How to use as socks/http proxy
Besides directly connect to the target (website, ftp, etc..), we can use other non-reverse proxy as the backend. If the backend target is an http proxy, the whole system works like a reverse http proxy, or if the backend is an socks4/5 proxy, the system becomes an reverse socks4/5 proxy,etc..  
`(same as the former) <--> slaver_machine <--> Socks5 Proxy (maybe in slaver_machine) <--> Real target`  
  
#### Tips2:  Load balance with many salvers
If we have more than on slaver machines, just copy the cfg and run then in every salver machines. Due to the internal slaver queue mechanism, load balance will be applied automatically  

