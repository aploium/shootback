# shootback

shootback is a reverse TCP tunnel let you access target behind NAT or firewall  
反向TCP隧道, 使得NAT或防火墙后的内网机器可以被外网访问.  
Consumes less than 1% CPU and 8MB memory under 800 concurrency.

## How it works

![How shoot back works](https://raw.githubusercontent.com/aploium/shootback/static/graph.png)


## Typical Scene

1. Access company/school computer(no internet IP) from home  
   从家里连接公司或学校里没有独立外网IP的电脑
2. Make private network/site public.  
   使内网或内网站点能从公网访问
3. Help private network penetration.  
   辅助内网渗透
4. Help CTF offline competitions.  
   辅助CTF线下赛, 使场外选手也获得比赛网络环境

## Getting started

1. requirement:
    * Master: Python3.4+, OS independent
    * Slaver: Python2.7/3.4+, OS independent
    * no external dependencies, only python std lib
2. download `git clone https://github.com/aploium/shootback`
3. (optional) if you need a single-file slaver.py, run `python3 build_standalone_slaver.py`
4. ```bash
    # master listen :10000 for slaver, :10080 for you
    python3 master.py -m 127.0.0.1:10000 -c 127.0.0.1:10080
    
    # slaver connect to master, and use example.com as tunnel target
    # ps: you can use python2 in slaver, not only py3
    python3 slaver.py -m 127.0.0.1:10000 -t example.com:80
    
    # doing request to master
    curl -v -H "host: example.com" 127.0.0.1:10080
    # -- some HTML content from example.com --
    # -- some HTML content from example.com --
    # -- some HTML content from example.com --
    ```
5. for more help, please see `python3 master.py --help` and `python3 slaver.py --help`
   

## Warning

1. in windows, due to the limit of CPython `select.select()`,
   shootback can NOT handle more than 512 concurrency, you may meet  
    `ValueError: too many file descriptors in select()`  
   If you have to handle such high concurrency in windows,
   [Anaconda-Python3](https://www.continuum.io/downloads) is recommend,
   [it's limit in windows is 2048](https://github.com/ContinuumIO/anaconda-issues/issues/1241)



