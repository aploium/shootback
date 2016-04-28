# coding=utf-8
# PyReverseTcpTunnel: a fast and reliable reverse tcp tunnel implant Gzip and AES

# Brief explain about the reverse tcp proxy:
# Master will listen 127.0.0.1:1088 and 0.0.0.0:20822
# Slaver will connection master_ip:20822 and target_ip:80  (in default cfg, they are all 127.0.0.1)
#
# Working stage data stream:
#    BobSoft <--> master_machine:1088 <--> master_machine:20822 <--[Gzip/AES]--> slaver_machine <--> target_machine
#
# The proxy it self is transparent to the BobSoft
# Which means, as the view of the BobSoft, it will equal to:
#  BobSoft <--> target_machine


# Tips1:  How to use as socks/http proxy
#
#     Besides connect the target (website, ftp, etc..) directly,
# we can use other non-reverse proxy as the backend.
# If the backend target is an http proxy, the whole system works like an reverse http proxy
# or if it is a socks5 proxy, the system becomes an reverse socks5 proxy
# etc...
# (same as the former) <--> slaver_machine <--> Socks5 Proxy (maybe in slaver_machine) <--> Real target


# Tips2:  Load Balance With Many Salvers
#
#    If we have more than on slaver machines, just copy the cfg and run then in every salver machines.
# Due to the internal slaver queue mechanism, load balance will be applied automatically

# ################ Config Begin ################
# Master's Config
master_local_listen_addr = ('127.0.0.1', 1088)
master_remote_listen_addr = ('0.0.0.0', 20822)
# How many unused slavers connections can connect to the server.
master_connection_pool_size = 80

# Slaver's Config
# The master's adder, slaver will connect to it
master_addr = ('127.0.0.1', 20822)
# ## Slaver's Target Address
# where master's data would be tunneled to.
slaver_target_addr = ('127.0.0.1', 80)
# Slaver will always try to maintain this counts connection to face potential request bust.
# size 20 may be able to face most concurrency cases
# ONLY limit UNUSED connection counts, connections in use are unlimited.
slaver_unused_connection_pool_size = 20

# ## Send an short hello msg before main data transfer.
# If True, will increase the system's stability, however cost extra time (about 2x ping latency time)
# Please turn it to True in (on or more of the following):
#     1. Low master-slaver transfer latency
#     2. Slaver's network or itself are not stable
#     3. Slaver's target does not allow long time connection
#           (slaver will establish connection to target only AFTER received an hello msg)
#
# Recommend to turn True in most cases
# Only turn it to False if latency is critical or very large.
hello_before_transfer = True
# Timeout to wait slaver's response, seconds. If timeout, will drop this connection and try next.
hello_timeout = 0.8  # a small value

# ## Using Gzip between the master and slaver.
# In most cases, bandwidth between master and slaver are limited.
# Enabling Gzip may save 70% bandwidth(and time!) in text transmission.
#     Due to the unawareness of the TCP data type, now we can just enable or disable Gzip for all data,
# enable in text but disable in img is not possible.
compress_during_transfer = True
# Gzip compress level
# 0-9, larger is better, zero means disable Gzip (equal to set compress_during_transfer to False )
compress_level = 7

# Secret keys, used to verify master and slaver
# ####### PLEASE CHANGE THEM #######
secret_key_slaver = 'Love'
secret_key_master = 'Lucia'

# Verbose Level (-1~4) -1 is completely quiet, 0 only errors, 4 print a lots
verbose_level = 2

# unused connect timeout seconds.
# if time exceed, master will close this connection if it is not in use.
# connections in use do not have timeout.
SLAVER_CONNECTION_TTL = 120
