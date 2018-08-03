#!/usr/bin/python
# -*- coding: utf-8 -*-
#****************************************************************#
# Create Date: 2017-01-06 17:58
#***************************************************************#

import socket
import shutil
from time import gmtime, strftime

# get host_name
host_name = socket.gethostname()
tmp_file = "/tmp/.lark-fix-host.hosts"
host_file = "/etc/hosts"
bak_file_name = "/tmp/hosts-fix-bak.%s" % ( strftime("%Y-%m-%d_%H-%M-%S", gmtime()) )

# load /etc/hosts file context
FH = open(host_file,"r")
file_lines = [ i.rstrip() for i in FH.readlines()]
FH.close()
file_lines_reverse = file_lines[::-1]
new_lines = []
bad_lines = []
last_match_line = ""

for line in file_lines_reverse:
    if line.find(host_name) < 0:  # 不匹配的行直接跳过
        new_lines.append(line + "\n")
        continue

    cols = line.split()
    new_cols = []
    if cols[0].startswith("#"): # 跳过已经注释掉的行
        new_lines.append(line + "\n")
        continue
    for col in cols:
        if not col == host_name: # 跳过不匹配的列
            new_cols.append(col)
            continue

        if cols[0] == "127.0.0.1": # 如果第一列是 127.0.0.1 就跳过匹配的列, 防止 hostname -i 返回 127.0.0.1
            continue

        # 如果已经发现过匹配的列, 就丢掉重复的列
        if not len(last_match_line) == 0:
            continue

        new_cols.append(col)
        last_match_line = line

    # 跳过 xx.xx.xx.xx hostname 这样的重复列
    if len(new_cols) == 1:
        continue

    new_l = "%s\n" % " ".join(new_cols)
    new_lines.append(new_l)

# save tmp hosts

FH2=file(tmp_file,"w+")
FH2.writelines( new_lines[::-1])
FH2.close()

# mv to /etc/hosts
shutil.copy(host_file, bak_file_name)
shutil.move(tmp_file, host_file)
