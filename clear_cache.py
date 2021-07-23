#!/usr/bin/python3

import os
import sys

print('Content-Type: text/html')
print()


def stop_check():
    if not os.path.exists('stop.txt'):
        print('file deleted<br>')
        print('<h2>Script stopped</h2>')
        sys.exit(0)


filelist = [ f for f in os.listdir('cache/') if f.endswith(".html") ]
for f in filelist:
    stop_check()
    os.remove(os.path.join('cache/', f))
    print(f"<br>Cache for - {f} was deleted..<br>")