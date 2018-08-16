#!/usr/bin/env python

from bson import json_util

def on_insert(stream):
    print("From streams:", json_util.loads(stream))
    return 1

def on_update(stream):
    return 1

def on_replace(stream):
    return 1

def on_delete(stream):
    print("From streams:", json_util.loads(stream))
    return 1

def on_invalidate(stream):
    return 1

def health_check(stream): 
    return 1 
    
if __name__ == "__main__":
    pass
    