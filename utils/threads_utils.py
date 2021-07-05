import os
import threading

threads = []


def collect_thread(thread):
    global threads
    threads += [thread]


def join_all_threads():
    for t in threads:
        t.join()
