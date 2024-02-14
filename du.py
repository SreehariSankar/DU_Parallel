"""
Parallelized disk usage equivalent to the UNIX du command.
The script takes a directory and the number of cores to use as input.

Usage:
    python du.py <directory> <number_of_cores>

Sreehari Sankar, 2024, Feb 14th.
Happy Valentine's Day!

"""
import os
import threading
import queue


class DUThreading(threading.Thread):
    def __init__(self, dir_queue, result_queue):
        threading.Thread.__init__(self)
        self.dir_queue = dir_queue
        self.result_queue = result_queue

    def run(self):
        while True:
            current_dir = self.dir_queue.get()
            if current_dir is None:
                # STOP THREAD
                self.dir_queue.task_done()
                break

            total_size = 0
            for dirpath, dirnames, filenames in os.walk(current_dir):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    try:
                        total_size += os.path.getsize(fp)
                    except OSError:
                        # IGNORE ACCESS ERRORS
                        continue

            self.result_queue.put(total_size)
            self.dir_queue.task_done()


def DU(start_path, num_cores):
    if not os.path.exists(start_path):
        print("Directory does not exist")
        return

    dir_queue = queue.Queue()
    result_queue = queue.Queue()

    # START THREADS
    threads = []
    for _ in range(num_cores):
        thread = DUThreading(dir_queue, result_queue)
        thread.start()
        threads.append(thread)
    # ADD START PATH
    dir_queue.put(start_path)
    # WAIT
    dir_queue.join()
    # TO TERMINATE
    for _ in range(num_cores):
        dir_queue.put(None)
    for t in threads:
        t.join()

    total_size = 0
    while not result_queue.empty():
        total_size += result_queue.get()

    size_mb = total_size / (1024 * 1024)
    size_gb = total_size / (1024 * 1024 * 1024)
    print(f"Total disk usage: {size_mb:.2f} MB ({size_gb:.2f} GB)")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python du_equivalent.py <directory> <number_of_cores>")
    else:
        directory = sys.argv[1]
        num_cores = int(sys.argv[2])
        DU(directory, num_cores)
