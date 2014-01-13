#!/usr/bin/env python

import argparse
import imghdr
import Queue
import os
import sys
import tempfile
import threading
import time
import urllib2

def download(url, timeout):
    """Downloads a file at given URL."""
    f = urllib2.urlopen(url, timeout=timeout)
    if f is None:
        raise Exception('Cannot open URL {0}'.format(url))
    content = f.read()
    f.close()
    return content

def imgtype2ext(typ):
    """Converts an image type given by imghdr.what() to a file extension."""
    if typ == 'jpeg':
        return 'jpg'
    if typ is None:
        raise Exception('Cannot detect image type')
    return typ

def make_directory(path):
    if not os.path.isdir(path):
        os.makedirs(path)

def download_imagenet(list_filename, out_dir, timeout=10, num_jobs=1, verbose=False):
    """Downloads to out_dir all images whose names and URLs are written in file
    of name list_filename.
    """

    make_directory(out_dir)

    count_total = 0
    with open(list_filename) as list_in:
        for line in list_in:
            count_total += 1

    sys.stderr.write('Total: {0}\n'.format(count_total))

    num_jobs = max(num_jobs, 1)

    entries = Queue.Queue(num_jobs)
    done = [False]

    counts_fail = [0 for i in xrange(num_jobs)]
    counts_success = [0 for i in xrange(num_jobs)]

    def producer():
        with open(list_filename) as list_in:
            for line in list_in:
                name, url = line.strip().split()
                entries.put((name, url), block=True)

        entries.join()
        done[0] = True

    def consumer(i):
        while not done[0]:
            try:
                name, url = entries.get(timeout=1)
            except:
                continue

            try:
                content = download(url, timeout)
                ext = imgtype2ext(imghdr.what('', content))
                directory = os.path.join(out_dir, name.split('_')[0])
                try:
                    make_directory(directory)
                except:
                    pass
                path = os.path.join(directory, '{0}.{1}'.format(name, ext))
                with open(path, 'w') as f:
                    f.write(content)
                counts_success[i] += 1
            except Exception as e:
                counts_fail[i] += 1
                if verbose:
                    sys.stdout.write('Error: {0}: {1}\n'.format(name, e))

            entries.task_done()

    def message_loop():
        if verbose:
            delim = '\n'
        else:
            delim = '\r'

        while not done[0]:
            count_success = sum(counts_success)
            count = count_success + sum(counts_fail)
            rate_done = count * 100.0 / count_total
            if count == 0:
                rate_success = 0
            else:
                rate_success = count_success * 100.0 / count
            sys.stderr.write(
                '{0} / {1} ({2}%) done, {3} / {0} ({4}%) succeeded                    {5}'.format(
                    count, count_total, rate_done, count_success, rate_success, delim))

            time.sleep(2)
        sys.stderr.write('done')

    producer_thread = threading.Thread(target=producer)
    consumer_threads = [threading.Thread(target=consumer, args=(i,)) for i in xrange(num_jobs)]
    message_thread = threading.Thread(target=message_loop)

    producer_thread.start()
    for t in consumer_threads:
        t.start()
    message_thread.start()

    # Explicitly wait to accept SIGINT
    try:
        while producer_thread.isAlive():
            time.sleep(1)
    except:
        sys.exit(1)

    producer_thread.join()
    for t in consumer_threads:
        t.join()
    message_thread.join()

    sys.stderr.write('\ndone\n')

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('list', help='Imagenet list file')
    p.add_argument('outdir', help='Output directory')
    p.add_argument('--jobs', '-j', type=int, default=1,
                   help='Number of parallel threads to download')
    p.add_argument('--timeout', '-t', type=int, default=10,
                   help='Timeout per image in seconds')
    p.add_argument('--verbose', '-v', action='store_true',
                   help='Enable verbose messages')
    args = p.parse_args()

    download_imagenet(args.list, args.outdir,
                      timeout=args.timeout, num_jobs=args.jobs,
                      verbose=args.verbose)
