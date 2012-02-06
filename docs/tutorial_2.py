import pypipegraph 
import urllib2
import hashlib
pypipegraph.new_pipegraph() 

output_filename = 'result.tab'  # where to store the final counts

# each call to download_job will return a job that downloads just this url. 
def download_job(url):
    target_file = 'website_%s' % hashlib.md5(url).hexdigest() # we need a unique name for each target file.
    def do_download():
        request = urllib2.urlopen(url)
        data = request.read()
        request.close()
        file_handle = open(target_file, 'wb')
        file_handle.write(data)
        file_handle.close()
    return pypipegraph.FileGeneratingJob(target_file, do_download)

urls = ['http://code.google.com/p/pypipegraph',
        'http://code.google.com/p/pypipegraph/w/list',
        ]
jobs_for_downloading = [download_job(url) for url in urls]

# now, this needs to be more fancy as well.
def count_characters_and_write_output():
    counts = {}
    for url in urls:
        dj = download_job(url) # this will get us the *same* object as download_job(url) returned the first time
        target_file = dj.job_id # the job_id for FileGeneratingJob is the output filename.

        file_handle = open(target_file, 'rb')
        data = file_handle.read()
        count = len(data)
        counts[url] = len(data)
        file_handle.close()
    file_handle = open(output_filename, 'wb')
    for url, count in counts.items():
        file_handle.write("%s\t%i\n" % (url, count))
    file_handle.close()

# this also becomes a FileGeneratingJob
job_count = pypipegraph.FileGeneratingJob(output_filename, count_characters_and_write_output)
# and we can tell it to depend on all jobs at once!
job_count.depends_on(jobs_for_downloading)

# and submit the go on 
pypipegraph.run_pipegraph()




