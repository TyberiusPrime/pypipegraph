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

def retrieve_urls():
    # now I said we were downloading these, but to make the tutorial independand,
    # we'll fake that bit, ok?
    # just pretend ;).
    return ['http://code.google.com/p/pypipegraph',
            'http://code.google.com/p/pypipegraph/w/list'
            ]

# this function will be executed after pypipegraph.run_pipegraph and returns
# a list of jobs that job_count will then depend on.
urls = None
def generate_download_jobs():
    jobs = []
    global urls # we use a global to communicate with the later job
    # please note that this is only possible in Job generating and data loading jobs,
    # not in the output jobs.
    urls = retrieve_urls()
    for url in urls:
        jobs.append(download_job(url))
    jobs.append( # this makes sure that if you remove urls, the output job would also rerun.
            # adding urls not seen before would make it rerun either way.
            pypipegraph.ParameterInvariant('retrieved_urls', urls) )
    return jobs

# A DependencyInjectionJob allows us to make job_count depend on the jobs returned by generate_download_jobs
# during the runtime of the graph
job_generate = pypipegraph.DependencyInjectionJob('retrieve_url', generate_download_jobs)

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
job_count.depends_on(job_generate)

# and submit the go on 
pypipegraph.run_pipegraph()




