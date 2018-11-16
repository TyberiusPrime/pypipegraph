import pypipegraph
import urllib2

# This creates a global Pipegraph object
# All new jobs will automatically register with it.
pypipegraph.new_pipegraph()

url = "http://code.google.com/p/pypipegraph"  # what website to download.
target_file = "website.html"  # where to store the downloaded website
output_filename = "result.tab"  # where to store the final counts


# now, we need a function that downloads from url and stores to target_file
def download():
    request = urllib2.urlopen(url)
    data = request.read()
    request.close()
    file_handle = open(target_file, "wb")
    file_handle.write(data)
    file_handle.close()


# and we turn it into a FileGeneratingJob
# which receives two parameters: the filename, and the function to call
# function will be called without parameters.
# Note how we pass in the function download, not it's result download()
job_download = pypipegraph.FileGeneratingJob(target_file, download)


# read the downloaded file, count characters, and write to output_filename
def count_characters_and_write_output():
    file_handle = open(target_file, "rb")
    data = file_handle.read()
    count = len(data[data.find("<body"):])
    file_handle.close()
    file_handle = open(output_filename, "wb")
    file_handle.write("%s\t%i\n" % (url, count))
    file_handle.close()


# this also becomes a FileGeneratingJob
job_count = pypipegraph.FileGeneratingJob(
    output_filename, count_characters_and_write_output
)
# now, job_count needs target_file, which is created by job_download, and we need to tell it so
job_count.depends_on(job_download)

# and submit the go on
pypipegraph.run_pipegraph()
# first job_download will be run by calling download(), then job_count will be executed.
# You'll see lines like 'Done 0 of 2 jobs, 1 running', followed by 'Pipegraph done. Executed 4 jobs. 0 failed.
