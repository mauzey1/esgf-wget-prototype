
from django.http import HttpResponse
from django.shortcuts import render

import urllib.request
import urllib.parse
import datetime
import json

from .local_settings import ESGF_SOLR_SHARDS, ESGF_SOLR_URL, WGET_SCRIPT_FILE_LIMIT

def home(request):
    return HttpResponse('esgf-wget')

def generate_wget_script(request):

    query_url = ESGF_SOLR_URL + '/select?q=*:*&wt=json&facet=true&fq=type:File&sort=id%20asc'

    if len(ESGF_SOLR_SHARDS) > 0:
        query_url += '&shards=%s'%(','.join(ESGF_SOLR_SHARDS))

    query_url += '&rows={rows}&limit={limit}&fq=dataset_id:{datasets}'

    # Gather dataset_ids
    if request.GET.get('dataset_id'):
        dataset_id_list = request.GET.getlist('dataset_id')
    else:
        return HttpResponse('No datasets selected.')

    if len(dataset_id_list) == 1:
        datasets = urllib.parse.quote_plus(dataset_id_list[0])
    else:
        datasets = '({})'.format(urllib.parse.quote_plus(' || '.join(dataset_id_list)))

    # Fetch the number of files for the query
    query = query_url.format(rows=1, limit=WGET_SCRIPT_FILE_LIMIT, datasets=datasets)
    with urllib.request.urlopen(query) as url:
        results = json.loads(url.read().decode('UTF-8'))
    num_files = results['response']['numFound']

    # Limit the number of files to the maximum
    wget_warn = None
    if num_files == 0:
        return HttpResponse('No files found for datasets.')
    elif num_files > WGET_SCRIPT_FILE_LIMIT:
        wget_warn = 'Warning! The total number of files was {} ' \
                    'but this script will only process {}.'.format(num_files, WGET_SCRIPT_FILE_LIMIT)
        num_files = WGET_SCRIPT_FILE_LIMIT

    # Fetch files for the query
    file_list = []
    query = query_url.format(rows=num_files, limit=WGET_SCRIPT_FILE_LIMIT, datasets=datasets)
    with urllib.request.urlopen(query) as url:
        results = json.loads(url.read().decode('UTF-8'))
    for file_info in results['response']['docs']:
        filename = file_info['title']
        checksum_type = file_info['checksum_type'][0]
        checksum = file_info['checksum'][0]
        for url in file_info['url']:
            url_split = url.split('|')
            if url_split[2] == "HTTPServer":
                file_list.append(dict(filename=filename, 
                                        url=url_split[0], 
                                        checksum_type=checksum_type, 
                                        checksum=checksum))
                break

    # Build wget script
    current_datetime = datetime.datetime.now()
    timestamp = current_datetime.strftime("%Y/%m/%d %H:%M:%S")

    context = dict(timestamp=timestamp, files=file_list, warning_message=wget_warn)
    wget_script = render(request, 'wget-template.sh', context)

    script_filename = current_datetime.strftime("wget-%Y%m%d%H%M%S.sh")

    response = HttpResponse(wget_script, content_type='text/x-sh')
    response['Content-Disposition'] = 'attachment; filename={}'.format(script_filename)
    return response