
from django.http import HttpResponse
from django.shortcuts import render

import urllib.request
import datetime
import json

from .local_settings import ESGF_SOLR_SHARDS, ESGF_SOLR_URL, WGET_SCRIPT_FILE_LIMIT

def home(request):
    return HttpResponse('esgf-wget')

def generate_wget_script(request):

    query_url = ESGF_SOLR_URL + '/select?q=*:*&wt=json&facet=true&fq=type:File&sort=id%20asc'

    if len(ESGF_SOLR_SHARDS) > 0:
        query_url += '&shards=%s'%(','.join(ESGF_SOLR_SHARDS))

    query_url += '&rows={rows}&fq=dataset_id:{dataset_id}'

    # Gather dataset_ids
    if request.GET.get('dataset_id'):
        dataset_id_list = request.GET.getlist('dataset_id')
    else:
        return HttpResponse('No datasets selected.')

    # Fetch dataset file numbers that are within the file limit
    dataset_file_total = 0
    datasets_num_files = []
    for dataset_id in dataset_id_list:
        # Query for the number of files
        query = query_url.format(rows=1, dataset_id=dataset_id)
        with urllib.request.urlopen(query) as url:
            results = json.loads(url.read().decode('UTF-8'))
        num_files = results['response']['numFound']
        if num_files > 0:
            dataset_file_total += num_files
            if dataset_file_total >= WGET_SCRIPT_FILE_LIMIT:
                num_files = num_files - (dataset_file_total - WGET_SCRIPT_FILE_LIMIT)
                datasets_num_files.append(dict(dataset_id=dataset_id,num_files=num_files))
                break
            else:
                datasets_num_files.append(dict(dataset_id=dataset_id,num_files=num_files))

    if dataset_file_total == 0:
        return HttpResponse('No files found for datasets.')

    # Fetch files for datasets
    file_list = []
    for dataset_info in datasets_num_files:
        num_files = dataset_info['num_files']
        dataset_id = dataset_info['dataset_id']

        # Query files
        query = query_url.format(rows=num_files, dataset_id=dataset_id)
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

    context = dict(timestamp=timestamp, datasets=dataset_id_list, files=file_list)
    wget_script = render(request, 'wget-template.sh', context)

    script_filename = current_datetime.strftime("wget-%Y%m%d%H%M%S.sh")

    response = HttpResponse(wget_script, content_type='application/sh')
    response['Content-Disposition'] = 'attachment; filename={}'.format(script_filename)
    return response