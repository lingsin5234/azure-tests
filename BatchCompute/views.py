from django.shortcuts import render
from .azure_blob_handler import blob_handler_test


# initial azure compute test
def azure_compute(request):

    filename = 'Hello.txt'
    azure_blob = blob_handler_test(filename)
    container = azure_blob['container']
    print('INPUTS:', container, filename)

    return render(request, 'pages/azure_compute.html')
