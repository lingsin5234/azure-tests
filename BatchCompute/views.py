from django.shortcuts import render
from .oper.azure_blob_handler import blob_handler_test
from .oper import azure_batch_functions as abf
import datetime as dte
import azure.batch.models as batchmodels
import uuid, os, sys


# initial azure blob test
def azureBlob(request):

    filename = 'Hello.txt'
    azure_blob = blob_handler_test(filename)
    container = azure_blob['container']
    print('INPUTS:', container, filename)

    context = {
        'title': 'Blob'
    }

    return render(request, 'pages/azure_compute.html', context)


def azureCompute(request):

    start_time = dte.datetime.now().replace(microsecond=0)
    print('Sample start: {}'.format(start_time))
    print()

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.
    blob_name = 'python-test' + str(uuid.uuid4())
    # input_container_name = 'compute-test' + str(uuid.uuid4())
    input_container_name = 'compute-testcfc006ac-a052-4cba-a57e-25bbb8d7a4ab'
    blob_service_client = abf.createBlobServiceClient()

    # Use Blob Service Client to get the Container and the Blob Client
    # container = abf.createContainerClient(blob_service_client, input_container_name)
    container = abf.getContainerClient(input_container_name)
    print("Container Retrieved.")
    # blob_client = blob_service_client.get_blob_client(container=input_container_name, blob=blob_name)

    # Upload Input Files
    # filenames = ['Test0.txt', 'Test1.txt', 'Test2.txt']
    filenames = ['requirements.txt',
                 'hexgrid_constructor.py',
                 'calculate_hexgrid_standalone.py',
                 'stations_2020-01-01.json']
    input_files = abf.uploadInputFiles(container, input_container_name, 'BatchCompute/data', filenames, blob_name, False)
    print("INPUT FILES:", input_files)

    # Create a Batch service client. We'll now be interacting with the Batch service in addition to Storage
    batch_client = abf.createBatchClient()

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        # abf.createBatchPool(batch_client, os.environ.get('POOL_ID'))

        # Create the job that will run the tasks.
        # abf.createBatchJob(batch_client, os.environ.get('JOB_ID'), os.environ.get('POOL_ID'))

        # Add the tasks to the job.
        abf.createTasks(batch_client, os.environ.get('JOB_ID'), input_files, filenames)

        # Pause execution until tasks reach Completed state.
        abf.waitTaskCompletion(batch_client, os.environ.get('JOB_ID'), dte.timedelta(minutes=60))

        print("  Success! All tasks reached the 'Completed' state within the "
              "specified timeout period.")

        # Print the stdout.txt and stderr.txt files for each task to the console
        abf.printTaskOutput(batch_client, os.environ.get('JOB_ID'))

    except batchmodels.BatchErrorException as err:
        abf.printBatchException(err)
        # raise -- continue and delete container + job + pool

    # Clean up storage resources -- DO NOT DELETE THE CONTAINER
    # print('Deleting container [{}]...'.format(input_container_name))
    # blob_service_client.delete_container(input_container_name)

    # Print out some timing info
    end_time = dte.datetime.now().replace(microsecond=0)
    print()
    print('Sample end: {}'.format(end_time))
    print('Elapsed time: {}'.format(end_time - start_time))
    print()

    '''
    # Clean up Batch resources (if the user so chooses).
    if abf.queryYorN('Delete job?') == 'yes':
        batch_client.job.delete(os.environ.get('JOB_ID'))

    if abf.queryYorN('Delete pool?') == 'yes':
        batch_client.pool.delete(os.environ.get('POOL_ID'))
    '''

    print()
    input('Press ENTER to exit...')

    context = {
        'title': 'Blob'
    }

    return render(request, 'pages/azure_compute.html', context)
