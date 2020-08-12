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

    blob_client = abf.createBlobClient()

    # Use the blob client to create the containers in Azure Storage if they don't yet exist.

    input_container_name = 'compute-test' + str(uuid.uuid4())
    blob_client.create_container(input_container_name)  # , fail_on_exist=False)

    # Upload Input Files
    filenames = ['Test0.txt', 'Test1.txt', 'Test2.txt']
    input_files = abf.uploadInputFiles(blob_client, input_container_name, 'BatchCompute/data', filenames)
    print("INPUT FILES:", input_files)

    # Create a Batch service client. We'll now be interacting with the Batch service in addition to Storage
    batch_client = abf.createBatchClient()

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        abf.createBatchPool(batch_client, os.environ.get('POOL_ID'))

        # Create the job that will run the tasks.
        abf.createBatchJob(batch_client, os.environ.get('JOB_ID'), os.environ.get('POOL_ID'))

        # Add the tasks to the job.
        abf.createTasks(batch_client, os.environ.get('JOB_ID'), input_files)

        # Pause execution until tasks reach Completed state.
        abf.waitTaskCompletion(batch_client, os.environ.get('JOB_ID'), dte.timedelta(minutes=30))

        print("  Success! All tasks reached the 'Completed' state within the "
              "specified timeout period.")

        # Print the stdout.txt and stderr.txt files for each task to the console
        abf.printTaskOutput(batch_client, os.environ.get('JOB_ID'))

    except batchmodels.BatchErrorException as err:
        abf.printBatchException(err)
        # raise -- continue and delete container + job + pool

    # Clean up storage resources
    print('Deleting container [{}]...'.format(input_container_name))
    blob_client.delete_container(input_container_name)

    # Print out some timing info
    end_time = dte.datetime.now().replace(microsecond=0)
    print()
    print('Sample end: {}'.format(end_time))
    print('Elapsed time: {}'.format(end_time - start_time))
    print()

    # Clean up Batch resources (if the user so chooses).
    if abf.queryYorN('Delete job?') == 'yes':
        batch_client.job.delete(os.environ.get('JOB_ID'))

    if abf.queryYorN('Delete pool?') == 'yes':
        batch_client.pool.delete(os.environ.get('POOL_ID'))

    print()
    input('Press ENTER to exit...')

    context = {
        'title': 'Blob'
    }

    return render(request, 'pages/azure_compute.html', context)
