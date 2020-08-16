#################################################################
#                                                               #
#   azure_batch_functions.py                                    #
#   a group of functions used for setting up the blob storage,  #
#   batch service client, compute nodes, batch job and tasks.   #
#                                                               #
#################################################################
# from azure.storage.blob import BlockBlobService
import azure.storage.blob as azureblob
import azure.batch.batch_auth as batch_auth
import azure.batch as batch
import azure.batch.models as batchmodels
from azure.storage.blob import BlobSasPermissions
import BatchCompute.oper.azure_blob_handler as abh
import os, sys, time, io, re
import datetime as dte


'''
# DEPRECATED / other issue: https://github.com/Azure/azure-storage-python/issues/389
def createBlobClient():

    blob_client = azureblob.BlockBlobService(
        account_name=os.environ.get('AZURE_BLOB_ACCOUNT_NAME'),
        account_key=os.environ.get('AZURE_BLOB_ACCOUNT_KEY'))

    return blob_client
'''


def createBlobServiceClient():

    connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    connect_str = connect_str.replace("'", '').replace('$', '')  # remove auto-created $ and '
    print(connect_str)

    blob_service_client = azureblob.BlobServiceClient.from_connection_string(connect_str)

    return blob_service_client


def createContainerClient(blob_service_client, input_container_name):

    sas_token = azureblob.generate_container_sas(
        os.environ.get('AZURE_BLOB_ACCOUNT_NAME'),
        input_container_name,
        account_key=os.environ.get('AZURE_BLOB_ACCOUNT_KEY'),
        policy_id='my-access-policy-id'
    )

    container = azureblob.ContainerClient.from_container_url(
        container_url="https://" + os.environ.get('AZURE_BLOB_ACCOUNT_NAME') + ".blob.core.windows.net/" +
                      input_container_name,
        credential=sas_token
    )

    return container


def getContainerClient(input_container_name):

    sas_token = azureblob.generate_container_sas(
        os.environ.get('AZURE_BLOB_ACCOUNT_NAME'),
        input_container_name,
        account_key=os.environ.get('AZURE_BLOB_ACCOUNT_KEY'),
        policy_id='container_policy'
    )

    container = azureblob.ContainerClient.from_container_url(
        container_url="https://" + os.environ.get('AZURE_BLOB_ACCOUNT_NAME') + ".blob.core.windows.net/" +
                      input_container_name,
        credential=sas_token
    )

    return container


def uploadInputFiles(container_client, container_name, file_path, filenames, blob_name, loaded):

    input_file_paths = [os.path.join(file_path, filename) for filename in filenames]

    input_files = [uploadFile2Blob(container_client, container_name, input_file, loaded)
                   for input_file in input_file_paths]

    return input_files


def uploadFile2Blob(container_client, container_name, upload_file_path, loaded):

    # local file name
    filename = os.path.basename(upload_file_path)

    print("\nUploading to Azure Storage as blob:\n\t" + filename)
    if not loaded:
        # Upload the created file
        with open(upload_file_path, "rb") as data:
            container_client.upload_blob(filename, data=data)

    # blob_url = blob_client.getBlobUrl()
    account_name = os.environ.get('AZURE_BLOB_ACCOUNT_NAME')
    # blob_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}"
    container_url = blob_url = f"https://{account_name}.blob.core.windows.net/{container_name}"
    # print(blob_url, filename)
    print(container_url, filename)

    # return batchmodels.ResourceFile(http_url=blob_url, file_path=filename)
    return batchmodels.ResourceFile(storage_container_url=container_url)


def createBatchClient():

    credentials = batch_auth.SharedKeyCredentials(os.environ.get('AZURE_BATCH_ACCOUNT_NAME'),
                                                  os.environ.get('AZURE_BATCH_ACCOUNT_KEY'))

    batch_client = batch.BatchServiceClient(credentials, batch_url=os.environ.get('AZURE_BATCH_ACCOUNT_URL'))

    return batch_client


def createBatchPool(batch_client, pool_id):

    new_pool = batch.models.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="Canonical",
                offer="UbuntuServer",
                sku="18.04-LTS",
                version="latest"
            ),
            node_agent_sku_id="batch.node.ubuntu 18.04"),
        vm_size='STANDARD_A2_v2',   # VM Type/Size
        target_dedicated_nodes=1    # pool node count
    )
    batch_client.pool.add(new_pool)


def createBatchJob(batch_client, job_id, pool_id):

    job = batch.models.JobAddParameter(
        id=job_id,
        pool_info=batch.models.PoolInformation(pool_id=pool_id))
    batch_client.job.add(job)


def createTasks(batch_client, job_id, input_files, filenames):

    tasks = list()

    # Environment Variables
    acc_name = batchmodels.EnvironmentSetting(name='AZURE_BLOB_ACCOUNT_NAME',
                                              value=os.environ.get('AZURE_BLOB_ACCOUNT_NAME'))
    acc_key = batchmodels.EnvironmentSetting(name='AZURE_BLOB_ACCOUNT_KEY',
                                             value=os.environ.get('AZURE_BLOB_ACCOUNT_KEY'))

    # input_file = input_files[0]
    filename = filenames[0]
    # for idx, input_file in enumerate(input_files):
    task_commands = [
        # install latest requirements
        "/bin/bash -c \"python3 -m venv env && source env/bin/activate && " +
        "python3 -m pip install -r {} && deactivate\"".format(filenames[0]),

        # run the python script
        "/bin/bash -c \"python3 -m venv env && source env/bin/activate && " +
        "python3 -m calculate_hexgrid_standalone && deactivate\""
    ]

    '''
    # Status 0 -- printenv
    tasks.append(batch.models.TaskAddParameter(
        id='Status{}'.format(0),
        command_line="/bin/bash -c \"printenv\"",
        environment_settings=[acc_name, acc_key]
    ))
    '''

    # Task 0 -- install latest requirements
    tasks.append(batch.models.TaskAddParameter(
        id='Task{}'.format(0),
        command_line=task_commands[0],
        resource_files=[input_files[0]]
    ))

    # Status 1 -- pip list
    tasks.append(batch.models.TaskAddParameter(
        id='Status{}'.format(1),
        command_line="/bin/bash -c \"python3 -m venv env && source env/bin/activate && python3 -m pip list\"",
        environment_settings=[acc_name, acc_key]
    ))

    # Task 1 -- run the python script
    tasks.append(batch.models.TaskAddParameter(
        id='Task{}'.format(1),
        command_line=task_commands[1],
        resource_files=[input_files[1], input_files[2], input_files[3]],
        environment_settings=[acc_name, acc_key]
    ))

    # add tasks to task collection
    batch_client.task.add_collection(job_id, tasks)


def waitTaskCompletion(batch_client, job_id, timeout):

    timeout_expiration = dte.datetime.now() + timeout

    print("Monitoring all tasks for 'Completed' state, timeout in {}..."
          .format(timeout), end='')

    while dte.datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True
        else:
            time.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))


def printTaskOutput(batch_client, job_id, encoding=None):

    print('Printing task output...')

    tasks = batch_client.task.list(job_id)

    for task in tasks:

        node_id = batch_client.task.get(
            job_id, task.id).node_info.node_id
        print("Task: {}".format(task.id))
        print("Node: {}".format(node_id))

        stream = batch_client.file.get_from_task(
            job_id, task.id, 'stdout.txt')

        file_text = readStreamString(stream, encoding)
        print("Standard output:")
        print(file_text)


def readStreamString(stream, encoding):
    output = io.BytesIO()
    try:
        for data in stream:
            output.write(data)
        if encoding is None:
            encoding = 'utf-8'
        return output.getvalue().decode(encoding)
    finally:
        output.close()
    raise RuntimeError('could not write data to stream or decode bytes')


def printBatchException(batch_exception):

    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print('{}:\t{}'.format(mesg.key, mesg.value))
    print('-------------------------------------------')


def queryYorN(question, default="yes"):

    valid = {'y': 'yes', 'n': 'no'}
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError("Invalid default answer: '{}'".format(default))

    while 1:
        choice = input(question + prompt).lower()
        if default and not choice:
            return default
        try:
            return valid[choice[0]]
        except (KeyError, IndexError):
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")
