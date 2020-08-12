# this is a quickstart tutorial to Azure Blob Storage
import os, uuid, urllib
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


# run the blob handler test function
def blob_handler_test(filename):
    try:
        print("Azure Blob Storage v12 - Python quickstart example")

        #  ---------  Quick start code here  ---------  #
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        connect_str = connect_str.replace("'", '').replace('$', '')  # remove auto-created $ and '
        print(connect_str)

        #  --------- Create a container here ---------  #
        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Create a unique name for the container
        container_name = "azure-compute" + str(uuid.uuid4())
        print(container_name)

        # Create the container
        container_client = blob_service_client.create_container(container_name)

        #  --------- Upload blob to container --------- #
        # Create a file in local data directory to upload and download
        local_path = "BatchCompute/data"
        local_file_name = filename
        upload_file_path = os.path.join(local_path, local_file_name)

        # Write text to the file
        file = open(upload_file_path, 'w')
        file.write("Testing... Azure Compute!")
        file.close()

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

        print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

        # Upload the created file
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data)

        #  --------- List blobs in container ---------  #
        print("\nListing blobs...")

        # List the blobs in the container
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            print("\t" + blob.name)
        print(blob_list)

        #  ---------  Download uploaded blob ---------  #
        # Download the blob to a local file
        # Add 'DOWNLOAD' before the .txt extension so you can see both files in the data directory
        local_download_path = "BatchCompute/download"
        download_file_path = os.path.join(local_download_path, str.replace(local_file_name, '.txt', 'DOWNLOAD.txt'))
        print("\nDownloading blob to \n\t" + download_file_path)

        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

        #  ---------   Delete the container  ---------  #
        # Clean up
        # print("\nPress the Enter key to begin clean up")
        # input()

        # print("Deleting blob container...")
        # container_client.delete_container()

        # print("Deleting the local source and downloaded files...")
        # os.remove(upload_file_path)
        # os.remove(download_file_path)

        print("Done")

    except Exception as e:
        print('Exception')
        print(e)

        output_dict = {
            'file_name': filename,
            'status': e,
            'container': ''
        }

    else:
        output_dict = {
            'file_name': filename,
            'status': 'Success',
            'container': container_name
        }

    return output_dict


# blob downloader
def blob_downloader(container_name, image_name):

    try:
        #  ---------  Local Directory Setup  ---------  #
        local_path = "RedisLog/data"
        download_file_path = os.path.join(local_path, image_name)
        print("Download File Path:", download_file_path)

        #  --------- Connection String Setup ---------  #
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        connect_str = connect_str.replace("'", '').replace('$', '')  # remove auto-created $ and '
        print("Connection String:", connect_str)

        #  --------- Create a container here ---------  #
        # Create the BlobServiceClient object which will be used to load the container client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Create a container client
        container_client = blob_service_client.get_container_client(container=container_name)

        #  --------- List blobs in container ---------  #
        print("\nListing blobs...")

        # List the blobs in the container
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            print("\t" + blob.name)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=image_name)

            #  ---------  Download uploaded blob ---------  #
            # Download the blob to a local file
            print("\nDownloading blob to \n\t" + download_file_path)

            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())

    except Exception as e:
        download_file_path = ''
        print(e)

    return download_file_path
