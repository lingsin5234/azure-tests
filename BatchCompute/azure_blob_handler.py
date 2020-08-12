# this is a quickstart tutorial to Azure Blob Storage
import os, uuid, urllib
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

try:
    print("Azure Blob Storage v12 - Python quickstart example")

    #  ---------  Quick start code here  ---------  #
    connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    connect_str = connect_str.replace("'", '').replace('$', '')  # remove auto-created $ and '

    #  --------- Create a container here ---------  #
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Create a unique name for the container
    container_name = "quickstart" + str(uuid.uuid4())

    # Create the container
    container_client = blob_service_client.create_container(container_name)

    #  --------- Upload blob to container --------- #
    # Create a file in local data directory to upload and download
    local_path = "groceryapp/data"
    local_file_name = "shoppers.jpg"
    upload_file_path = os.path.join(local_path, local_file_name)

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

    #  ---------  Download uploaded blob ---------  #
    # Download the blob to a local file
    # Add 'DOWNLOAD' before the .txt extension so you can see both files in the data directory
    local_path = 'azureblob/data'
    download_file_path = os.path.join(local_path, local_file_name)
    print("\nDownloading blob to \n\t" + download_file_path)

    with open(download_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    #  ---------   Delete the container  ---------  #
    # Clean up
    print("\nPress the Enter key to begin clean up")
    input()

    print("Deleting blob container...")
    container_client.delete_container()

    print("Deleting the local source and downloaded files...")
    os.remove(upload_file_path)
    os.remove(download_file_path)

    print("Done")

except Exception as e:
    print('Exception')
    print(e)
