# this script is to run the hexgrid constructor as a standalone file -- for azure batch
import hexgrid_constructor as hc
import azure.storage.blob as azureblob
import os, re, json

# initialize variables
bot_lat = 24
top_lat = 50
mid_lat = (bot_lat + top_lat) / 2
levels = 8
bbox = [-126, bot_lat, -66.5, top_lat]  # USA
cellSide = 15
input_container_name = 'compute-testcfc006ac-a052-4cba-a57e-25bbb8d7a4ab'


# write to blob
def writeToBlob(input_container_name, filename, data):

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

    try:
        container.upload_blob(filename, data=data)
    except Exception as e:
        print("Upload to Blob: Failed", e)
    else:
        print("Upload to Blob: Success!")

    return True


def runHexGrid():
    list_files = os.listdir("")
    json_files = [f for f in list_files if re.search(r'^stations.*.json$', f)]
    if len(json_files) > 0:
        json_file = json_files[0]
        this_date = re.sub(r'^stations-(.*).json$', '\\1', json_file)

        try:
            hexGrid = hc.hexgrid_constructor(bbox, cellSide, json_file, levels, mid_lat)
            print("Calculations completed!")
            filename = 'hexGrid_' + str(this_date) + '.json'
            try:
                # add it to blob
                writeToBlob(input_container_name, filename, hexGrid)
            except Exception as e2:
                print('Write Calculation File to Blob: Failed', e2)
            else:
                print('Write Calculation File to Blob: Success!')
        except Exception as e1:
            print('Calculations Failed.', e1)
        else:
            print('HexGrid Constructor Function Completed.')
    else:
        print("No JSON file found. Exit.")

    return True


# run HexGrid
runHexGrid()
