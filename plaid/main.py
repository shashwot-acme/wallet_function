import base64
import os
import glob
import tempfile
from time import sleep
import plaid
from dotenv import load_dotenv
from google.cloud import storage
from plaid.api import plaid_api
from plaid.model.institutions_get_request import InstitutionsGetRequest
from plaid.model.country_code import CountryCode
from plaid.model.institutions_get_request_options import InstitutionsGetRequestOptions


def main(data,context):

    load_dotenv()
    PROJECT_ID = os.getenv('PROJECT_ID')
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    TEMP_DIR = tempfile.gettempdir()
    FOLDER_NAME = os.getenv('FOLDER_NAME')
    CREATED_FOLDER = os.path.join(TEMP_DIR, FOLDER_NAME)
    PLAID_ID = os.getenv('PLAID_ID')
    PLAID_SECRET = os.getenv('PLAID_SECRET')

    # Credentials for cloud storage
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./cloud_storage.json"

    if os.path.exists(CREATED_FOLDER):
        os.system(str("rm -rf "+CREATED_FOLDER))
        path = os.path.join(TEMP_DIR, FOLDER_NAME)
        os.mkdir(path)
    else:
        path = os.path.join(TEMP_DIR, FOLDER_NAME)
        os.mkdir(path)

    configuration = plaid.Configuration(
        host=plaid.Environment.Sandbox,
        api_key={
            'clientId': PLAID_ID,
            'secret': PLAID_SECRET,
        }
    )
    
    initial_request = InstitutionsGetRequest(
        country_codes=[CountryCode('US')],
        count=1,
        offset=1,
        options=InstitutionsGetRequestOptions(
            include_optional_metadata=True
        )
    )

    api_client = plaid.ApiClient(configuration)
    client = plaid_api.PlaidApi(api_client)
    initial_response = client.institutions_get(initial_request)
    initial_institutions = initial_response['total']

    # Loop through the institution Lists
    limit_value=500
    total_length=initial_institutions
    if total_length >= limit_value:
        offset_value = round(total_length/limit_value)
    else:
        offset_value = round(total_length/limit_value)+1

    for j in range(offset_value):
        request = InstitutionsGetRequest(
            country_codes=[CountryCode('US')],
            count=limit_value,
            offset=j,
            options=InstitutionsGetRequestOptions(
                include_optional_metadata=True
            )
        )
        response = client.institutions_get(request)
        institutions = response['institutions']
        
        print(response)
        
        if j == 7:
            print("Limit 7 reached")
            sleep(60)
        if j == 14:
            print("Limit 14 reached")
            sleep(60)
        if j == 21:
            sleep(60)
        if j > 25:
            exit()

        for i in range(len(institutions)):
            get_inst_id = institutions[i].institution_id
            get_inst_logo = institutions[i].logo
            if get_inst_logo is not None:
                cwd = os.getcwd()
                change_dir = str(CREATED_FOLDER)
                os.chdir(change_dir)
                decoded_data = base64.b64decode((get_inst_logo))
                img_file = open(str(get_inst_id+".jpg"), 'wb')
                img_file.write(decoded_data)
                img_file.close()
                os.chdir(cwd)
            else:
                pass

    STORAGE_CLIENT = storage.Client(project=PROJECT_ID)
    BUCKET = STORAGE_CLIENT.bucket(BUCKET_NAME)
    assert os.path.isdir(CREATED_FOLDER)
    for local_file in glob.glob(CREATED_FOLDER + '/**'):
        if not os.path.isfile(local_file):
            print("File "+str(local_file)+" ignored")
            continue
        else:
            remote_path = os.path.join(
                FOLDER_NAME, os.path.split(local_file)[-1])
            blob = BUCKET.blob(remote_path)
            blob.upload_from_filename(local_file)


if __name__ == "__main__":
    main('data','context')
    print("Success")
