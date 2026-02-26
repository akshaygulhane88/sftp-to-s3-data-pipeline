
from datetime import date
import time
import io
import boto3
from boto3.s3.transfer import TransferConfig
import paramiko
import  config_wiser as cnf

S3_BUCKET_NAME = cnf.S3_BUCKET
SFTP_FILE_PATH = cnf.PARENT_DIR_PATH
SFTP_HOST = cnf.SFTP_HOST
SFTP_PORT = cnf.SFTP_PORT
SFTP_USERNAME = cnf.SFTP_USERNAME
SFTP_PASSWORD = cnf.SFTP_PASSWORD
DESTINATION_PATH = cnf.DESTINATION_PATH

CHUNK_SIZE = 6291456

KB = 1024
MB = KB * KB

s3_connection = boto3.client("s3")
files_to_move = []
def open_sftp_connection(sftp_host, sftp_port, sftp_username, sftp_password):
    """
    Opens sftp connection and returns connection object
    """
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    try:
        transport = paramiko.Transport(sftp_host, sftp_port)
        print(transport)
    except Exception as e:
        return "conn_error"
    try:
        transport.connect(username=sftp_username, password=sftp_password)
    except Exception as identifier:
        return "auth_error"
    
    sftp_connection = paramiko.SFTPClient.from_transport(transport)
    print(sftp_connection)
    return sftp_connection


def move_files_processed(sftp_conn, source_file_name):
    """
    moves the transfered files to 'processed' folder
    """
    src = sftp_file_path  + source_file_name
    dest = sftp_file_path + 'T&F_Processed/' 
    print("source",src,"dest",dest)
    try:
        print("source_file_name :", source_file_name)
        sftp_conn.rename(src,dest)
        #time.sleep(15)
        # sftp_conn.remove(src)
        print("ok file moved")
    except Exception as error:
        print("error moving file, error: ", error)
        
def transfer_file_from_sftp_to_s3(sftp_conn,bucket_name, sftp_file_path, s3_file_path, sftp_username, sftp_password, chunk_size):
    print("started transfer_file_from_sftp_to_s3")
    sftp_connection = open_sftp_connection(SFTP_HOST, int(SFTP_PORT), sftp_username, sftp_password)
    files_list = sftp_connection.listdir(sftp_file_path)
    files_to_upload=[item for item in files_list if "csv" in item]
    print(files_list, "files_list")
    try:
        
        for files in files_to_upload:
            sftp_file = sftp_connection.file(sftp_file_path+files, "rb")
    
            sftp_file_size = sftp_file._get_size()
            print("sftp file size is :", sftp_file_size)
    
            print('files', files)
            print('s3 files path', s3_file_path+files)
            start_time = time.time()
            config = TransferConfig(multipart_chunksize=10 * MB, multipart_threshold=12 * MB, max_concurrency=10)
            s3_connection.upload_fileobj(sftp_file, bucket_name, s3_file_path+files, Config=config)
            end_time = time.time()
            files_to_move.append(files)
            sftp_file.close()
            print(files," Transferred to S3!","time =",end_time - start_time)
            
        
            #sys.exit() # remove this if you dont want to fail workflow for file not found condition    
    except Exception as error:
        print("file ingestion failed, error: ", error)


if __name__ == "__main__":

    sftp_username = SFTP_USERNAME
    sftp_password = SFTP_PASSWORD
    current_date = date.today()
    sftp_file_path = SFTP_FILE_PATH
    #s3_file_path = DESTINATION_PATH+str(current_date)+"/"
    s3_file_path= "product_scraped_data/books/wiser/prod/"+str(current_date)+"/"
    sftp_connection = open_sftp_connection(SFTP_HOST, int(SFTP_PORT), sftp_username, sftp_password)
    if sftp_connection == "conn_error":
        print("Failed to connect SFTP Server!")
    elif sftp_connection == "auth_error":
        print("Incorrect username or password!")
    else:
        p_start_time = time.time()
        transfer_file_from_sftp_to_s3(sftp_connection, S3_BUCKET_NAME, sftp_file_path, s3_file_path,
                                      sftp_username, sftp_password, CHUNK_SIZE)
        p_end_time = time.time()
        print("file transfer completed in ",p_end_time - p_start_time)
        if files_to_move:
            sftp_connection = open_sftp_connection(SFTP_HOST, int(SFTP_PORT), sftp_username, sftp_password) 
            for uploaded_file in files_to_move:
                print("uploaded file is ", uploaded_file)
                move_files_processed(sftp_connection,uploaded_file)
        else:
            print("nothing to upload")
        