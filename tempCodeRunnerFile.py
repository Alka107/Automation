import os
import time
import boto3
import schedule
from faker import Faker
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import csv
file_counter = 0


def generate_dummy_data_file(directory):
  global file_counter
  fake = Faker()

  if not os.path.exists(directory):
    os.makedirs(directory)

  file_counter += 1
  file_name = f"dummy_data_{file_counter}.csv" 
  file_path = os.path.join(directory, file_name)

  with open(file_path, 'w', newline='') as file: 
    csv_writer = csv.writer(file)
    csv_writer.writerow(["Name", "Email", "Phone", "SSN", "Address"])  

    for _ in range(10):
      
      
      name = fake.name()
      email = fake.email()
      phone = fake.phone_number()
      ssn = fake.ssn()
      address = fake.address().replace("\n", ", ")
      csv_writer.writerow([name, email, phone, ssn, address])
    

    print(f"Generated {file_path.replace(os.sep, '/')}")
  return file_path
def upload_to_s3(file_path, bucket_name):
    
    if not isinstance(file_path, (str, bytes, os.PathLike)):
        print(f"Invalid file path: {file_path}")
        return

    #s3 = boto3.client('s3')
    s3 = boto3.client('s3', region_name='your-bucket-region')  
    s3_file_name = os.path.basename(file_path)  
    try:
       
        s3.upload_file(file_path, bucket_name, s3_file_name)
        
        print(f"Uploaded {file_path.replace(os.sep, '/')} to s3://{bucket_name}/{s3_file_name}")
        return True
    except FileNotFoundError:
        print(f"The file {file_path} was not found.")
    except NoCredentialsError:
        print("Credentials not available.")
        return False 
    except PartialCredentialsError:
        print("Incomplete credentials provided.")
        return False 
    
    return False


def generate_and_upload(directory, bucket_name):
    file_path = generate_dummy_data_file(directory)
    if file_path: 
        uploaded = upload_to_s3(file_path, bucket_name)
        return uploaded
    return False

def schedule_file_generation(directory, bucket_name, interval_seconds, max_files_before_pause, pause_duration):
    uploaded_files_count = 0
    start_time = time.time()

    def job():
        nonlocal uploaded_files_count, start_time

        uploaded = generate_and_upload(directory, bucket_name)
        if uploaded:
            uploaded_files_count += 1

       
        if uploaded_files_count >= max_files_before_pause:
            elapsed_time = time.time() - start_time
            if elapsed_time < interval_seconds: 
                print(f"Paused for {pause_duration} seconds after uploading {uploaded_files_count} files.")
                time.sleep(pause_duration)
            uploaded_files_count = 0
            start_time = time.time()
   
    schedule.every(interval_seconds).seconds.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1) 
output_directory = input("Enter the output directory name: ")
bucket_name = input("Enter the bucket name: ")
interval_seconds = int(input("Enter the interval between file generations (seconds): "))
max_files_before_pause = int(input("Enter the maximum number of files before pausing: "))
pause_duration = int(input("Enter the pause duration in seconds: "))
schedule_file_generation(output_directory, bucket_name, interval_seconds, max_files_before_pause, pause_duration)
