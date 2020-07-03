##############################################################
#### Written By: ANKHI PAUL                               ####
#### Written On: 03-MAR-2020                              ####
#### Modified On 03-MAR-2020                              ####
#### Objective: AWS Lambda script written in Python       ####
####                 to read csv file from S3             ####
##############################################################

import boto3
s3 =  boto3.client('s3') #boto3 is Python SDK for AWS
URL = "https://maps.googleapis.com/maps/api/geocode/json"
key = config.api_key
bucket = 'pythonusecase'

def lambda_handler(event, context):
    # Reading file into dictionary - https://stackoverflow.com/questions/42312196/how-do-i-read-a-csv-stored-in-s3-with-csv-dictreader
    csvfile=s3.get_object(Bucket=bucket, Key='employee_demo.txt')
    csvfile = csv.DictReader(codecs.getreader('utf-8')(csvfile[u'Body']))
    
    # In memory file writing
    csvio = io.StringIO()
    writer = csv.DictWriter(csvio, fieldnames=["Name", "Address_line_1", "Postal_Code"])
    writer.writeheader()
    
    for row in csvfile:
        print(row)
        # defining a params dict for the parameters to be sent to the API
        PARAMS = {'address': row['Address_line_1'],'key': key}
        # sending get request and saving the response as response object
        r = requests.get(url=URL, params=PARAMS)

        # extracting data in json format
        output = r.json()
        correct_postal_code = output['results'][0]['address_components'][7]['long_name']
        print('Correct postal code:'+correct_postal_code)
        row['Postal_Code'] = correct_postal_code
        writer.writerow(row)
    print('============Put output file into s3 =========')
    #s3 - https://stackoverflow.com/questions/57768041/how-do-i-use-aws-lambda-to-create-csv-file-from-json-data
    s3.put_object(Body=csvio.getvalue(), ContentType='text/csv', Bucket=bucket, Key='employee_demo_out.txt') 
    csvio.close()
        
    return {
        'statusCode': 200,
        'message': 'Success'
    }