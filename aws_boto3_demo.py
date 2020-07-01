"""
This is a python demo of boto3 library.
Written By: ANKHI PAUL
"""
import boto3

s3_resource = boto3.resource('s3')

#Create a Bucket
s3_resource.create_bucket(Bucket="first-aws-bucket-1")

#List all buckets in S3
for bucket in s3_resource.buckets.all():
    print(bucket.name)

#Uploading an object into the Bucket
s3_resource.Object('first-aws-bucket-1', 'Screen_Shot.png').\
    upload_file(Filename='/Users/ankhipaul/Documents/Screenshots/Screen_Shot.png')

#Downloading an object from Bucket to local
s3_resource.Object('pythonusecase', 'doc.pdf').download_file(
    f'/Users/ankhipaul/Documents/doc.pdf')


#List all objects of one bucket
pythonusecase = s3_resource.Bucket(name = 'pythonusecase')
for object in pythonusecase.objects.all():
          print(object.key)

#Copy object old_convertcsv.csv as object new_convertcsv.csv
s3_resource.Object("pythonusecase", "new_convertcsv.csv").copy_from(CopySource="pythonusecase/old_convertcsv.csv")

#Delete object old_convertcsv.csv
s3_resource.Object("pythonusecase", "old_convertcsv.csv").delete()

#Delete bucket first-aws-bucket-1
bucket = s3_resource.Bucket('first-aws-bucket-1')
bucket.objects.all().delete()
s3_resource.Bucket("first-aws-bucket-1").delete()


#Encrypting an object with ServerSideEncryption
s3_resource.Object('pythonusecase', 'random_pic.jpg').\
    upload_file(Filename='/Users/ankhipaul/Documents/random_pic.jpg',ExtraArgs={
                         'ServerSideEncryption': 'AES256'})

#Enable versioning of a Bucket
s3_resource.BucketVersioning("pythonusecase").enable()



