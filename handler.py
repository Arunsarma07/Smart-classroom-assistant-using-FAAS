#import necessary modules
import boto3
import face_recognition
import pickle
import numpy
import os
import csv

#S3 buckets
input_bucket = "inputbucketgroup13proj2"
output_bucket = "outputbucketgroup13proj2"

#initializing S3 and DynamoDB 
s3client = boto3.resource('s3')
dbclient = boto3.resource('dynamodb')
table = dbclient.Table('student_data')

# Function to read the 'encoding' file
def open_encoding(filename):
	file = open(filename, "rb")
	data = pickle.load(file)
	file.close()
	return data

#handler function
def face_recognition_handler(event, context):
	#fetch the bucket and file name
	bucket = event['Records'][0]['s3']['bucket']['name']
	key = event['Records'][0]['s3']['object']['key']
	file_name = key	

	#save the video at /tmp/ folder to perform the face recognition
	print("Downloading video file from input S3 bucket..")
	local_file_name = '/tmp/'+key
	s3client.Bucket(bucket).download_file(key, local_file_name)
	
	#fetch the known encoding data 
	encoding_data = open_encoding("encoding")
	
	#extracting the frames from video file
	path = "/tmp/"
	os.system("ffmpeg -i " + str(local_file_name) + " -r 1 " + str(path) + "image-%3d.jpeg")
	
	#extract face from the frame and encode the data
	print("Extracting frames from video...")
	extracted_image = face_recognition.load_image_file("/tmp/image-001.jpeg")
	extracted_face_encoding = face_recognition.face_encodings(extracted_image)[0]

	#compare the extracted encodings with the known encoding data 
	print("Performing face recognition..")
	index = 0
	for known_encoding in encoding_data['encoding']:
		results = face_recognition.compare_faces([known_encoding], extracted_face_encoding)
		if results[0] == True:
			break
		else:
			index=index+1
	
	#get the name of the person from the image
	name = encoding_data['name'][index]
	print("Face recognized successfully.. ")
	print("Name: ", name)

	#query the name in the DynamoDB table to get the year and major
	print("Querying the name from DynamoDB")
	response_db = table.get_item(
		Key={
			'name': name
		}
	)
	
	#save the results from the response
	details = response_db['Item']
	year = details['year']
	major = details['major']

	#printing the results to console
	print("File name: ", file_name)
	print("Person Name: ", name)
	print("Year: ", year)
	print("Major: ",major)

	#save the results into a .csv file and send it to output bucket
	split_name = file_name.split('.',1)[0]
	csv_file_name = split_name + '.csv'
	
	#create the csv file and save it in the /tmp/ folder
	with open('/tmp/'+csv_file_name, 'w', newline='') as file:
		writer = csv.writer(file)
		writer.writerow([file_name, major, year])
	
	#send the file to output S3 bucket
	s3client.Bucket(output_bucket).upload_file('/tmp/'+csv_file_name, csv_file_name)
	print('"Uploaded {0} to {1} bucket successfully!!"'.format(csv_file_name, output_bucket))
