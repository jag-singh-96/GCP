try:
    from google.cloud import storage
    import google.cloud.storage
    import json
    import os
    import sys
except Exception as e:
    print("Error : {} ".format(e))

client = storage.Client.from_service_account_json(
    json_credentials_path='/Users/sigmoid/Desktop/Python/assignment/lively-folder-325913-a08e79c8b716.json')


def createBucket(bucketName):
    print('Bucket is creating')
    try:
        bucket = client.create_bucket(bucketName)
        print("Bucket {} created.".format(bucket.name))
        return True
    except Exception as err:
        print(err.args)
        return False

def uploadCsv(file_name, file_path, bucketName):
    try:
        bucket = client.get_bucket(bucketName)
        blob = bucket.blob(file_name)
        blob.upload_from_filename(file_path)
        # print(bucket)
    except Exception as err:
        print(err.args)
        raise Exception('uploading file failed')


def bucketLists():
    try:
        buckets = client.list_buckets()
        buckets = list(buckets)
        return buckets
    except Exception as err:
        print(err.args)


def solve():
    try:
        buckets = bucketLists()
        if buckets:
            print(buckets)
        else:
            print('No bucket found')
            bucketCreated = createBucket('sig-buck')
            if bucketCreated == False:
                return
                raise Exception('failed bucket creating')

        uploadCsv('Customers.csv', '/Users/sigmoid/Desktop/Python/assignment/Customers.csv', 'sig-buck')
        uploadCsv('Orders.csv', '/Users/sigmoid/Desktop/Python/assignment/Orders.csv', 'sig-buck')
    except Exception as err:
        print(err.args)


if __name__ == '__main__':
    solve()
