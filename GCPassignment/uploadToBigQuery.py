try:
    from google.cloud import bigquery
    from google.cloud import storage
    from google.oauth2 import service_account
    import pandas as pd
    from pandas.io import gbq
except Exception as e:
    print(f"{e} found in this module")

jsonFile = 'lively-folder-325913-a08e79c8b716.json'
client = storage.Client.from_service_account_json(jsonFile)
bigQueryClient = bigquery.Client.from_service_account_json(jsonFile)


def getCsvFilesFromBucket(bucketName):
    try:
        csvLists = []
        for blob in client.list_blobs(bucketName):
            if '.csv' in str(blob.name):
                csvLists.append(str(blob.name))
        return csvLists

    except Exception as err:
        print(err.args)


def downloadFile(bigQueryObj):
    bucketName = bigQueryObj['bucketName']
    downloadedFilePath = bigQueryObj['downloadedFilePath']
    try:
        downloadedFilePathLists = []
        bucket = client.bucket(bucketName)
        csvLists = getCsvFilesFromBucket(bucketName)
        if not csvLists:
            return

        for i in range(len(csvLists)):
            blob = bucket.blob(csvLists[i])
            downloadedFilePathLists.append(downloadedFilePath + csvLists[i])
            blob.download_to_filename(downloadedFilePath + csvLists[i])

        print(
            "Blob {} downloaded to {}.".format(
                blob, downloadedFilePath + "FileName"
            )
        )

        return downloadedFilePathLists
    except Exception as err:
        print(err.args)
        raise Exception('failed in downloading the file')


def leftJoin(bigQueryObj):
    downloadedFilePathLists = bigQueryObj['downloadedFilePathLists']
    customerFrame = pd.read_csv(downloadedFilePathLists[0])
    orderFrame = pd.read_csv(downloadedFilePathLists[1])
    leftJoinData = pd.merge(customerFrame, orderFrame, on='CustomerID', how='left')
    return leftJoinData


def createBigQueryDataSet():
    dataset_id = ''
    try:
        dataset_id = "{}.customerDataset".format(client.project)
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = bigQueryClient.create_dataset(dataset, timeout=30)
        print('dataset created', dataset)

    except Exception as err:
        print(err.args)

    return dataset_id


def createBigQueryTable(bigQueryObj):
    datasetId = bigQueryObj['datasetId']
    tableId = ''
    try:
        tableId = "{}.mergedDataTable".format(datasetId)
        table = bigquery.Table(tableId)
        table = bigQueryClient.create_table(table)
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    except Exception as err:
        print(err.args)
    return tableId


def loadDataInBigQuery(bigQueryObj):
    'lively-folder-325913.customerDataset.mergedDataTable'

    leftJoinData = bigQueryObj['bigQueryObj']
    tableId = bigQueryObj['tableId']

    table = tableId.split('.')
    projectId = table[0]
    datasetName = table[1]
    tableName = table[2]

    try:
        print(leftJoinData)
        leftJoinData.to_gbq(destination_table='{}.{}'.format(datasetName, tableName), project_id='{}'.format(projectId),
                            if_exists="replace")
        print('done')
    except Exception as err:
        print(err.args)


def solve():

    bigQueryObj = {'downloadedFilePath': '/Users/sigmoid/Desktop/Python/assignment/downloaded',
                   'bucketName': 'sig-buck'}

    # downloadedFilePath = '/Users/sigmoid/Desktop/Python/assignment/downloaded'
    downloadedFilePathLists = downloadFile(bigQueryObj)
    bigQueryObj['downloadedFilePathLists'] = downloadedFilePathLists

    leftJoinData = leftJoin(bigQueryObj)
    bigQueryObj['leftJoinData'] = leftJoinData

    datasetId = createBigQueryDataSet()
    bigQueryObj['datasetId'] = datasetId

    tableId = createBigQueryTable(bigQueryObj)
    bigQueryObj['tableId'] = tableId

    loadDataInBigQuery(bigQueryObj)


if __name__ == '__main__':
    solve()
