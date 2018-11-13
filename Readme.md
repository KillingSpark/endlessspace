# ELS #
```
EEEEEEEEEE     LL            SSSSSSS  
E              LL           S       S  
E              LL            SS  
EEEEEEEE       LL               SS  
E              LL           S      S  
EEEEEEEEEE     LLLLLLLL      SSSSSS
``` 
_**E**_ nd - _**L**_ ess - _**S**_ pace

# How would you like a database that can store your data without using space in your File-Explorer? #
Basic idea: store data in the names of files instead of the files. 

If anyone has a valid usecase for this thing, PLEASE let me know.

## Buckets ##
Bucktes are directories. The Bucketnames are hashed into a fixed size that fits into the size of the max lenght of a filename on most filesystems.

## Values ##
Values are directories containing files that encode the data in their names. These are called 'ValueBuckets'.
Because filenames are not infitely long, we need to chunk data into multiple chunks. These get prefixed with their number in the list of chunks. The resulting filenames look like this:

```  
    filename := base64enc(varintenc(index)) + base64(dataChunk)
```

Write into the fileStructure:
1. Encode data with base64
1. Encode next index as Varint and base64
2. The rest of the name is filled with data
3. Create file with this name
4. While more data is left, repeat


For reconstruction:
1. Split filename into index/datachunk
2. decode index into int
3. save with index into slice
4. repeate with all files
5. concatenate all chunks
6. decode concatenated chunks
7. return data


## Accessing data ##
You define your bucket with a list of bucket names : ["myBigBucket", "mySmallerBucket", "myValueBucket"].
After opening the Bucket with a ELS instance, you can Write and Read to it. (Read is a bit wonky if your buffer is too small better use ReadValue and be done with it. The Read is more of a joke implementation in this absolute joke of a library).

## JSON to ELS ##
* Name -> Bucket of Values or Names
* Value -> Collection of Files as described above
* Arrays -> Bucket with a sentinel entry thats marks it as an array

Writing: 
1. Get all paths through the json to the values. Every path has its own els bucket
2. Write the values into these bucktes

Reading:
1. Split the schema into pathes. open bucket of theses pathes
2. Read the buckets
3. merge the pathes into one
4. return the merged jsonfile
Arrays are very wonky. Everything in this repo is. What did you expect?
