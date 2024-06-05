# BoltDB cli viewer

## - CLI utility written in go for reading boltDB data

### Usage :- 
```
listbuckets <db file>                       - List all buckets
createbucket <db file> <bucket name>        - Create a new bucket
deletebucket <db file> <bucket name>        - Delete a bucket
listkeys <db file> <bucket name>            - List all keys in a bucket
readall <db file> <bucket name>             - Read all keys and values in a bucket
readkey <db file> <bucket name> <key>       - Read a value for a specific key in a bucket
insert <db file> <bucket name> <key> <value> - Insert a key-value pair into a bucket
delete <db file> <bucket name> <key>        - Delete a key from a bucket

```