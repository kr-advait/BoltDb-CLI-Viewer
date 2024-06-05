package main

import (
	"fmt"
	"log"
	"os"

	bolt "go.etcd.io/bbolt"
)

func listBuckets(db *bolt.DB) {
	err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			fmt.Println(string(name))
			return nil
		})
	})
	if err != nil {
		log.Fatal(err)
	}
}

func createBucket(db *bolt.DB, bucketName string) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
}

func deleteBucket(db *bolt.DB, bucketName string) {
	err := db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(bucketName))
	})
	if err != nil {
		log.Fatal(err)
	}
}

func listKeys(db *bolt.DB, bucketName string) {
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return fmt.Errorf("Bucket %s not found", bucketName)
		}

		return bucket.ForEach(func(k, _ []byte) error {
			fmt.Println(string(k))
			return nil
		})
	})
	if err != nil {
		log.Fatal(err)
	}
}

func readAllValues(db *bolt.DB, bucketName string) {
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return fmt.Errorf("Bucket %s not found", bucketName)
		}

		return bucket.ForEach(func(k, v []byte) error {
			fmt.Printf("Key: %s, Value: %s\n", k, v)
			return nil
		})
	})
	if err != nil {
		log.Fatal(err)
	}
}

func readKeyValue(db *bolt.DB, bucketName, key string) {
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return fmt.Errorf("Bucket %s not found", bucketName)
		}

		value := bucket.Get([]byte(key))
		if value == nil {
			return fmt.Errorf("Key %s not found in bucket %s", key, bucketName)
		}

		fmt.Printf("Value: %s\n", value)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

func insertKeyValue(db *bolt.DB, bucketName, key, value string) {
	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return fmt.Errorf("Bucket %s not found", bucketName)
		}

		return bucket.Put([]byte(key), []byte(value))
	})
	if err != nil {
		log.Fatal(err)
	}
}

func deleteKey(db *bolt.DB, bucketName, key string) {
	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return fmt.Errorf("Bucket %s not found", bucketName)
		}

		return bucket.Delete([]byte(key))
	})
	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Println("Usage:")
	fmt.Println("  listbuckets <db file>                       - List all buckets")
	fmt.Println("  createbucket <db file> <bucket name>        - Create a new bucket")
	fmt.Println("  deletebucket <db file> <bucket name>        - Delete a bucket")
	fmt.Println("  listkeys <db file> <bucket name>            - List all keys in a bucket")
	fmt.Println("  readall <db file> <bucket name>             - Read all keys and values in a bucket")
	fmt.Println("  readkey <db file> <bucket name> <key>       - Read a value for a specific key in a bucket")
	fmt.Println("  insert <db file> <bucket name> <key> <value> - Insert a key-value pair into a bucket")
	fmt.Println("  delete <db file> <bucket name> <key>        - Delete a key from a bucket")
}

func main() {
	if len(os.Args) < 3 {
		usage()
		os.Exit(1)
	}

	command := os.Args[1]
	dbFile := os.Args[2]

	db, err := bolt.Open(dbFile, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	switch command {
	case "listbuckets":
		listBuckets(db)
	case "createbucket":
		if len(os.Args) != 4 {
			usage()
			os.Exit(1)
		}
		bucketName := os.Args[3]
		createBucket(db, bucketName)
	case "deletebucket":
		if len(os.Args) != 4 {
			usage()
			os.Exit(1)
		}
		bucketName := os.Args[3]
		deleteBucket(db, bucketName)
	case "listkeys":
		if len(os.Args) != 4 {
			usage()
			os.Exit(1)
		}
		bucketName := os.Args[3]
		listKeys(db, bucketName)
	case "readall":
		if len(os.Args) != 4 {
			usage()
			os.Exit(1)
		}
		bucketName := os.Args[3]
		readAllValues(db, bucketName)
	case "readkey":
		if len(os.Args) != 5 {
			usage()
			os.Exit(1)
		}
		bucketName := os.Args[3]
		key := os.Args[4]
		readKeyValue(db, bucketName, key)
	case "insert":
		if len(os.Args) != 6 {
			usage()
			os.Exit(1)
		}
		bucketName := os.Args[3]
		key := os.Args[4]
		value := os.Args[5]
		insertKeyValue(db, bucketName, key, value)
	case "delete":
		if len(os.Args) != 5 {
			usage()
			os.Exit(1)
		}
		bucketName := os.Args[3]
		key := os.Args[4]
		deleteKey(db, bucketName, key)
	default:
		usage()
	}
}
