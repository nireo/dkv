# dkv: distributed key-value storage

The idea behind this project is to learn about distributed systems by building a simple key-value storage. I'm not going to be implementing a efficient key-value store from scratch, since I'm going to be using [leveldb](https://github.com/syndtr/goleveldb) such that I can focus on making the distributed system.

I'm aiming to make dkv production ready but let's see where it goes!

## Buckets

dkb contains a bucket implementation for the underlying database in the `bucket/db.go` file. This makes creating data replications and other stuff easier without needing to create an additional database. It is quite simple, it takes in a byte identifier name for a bucket and prefixes all the keys with that id.
