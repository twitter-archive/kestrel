namespace java net.lag.kestrel.thrift
namespace rb Kestrelthrift

exception KestrelException {
  1: string description
}

struct Item {
  /* the actual data */
  1: binary data

  /* transaction ID, to be used in the `confirm` call */
  2: optional i32 xid
}


/**
 * A simple memcache-like service, which stores strings by key/value.
 * You should replace this with your actual service.
 */
service Kestrel {
  list<Item> get(1: string queue_name, 2: i32 max_items = 1, 3: bool reliable = 0)

  i32 put(1: string queue_name, 2: list<binary> items)

  void confirm(1: string queue_name, 2: set<i32> xids)
  void abort(1: string queue_name, 2: set<i32> xids)
  void flush(1: string queue_name)
}
