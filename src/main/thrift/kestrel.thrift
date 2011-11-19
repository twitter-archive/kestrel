namespace java net.lag.kestrel.thrift

struct Item {
  /* the actual data */
  1: binary data

  /* transaction ID, to be used in the `confirm` call */
  2: i32 xid
}

struct QueueInfo {
  /* the head item on the queue, if there is one */
  1: optional binary head_item

  /* # of items currently in the queue */
  2: i64 items

  /* total bytes of data currently in the queue */
  3: i64 bytes

  /* total bytes of journal currently on-disk */
  4: i64 journal_bytes

  /* age (in milliseconds) of the head item on the queue, if present */
  5: i64 age

  /* # of clients currently waiting to fetch an item */
  6: i32 waiters

  /* # of items that have been fetched but not confirmed */
  7: i32 open_transactions
}

service Kestrel {
  /*
   * Put one or more items into a queue.
   *
   * If the named queue doesn't exist, it will be created.
   *
   * Optionally, an expiration time can be set on the items. If they sit in
   * the queue without being fetched for longer than the expiration, then
   * they will vanish.
   *
   * Returns the number of items actually put. This may be fewer than were
   * requested if the queue has a size/length limit and its policy when full
   * is to refuse new items.
   */
  i32 put(1: string queue_name, 2: list<binary> items, 3: i32 expiration_msec = 0)

  /*
   * Get one or more items from a queue.
   *
   * If the timeout is set, then this call will block until at least
   * `max_items` have been fetched, or the timeout occurs. If the timeout
   * occurs, this call may return from zero to `max_items` items.
   *
   * If `auto_confirm` is true (the default), the fetched items will behave
   * as if a `confirm` call has been made for them already: they will be
   * permanently removed from the queue. The `xid` field in each `Item` will
   * be zero.
   */
  list<Item> get(1: string queue_name, 2: i32 max_items, 3: i32 timeout_msec = 0, 4: bool auto_confirm = 1)

  /*
   * Confirm a set of items previously fetched with `get`.
   * Returns the count of confirmed items.
   */
  i32 confirm(1: string queue_name, 2: set<i32> xids)

  /*
   * Abort a set of items previously fetched with `get`.
   * Returns the count of aborted items.
   */
  i32 abort(1: string queue_name, 2: set<i32> xids)

  /*
   * Return some basic info about a queue, and the head item if there is
   * at least one item in the queue. The item is not dequeued, and there is
   * no guarantee that the item still exists by the time this method
   * returns.
   */
  QueueInfo peek(1: string queue_name)

  /*
   * Flush (clear out) a queue. All unfetched items are lost.
   */
  void flush_queue(1: string queue_name)

  /*
   * Flush (clear out) ALL QUEUES. All unfetched items from all queues are
   * lost.
   */
  void flush_all_queues()

  /*
   * Delete a queue, removing any journal. All unfetched items are lost.
   * ("delete" is a reserved word in some thrift variants.)
   */
  void delete_queue(1: string queue_name)

  /*
   * Return a string form of the version of this kestrel server.
   */
  string get_version()
}

