# Text protocol for kestrel

## Assumptions

All items in queues are assumed to be encoded such that they have no embedded linefeeds. Typically
this would be json.

Linefeeds are used to terminate items in requests and responses.

All times are in milliseconds, relative to now.

## Responses

Responses always start with a single character that tells you what kind of response it is, and ends
with a linefeed.

Success/failure will either indicate the number of successful things done, or a human error message.

    +<number>
    -<error message>

Items will be prefixed with a colon and end with a linefeed.

    :<item>

If multiple items were requested, the last item will be followed by a success indicator telling you
how many items you got.

If you specified a timeout or were just peeking, you may get "no item", represented by a star.

    *

## Requests

Put items into a queue. Response will be success/failure.

    put <queue> [expiration]:
    <item>
    <item>
    ...
    <empty-line>

Get items from a queue. Response will be one or more items. All fetched items are "transactional" --
if you disconnect before confirming them, they may be given to other consumers. Any fetch operation
is an implicit confirmation of a previous fetch operation, or in other words, every fetch operation
closes the previous transaction and opens a new one.

    peek <queue> [timeout]

    get <queue> [timeout]

    get-many <queue> [timeout]

You can explicitly confirm previous fetches if you like.

    confirm <queue> <count>


flush
delete
flush_all
shutdown
flush_expired
flush_all_expired
roll
version
quit
reload
