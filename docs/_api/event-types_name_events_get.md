---
title: Event schema
position: 100
---
The HTTP response on the wire will look something like this (the newline is show as `\n` for clarity):

```sh
curl -v https://localhost:8080/event-types/order_received/events 
    

HTTP/1.1 200 OK
Content-Type: application/x-json-stream

{"cursor":{"partition":"0","offset":"6"},"events":[...]}\n
{"cursor":{"partition":"0","offset":"5"},"events":[...]}\n
{"cursor":{"partition":"0","offset":"4"},"events":[...]}\n
```
