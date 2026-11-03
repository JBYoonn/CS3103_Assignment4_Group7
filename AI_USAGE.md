# AI / Coding-Assistant Usage

## Tools used

Claude AI: 
- Review client and server code and git history
- Running public tests
- Drafting documentation
- Second opinion on implementation and design choices

## One useful suggestion

The AI suggested `_shrink_send_buffer` in `solution_server.py`. 

The function makes each client's operating-system send buffer smaller (`SO_SNDBUF = 32 KiB`) and turns off TCP's batching of small messages (`TCP_NODELAY`), which is helpful for the system because the server cannot retrieve data once the operating system holds it. 

With a small buffer, a slow client's backlog stays in our own queue, where newer revisions can still replace older ones, making the queuing design more efficient. 

## One suggestion that was wrong, inefficient, or required modification

**Use case:** 

When we rerun `caption_source.py` against a server that is already running, no captions appear. The previous run ended at segment 398, and `accept_update` drops every update at or below `last_final_segment`:

```python
if seg < 1 or rev < 1 or seg <= self.last_final_segment:
    self.updates_ignored += 1
    return
```


The AI suggests clearing the lecture state whenever a source connects:

> A new `SOURCE_HELLO` should reset `last_final_segment`, `segment_state`, `history` and `active`, so `caption_source.py` can be rerun against the same server.

The team decided the suggestion is **incorrect** and not applicable to our system as it breaks the server's recovery guarantee, so we reject it entirely rather than modify it.

 It is incorrect because a new lecture and a source that reconnects after a short drop send the same `SOURCE_HELLO` message, which is ambiguous for the server to distinguish.
 
 If the server wipes its state on every `SOURCE_HELLO`, a short source drop will also wipe the history of the list of recent final captions it sends to reconnecting clients. For example, if a client drops at segment `110` while the source reconnects, the client comes back to an empty `HISTORY` and never receives segments `111` to `120`. This breaks the server's main guarantee that no final caption is lost.

The correction after is noticing that problem is from reusing a running server for a second lecture, while our design (`SERVER_DESIGN.md`) runs one lecture per server. We keep the server unchanged, so a reconnecting source simply continues its numbering (121, 122, …), and reconnecting clients still recover what they missed. 

To run a new lecture, we restart the server.

## Responsibility statement

We understand that the team remains responsible for the submitted code, experimental results, and claims, and that every member may be asked to explain or modify relevant code during the viva.
