# Privacy

The HawDB plugin collects no data and sends nothing to Nowledge or any other
third party.

It runs inside the Dify plugin daemon. The tools run the statements and
parameters they are given against the HawDB database at the configured path
on that host, and return the rows to Dify. The retrieval endpoint receives
Dify's query and knowledge ID, runs the configured statement against that
database, and returns the matching records to the caller that presented the
configured API key. Whatever those statements write stays in that database.
