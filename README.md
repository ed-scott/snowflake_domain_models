# Domain Model
This project provides a reusable means of partitioning your data by domains.
There are 2 key principles:
1. Deduplication across multiple sources by using a matching algorithm
2. Centralised identification of a subject

The example domain I have provided is patient, but the same concept could be used for any other central object. E.g. Employee, Colleague, etc.
This process can also account for multiple domains, however the sources table would need to be expanded, or potentially separated out if the domains differed significantly.
