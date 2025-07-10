# Domain Model
This project provides a reusable means of partitioning your data by domains.
There are 2 key principles:
1. Deduplication across multiple sources by using a matching algorithm
2. Centralised identification of a subject

The example domain I have provided is patient, but the same concept could be used for any other central object. E.g. Employee, Colleague, etc.
This process can also account for multiple domains, however the sources table would need to be expanded, or potentially separated out if the domains differed significantly.

# Sources
This table contains each source of data, and the fields used to identify the domain subject within each.
As mentioned this is designed for person related domains, but this could be redesigned for other central subjects.
This table also contains the root table for each source, where the subject is stored (e.g the Patient table within each source)

# Table Links
This table will store the various links from the root table out, allowing all patient data to be retrievable.
This table tries to immitate Primary Key - Foreign Key relationships.
