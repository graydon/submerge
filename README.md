# Submerge
Experimental databasey thing

This is a very early and experimental sketch of a distributed database. Justification
and explanation will be forthcoming if it ever amounts to anything. Not serious yet.

Rough picture:

  - Tiered local storage
    - Small redb tables for hot data (BTrees, direct)
    - Large newel slabs for cold data (LSM, vectorized)
  - Simple replication and commit protocol (clepsydra)
  - Staged typed query language on top (brevet)
