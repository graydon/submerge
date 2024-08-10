# Submerge

## Experimental database thing

This is a very early and experimental sketch of a distributed database. Justification
and explanation will be forthcoming if it ever amounts to anything. Not serious yet.

## Slogan:

"A 21st century 4GL from a parallel universe where the web didn't win"

## Rough picture:

  - Nested-relational data model
  - Tiered local storage
    - Small BTree tables for hot data 
    - Large LSM slabs for cold data
  - High-consistency replication and commit protocol (Ocean Vista)
  - Low-consistency replication and commit protocol (Atomic CRDTs)
  - Typed query language on top (vectorized Ei calculus)
  - Incremental evaluation on top (Dyn-FO + staging)
  - Version control on top (nested branch-stage-update-commit)
  - Automated QA, workflow triggering and CI framework on top
  - Information-flow provenance/integrity/confidentiality
  - Continuous incremental cloud backup and replica-provision
  - End-to-end "full application" support:
    - Integrated simple auth and admin
    - Integrated simple multi-platform UI
    - Integrated simple billing / payment
  - External-system interoperation:
    - Pub/sub between administrative domains
    - Format and protocol adaptors for web & legacy systems
