# Security Policy

## Supported Versions

We release patches for security vulnerabilities. Which versions are eligible
receiving such patches depend on the CVSS v3.0 Rating:

| CVSS v3.0 | Supported Versions                        |
| --------- | ----------------------------------------- |
| 9.0-10.0  | Releases within the previous three months |
| 4.0-8.9   | Most recent release                       |

## Resolved Advisories

### asyncmy (optional `mysql` extra) — GHSA-qhqw-rrw9-25rm

`asyncmy <= 0.2.11` was affected by a SQL injection issue via crafted
dictionary keys. The `mysql` extra now requires `asyncmy >= 0.2.12`, which
contains the fix. Upgrade any environment still pinned to `0.2.11`.

## Reporting a Vulnerability

Please report (suspected) security vulnerabilities to
**[jesularag@gmail.com](mailto:jesularag@gmail.com)**. You will receive a response from
us within 48 hours. If the issue is confirmed, we will release a patch as soon
as possible depending on complexity but historically within a few days.
