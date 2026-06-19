# Security Policy

## Supported Versions

We release patches for security vulnerabilities. Which versions are eligible
receiving such patches depend on the CVSS v3.0 Rating:

| CVSS v3.0 | Supported Versions                        |
| --------- | ----------------------------------------- |
| 9.0-10.0  | Releases within the previous three months |
| 4.0-8.9   | Most recent release                       |

## Known Accepted Risks

### asyncmy (optional `mysql` extra) — GHSA-qhqw-rrw9-25rm

`asyncmy` (used only by the optional `mysql` driver) is affected by a SQL
injection issue via crafted dictionary keys, and there is **no patched upstream
release** (all versions `<= 0.2.11` are affected). We pin the latest available
version (`0.2.11`) and accept the risk under the following condition:

- **Never pass dictionaries with untrusted / attacker-controlled keys** as query
  parameters to the MySQL driver. Parameter *values* are escaped correctly; the
  issue only affects *keys*, which in normal usage are developer-defined column
  names, not user input.

If your application forwards externally-controlled dict keys into queries, use
the `mariadb` extra (`aiomysql >= 0.3.2`) or `mysqlclient` instead.

## Reporting a Vulnerability

Please report (suspected) security vulnerabilities to
**[jesularag@gmail.com](mailto:jesularag@gmail.com)**. You will receive a response from
us within 48 hours. If the issue is confirmed, we will release a patch as soon
as possible depending on complexity but historically within a few days.
