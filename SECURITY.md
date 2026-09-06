# Security

LakeDucktor is experimental maintenance software with permission to modify
lake metadata and delete eligible storage objects. No version currently has
a guaranteed security-support period.

Do not report credentials, sensitive data, or exploitable vulnerabilities in
public issues. Use the repository's **Security → Report a vulnerability**
option if available. If it is unavailable, open a minimal issue requesting a
private reporting channel without disclosing the vulnerability.

Include the affected version or commit, configuration with secrets removed,
and a minimal reproduction using disposable data.

## Operating boundaries

- Select the intended lake explicitly with `MAINTAIN_LAKES`.
- Use a dedicated data root. Enable orphan cleanup only when the entire root
  belongs to that DuckLake; unrelated objects can become deletion candidates.
- Review persisted snapshot retention and file-deletion policy before starting.
- Keep health and metrics endpoints on a trusted network; they are unauthenticated.
- Use only trusted extensions. The current container enables unsigned extensions
  to load its source-built compatibility patch.
- Restrict database and storage credentials to the intended lake. Required
  privileges and backend-specific permission recipes still need validation.

The native error path redacts configured database/storage credentials and URL
userinfo before truncating messages. This is not a comprehensive sanitizer for
all provider messages or data. Review logs before sharing them.
