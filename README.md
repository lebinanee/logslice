# logslice

Stream and filter structured JSON logs from multiple services with a unified query syntax.

---

## Installation

```bash
pip install logslice
```

Or install from source:

```bash
git clone https://github.com/yourname/logslice.git && cd logslice && pip install .
```

---

## Usage

Stream logs from one or more services and filter them using a simple query syntax:

```bash
# Stream logs from a single service and filter by level
logslice stream --service api --query "level=error"

# Tail logs from multiple services with a field filter
logslice stream --service api,worker,scheduler --query "level=warn AND status>=500"

# Output pretty-printed JSON
logslice stream --service api --query "user_id=42" --pretty
```

You can also pipe existing log files through logslice:

```bash
cat app.log | logslice filter --query "level=error AND response_time>200"
```

### Query Syntax

| Operator | Example | Description |
|----------|---------|-------------|
| `=` | `level=error` | Exact match |
| `!=` | `env!=prod` | Not equal |
| `>` / `<` | `status>499` | Numeric comparison |
| `AND` | `level=error AND service=api` | Logical AND |
| `OR` | `level=warn OR level=error` | Logical OR |

---

## Configuration

Create a `logslice.toml` in your project root to define service sources:

```toml
[services]
api     = "ssh://logs.internal/api/app.log"
worker  = "/var/log/worker/app.log"
```

---

## License

MIT © 2024 Your Name