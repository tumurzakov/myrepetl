# Evening Vibecoder: MyRepETL — MySQL ETL through Replication

Hey, colleagues! 👋 

Welcome back to the "evening vibecoder" series, and today I brought you **MyRepETL** ([GitHub link](https://github.com/tumurzakov/myrepetl)) — a tool for ETL through MySQL replication. 

## Why do you need this?

Classic scenario: you have a bunch of MySQL databases in microservices, and you need to pull all that data into **Metabase** for beautiful reports. 

The problem is:
- Each database has its own schema and structure
- Data needs to be combined and normalized
- Metabase loves when everything is in one place
- Manual export/import is a pain

**MyRepETL** solves this: takes data from all your databases, transforms it on the fly, and puts it into a single analytics database for Metabase.

## What MyRepETL can do

### 🚀 Key features

**Multi-threading out of the box**
- Each source runs in its own thread
- Don't block each other
- Automatically recovers from failures

**Flexible transformations**
- Rename tables and columns
- Calculated fields
- Data filtering
- Custom Python functions

**JSON configuration**
- Everything configured through config

## How to use

### Simple synchronization

The most basic case — just copy data from one database to another:

```json
{
  "sources": {
    "prod_db": {
      "host": "prod-mysql",
      "user": "repl_user", 
      "password": "repl_pass",
      "database": "production"
    }
  },
  "targets": {
    "backup_db": {
      "host": "backup-mysql",
      "user": "backup_user",
      "password": "backup_pass", 
      "database": "backup"
    }
  },
  "mapping": {
    "prod_db.users": {
      "source": "prod_db",
      "target": "backup_db",
      "source_table": "users",
      "target_table": "users"
    }
  }
}
```

### With transformations

Now let's add some magic — rename table, add calculated fields:

```json
{
  "mapping": {
    "prod_db.customers": {
      "source": "prod_db",
      "target": "analytics_db",
      "source_table": "customers",
      "target_table": "users",
      "column_mapping": {
        "id": {"column": "user_id"},
        "name": {"column": "full_name"},
        "email": {"column": "email"},
        "birth_date": {"column": "age", "transform": "transform.calculate_age"},
        "phone": {"column": "formatted_phone", "transform": "transform.format_phone"},
        "created_at": {"column": "registration_date"},
        "source": {"column": "source_system", "value": "production"}
      }
    }
  }
}
```

Create a `transform.py` file with your functions:

```python
# transform.py
def calculate_age(birth_date, row_data, table):
    from datetime import datetime
    if not birth_date:
        return None
    birth = datetime.strptime(birth_date, '%Y-%m-%d')
    return (datetime.now() - birth).days // 365

def format_phone(phone, row_data, table):
    if not phone:
        return None
    # 79991234567 -> +7 (999) 123-45-67
    return f"+7 ({phone[1:4]}) {phone[4:7]}-{phone[7:9]}-{phone[9:11]}"
```

### Running

```bash
# Install from GitHub
pip install git+https://github.com/tumurzakov/myrepetl.git

# Or clone and install locally
git clone https://github.com/tumurzakov/myrepetl.git
cd myrepetl
pip install -e .

# Run with config
myrepetl run config.json

# Or via Docker
docker run -v ./config.json:/app/config.json myrepetl:latest
```

*That's it, happy coding! 👨‍💻*
