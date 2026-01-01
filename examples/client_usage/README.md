# DEngine Client Example

Command-line interface demonstrating DEngine's CRUD operations with Raft consensus.

## Features

- Put/Update key-value pairs
- Delete keys
- Read values (eventual consistency)
- Linearizable reads (strong consistency)

## Usage

```bash
# Put key-value pair (creates or updates)
cargo run -- --endpoints "http://127.0.0.1:9083,http://127.0.0.1:9082,http://127.0.0.1:9081" put 42 100

# Get value (eventually consistent)
cargo run -- --endpoints "http://127.0.0.1:9083,http://127.0.0.1:9082,http://127.0.0.1:9081" get 42

# Lease read
cargo run -- --endpoints "http://127.0.0.1:9083,http://127.0.0.1:9082,http://127.0.0.1:9081" sget 42

# Linearizable read (strongly consistent)
cargo run -- --endpoints "http://127.0.0.1:9083,http://127.0.0.1:9082,http://127.0.0.1:9081" lget 42

# Delete key
cargo run -- --endpoints "http://127.0.0.1:9083,http://127.0.0.1:9082,http://127.0.0.1:9081" delete 42


```
