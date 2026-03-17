## MongoDB Atlas Quick Setup (5 min)

### 1. Create Free Cluster
- Go to https://cloud.mongodb.com (sign up free if needed)
- Create a free M0 cluster (any cloud provider/region)

### 2. Create Database User
- Security -> Database Access -> Add New Database User
- Username: `hackathon`
- Password: (choose one, note it down)
- Role: Atlas Admin (or readWrite on `lime_vector_search`)

### 3. Allow Network Access
- Security -> Network Access -> Add IP Address
- Click "Allow Access from Anywhere" (0.0.0.0/0)
- This is required for Confluent Cloud Flink to connect

### 4. Get Connection String
- Deployment -> Database -> Connect
- Choose "Drivers" -> Copy the connection string
- It looks like: `mongodb+srv://cluster0.xxxxx.mongodb.net/`
- Note: just the host part, no username/password in the URL

### 5. Create Vector Search Index
- Deployment -> Database -> Browse Collections
- Create database: `lime_vector_search`
- Create collection: `seattle_events`
- Go to Atlas Search tab -> Create Search Index
- Choose "JSON Editor" and use this config:

```json
{
  "fields": [
    {
      "type": "vector",
      "path": "embedding",
      "numDimensions": 1024,
      "similarity": "cosine"
    }
  ]
}
```

- Index name: `vector_index`
- Collection: `lime_vector_search.seattle_events`

### 6. Update Flink SQL
Replace the placeholders in `00-vector-db-setup.sql`:
- `<YOUR_MONGODB_HOST>` -> your cluster hostname (e.g., `cluster0.xxxxx.mongodb.net`)
- `<YOUR_MONGODB_USER>` -> `hackathon`
- `<YOUR_MONGODB_PASSWORD>` -> your password
