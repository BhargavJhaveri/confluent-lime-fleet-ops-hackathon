#!/usr/bin/env python3
"""
Load Seattle event documents into MongoDB with embeddings.
Uses Azure OpenAI to generate embeddings, then inserts into MongoDB Atlas.
"""

import json
import sys
from pathlib import Path

import requests
import yaml
from pymongo import MongoClient

# Azure OpenAI embedding endpoint (from terraform state)
AZURE_OPENAI_ENDPOINT = "https://streaming-agents-openai-tlo852.openai.azure.com/openai/deployments/text-embedding-ada-002/embeddings?api-version=2024-08-01-preview"
AZURE_OPENAI_KEY = "3xVZXzGZtIxWJSQzNDrBi2rFojHSwl15AN1pDcRmnpnQbMMhHGxlJQQJ99CCACHYHv6XJ3w3AAABACOG3aeU"

# MongoDB Atlas
MONGODB_URI = "mongodb+srv://bhargav_hackathon:Bhargav%40123@cluster0.zgbnv2f.mongodb.net/"
MONGODB_DB = "lime_vector_search"
MONGODB_COLLECTION = "seattle_events"

DOCS_DIR = Path(__file__).parent.parent / "assets" / "hackathon" / "seattle_events_docs"


def get_embedding(text: str) -> list:
    """Get embedding from Azure OpenAI."""
    response = requests.post(
        AZURE_OPENAI_ENDPOINT,
        headers={
            "Content-Type": "application/json",
            "api-key": AZURE_OPENAI_KEY,
        },
        json={"input": text},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["data"][0]["embedding"]


def parse_markdown(file_path: Path) -> dict:
    """Parse markdown with YAML frontmatter."""
    content = file_path.read_text()
    if content.startswith("---"):
        parts = content.split("---", 2)
        if len(parts) >= 3:
            frontmatter = yaml.safe_load(parts[1])
            text = parts[2].strip()
        else:
            frontmatter = {}
            text = content
    else:
        frontmatter = {}
        text = content

    title = frontmatter.get("title", "")
    doc_id = frontmatter.get("document_id", file_path.stem)
    full_text = f"# {title}\n\n{text}" if title else text

    return {"document_id": doc_id, "chunk": full_text}


def main():
    print("Loading Seattle event documents into MongoDB with embeddings...\n")

    # Connect to MongoDB
    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DB]
    collection = db[MONGODB_COLLECTION]

    # Clear existing docs
    deleted = collection.delete_many({})
    print(f"Cleared {deleted.deleted_count} existing documents")

    # Process each markdown file
    md_files = sorted(DOCS_DIR.glob("*.md"))
    print(f"Found {len(md_files)} documents to process\n")

    for file_path in md_files:
        doc = parse_markdown(file_path)
        print(f"  Embedding: {doc['document_id']}...", end=" ", flush=True)

        try:
            embedding = get_embedding(doc["chunk"])
            collection.insert_one({
                "document_id": doc["document_id"],
                "chunk": doc["chunk"],
                "embedding": embedding,
            })
            print(f"OK ({len(embedding)} dims)")
        except Exception as e:
            print(f"FAILED: {e}")

    # Verify
    count = collection.count_documents({})
    print(f"\nDone! {count} documents in MongoDB '{MONGODB_DB}.{MONGODB_COLLECTION}'")

    client.close()


if __name__ == "__main__":
    main()
