#!/usr/bin/env python3
"""ingest_pinecone.py

Ingest a CSV of text records (+ metadata) into a Pinecone serverless index for RAG.

Example:
    python ingest_pinecone.py --csv policies.csv --index energy-policies \
                              --chunk-size 800 --overlap 120

Environment variables required:
    PINECONE_API_KEY   – your Pinecone key
    PINECONE_ENV       – e.g. "us-east1-gcp" (for older 2.x clients) **ignored** when using serverless
    OPENAI_API_KEY     – key for embedding calls

Dependencies:
    pip install "pinecone-client>=3" langchain openai tiktoken pandas python-dotenv
"""
import argparse
import os
import sys
from uuid import uuid4
from datetime import datetime

import pandas as pd
import pinecone

try:
    from langchain.embeddings.openai import OpenAIEmbeddings
    from langchain.text_splitter import RecursiveCharacterTextSplitter
except ImportError as e:
    sys.exit("Missing langchain (pip install langchain).")

def create_index_if_needed(pc, index_name: str, dimension: int, metric: str = "cosine"):
    """Create a serverless index if it does not exist."""
    if index_name in pc.list_indexes().names():
        return
    print(f"Creating serverless index '{index_name}' (dim={dimension}, metric={metric}) ...")
    pc.create_index(
        name=index_name,
        dimension=dimension,
        metric=metric,
        spec=pinecone.ServerlessSpec(cloud="aws", region="us-east-1"),
    )
    # Wait until index is ready
    while True:
        desc = pc.describe_index(index_name)
        if desc.status.ready:
            break

def normalise_dataframe(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    
    # Convert column names to lowercase and replace spaces with underscores
    # This handles: "State" -> "state", "Policy Name" -> "policy_name", "Content" -> "content"
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["date_iso"] = df["date"].dt.strftime("%Y-%m-%d")
    else:
        df["date_iso"] = ""
    
    # Ensure required columns exist after transformation
    required = {"content"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing required columns: {', '.join(missing)}")
    return df

def build_vectors(df: pd.DataFrame, embedder, splitter, namespace: str | None = None):
    """Yield vector dicts suitable for Pinecone upsert."""
    for _, row in df.iterrows():
        text = str(row["content"])
        # Let the splitter handle chunking - it will return a list even for short text
        chunks = splitter.split_text(text)
        # If no chunks returned, use the original text
        if not chunks:
            chunks = [text]
        
        for idx, chunk in enumerate(chunks):
            metadata = {
                "state": str(row.get("state", "")),
                "date": str(row.get("date_iso", "")),
                "policy_name": str(row.get("policy_name", "")),
                "chunk_id": idx,
                "text": chunk,
            }
            yield {
                "id": f"{row.get('policy_name', 'row')}_{idx}_{uuid4().hex[:8]}",
                "values": embedder.embed_query(chunk),
                "metadata": metadata,
                # Remove namespace from individual vector - it goes in upsert call instead
            }

def upsert_batches(index, vectors, batch_size: int = 100, namespace: str | None = None):
    batch = []
    count = 0
    for vec in vectors:
        batch.append(vec)
        if len(batch) == batch_size:
            # Pass namespace to upsert method, not in individual vectors
            index.upsert(batch, namespace=namespace)
            count += len(batch)
            print(f"Upserted {count} vectors …", flush=True)
            batch.clear()
    if batch:
        index.upsert(batch, namespace=namespace)
        count += len(batch)
        print(f"Upserted {count} vectors (final batch).", flush=True)

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest CSV into Pinecone for RAG.")
    parser.add_argument("--csv", required=True, help="Path to CSV file to ingest")
    parser.add_argument("--index", required=True, help="Pinecone index name")
    parser.add_argument("--chunk-size", type=int, default=800, help="Chunk size in characters")
    parser.add_argument("--overlap", type=int, default=120, help="Chunk overlap in characters")
    parser.add_argument("--model", default="text-embedding-3-small", help="OpenAI embedding model")
    parser.add_argument("--namespace", default=None, help="Optional Pinecone namespace")
    parser.add_argument("--batch", type=int, default=100, help="Upsert batch size")
    return parser.parse_args()

def main():
    args = parse_args()

    # Init Pinecone client (v3+)
    pc = pinecone.Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
    embedder = OpenAIEmbeddings(model=args.model)

    dimension = len(embedder.embed_query("dimension_check"))
    create_index_if_needed(pc, args.index, dimension)
    index = pc.Index(args.index)

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=args.chunk_size, chunk_overlap=args.overlap
    )
    df = normalise_dataframe(args.csv)

    vectors = build_vectors(df, embedder, splitter, args.namespace)
    upsert_batches(index, vectors, args.batch, args.namespace)

    print("Ingestion complete.")

if __name__ == "__main__":
    main()