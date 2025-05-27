#!/usr/bin/env python3
"""csv_to_pinecone.py

Encode CSV with text, year, location columns to Pinecone vector database.

Example usage:
pip install "pinecone-client>=3" langchain openai tiktoken pandas python-dotenv

export PINECONE_API_KEY="..."     # Pinecone
export OPENAI_API_KEY="..."       # OpenAI embeddings

python csv_to_pinecone.py \
       --csv data.csv \
       --index text-data \
       --chunk-size 800 \
       --overlap 120

CSV should have columns: text, year, location
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
        print(f"Index '{index_name}' already exists.")
        return
    print(f"Creating serverless index '{index_name}' (dim={dimension}, metric={metric}) ...")
    pc.create_index(
        name=index_name,
        dimension=dimension,
        metric=metric,
        spec=pinecone.ServerlessSpec(cloud="aws", region="us-east-1"),
    )
    # Wait until index is ready
    print("Waiting for index to be ready...")
    while True:
        desc = pc.describe_index(index_name)
        if desc.status.ready:
            print("Index is ready!")
            break

def validate_and_prepare_dataframe(csv_path: str) -> pd.DataFrame:
    """Load and validate CSV with required columns: text, year, location."""
    df = pd.read_csv(csv_path)
    
    # Check for required columns
    required_columns = {"text", "year", "location"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"CSV missing required columns: {', '.join(missing_columns)}")
    
    # Clean and normalize data
    df["text"] = df["text"].fillna("").astype(str)
    df["year"] = df["year"].fillna("").astype(str)
    df["location"] = df["location"].fillna("").astype(str)
    
    # Remove rows with empty text
    df = df[df["text"].str.strip() != ""]
    
    print(f"Loaded {len(df)} records from {csv_path}")
    return df

def build_vectors(df: pd.DataFrame, embedder, splitter, namespace: str | None = None):
    """Generate vector dictionaries suitable for Pinecone upsert."""
    total_chunks = 0
    
    for row_idx, row in df.iterrows():
        text = str(row["text"])
        
        # Split text into chunks if it's too long
        if len(text) <= splitter.chunk_size:
            chunks = [text]
        else:
            chunks = splitter.split_text(text)
        
        for chunk_idx, chunk in enumerate(chunks):
            total_chunks += 1
            
            # Create metadata for this chunk
            metadata = {
                "text": chunk,
                "year": str(row["year"]),
                "location": str(row["location"]),
                "chunk_index": chunk_idx,
                "total_chunks": len(chunks),
                "row_index": int(row_idx),
                "ingested_at": datetime.now().isoformat(),
            }
            
            # Generate unique ID for this vector
            vector_id = f"row_{row_idx}_chunk_{chunk_idx}_{uuid4().hex[:8]}"
            
            # Create embedding
            try:
                embedding = embedder.embed_query(chunk)
            except Exception as e:
                print(f"Error creating embedding for row {row_idx}, chunk {chunk_idx}: {e}")
                continue
                
            vector_dict = {
                "id": vector_id,
                "values": embedding,
                "metadata": metadata,
            }
            
            if namespace:
                vector_dict["namespace"] = namespace
                
            yield vector_dict
    
    print(f"Generated {total_chunks} chunks total")

def upsert_vectors_in_batches(index, vectors, batch_size: int = 100):
    """Upsert vectors to Pinecone in batches."""
    batch = []
    total_upserted = 0
    
    for vector in vectors:
        batch.append(vector)
        
        if len(batch) >= batch_size:
            try:
                index.upsert(batch)
                total_upserted += len(batch)
                print(f"Upserted {total_upserted} vectors...", flush=True)
                batch.clear()
            except Exception as e:
                print(f"Error upserting batch: {e}")
                # Continue with next batch
                batch.clear()
    
    # Upsert remaining vectors
    if batch:
        try:
            index.upsert(batch)
            total_upserted += len(batch)
            print(f"Upserted {total_upserted} vectors (final batch).", flush=True)
        except Exception as e:
            print(f"Error upserting final batch: {e}")
    
    return total_upserted

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Encode CSV with text, year, location columns to Pinecone vector database."
    )
    parser.add_argument("--csv", required=True, help="Path to CSV file (must have text, year, location columns)")
    parser.add_argument("--index", required=True, help="Pinecone index name")
    parser.add_argument("--chunk-size", type=int, default=800, help="Maximum chunk size in characters (default: 800)")
    parser.add_argument("--overlap", type=int, default=120, help="Chunk overlap in characters (default: 120)")
    parser.add_argument("--model", default="text-embedding-3-small", help="OpenAI embedding model (default: text-embedding-3-small)")
    parser.add_argument("--namespace", default=None, help="Optional Pinecone namespace")
    parser.add_argument("--batch-size", type=int, default=100, help="Upsert batch size (default: 100)")
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    # Validate environment variables
    if not os.getenv("PINECONE_API_KEY"):
        sys.exit("Error: PINECONE_API_KEY environment variable not set")
    if not os.getenv("OPENAI_API_KEY"):
        sys.exit("Error: OPENAI_API_KEY environment variable not set")
    
    print(f"Starting CSV to Pinecone encoding...")
    print(f"CSV file: {args.csv}")
    print(f"Index: {args.index}")
    print(f"Chunk size: {args.chunk_size}")
    print(f"Overlap: {args.overlap}")
    print(f"Embedding model: {args.model}")
    print(f"Namespace: {args.namespace or 'default'}")
    print("-" * 50)
    
    try:
        # Initialize Pinecone client
        pc = pinecone.Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
        
        # Initialize OpenAI embeddings
        embedder = OpenAIEmbeddings(model=args.model)
        
        # Test embedding to get dimension
        print("Testing embedding model...")
        test_embedding = embedder.embed_query("test")
        dimension = len(test_embedding)
        print(f"Embedding dimension: {dimension}")
        
        # Create index if needed
        create_index_if_needed(pc, args.index, dimension)
        index = pc.Index(args.index)
        
        # Initialize text splitter
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=args.chunk_size, 
            chunk_overlap=args.overlap
        )
        
        # Load and validate CSV
        df = validate_and_prepare_dataframe(args.csv)
        
        # Generate vectors
        print("Generating embeddings and upserting to Pinecone...")
        vectors = build_vectors(df, embedder, splitter, args.namespace)
        
        # Upsert to Pinecone
        total_upserted = upsert_vectors_in_batches(index, vectors, args.batch_size)
        
        print("-" * 50)
        print(f"Encoding complete!")
        print(f"Total vectors upserted: {total_upserted}")
        print(f"Index: {args.index}")
        
        # Show index stats
        stats = index.describe_index_stats()
        print(f"Index total vector count: {stats.total_vector_count}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 