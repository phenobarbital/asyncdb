"""
Complete example of using ArangoDB for Graph RAG with AI-parrot.
"""
import asyncio
import pandas as pd
from typing import List, Dict
from pathlib import Path

# Assuming your imports
from asyncdb.drivers.arangodb import arangodb


async def example_basic_operations():
    """
    Example 1: Basic database operations
    """
    print("=== Example 1: Basic Operations ===\n")

    # Initialize connection
    db = arangodb(
        params={
            'host': 'localhost',
            'port': 8529,
            'username': 'root',
            'password': '12345678',
            'database': 'navigator'
        }
    )

    # Connect
    await db.connection()

    # Clean up first
    try:
        await db.drop_collection('documents')
    except Exception:
        pass

    # Create a collection
    await db.create_collection('documents', edge=False)

    # Insert a document
    doc = {
        'title': 'Introduction to Graph Databases',
        'content': 'Graph databases are powerful for connected data...',
        'tags': ['database', 'graphs', 'nosql']
    }

    result = await db.insert_document('documents', doc)
    print(f"Inserted document: {result['_key']}\n")

    # Query with AQL
    query = """
    FOR doc IN documents
        FILTER 'graphs' IN doc.tags
        RETURN doc
    """
    results, error = await db.query(query)
    print(f"Query results: {len(results)} documents found\n")

    # Fetch one row
    row = await db.fetch_one(query)
    print(f"First document: {row['title']}\n")

    # Get a single value
    val = await db.fetchval(
        "FOR doc IN documents RETURN doc.title",
        column=0
    )
    print(f"First title: {val}\n")

    await db.close()


async def example_graph_operations():
    """
    Example 2: Graph operations
    """
    print("=== Example 2: Graph Operations ===\n")

    db = arangodb(
        params={
            'host': 'localhost',
            'port': 8529,
            'username': 'root',
            'password': '12345678',
            'database': 'navigator'
        },
        default_graph='knowledge_graph'
    )

    await db.connection()

    # Clean up first (handle if doesn't exist)
    try:
        await db.drop_graph('knowledge_graph', drop_collections=True)
    except Exception:
        pass

    # Create graph
    graph = await db.create_graph(
        'knowledge_graph',
        edge_definitions=[{
            'edge_collection': 'knows',
            'from_vertex_collections': ['persons'],
            'to_vertex_collections': ['persons']
        }]
    )

    # Add vertices
    persons = [
        {'_key': 'alice', 'name': 'Alice', 'age': 30},
        {'_key': 'bob', 'name': 'Bob', 'age': 35},
        {'_key': 'charlie', 'name': 'Charlie', 'age': 28}
    ]

    for person in persons:
        await db.insert_document('persons', person)

    # Add edges
    await db.create_edge(
        'knowledge_graph',
        'knows',
        {'_from': 'persons/alice', '_to': 'persons/bob', 'since': 2020}
    )
    await db.create_edge(
        'knowledge_graph',
        'knows',
        {'_from': 'persons/bob', '_to': 'persons/charlie', 'since': 2021}
    )

    # Traverse the graph
    related = await db.traverse(
        start_vertex='persons/alice',
        direction='outbound',
        max_depth=2
    )

    print(f"Found {len(related)} related persons\n")

    # Find shortest path
    path = await db.shortest_path(
        'persons/alice',
        'persons/charlie',
        direction='any',
        graph_name='knowledge_graph'
    )

    print(f"Shortest path: {path}\n")

    await db.close()


async def example_bulk_write():
    """
    Example 3: Bulk write operations
    """
    print("=== Example 3: Bulk Write ===\n")

    db = arangodb(
        params={
            'host': 'localhost',
            'port': 8529,
            'username': 'root',
            'password': '12345678',
            'database': 'navigator'
        }
    )

    await db.connection()

    # Clean up first
    try:
        await db.drop_collection('products')
    except Exception:
        pass

    # Write from pandas DataFrame
    data = {
        'product_id': ['P001', 'P002', 'P003'],
        'name': ['Laptop', 'Mouse', 'Keyboard'],
        'price': [1200, 25, 75],
        'category': ['Electronics', 'Accessories', 'Accessories']
    }
    df = pd.DataFrame(data)

    await db.create_collection('products')
    count = await db.write(df, collection='products', batch_size=100)
    print(f"Inserted {count} products from DataFrame\n")

    # Write from CSV
    # csv_path = Path('data/products.csv')
    # count = await db.write(csv_path, collection='products')

    # Write from list of dicts
    new_products = [
        {'product_id': 'P004', 'name': 'Monitor', 'price': 300},
        {'product_id': 'P005', 'name': 'Webcam', 'price': 80}
    ]

    count = await db.write(new_products, collection='products')
    print(f"Inserted {count} more products\n")

    await db.close()


async def example_graph_rag():
    """
    Example 4: Complete Graph RAG implementation
    """
    print("=== Example 4: Graph RAG ===\n")

    db = arangodb(
        params={
            'host': 'localhost',
            'port': 8529,
            'username': 'root',
            'password': '12345678',
            'database': 'navigator'
        },
        default_graph='research_graph'
    )

    async with await db.connection() as rag:
        # Clean up first
        try:
            await db.drop_graph('research_graph', drop_collections=True)
        except Exception:
            pass
        # Create knowledge graph
        await rag.create_knowledge_graph(
            'research_graph',
            vertex_collections=['papers', 'authors', 'topics'],
            edge_collection='citations'
        )

        # Add research papers
        papers = [
            {
                'id': 'paper1',
                'content': 'Introduction to Neural Networks...',
                'metadata': {'year': 2020, 'journal': 'AI Review'}
            },
            {
                'id': 'paper2',
                'content': 'Deep Learning Applications...',
                'metadata': {'year': 2021, 'journal': 'ML Today'}
            }
        ]

        for paper in papers:
            await rag.add_document_node(
                'papers',
                paper['id'],
                paper['content'],
                metadata=paper['metadata']
            )

        # Add entities (authors, topics)
        await rag.add_entity_node(
            'authors',
            'author1',
            'researcher',
            properties={'name': 'Dr. Smith', 'institution': 'MIT'}
        )

        await rag.add_entity_node(
            'topics',
            'neural_networks',
            'concept',
            properties={'field': 'Machine Learning', 'category': 'Deep Learning'}
        )

        # Add relationships
        await rag.add_relationship(
            'citations',
            'papers/paper1',
            'papers/paper2',
            'cites',
            weight=1.0
        )

        await rag.add_relationship(
            'citations',
            'papers/paper1',
            'authors/author1',
            'authored_by',
            weight=1.0
        )

        await rag.add_relationship(
            'citations',
            'papers/paper1',
            'topics/neural_networks',
            'discusses',
            weight=0.9
        )

        # Find related nodes
        related = await rag.find_related_nodes(
            'papers/paper1',
            max_depth=2,
            limit=10,
            graph_name='research_graph'
        )

        print(f"Found {len(related)} related nodes\n")
        for item in related:
            print(f"  - {item['node']['_id']} (depth: {item['depth']})")

        # Get subgraph
        subgraph = await rag.get_subgraph(
            ['papers/paper1', 'papers/paper2'],
            max_depth=1
        )

        print(f"\nSubgraph contains:")
        print(f"  - {len(subgraph['nodes'])} nodes")
        print(f"  - {len(subgraph['edges'])} edges\n")

        # Centrality analysis
        centrality = await rag.centrality_analysis('papers', metric='degree')
        print("Top papers by degree centrality:")
        for node_id, score in sorted(centrality, key=lambda x: x[1], reverse=True)[:5]:
            print(f"  - {node_id}: {score}")


async def example_semantic_search():
    """
    Example 5: Semantic search with graph context
    """
    print("\n=== Example 5: Semantic Search with Graph Context ===\n")

    db = arangodb(
        params={
            'host': 'localhost',
            'port': 8529,
            'username': 'root',
            'password': '12345678',
            'database': 'navigator'
        }
    )

    async with await db.connection() as rag:

        # Simulate a query embedding (in production, use actual embeddings)
        query_embedding = [0.1] * 768  # Example 768-dim vector
        # Perform semantic search with graph context
        results = await rag.semantic_search_with_context(
            query_embedding,
            collection='papers',
            top_k=5,
            include_neighbors=True,
            neighbor_depth=1
        )

        print(f"Found {len(results)} results with graph context\n")

        for i, result in enumerate(results, 1):
            print(f"{i}. Document: {result['document']['_id']}")
            print(f"   Similarity: {result['similarity']:.4f}")
            print(f"   Graph context: {len(result.get('graph_context', []))} neighbors")
            print()


async def example_build_graph_from_documents():
    """
    Example 6: Build knowledge graph from documents
    """
    print("=== Example 6: Build Graph from Documents ===\n")

    db = arangodb(
        params={
            'host': 'localhost',
            'port': 8529,
            'username': 'root',
            'password': '12345678',
            'database': 'navigator'
        }
    )

    async with await db.connection() as rag:
        # drop at start:
        try:
            await db.drop_graph('python_knowledge', drop_collections=True)
        except Exception:
            pass
        # Sample documents
        documents = [
            {
                'id': 'doc1',
                'content': 'Python is a programming language created by Guido van Rossum.',
                'metadata': {'source': 'wiki'}
            },
            {
                'id': 'doc2',
                'content': 'Guido van Rossum started Python development in 1989.',
                'metadata': {'source': 'history'}
            },
            {
                'id': 'doc3',
                'content': 'Python is widely used in data science and machine learning.',
                'metadata': {'source': 'applications'}
            }
        ]

        # Simple entity extractor (in production, use NLP like spaCy)
        async def extract_entities(text: str) -> List[Dict]:
            # Simplified - in practice use NER
            entities = []
            keywords = {
                'Python': {'type': 'language', 'id': 'python'},
                'Guido van Rossum': {'type': 'person', 'id': 'guido'},
                'data science': {'type': 'field', 'id': 'data_science'},
                'machine learning': {'type': 'field', 'id': 'ml'}
            }

            for keyword, info in keywords.items():
                if keyword in text:
                    entities.append({
                        'id': info['id'],
                        'type': info['type'],
                        'text': keyword
                    })

            return entities

        # Simple relationship extractor
        async def extract_relationships(text: str, entities: List[Dict]) -> List[Dict]:
            relationships = []

            # Simplified pattern matching
            if 'created by' in text.lower():
                # Python created_by Guido
                relationships.append({
                    'from': 'python',
                    'to': 'guido',
                    'type': 'created_by',
                    'weight': 1.0
                })

            if 'used in' in text.lower():
                relationships.append({
                    'from': 'python',
                    'to': 'data_science',
                    'type': 'used_in',
                    'weight': 0.8
                })

            return relationships

        # Build the graph
        graph_name = await rag.build_graph_from_documents(
            documents,
            'python_knowledge',
            entity_extractor=extract_entities,
            relationship_extractor=extract_relationships
        )

        print(f"Built knowledge graph: {graph_name}\n")

        # Query the built graph
        query = """
        FOR doc IN documents
            FOR entity IN 1..1 OUTBOUND doc._id relationships
            RETURN {
                document: doc.content,
                entity: entity
            }
        """

        results, error = await db.query(query)

        print("Document-Entity relationships:")
        for result in results[:5]:  # Show first 5
            print(f"  - Doc mentions: {result['entity'].get('properties', {}).get('text', 'N/A')}")


async def example_error_handling():
    """
    Example 7: Proper error handling
    """
    print("\n=== Example 7: Error Handling ===\n")

    db = arangodb(
        params={
            'host': 'localhost',
            'port': 8529,
            'username': 'root',
            'password': '12345678',
            'database': 'navigator'
        }
    )

    try:
        await db.connection()

        # Query with error handling
        result, error = await db.query("FOR doc IN nonexistent RETURN doc")

        if error:
            print(f"Query error: {error}")
        else:
            print(f"Query successful: {len(result)} results")

        # Queryrow with error handling
        row, error = await db.queryrow("RETURN 42")

        if error:
            print(f"Queryrow error: {error}")
        else:
            print(f"Queryrow result: {row}")

        # Native fetch (raises exception on error)
        try:
            result = await db.fetch_all("FOR doc IN documents RETURN doc")
            print(f"Fetch all: {len(result)} documents")
        except Exception as e:
            print(f"Fetch error (exception): {e}")

    finally:
        await db.close()

async def example_arangosearch():
    """
    Example: ArangoSearch for full-text and vector search
    """
    print("\n=== Example: ArangoSearch ===\n")

    db = arangodb(
        params={
            'host': 'localhost',
            'port': 8529,
            'username': 'root',
            'password': '12345678',
            'database': 'navigator'
        }
    )

    await db.connection()

    # Clean up
    try:
        await db.drop_arangosearch_view('articles_search')
        await db.drop_collection('articles')
    except Exception:
        pass

    # Create collection
    await db.create_collection('articles')

    # Insert sample documents with embeddings
    import random
    articles = [
        {
            '_key': 'art1',
            'title': 'Introduction to Machine Learning',
            'content': 'Machine learning is a subset of artificial intelligence...',
            'category': 'AI',
            'embedding': [random.random() for _ in range(128)]
        },
        {
            '_key': 'art2',
            'title': 'Deep Learning Fundamentals',
            'content': 'Deep learning uses neural networks with multiple layers...',
            'category': 'AI',
            'embedding': [random.random() for _ in range(128)]
        },
        {
            '_key': 'art3',
            'title': 'Python Programming Guide',
            'content': 'Python is a versatile programming language...',
            'category': 'Programming',
            'embedding': [random.random() for _ in range(128)]
        }
    ]

    for article in articles:
        await db.insert_document('articles', article)

    # Create ArangoSearch view
    links = {
        'articles': {
            'fields': {
                'title': {'analyzers': ['text_en']},
                'content': {'analyzers': ['text_en']},
                'category': {'analyzers': ['identity']},
                'embedding': {'analyzers': ['identity']}
            }
        }
    }

    await db.create_arangosearch_view('articles_search', links)

    print("1. Full-text search:")
    results = await db.fulltext_search(
        'articles_search',
        'machine learning',
        fields=['title', 'content'],
        top_k=5
    )
    for r in results:
        print(f"   - {r['document']['title']} (score: {r['score']:.2f})")

    print("\n2. Vector search:")
    query_embedding = [random.random() for _ in range(128)]
    results = await db.vector_search(
        'articles_search',
        'articles',
        query_embedding,
        top_k=3
    )
    for r in results:
        print(f"   - {r['document']['title']} (similarity: {r['similarity']:.4f})")

    print("\n3. Hybrid search:")
    results = await db.hybrid_search(
        'articles_search',
        'articles',
        query_text='neural networks',
        query_vector=query_embedding,
        text_fields=['title', 'content'],
        text_weight=0.6,
        vector_weight=0.4,
        top_k=3
    )
    for r in results:
        print(f"   - {r['document']['title']}")
        print(f"     Combined: {r['combined_score']:.4f}, "
              f"Text: {r['text_score']:.4f}, "
              f"Vector: {r['vector_similarity']:.4f}")

    await db.close()


async def main():
    """
    Run all examples
    """
    # Run examples sequentially
    await example_basic_operations()
    await example_graph_operations()
    await example_bulk_write()
    await example_graph_rag()
    await example_semantic_search()
    await example_build_graph_from_documents()
    await example_error_handling()
    await example_arangosearch()

    print("\n=== All examples completed ===")


if __name__ == "__main__":
    asyncio.run(main())
