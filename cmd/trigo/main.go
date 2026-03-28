package main

import (
	"fmt"
	"log"
	"os"

	"github.com/carmel/triplestore/internal/encoding"
	"github.com/carmel/triplestore/internal/storage"
	"github.com/carmel/triplestore/rdf"
	"github.com/carmel/triplestore/sparql/executor"
	"github.com/carmel/triplestore/sparql/optimizer"
	"github.com/carmel/triplestore/sparql/parser"
	"github.com/carmel/triplestore/store"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: trigo <command> [args]")
		fmt.Println("Commands:")
		fmt.Println("  demo         - Run a demo with sample data")
		fmt.Println("  query <q>    - Execute a SPARQL query")
		fmt.Println("  serve [addr] - Start HTTP SPARQL endpoint (default: localhost:8080)")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "demo":
		runDemo()
	case "query":
		if len(os.Args) < 3 {
			fmt.Println("Usage: trigo query <sparql-query>")
			os.Exit(1)
		}
		runQuery(os.Args[2])
	default:
		fmt.Printf("Unknown command: %s\n", command)
		os.Exit(1)
	}
}

func runDemo() {
	fmt.Println("=== Trigo RDF Triplestore Demo ===")
	fmt.Println()

	// Create storage
	dbPath := "./trigo_data"
	fmt.Printf("Opening database at: %s\n", dbPath)

	badgerStorage, err := storage.NewBadgerStorage(dbPath)
	if err != nil {
		log.Fatalf("Failed to create storage: %v", err)
	}
	defer badgerStorage.Close()

	// Create triplestore
	tripleStore := store.NewTripleStore(badgerStorage, encoding.NewTermEncoder(), encoding.NewTermDecoder())
	fmt.Println("Triplestore initialized")
	fmt.Println()

	// Insert sample data
	fmt.Println("Inserting sample data...")

	// Create some example triples
	alice := rdf.NewNamedNode("http://example.org/alice")
	bob := rdf.NewNamedNode("http://example.org/bob")
	carol := rdf.NewNamedNode("http://example.org/carol")

	knows := rdf.NewNamedNode("http://xmlns.com/foaf/0.1/knows")
	name := rdf.NewNamedNode("http://xmlns.com/foaf/0.1/name")
	age := rdf.NewNamedNode("http://xmlns.com/foaf/0.1/age")

	// Insert triples
	triples := []*rdf.Triple{
		rdf.NewTriple(alice, name, rdf.NewLiteral("Alice")),
		rdf.NewTriple(alice, age, rdf.NewIntegerLiteral(30)),
		rdf.NewTriple(alice, knows, bob),

		rdf.NewTriple(bob, name, rdf.NewLiteral("Bob")),
		rdf.NewTriple(bob, age, rdf.NewIntegerLiteral(25)),
		rdf.NewTriple(bob, knows, carol),

		rdf.NewTriple(carol, name, rdf.NewLiteral("Carol")),
		rdf.NewTriple(carol, age, rdf.NewIntegerLiteral(28)),
	}

	for _, triple := range triples {
		if err := tripleStore.InsertTriple(triple); err != nil {
			log.Fatalf("Failed to insert triple: %v", err)
		}
		fmt.Printf("  ✓ %s\n", triple)
	}

	// Insert some quads with named graphs
	fmt.Println("\nInserting data into named graphs...")
	graph1 := rdf.NewNamedNode("http://example.org/graph1")
	graph2 := rdf.NewNamedNode("http://example.org/graph2")

	quads := []*rdf.Quad{
		rdf.NewQuad(alice, name, rdf.NewLiteral("Alice in Graph1"), graph1),
		rdf.NewQuad(bob, name, rdf.NewLiteral("Bob in Graph1"), graph1),
		rdf.NewQuad(alice, name, rdf.NewLiteral("Alice in Graph2"), graph2),
		rdf.NewQuad(carol, name, rdf.NewLiteral("Carol in Graph2"), graph2),
	}

	for _, quad := range quads {
		if err := tripleStore.InsertQuad(quad); err != nil {
			log.Fatalf("Failed to insert quad: %v", err)
		}
		fmt.Printf("  ✓ Quad in graph <%s>: %s %s %s\n",
			quad.Graph.(*rdf.NamedNode).IRI,
			formatTerm(quad.Subject),
			formatTerm(quad.Predicate),
			formatTerm(quad.Object))
	}

	// Count triples
	count, err := tripleStore.Count()
	if err != nil {
		log.Fatalf("Failed to count triples: %v", err)
	}
	fmt.Printf("\nTotal triples stored: %d\n", count)

	// Query example
	fmt.Println()
	fmt.Println("=== Querying Data ===")
	fmt.Println()

	sparqlQuery := `
		SELECT ?person ?name ?age
		WHERE {
			?person <http://xmlns.com/foaf/0.1/name> ?name .
			?person <http://xmlns.com/foaf/0.1/age> ?age .
		}
	`

	fmt.Printf("Query:\n%s\n", sparqlQuery)

	// Parse query
	p := parser.NewParser(sparqlQuery)
	query, err := p.Parse()
	if err != nil {
		log.Fatalf("Failed to parse query: %v", err)
	}
	fmt.Println("✓ Query parsed successfully")

	// Optimize query
	stats := &optimizer.Statistics{TotalTriples: count}
	opt := optimizer.NewOptimizer(stats)
	optimizedQuery, err := opt.Optimize(query)
	if err != nil {
		log.Fatalf("Failed to optimize query: %v", err)
	}
	fmt.Println("✓ Query optimized successfully")

	// Execute query
	exec := executor.NewExecutor(tripleStore, ".")
	result, err := exec.Execute(optimizedQuery)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	fmt.Println("✓ Query executed successfully")
	fmt.Println()

	// Display results
	fmt.Println("Results:")
	if selectResult, ok := result.(*executor.SelectResult); ok {
		// Print header
		fmt.Print("| ")
		if selectResult.Variables != nil {
			for _, v := range selectResult.Variables {
				fmt.Printf("%-20s | ", v.Name)
			}
		}
		fmt.Println()
		fmt.Println("|" + "----------------------|" + "----------------------|" + "----------------------|")

		// Print rows
		for _, binding := range selectResult.Bindings {
			fmt.Print("| ")
			if selectResult.Variables != nil {
				for _, v := range selectResult.Variables {
					if term, exists := binding.Vars[v.Name]; exists {
						fmt.Printf("%-20s | ", formatTerm(term))
					} else {
						fmt.Printf("%-20s | ", "")
					}
				}
			}
			fmt.Println()
		}

		fmt.Printf("\nFound %d results\n", len(selectResult.Bindings))
	}

	fmt.Println("\n=== Demo Complete ===")
}

func runQuery(sparqlQuery string) {
	// Open existing database
	dbPath := "./trigo_data"
	badgerStorage, err := storage.NewBadgerStorage(dbPath)
	if err != nil {
		log.Fatalf("Failed to open storage: %v", err)
	}
	defer badgerStorage.Close()

	tripleStore := store.NewTripleStore(badgerStorage, encoding.NewTermEncoder(), encoding.NewTermDecoder())

	// Parse query
	p := parser.NewParser(sparqlQuery)
	query, err := p.Parse()
	if err != nil {
		log.Fatalf("Failed to parse query: %v", err)
	}

	// Get statistics
	count, _ := tripleStore.Count()
	stats := &optimizer.Statistics{TotalTriples: count}

	// Optimize query
	opt := optimizer.NewOptimizer(stats)
	optimizedQuery, err := opt.Optimize(query)
	if err != nil {
		log.Fatalf("Failed to optimize query: %v", err)
	}

	// Execute query
	exec := executor.NewExecutor(tripleStore, ".")
	result, err := exec.Execute(optimizedQuery)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}

	// Display results
	if selectResult, ok := result.(*executor.SelectResult); ok {
		fmt.Println("Results:")
		for _, binding := range selectResult.Bindings {
			for varName, term := range binding.Vars {
				fmt.Printf("  %s = %s\n", varName, formatTerm(term))
			}
			fmt.Println()
		}
	} else if askResult, ok := result.(*executor.AskResult); ok {
		fmt.Printf("Result: %t\n", askResult.Result)
	} else if constructResult, ok := result.(*executor.ConstructResult); ok {
		fmt.Printf("Constructed %d triples:\n", len(constructResult.Triples))
		for _, triple := range constructResult.Triples {
			// Format as N-Triples
			fmt.Printf("<%s> <%s> ", triple.Subject.Value, triple.Predicate.Value)
			switch triple.Object.Type {
			case "iri":
				fmt.Printf("<%s>", triple.Object.Value)
			case "literal":
				fmt.Printf("\"%s\"", triple.Object.Value)
			default:
				fmt.Printf("_:%s", triple.Object.Value)
			}
			fmt.Println(" .")
		}
	}
}

func formatTerm(term rdf.Term) string {
	switch t := term.(type) {
	case *rdf.NamedNode:
		// Return just the local name if possible
		iri := t.IRI
		if idx := len(iri) - 1; idx >= 0 {
			for i := idx; i >= 0; i-- {
				if iri[i] == '/' || iri[i] == '#' {
					return iri[i+1:]
				}
			}
		}
		return iri
	case *rdf.Literal:
		return t.Value
	default:
		return term.String()
	}
}
